package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var legacyDeviceRE = regexp.MustCompile(`^[0-9a-fA-F]{32,64}$`)
var legacyAgentRE = regexp.MustCompile(`^Alas \((.{1,128})\)$`)
var legacyTimeRE = regexp.MustCompile(`^([0-9]{13})(?:_|\.png$)`)

func (s *Server) redirectLegacyStatistics(w http.ResponseWriter, r *http.Request) {
	u, err := url.Parse(s.C.StatisticsURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		reject(w, 503, fmt.Errorf("STATISTICS_PUBLIC_URL is not configured"))
		return
	}
	u.Path = r.URL.Path
	u.RawQuery = r.URL.RawQuery
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
}

// The historical AP client sends no per-file length. Reserve at most one
// file's upper bound, then stream it to disk; never parse multipart into RAM.
func (s *Server) receiveLegacy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		reject(w, 405, fmt.Errorf("POST required"))
		return
	}
	if !s.acquireReceiver(w) {
		return
	}
	defer func() { <-s.receivers }()
	controller := http.NewResponseController(w)
	defer controller.SetReadDeadline(time.Time{})
	r.Body = http.MaxBytesReader(w, idleBody{r.Body, controller}, s.C.FileBytes+(16<<10))
	mr, err := r.MultipartReader()
	if err != nil {
		reject(w, 415, err)
		return
	}
	fields := map[string]string{}
	for k, v := range r.URL.Query() {
		if len(v) != 1 {
			reject(w, 400, fmt.Errorf("duplicate field %s", k))
			return
		}
		fields[k] = v[0]
	}
	var f File
	registered := false
	defer func() {
		if f.Path != "" && !registered {
			if err := os.Remove(f.Path); err == nil {
				s.S.Quota.removed(f.Size)
			} else {
				slog.Error("remove legacy temporary file", "error", err)
			}
		}
	}()
	filename := ""
	fieldBytes := 0
	for {
		part, e := mr.NextPart()
		if e == io.EOF {
			break
		}
		if e != nil {
			reject(w, 400, e)
			return
		}
		if part.FileName() != "" {
			if part.FormName() != "file" || f.Path != "" {
				reject(w, 400, fmt.Errorf("exactly one file field is required"))
				return
			}
			filename = part.FileName()
			bound := min(s.C.FileBytes, s.C.GroupBytes)
			if r.ContentLength > 0 && r.ContentLength < bound {
				bound = r.ContentLength
			}
			f, err = s.spoolLegacy(part, bound)
			if err != nil {
				status := 400
				if errors.Is(err, errQuota) {
					status = 503
				}
				if errors.Is(err, errSize) {
					status = 413
				}
				reject(w, status, err)
				return
			}
		} else {
			b, e := io.ReadAll(io.LimitReader(part, 1025))
			fieldBytes += len(b)
			if e != nil || len(b) > 1024 || fieldBytes > 8192 {
				reject(w, 400, fmt.Errorf("legacy fields exceed limit"))
				return
			}
			if _, exists := fields[part.FormName()]; exists {
				reject(w, 400, fmt.Errorf("duplicate field %s", part.FormName()))
				return
			}
			fields[part.FormName()] = string(b)
		}
	}
	if f.Path == "" {
		reject(w, 400, fmt.Errorf("file is required"))
		return
	}
	m, err := s.legacyMetadata(r, fields, filename, f.Size)
	if err != nil {
		reject(w, 400, err)
		return
	}
	if err = s.S.claimGroup(m); err != nil {
		reject(w, 409, err)
		return
	}
	defer s.releaseGroup(m.UploadID)
	f.ID, f.Group, f.Index = newID(), m.UploadID, 0
	if _, err = s.S.DB.ExecContext(r.Context(), `INSERT INTO files(id,group_id,idx,path,size,md5,sha) VALUES(?,?,?,?,?,?,?)`, f.ID, f.Group, f.Index, f.Path, f.Size, f.MD5, f.SHA256); err != nil {
		reject(w, 500, err)
		return
	}
	registered = true
	s.transfers.record(false, f.Size)
	slog.Info("file received", "upload_id", m.UploadID, "file", f.ID, "index", f.Index, "bytes", f.Size)
	hash, duplicate, err := s.S.seal(m.UploadID)
	if err != nil {
		reject(w, 500, err)
		return
	}
	logGroupReceived(m, hash, duplicate)
	respond(w, 200, map[string]any{"status": true, "code": 200, "msg": "accepted", "accepted": true, "duplicate": duplicate, "imgid": hash, "upload_id": m.UploadID, "data": map[string]any{"md5": f.MD5, "sha256": hash}})
}

func legacyPublic(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next(w, r)
	}
}

func (s *Server) spoolLegacy(src io.Reader, bound int64) (f File, err error) {
	if err = s.S.Quota.reserve(bound); err != nil {
		return f, err
	}
	p, err := os.CreateTemp(s.S.Dir, "*.part")
	if err != nil {
		s.S.Quota.finish(bound, 0)
		return f, err
	}
	var n int64
	defer func() {
		_ = p.Close()
		if err != nil {
			if e := os.Remove(p.Name()); e == nil || os.IsNotExist(e) {
				s.S.Quota.finish(bound, 0)
			} else {
				s.S.Quota.finish(bound, n)
			}
		}
	}()
	var header [8]byte
	if _, err = io.ReadFull(src, header[:]); err != nil {
		return f, err
	}
	if !bytes.Equal(header[:], []byte{137, 80, 78, 71, 13, 10, 26, 10}) {
		return f, fmt.Errorf("only PNG files are accepted")
	}
	sha, md := sha256.New(), md5.New()
	n, err = io.CopyBuffer(io.MultiWriter(p, sha, md), io.LimitReader(io.MultiReader(bytes.NewReader(header[:]), src), bound), make([]byte, 64<<10))
	if err != nil {
		return f, err
	}
	var extra [1]byte
	if _, e := io.ReadFull(src, extra[:]); e != io.EOF {
		if e == nil {
			err = errSize
		} else {
			err = e
		}
		return f, err
	}
	if n < 8 {
		return f, fmt.Errorf("incomplete PNG")
	}
	if err = p.Sync(); err != nil {
		return f, err
	}
	if err = p.Close(); err != nil {
		return f, err
	}
	s.S.Quota.finish(bound, n)
	return File{Path: p.Name(), Size: n, MD5: hex.EncodeToString(md.Sum(nil)), SHA256: hex.EncodeToString(sha.Sum(nil))}, nil
}

func (s *Server) legacyMetadata(r *http.Request, fields map[string]string, filename string, size int64) (Metadata, error) {
	m := Metadata{Sizes: []int64{size}, Server: "cn", Project: "drop", Scene: fields["genre"], LegacyFilename: filename}
	if m.Scene == "" {
		m.Scene = "legacy_auto"
	}
	if m.Scene == "research" || m.Scene == "research_queue" || m.Scene == "research_complete" {
		m.Project = "research"
		m.Scene = "research_complete"
	}
	if m.Scene == "research_refresh" || m.Scene == "research_projects" {
		m.Project = "research"
		m.Scene = "research_refresh"
	}
	if fields["project"] != "" {
		m.Project = fields["project"]
	}
	if fields["scene"] != "" {
		m.Scene = fields["scene"]
	}
	if fields["server"] != "" {
		m.Server = fields["server"]
	}
	m.LegacyDeviceID = strings.ToLower(fields["device_id"])
	if m.LegacyDeviceID != "" {
		if !legacyDeviceRE.MatchString(m.LegacyDeviceID) {
			return m, fmt.Errorf("invalid legacy device_id")
		}
		m.DeviceID = m.LegacyDeviceID
		if len(m.DeviceID) != 32 {
			d := sha256.Sum256([]byte("legacy-device\x00" + m.DeviceID))
			m.DeviceID = hex.EncodeToString(d[:16])
		}
	} else {
		match := legacyAgentRE.FindStringSubmatch(r.UserAgent())
		if len(match) != 2 {
			return m, fmt.Errorf("device_id or Alas (AzurStatsID) user agent is required")
		}
		m.Submitter = match[1]
		d := sha256.Sum256([]byte("legacy-submitter\x00" + m.Submitter))
		m.DeviceID = hex.EncodeToString(d[:16])
	}
	for _, key := range []string{"X-AzurPilot-ID", "X-Device-Id"} {
		if v := r.Header.Get(key); v != "" && !strings.EqualFold(v, m.LegacyDeviceID) && !strings.EqualFold(v, m.DeviceID) {
			return m, fmt.Errorf("device identity mismatch")
		}
	}
	m.InstanceID = strings.ToLower(fields["instance_id"])
	if m.InstanceID == "" {
		d := sha256.Sum256([]byte("legacy-instance\x00" + m.DeviceID))
		m.InstanceID = hex.EncodeToString(d[:8])
	}
	if v := fields["combat_count"]; v != "" {
		var e error
		m.CombatCount, e = strconv.ParseInt(v, 10, 64)
		if e != nil {
			return m, fmt.Errorf("invalid combat_count")
		}
	}
	at := time.Now().UTC()
	if match := legacyTimeRE.FindStringSubmatch(filename); len(match) == 2 {
		ms, _ := strconv.ParseInt(match[1], 10, 64)
		at = time.UnixMilli(ms).UTC()
	}
	if v := fields["collected_at"]; v != "" {
		var e error
		at, e = time.Parse(time.RFC3339Nano, v)
		if e != nil {
			return m, fmt.Errorf("collected_at requires RFC3339 timezone")
		}
	}
	m.CollectedAt = at.Format(time.RFC3339Nano)
	id := newID()
	m.UploadID = fmt.Sprintf("%s-%s-%s-%s-%s", id[:8], id[8:12], id[12:16], id[16:20], id[20:])
	return m, s.validate(m)
}
