package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"time"
)

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var deviceRE = regexp.MustCompile(`^[0-9a-f]{32}$`)
var instanceRE = regexp.MustCompile(`^[0-9a-f]{16}$`)
var nameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
var errSize = errors.New("file or group exceeds size limit")

type idleBody struct {
	io.ReadCloser
	controller *http.ResponseController
}

func (b idleBody) Read(p []byte) (int, error) {
	_ = b.controller.SetReadDeadline(time.Now().Add(30 * time.Second))
	return b.ReadCloser.Read(p)
}

type Uploader interface {
	Upload(context.Context, File) (int64, error)
}
type Server struct {
	C             Config
	S             *Store
	Pan           Uploader
	receivers     chan struct{}
	workerSlot    chan struct{}
	workerContext context.Context
}

func newServer(c Config, s *Store, pan Uploader) *Server {
	return &Server{C: c, S: s, Pan: pan, receivers: make(chan struct{}, c.ReceiveConcurrency), workerSlot: make(chan struct{}, 1), workerContext: context.Background()}
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func reject(w http.ResponseWriter, status int, err error) {
	if status == 429 || status == 503 {
		w.Header().Set("Retry-After", "5")
	}
	respond(w, status, map[string]any{"error": err.Error()})
}
func (s *Server) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /api/v1/uploads", s.receive)
	m.HandleFunc("/api/upload", legacyPublic(s.receiveLegacy))
	m.HandleFunc("/api/upload/{$}", legacyPublic(s.receiveLegacy))
	m.HandleFunc("/api/telemetry/stats/stream", s.redirectLegacyStatistics)
	for _, path := range []string{"/api/data/opsi_items", "/api/azurstat", "/api/azurstat/stats", "/api/azurstat/items", "/api/azurstat/history", "/api/azurstat/filters", "/api/telemetry", "/api/telemetry/stats", "/api/telemetry/history", "/api/telemetry/global_history", "/api/get/announcement", "/api/force_update", "/api/maintenance"} {
		m.HandleFunc(path, s.redirectLegacyStatistics)
	}
	m.HandleFunc("POST /api/v1/uploads/init", s.initialize)
	m.HandleFunc("PUT /api/v1/uploads/{id}/files/{index}", s.receiveFile)
	m.HandleFunc("POST /api/v1/uploads/{id}/complete", s.complete)
	m.HandleFunc("GET /internal/v1/worker/connect", s.acceptWorker)
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		u, v := s.S.Quota.snapshot()
		respond(w, 200, map[string]any{"status": "ok", "spool_used": u, "spool_reserved": v, "spool_limit": s.C.SpoolBytes, "worker_connected": len(s.workerSlot) > 0, "worker_mode": s.C.WorkerMode, "batch_interval_seconds": int64(s.S.Interval / time.Second)})
	})
	return m
}
func (s *Server) validate(m Metadata) error {
	if m.CombatCount < 0 || m.CombatCount > 1e9 || len(m.LegacyFilename) > 256 || len(m.Submitter) > 128 || len(m.LegacyDeviceID) > 64 {
		return fmt.Errorf("invalid legacy metadata")
	}
	if !uuidRE.MatchString(m.UploadID) || !deviceRE.MatchString(m.DeviceID) || !instanceRE.MatchString(m.InstanceID) {
		return fmt.Errorf("invalid upload/device/instance ID")
	}
	if m.Project != "research" && m.Project != "drop" {
		return fmt.Errorf("project must be research or drop")
	}
	if !nameRE.MatchString(m.Scene) {
		return fmt.Errorf("invalid scene")
	}
	if m.Server != "cn" && m.Server != "cn_ios" && m.Server != "cn_channel" && m.Server != "en" && m.Server != "jp" && m.Server != "tw" {
		return fmt.Errorf("invalid server")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.CollectedAt); err != nil {
		return fmt.Errorf("collected_at requires RFC3339 timezone")
	}
	if len(m.Sizes) == 0 || len(m.Sizes) > 1000 {
		return fmt.Errorf("file_sizes must contain 1..1000 entries")
	}
	var total int64
	for _, n := range m.Sizes {
		if n < 8 || n > s.C.FileBytes {
			return fmt.Errorf("%w: invalid file size", errSize)
		}
		if n > s.C.GroupBytes-total {
			return errSize
		}
		total += n
	}
	return nil
}
func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	select {
	case s.receivers <- struct{}{}:
		defer func() { <-s.receivers }()
	default:
		reject(w, 429, fmt.Errorf("receive concurrency exhausted"))
		return
	}
	controller := http.NewResponseController(w)
	defer controller.SetReadDeadline(time.Time{})
	r.Body = idleBody{r.Body, controller}
	r.Body = http.MaxBytesReader(w, r.Body, s.C.GroupBytes+(2<<20))
	mr, err := r.MultipartReader()
	if err != nil {
		reject(w, 415, err)
		return
	}
	p, err := mr.NextPart()
	if err != nil || p.FormName() != "metadata" {
		reject(w, 400, fmt.Errorf("first multipart part must be metadata"))
		return
	}
	b, err := io.ReadAll(io.LimitReader(p, 16385))
	if err != nil || len(b) > 16384 {
		reject(w, 400, fmt.Errorf("metadata exceeds 16 KiB"))
		return
	}
	var m Metadata
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		reject(w, 400, err)
		return
	}
	if err = d.Decode(&struct{}{}); err != io.EOF {
		reject(w, 400, fmt.Errorf("trailing metadata"))
		return
	}
	if err = s.validate(m); err != nil {
		status := 400
		if errors.Is(err, errSize) {
			status = 413
		}
		reject(w, status, err)
		return
	}
	if err = s.S.claimGroup(m); err != nil {
		reject(w, 409, err)
		return
	}
	defer func() {
		_, e := s.S.DB.Exec(`UPDATE uploads SET busy=0 WHERE id=?`, m.UploadID)
		if e != nil {
			slog.Error("release upload", "error", e)
		}
	}()
	for i, n := range m.Sizes {
		p, err = mr.NextPart()
		if err != nil {
			reject(w, 400, fmt.Errorf("missing file %d: %w", i, err))
			return
		}
		existing, e := s.S.existing(m.UploadID, i)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			reject(w, 500, e)
			return
		}
		if e == nil {
			sum := sha256.New()
			written, e := io.CopyBuffer(sum, io.LimitReader(p, n+1), make([]byte, 64<<10))
			if e != nil || written != n || hex.EncodeToString(sum.Sum(nil)) != existing.SHA256 {
				reject(w, 409, fmt.Errorf("file %d differs from previous attempt", i))
				return
			}
			continue
		}
		if err = s.savePart(r.Context(), m.UploadID, i, n, p); err != nil {
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
	}
	if _, err = mr.NextPart(); err != io.EOF {
		reject(w, 400, fmt.Errorf("unexpected extra file"))
		return
	}
	hash, duplicate, err := s.S.seal(m.UploadID)
	if err != nil {
		reject(w, 500, err)
		return
	}
	respond(w, 202, map[string]any{"accepted": true, "upload_id": m.UploadID, "sha256": hash, "duplicate": duplicate})
}

func (s *Server) savePart(ctx context.Context, group string, index int, n int64, src io.Reader) (err error) {
	if err = s.S.Quota.reserve(n); err != nil {
		return err
	}
	reserved := true
	f, err := os.CreateTemp(s.S.Dir, "*.part")
	if err != nil {
		s.S.Quota.finish(n, 0)
		return err
	}
	path := f.Name()
	saved := false
	counted := false
	var written int64
	defer func() {
		_ = f.Close()
		if !saved {
			e := os.Remove(path)
			if e == nil || errors.Is(e, os.ErrNotExist) {
				if counted {
					s.S.Quota.removed(n)
				}
			} else if reserved {
				s.S.Quota.finish(n, written)
				reserved = false
			}
		}
		if reserved {
			s.S.Quota.finish(n, 0)
		}
	}()
	sha, md := sha256.New(), md5.New()
	writer := io.MultiWriter(f, sha, md)
	// Sniff a fixed signature, never decode/decompress images on A.
	header := make([]byte, 8)
	if _, err = io.ReadFull(src, header); err != nil {
		return err
	}
	if !bytes.Equal(header, []byte{137, 80, 78, 71, 13, 10, 26, 10}) {
		return fmt.Errorf("only PNG files are accepted")
	}
	written, err = io.CopyBuffer(writer, io.LimitReader(io.MultiReader(bytes.NewReader(header), src), n), make([]byte, 64<<10))
	if err != nil {
		return err
	}
	if written != n {
		return fmt.Errorf("file %d size mismatch", index)
	}
	var extra [1]byte
	if _, e := io.ReadFull(src, extra[:]); e != io.EOF {
		if e == nil {
			return errSize
		}
		return e
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	s.S.Quota.finish(n, n)
	reserved = false
	counted = true
	if _, err = s.S.DB.ExecContext(ctx, `INSERT INTO files(id,group_id,idx,path,size,md5,sha) VALUES(?,?,?,?,?,?,?)`, newID(), group, index, path, n, hex.EncodeToString(md.Sum(nil)), hex.EncodeToString(sha.Sum(nil))); err != nil {
		return err
	}
	saved = true
	return nil
}

func (s *Server) uploadLoop(ctx context.Context) {
	t := time.NewTicker(s.C.Poll)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		f, err := s.S.claimFile(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			continue
		}
		if err != nil {
			slog.Error("claim file", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			continue
		}
		uploadCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		id, e := s.Pan.Upload(uploadCtx, f)
		cancel()
		if e != nil {
			_, err = s.S.DB.Exec(`UPDATE files SET state='pending',retry_at=?,error=? WHERE id=?`, time.Now().Add(10*time.Second).Unix(), e.Error(), f.ID)
			slog.Warn("upload retry", "file", f.ID, "error", e)
		} else if id <= 0 {
			_, err = s.S.DB.Exec(`UPDATE files SET state='pending',retry_at=?,error='missing cloud file ID' WHERE id=?`, time.Now().Add(10*time.Second).Unix(), f.ID)
		} else {
			_, err = s.S.DB.Exec(`UPDATE files SET state='uploaded',remote_id=?,error='' WHERE id=?`, id, f.ID)
			if err == nil {
				err = s.S.removeFile(f)
			}
		}
		if err != nil {
			slog.Error("save upload state", "error", err)
		}
	}
}

func (s *Server) cleanupLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rows, e := s.S.DB.Query(`SELECT f.id,f.path,f.size FROM files f JOIN uploads u ON u.id=f.group_id WHERE f.path<>'' AND (f.state='uploaded' OR (u.state='duplicate' AND f.state='pending'))`)
		if e != nil {
			continue
		}
		var fs []File
		for rows.Next() {
			var f File
			if e = rows.Scan(&f.ID, &f.Path, &f.Size); e != nil {
				break
			}
			fs = append(fs, f)
		}
		rows.Close()
		for _, f := range fs {
			if e = s.S.removeFile(f); e != nil {
				slog.Error("cleanup file", "error", e)
			}
		}
	}
}
