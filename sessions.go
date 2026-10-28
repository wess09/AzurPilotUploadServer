package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Each HTTP request contains at most one image, so a 500 MiB group can pass
// through proxies with smaller request limits. The durable group is shared with
// the original multipart API; quota, upload workers and batching stay identical.
func (s *Server) initialize(w http.ResponseWriter, r *http.Request) {
	if !s.acquireReceiver(w) {
		return
	}
	defer func() { <-s.receivers }()
	controller := http.NewResponseController(w)
	defer controller.SetReadDeadline(time.Time{})
	r.Body = idleBody{r.Body, controller}
	r.Body = http.MaxBytesReader(w, r.Body, 16384)
	var m Metadata
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		reject(w, 400, err)
		return
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		reject(w, 400, fmt.Errorf("trailing metadata"))
		return
	}
	if err := s.validate(m); err != nil {
		status := 400
		if errors.Is(err, errSize) {
			status = 413
		}
		reject(w, status, err)
		return
	}
	if err := s.S.claimGroup(m); err != nil {
		reject(w, 409, err)
		return
	}
	defer s.releaseGroup(m.UploadID)
	slog.Info("upload initialized", "upload_id", m.UploadID, "project", m.Project,
		"scene", m.Scene, "files", len(m.Sizes))
	respond(w, 202, map[string]any{"accepted": true, "upload_id": m.UploadID})
}

func (s *Server) acquireReceiver(w http.ResponseWriter) bool {
	select {
	case s.receivers <- struct{}{}:
		return true
	default:
		reject(w, 429, fmt.Errorf("receive concurrency exhausted"))
		return false
	}
}

func (s *Server) releaseGroup(id string) {
	if s.S.unlockDirect(id) {
		return
	}
	if _, err := s.S.DB.Exec(`UPDATE uploads SET busy=0 WHERE id=?`, id); err != nil {
		slog.Error("release upload", "error", err)
	}
}

func (s *Server) acquireGroup(w http.ResponseWriter, id string) (Metadata, bool) {
	return s.acquireGroupProtocol(w, id, 1)
}

func (s *Server) acquireGroupProtocol(w http.ResponseWriter, id string, protocol int) (Metadata, bool) {
	var m Metadata
	if !uuidRE.MatchString(id) {
		reject(w, 400, fmt.Errorf("invalid upload ID"))
		return m, false
	}
	var data string
	var storedProtocol int
	err := s.S.DB.QueryRow(`UPDATE uploads SET busy=1 WHERE id=? AND busy=0 RETURNING metadata,protocol`, id).Scan(&data, &storedProtocol)
	if errors.Is(err, sql.ErrNoRows) {
		reject(w, 409, fmt.Errorf("upload does not exist or is busy; initialize or retry"))
		return m, false
	}
	if err != nil {
		reject(w, 500, err)
		return m, false
	}
	if storedProtocol != protocol {
		s.releaseGroup(id)
		reject(w, 409, fmt.Errorf("upload protocol version conflict"))
		return m, false
	}
	if err = json.Unmarshal([]byte(data), &m); err != nil {
		s.releaseGroup(id)
		reject(w, 500, err)
		return m, false
	}
	return m, true
}

func (s *Server) receiveFile(w http.ResponseWriter, r *http.Request) {
	if !s.acquireReceiver(w) {
		return
	}
	defer func() { <-s.receivers }()
	m, ok := s.acquireGroup(w, r.PathValue("id"))
	if !ok {
		return
	}
	defer s.releaseGroup(m.UploadID)
	i, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || i < 0 || i >= len(m.Sizes) {
		reject(w, 400, fmt.Errorf("invalid file index"))
		return
	}
	n := m.Sizes[i]
	if r.ContentLength >= 0 && r.ContentLength != n {
		status := 400
		if r.ContentLength > n {
			status = 413
		}
		reject(w, status, fmt.Errorf("file size must match declared size"))
		return
	}
	controller := http.NewResponseController(w)
	defer controller.SetReadDeadline(time.Time{})
	r.Body = http.MaxBytesReader(w, idleBody{r.Body, controller}, n+1)
	f, err := s.S.existing(m.UploadID, i)
	if err == nil {
		h := sha256.New()
		written, e := io.CopyBuffer(h, r.Body, make([]byte, 64<<10))
		if e != nil || written != n || hex.EncodeToString(h.Sum(nil)) != f.SHA256 {
			reject(w, 409, fmt.Errorf("file differs from previous attempt"))
			return
		}
		slog.Info("file reused", "upload_id", m.UploadID, "file", f.ID, "index", i, "bytes", n)
		s.transfers.recordReuse(n)
	} else if errors.Is(err, sql.ErrNoRows) {
		err = s.savePart(r.Context(), m.UploadID, i, n, r.Body)
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
		reject(w, 500, err)
		return
	}
	respond(w, 202, map[string]any{"accepted": true, "upload_id": m.UploadID, "index": i})
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	if !s.acquireReceiver(w) {
		return
	}
	defer func() { <-s.receivers }()
	m, ok := s.acquireGroup(w, r.PathValue("id"))
	if !ok {
		return
	}
	defer s.releaseGroup(m.UploadID)
	for i := range m.Sizes {
		if _, err := s.S.existing(m.UploadID, i); err != nil {
			status := 500
			if errors.Is(err, sql.ErrNoRows) {
				status = 409
			}
			reject(w, status, fmt.Errorf("file %d is missing: %w", i, err))
			return
		}
	}
	hash, duplicate, err := s.S.seal(m.UploadID)
	if err != nil {
		reject(w, 500, err)
		return
	}
	logGroupReceived(m, hash, duplicate)
	respond(w, 202, map[string]any{"accepted": true, "upload_id": m.UploadID, "sha256": hash, "duplicate": duplicate})
}
