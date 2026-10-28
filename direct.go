package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

var md5RE = regexp.MustCompile(`^[0-9a-f]{32}$`)
var sha256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type directHash struct {
	MD5    string `json:"md5"`
	SHA256 string `json:"sha256"`
}

type directManifest struct {
	Version  int          `json:"schema_version"`
	Metadata Metadata     `json:"metadata"`
	Files    []directHash `json:"files"`
}

type directPart struct {
	Number int    `json:"number"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	URL    string `json:"url"`
}

func directReply(w http.ResponseWriter, status int, id string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["schema_version"], fields["accepted"], fields["upload_id"] = 2, true, id
	w.Header().Set("Cache-Control", "no-store")
	respond(w, status, fields)
}

func (s *Server) initializeDirect(w http.ResponseWriter, r *http.Request) {
	s.controlActive.Add(1)
	defer s.controlActive.Add(-1)
	r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	var manifest directManifest
	if err := d.Decode(&manifest); err != nil {
		reject(w, 400, fmt.Errorf("invalid v2 manifest"))
		return
	}
	if err := d.Decode(&struct{}{}); err != io.EOF || manifest.Version != 2 {
		reject(w, 400, fmt.Errorf("schema_version must be 2 with one JSON document"))
		return
	}
	m := manifest.Metadata
	if err := s.validate(m); err != nil {
		status := 400
		if errors.Is(err, errSize) {
			status = 413
		}
		reject(w, status, err)
		return
	}
	if len(manifest.Files) != len(m.Sizes) {
		reject(w, 400, fmt.Errorf("hash list must match file_sizes"))
		return
	}
	for _, f := range manifest.Files {
		if !md5RE.MatchString(f.MD5) || !sha256RE.MatchString(f.SHA256) {
			reject(w, 400, fmt.Errorf("invalid MD5 or SHA-256"))
			return
		}
	}
	if err := s.S.claimGroupProtocolContext(r.Context(), m, 2); err != nil {
		controlError(w, err)
		return
	}
	defer s.releaseGroup(m.UploadID)
	encoded, _ := json.Marshal(manifest.Files)
	digest := sha256.Sum256(encoded)
	if cached, ok := s.S.manifestCache.get(m.UploadID); ok {
		if cached != digest {
			reject(w, 409, fmt.Errorf("upload_id file hash conflict"))
			return
		}
		directReply(w, 202, m.UploadID, nil)
		return
	}
	// Repeated initialization is a read, not another transaction containing
	// several INSERT/SELECT pairs for every file in the same manifest.
	rows, err := s.S.DB.QueryContext(r.Context(), `SELECT f.idx,f.size,f.md5,f.sha,d.file_id FROM files f LEFT JOIN direct_files d ON d.file_id=f.id WHERE f.group_id=? ORDER BY f.idx`, m.UploadID)
	if err != nil {
		reject(w, 503, fmt.Errorf("manifest lookup unavailable; retry"))
		return
	}
	count := 0
	valid := true
	conflict := false
	for rows.Next() {
		var index int
		var size int64
		var md5, sha string
		var direct sql.NullString
		if err = rows.Scan(&index, &size, &md5, &sha, &direct); err != nil {
			break
		}
		if index < 0 || index >= len(m.Sizes) || size != m.Sizes[index] || md5 != manifest.Files[index].MD5 || sha != manifest.Files[index].SHA256 {
			conflict = true
			break
		}
		count++
		valid = valid && direct.Valid
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if conflict {
		reject(w, 409, fmt.Errorf("upload_id file hash conflict"))
		return
	}
	if err != nil {
		reject(w, 503, fmt.Errorf("manifest lookup unavailable; retry"))
		return
	}
	if valid && count == len(m.Sizes) {
		s.S.manifestCache.put(m.UploadID, digest)
		directReply(w, 202, m.UploadID, nil)
		return
	}
	tx, err := s.S.DB.BeginTx(r.Context(), nil)
	if err != nil {
		controlError(w, err)
		return
	}
	defer tx.Rollback()
	for i, f := range manifest.Files {
		id := newID()
		if _, err = tx.Exec(`INSERT INTO files(id,group_id,idx,path,size,md5,sha,state) VALUES(?,?,?,'',?,?,?,'direct_pending') ON CONFLICT(group_id,idx) DO NOTHING`, id, m.UploadID, i, m.Sizes[i], f.MD5, f.SHA256); err != nil {
			break
		}
		var size int64
		var md5, sha string
		if err = tx.QueryRow(`SELECT id,size,md5,sha FROM files WHERE group_id=? AND idx=?`, m.UploadID, i).Scan(&id, &size, &md5, &sha); err != nil {
			break
		}
		if size != m.Sizes[i] || md5 != f.MD5 || sha != f.SHA256 {
			reject(w, 409, fmt.Errorf("upload_id file hash conflict"))
			return
		}
		if _, err = tx.Exec(`INSERT OR IGNORE INTO direct_files(file_id) VALUES(?)`, id); err != nil {
			break
		}
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		controlError(w, err)
		return
	}
	s.S.manifestCache.put(m.UploadID, digest)
	slog.Info("direct upload initialized", "upload_id", m.UploadID, "files", len(m.Sizes))
	directReply(w, 202, m.UploadID, nil)
}

func (s *Server) directGroup(w http.ResponseWriter, r *http.Request) (Metadata, func(), bool) {
	if r.ContentLength != 0 {
		reject(w, 400, fmt.Errorf("v2 control request must have an empty body"))
		return Metadata{}, nil, false
	}
	if s.Direct == nil {
		reject(w, 503, fmt.Errorf("direct uploads unavailable"))
		return Metadata{}, nil, false
	}
	// v2 carries only metadata. It must not compete with v1 image streams for
	// receive slots; per-group exclusion and cloud request deadlines still apply.
	s.controlActive.Add(1)
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) || !s.S.lockDirect(id) {
		s.controlActive.Add(-1)
		reject(w, 409, fmt.Errorf("upload does not exist or is busy; initialize or retry"))
		return Metadata{}, nil, false
	}
	release := func() { s.S.unlockDirect(id); s.controlActive.Add(-1) }
	cached, ok := s.S.metadataCache.get(id)
	if !ok {
		var raw string
		var protocol int
		err := s.S.DB.QueryRowContext(r.Context(), `SELECT metadata,protocol FROM uploads WHERE id=?`, id).Scan(&raw, &protocol)
		var m Metadata
		if err == nil {
			err = json.Unmarshal([]byte(raw), &m)
		}
		if err != nil || protocol != 2 {
			release()
			reject(w, 409, fmt.Errorf("initialize a v2 upload first"))
			return Metadata{}, nil, false
		}
		cached = groupMetadata{raw, m, protocol}
		s.S.metadataCache.put(id, cached)
	}
	if cached.protocol != 2 {
		release()
		reject(w, 409, fmt.Errorf("upload protocol version conflict"))
		return Metadata{}, nil, false
	}
	return cached.metadata, release, true
}

func (s *Server) directFile(w http.ResponseWriter, r *http.Request, m Metadata) (File, bool) {
	i, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || i < 0 || i >= len(m.Sizes) {
		reject(w, 400, fmt.Errorf("invalid file index"))
		return File{}, false
	}
	var f File
	err = s.S.DB.QueryRowContext(r.Context(), `SELECT id,group_id,idx,path,size,md5,sha,remote_id FROM files WHERE group_id=? AND idx=?`, m.UploadID, i).Scan(&f.ID, &f.Group, &f.Index, &f.Path, &f.Size, &f.MD5, &f.SHA256, &f.RemoteID)
	if err != nil {
		reject(w, 409, fmt.Errorf("initialize file manifest first"))
		return f, false
	}
	return f, true
}

func (s *Server) confirmDirect(ctx context.Context, f File, id int64) error {
	// Only IDs returned by the cloud completion API or an existing server-side
	// record reach here. B verifies the actual downloaded bytes against the
	// original SHA-256 and size before recognizing or submitting anything to C.
	if id <= 0 {
		return fmt.Errorf("invalid cloud file ID")
	}
	result, err := s.S.DB.ExecContext(ctx, `UPDATE files SET state='uploaded',remote_id=?,error='' WHERE id=? AND state<>'uploaded'`, id, f.ID)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 1 {
		s.S.contentCache.put(contentKey{f.SHA256, f.MD5, f.Size}, id)
		s.transfers.recordDirect(f.Size)
		slog.Info("direct upload cloud completed", "upload_id", f.Group, "file", f.ID, "index", f.Index, "bytes", f.Size)
	}
	return nil
}

func (s *Server) directFailure(w http.ResponseWriter, f File, err error) {
	slog.Warn("direct upload control failed", "upload_id", f.Group, "file", f.ID, "error", err)
	var limited *cloudThrottleError
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		controlError(w, err)
		return
	}
	if errors.As(err, &limited) {
		if limited.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(max(1, int64((limited.RetryAfter+time.Second-1)/time.Second)), 10))
		}
		if writer, ok := w.(interface{ setError(string) }); ok {
			writer.setError(err.Error())
		}
		respond(w, http.StatusTooManyRequests, map[string]any{"error": "cloud rate limited; retry later"})
		return
	}
	reject(w, 503, fmt.Errorf("cloud upload not completed; retry"))
}

func (s *Server) prepareDirect(w http.ResponseWriter, r *http.Request) {
	m, release, ok := s.directGroup(w, r)
	if !ok {
		return
	}
	defer release()
	f, ok := s.directFile(w, r, m)
	if !ok {
		return
	}
	ctx := r.Context()
	var state, preID string
	var slice int64
	var completing int
	err := s.S.DB.QueryRowContext(ctx, `SELECT f.state,d.preupload_id,d.slice_size,d.completing FROM files f JOIN direct_files d ON d.file_id=f.id WHERE f.id=?`, f.ID).Scan(&state, &preID, &slice, &completing)
	if err != nil {
		controlError(w, err)
		return
	}
	if state == "uploaded" {
		directReply(w, 200, m.UploadID, map[string]any{"completed": true})
		return
	}
	// Reuse only cloud-confirmed IDs for the same expected content. B still
	// verifies downloaded bytes; no account-level detail call is needed on A.
	var cached int64
	key := contentKey{f.SHA256, f.MD5, f.Size}
	if id, ok := s.S.contentCache.get(key); ok {
		cached, err = id, nil
	} else if !s.S.DB.postgres {
		err = s.S.DB.QueryRowContext(ctx, `SELECT remote_id FROM files WHERE sha=? AND md5=? AND size=? AND state='uploaded' AND remote_id>0 ORDER BY rowid LIMIT 1`, f.SHA256, f.MD5, f.Size).Scan(&cached)
		if err == nil {
			s.S.contentCache.put(key, cached)
		}
	} else {
		err = sql.ErrNoRows
	}
	if err == nil {
		if err = s.confirmDirect(ctx, f, cached); err == nil {
			directReply(w, 200, m.UploadID, map[string]any{"completed": true})
			return
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		controlError(w, err)
		return
	}
	if completing != 0 {
		directReply(w, 200, m.UploadID, map[string]any{"completed": false, "finalizing": true})
		return
	}
	if preID == "" {
		ticket, err := s.Direct.CreateDirect(ctx, f)
		if err != nil {
			s.directFailure(w, f, err)
			return
		}
		if ticket.Reuse {
			if err = s.confirmDirect(ctx, f, ticket.FileID); err != nil {
				s.directFailure(w, f, err)
				return
			}
			directReply(w, 200, m.UploadID, map[string]any{"completed": true})
			return
		}
		preID, slice = ticket.PreuploadID, ticket.SliceSize
		if preID == "" || len(preID) > 4096 || slice < 1 || (f.Size-1)/slice >= 32 {
			s.directFailure(w, f, fmt.Errorf("invalid cloud upload ticket"))
			return
		}
		if _, err = s.S.DB.ExecContext(ctx, `UPDATE direct_files SET preupload_id=?,slice_size=? WHERE file_id=?`, preID, slice, f.ID); err != nil {
			controlError(w, err)
			return
		}
	}
	parts := []directPart{}
	for offset, number := int64(0), 1; offset < f.Size; offset, number = offset+slice, number+1 {
		url, err := s.Direct.DirectURL(ctx, preID, number)
		if err != nil {
			s.directFailure(w, f, err)
			return
		}
		if !validDirectURL(url) {
			s.directFailure(w, f, fmt.Errorf("could not obtain a valid cloud upload URL"))
			return
		}
		parts = append(parts, directPart{Number: number, Offset: offset, Size: min(slice, f.Size-offset), URL: url})
	}
	directReply(w, 200, m.UploadID, map[string]any{"completed": false, "parts": parts})
}

func (s *Server) completeDirectFile(w http.ResponseWriter, r *http.Request) {
	m, release, ok := s.directGroup(w, r)
	if !ok {
		return
	}
	defer release()
	f, ok := s.directFile(w, r, m)
	if !ok {
		return
	}
	var state, preID string
	var poll int
	err := s.S.DB.QueryRowContext(r.Context(), `SELECT f.state,d.preupload_id,d.completing FROM files f JOIN direct_files d ON d.file_id=f.id WHERE f.id=?`, f.ID).Scan(&state, &preID, &poll)
	if err != nil {
		controlError(w, err)
		return
	}
	if state == "uploaded" {
		directReply(w, 200, m.UploadID, map[string]any{"completed": true})
		return
	}
	if preID == "" {
		reject(w, 409, fmt.Errorf("prepare cloud upload first"))
		return
	}
	ctx := r.Context()
	// Persist before the cloud call so a lost HTTP reply can be recovered by
	// polling, without creating another cloud file or trusting client file IDs.
	if poll == 0 {
		if _, err = s.S.DB.ExecContext(ctx, `UPDATE direct_files SET completing=1 WHERE file_id=? AND completing=0`, f.ID); err != nil {
			controlError(w, err)
			return
		}
	}
	result, err := s.Direct.CompleteDirect(ctx, preID, poll != 0)
	if err != nil {
		s.directFailure(w, f, err)
		return
	}
	if result.FileID > 0 && (result.Completed || !result.Async) {
		if err = s.confirmDirect(ctx, f, result.FileID); err != nil {
			s.directFailure(w, f, err)
			return
		}
		directReply(w, 200, m.UploadID, map[string]any{"completed": true})
		return
	}
	w.Header().Set("Retry-After", "1")
	directReply(w, 202, m.UploadID, map[string]any{"completed": false, "retry_after": 1})
}

func (s *Server) completeDirect(w http.ResponseWriter, r *http.Request) {
	m, release, ok := s.directGroup(w, r)
	if !ok {
		return
	}
	defer release()
	var pending, files int
	if err := s.S.DB.QueryRowContext(r.Context(), `SELECT count(*),count(*) FILTER(WHERE state<>'uploaded' OR remote_id<=0) FROM files WHERE group_id=?`, m.UploadID).Scan(&files, &pending); err != nil {
		controlError(w, err)
		return
	}
	if files != len(m.Sizes) || pending != 0 {
		reject(w, 409, fmt.Errorf("cloud files have not all passed verification"))
		return
	}
	hash, duplicate, err := s.S.seal(m.UploadID)
	if err != nil {
		controlError(w, err)
		return
	}
	logGroupReceived(m, hash, duplicate)
	directReply(w, 202, m.UploadID, map[string]any{"sha256": hash, "duplicate": duplicate})
}
