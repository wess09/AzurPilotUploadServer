package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Metadata struct {
	UploadID       string  `json:"upload_id"`
	DeviceID       string  `json:"device_id"`
	InstanceID     string  `json:"instance_id"`
	Project        string  `json:"project"`
	Scene          string  `json:"scene"`
	Server         string  `json:"server"`
	CollectedAt    string  `json:"collected_at"`
	Sizes          []int64 `json:"file_sizes"`
	CombatCount    int64   `json:"combat_count,omitempty"`
	LegacyFilename string  `json:"legacy_filename,omitempty"`
	LegacyDeviceID string  `json:"legacy_device_id,omitempty"`
	Submitter      string  `json:"submitter,omitempty"`
}
type File struct {
	ID       string `json:"-"`
	Group    string `json:"-"`
	Path     string `json:"-"`
	MD5      string `json:"-"`
	Index    int    `json:"index"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	RemoteID int64  `json:"file_id"`
}
type Sample struct {
	SHA256   string   `json:"sha256"`
	Metadata Metadata `json:"metadata"`
	Files    []File   `json:"files"`
}
type Batch struct {
	Type        string   `json:"type"`
	Version     int      `json:"protocol_version"`
	ID          string   `json:"batch_id"`
	Items       []Sample `json:"items"`
	WindowStart string   `json:"window_start,omitempty"`
	WindowEnd   string   `json:"window_end,omitempty"`
}
type Store struct {
	cleanup  sync.Mutex
	DB       *sql.DB
	Dir      string
	Quota    *Quota
	Interval time.Duration
	Now      func() time.Time
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func openStore(c Config) (*Store, error) {
	dir := filepath.Join(c.DataDir, "spool")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.Join(c.DataDir, "state.sqlite"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	interval := c.BatchInterval
	if interval == 0 {
		interval = 30 * time.Minute
	}
	s := &Store{DB: db, Dir: dir, Interval: interval, Now: time.Now, Quota: &Quota{Limit: c.SpoolBytes, MinFree: c.MinFreeBytes, Free: func() (int64, error) { return diskFree(dir) }}}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS uploads(id TEXT PRIMARY KEY, metadata TEXT NOT NULL, hash TEXT UNIQUE, state TEXT NOT NULL DEFAULT 'receiving', busy INTEGER NOT NULL DEFAULT 0, batch_id TEXT, canonical TEXT, created_at INTEGER NOT NULL, protocol INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS files(id TEXT PRIMARY KEY, group_id TEXT NOT NULL REFERENCES uploads(id), idx INTEGER NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, md5 TEXT NOT NULL, sha TEXT NOT NULL, remote_id INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending', retry_at INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', UNIQUE(group_id,idx));
CREATE INDEX IF NOT EXISTS files_pending ON files(state,retry_at);
CREATE INDEX IF NOT EXISTS files_uploaded_content ON files(sha,md5,size) WHERE state='uploaded' AND remote_id>0;
CREATE TABLE IF NOT EXISTS direct_files(file_id TEXT PRIMARY KEY REFERENCES files(id),preupload_id TEXT NOT NULL DEFAULT '',slice_size INTEGER NOT NULL DEFAULT 0,completing INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY, payload TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS dispatch_state(id INTEGER PRIMARY KEY CHECK(id=1),next_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS dispatch_queue(group_id TEXT PRIMARY KEY,window_end INTEGER NOT NULL);
UPDATE uploads SET busy=0; UPDATE files SET state='pending' WHERE state='uploading';`); err != nil {
		db.Close()
		return nil, err
	}
	// Existing v1 databases keep their rows and receive the protocol marker.
	columns, err := db.Query(`PRAGMA table_info(uploads)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	hasProtocol := false
	for columns.Next() {
		var cid, notnull, pk int
		var name, typ string
		var value any
		if err = columns.Scan(&cid, &name, &typ, &notnull, &value, &pk); err != nil {
			break
		}
		hasProtocol = hasProtocol || name == "protocol"
	}
	if e := columns.Err(); err == nil {
		err = e
	}
	columns.Close()
	if err == nil && !hasProtocol {
		_, err = db.Exec(`ALTER TABLE uploads ADD COLUMN protocol INTEGER NOT NULL DEFAULT 1`)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	if _, err = db.Exec(`INSERT OR IGNORE INTO dispatch_state VALUES(1,?)`, time.Now().Truncate(interval).Add(interval).Unix()); err != nil {
		db.Close()
		return nil, err
	}
	// Only files referenced by durable rows survive a restart. Uploaded rows can
	// still reference a file if the process crashed between commit and unlink.
	rows, err := db.Query(`SELECT path,size,state FROM files WHERE path<>''`)
	if err != nil {
		db.Close()
		return nil, err
	}
	keep := map[string]bool{}
	var cleanup []string
	for rows.Next() {
		var p, state string
		var n int64
		if err = rows.Scan(&p, &n, &state); err != nil {
			break
		}
		if state == "uploaded" || state == "discarded" {
			cleanup = append(cleanup, p)
		} else {
			keep[p] = true
		}
	}
	if e := rows.Err(); err == nil {
		err = e
	}
	rows.Close()
	if err != nil {
		db.Close()
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(dir, e.Name())
		if keep[p] {
			info, e := e.Info()
			if e != nil {
				db.Close()
				return nil, e
			}
			s.Quota.Used += info.Size()
		} else {
			if err = os.Remove(p); err != nil {
				db.Close()
				return nil, err
			}
		}
	}
	for _, p := range cleanup {
		if _, err = db.Exec(`UPDATE files SET path='' WHERE path=?`, p); err != nil {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) claimGroup(m Metadata) error {
	return s.claimGroupProtocol(m, 1)
}

func (s *Store) claimGroupProtocol(m Metadata, protocol int) error {
	b, _ := json.Marshal(m)
	_, err := s.DB.Exec(`INSERT INTO uploads(id,metadata,created_at,protocol) VALUES(?,?,?,?) ON CONFLICT(id) DO NOTHING`, m.UploadID, string(b), time.Now().Unix(), protocol)
	if err != nil {
		return err
	}
	var original, state string
	var busy, storedProtocol int
	if err = s.DB.QueryRow(`SELECT metadata,state,busy,protocol FROM uploads WHERE id=?`, m.UploadID).Scan(&original, &state, &busy, &storedProtocol); err != nil {
		return err
	}
	if original != string(b) || storedProtocol != protocol {
		return fmt.Errorf("upload_id metadata conflict")
	}
	if busy != 0 {
		return fmt.Errorf("upload is already being received")
	}
	r, err := s.DB.Exec(`UPDATE uploads SET busy=1 WHERE id=? AND busy=0`, m.UploadID)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return fmt.Errorf("upload is busy")
	}
	return nil
}
func (s *Store) existing(group string, index int) (File, error) {
	var f File
	err := s.DB.QueryRow(`SELECT id,group_id,idx,path,size,md5,sha,remote_id FROM files WHERE group_id=? AND idx=?`, group, index).Scan(&f.ID, &f.Group, &f.Index, &f.Path, &f.Size, &f.MD5, &f.SHA256, &f.RemoteID)
	return f, err
}

func (s *Store) seal(id string) (string, bool, error) {
	rows, err := s.DB.Query(`SELECT sha FROM files WHERE group_id=? ORDER BY idx`, id)
	if err != nil {
		return "", false, err
	}
	h := sha256.New()
	for rows.Next() {
		var d string
		if err = rows.Scan(&d); err != nil {
			break
		}
		b, e := hex.DecodeString(d)
		if e != nil {
			err = e
			break
		}
		h.Write(b)
	}
	if e := rows.Err(); err == nil {
		err = e
	}
	rows.Close()
	if err != nil {
		return "", false, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	var canonical string
	err = s.DB.QueryRow(`SELECT id FROM uploads WHERE hash=? AND id<>?`, digest, id).Scan(&canonical)
	if err == nil {
		_, err = s.DB.Exec(`UPDATE uploads SET state='duplicate',canonical=? WHERE id=?`, canonical, id)
		return digest, true, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	_, err = s.DB.Exec(`UPDATE uploads SET hash=?,state=CASE WHEN state IN ('receiving','abandoned') THEN 'sealed' ELSE state END WHERE id=?`, digest, id)
	if err != nil { // A concurrent group may have sealed the same content.
		if e := s.DB.QueryRow(`SELECT id FROM uploads WHERE hash=?`, digest).Scan(&canonical); e != nil {
			return "", false, err
		}
		_, err = s.DB.Exec(`UPDATE uploads SET state='duplicate',canonical=? WHERE id=?`, canonical, id)
		return digest, true, err
	}
	return digest, false, nil
}

// Only sealed groups are cloud-bound. A group the client never finished
// sending can never be dispatched, so uploading its files would spend the
// limited 123 Pan capacity and spool space on work B can never consume.
func (s *Store) claimFile(ctx context.Context) (File, error) {
	var f File
	err := s.DB.QueryRowContext(ctx, `UPDATE files SET state='uploading' WHERE id=(SELECT f.id FROM files f JOIN uploads u ON u.id=f.group_id WHERE f.state='pending' AND f.retry_at<=? AND u.state='sealed' ORDER BY u.created_at,f.idx LIMIT 1) RETURNING id,group_id,idx,path,size,md5,sha,remote_id`, time.Now().Unix()).Scan(&f.ID, &f.Group, &f.Index, &f.Path, &f.Size, &f.MD5, &f.SHA256, &f.RemoteID)
	return f, err
}
func (s *Store) removeFile(f File) error {
	s.cleanup.Lock()
	defer s.cleanup.Unlock()
	if err := s.DB.QueryRow(`SELECT path,size FROM files WHERE id=?`, f.ID).Scan(&f.Path, &f.Size); err != nil {
		return err
	}
	if f.Path == "" {
		return nil
	}
	if err := os.Remove(f.Path); err == nil {
		s.Quota.removed(f.Size)
	} else if !os.IsNotExist(err) {
		return err
	}
	_, err := s.DB.Exec(`UPDATE files SET path='' WHERE id=?`, f.ID)
	if err == nil {
		used, reserved := s.Quota.snapshot()
		slog.Info("local file cleared", "file", f.ID, "bytes", f.Size,
			"spool_used", used, "spool_reserved", reserved)
	}
	return err
}

func (s *Store) makeBatch(size int) (*Batch, error) {
	// Recover the durable outbox before creating another notification.
	var data string
	err := s.DB.QueryRow(`SELECT payload FROM batches WHERE acked=0 ORDER BY created_at,id LIMIT 1`).Scan(&data)
	if err == nil {
		var b Batch
		err = json.Unmarshal([]byte(data), &b)
		return &b, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var next int64
	if err = tx.QueryRow(`SELECT next_at FROM dispatch_state WHERE id=1`).Scan(&next); err != nil {
		return nil, err
	}
	now := s.Now()
	if now.Unix() >= next {
		cutoff := now.Truncate(s.Interval).Unix()
		if _, err = tx.Exec(`INSERT OR IGNORE INTO dispatch_queue SELECT id,? FROM uploads u WHERE state='sealed' AND NOT EXISTS(SELECT 1 FROM files f WHERE f.group_id=u.id AND f.state<>'uploaded')`, cutoff); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(`UPDATE dispatch_state SET next_at=? WHERE id=1`, now.Truncate(s.Interval).Add(s.Interval).Unix()); err != nil {
			return nil, err
		}
	}
	rows, err := tx.Query(`SELECT u.id,u.metadata,u.hash,d.window_end FROM uploads u JOIN dispatch_queue d ON d.group_id=u.id WHERE u.state='sealed' AND d.window_end=(SELECT min(window_end) FROM dispatch_queue) ORDER BY u.created_at,u.id LIMIT ?`, size)
	if err != nil {
		return nil, err
	}
	var ids []string
	var windowEnd int64
	b := &Batch{Type: "batch_ready", Version: 1, ID: newID(), Items: []Sample{}}
	for rows.Next() {
		var id, meta, hash string
		if err = rows.Scan(&id, &meta, &hash, &windowEnd); err != nil {
			break
		}
		var sample Sample
		sample.SHA256 = hash
		if err = json.Unmarshal([]byte(meta), &sample.Metadata); err != nil {
			break
		}
		ids = append(ids, id)
		b.Items = append(b.Items, sample)
	}
	if e := rows.Err(); err == nil {
		err = e
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit()
	}
	b.WindowEnd = time.Unix(windowEnd, 0).UTC().Format(time.RFC3339)
	b.WindowStart = time.Unix(windowEnd, 0).Add(-s.Interval).UTC().Format(time.RFC3339)
	for i, id := range ids {
		fr, e := tx.Query(`SELECT idx,size,sha,remote_id FROM files WHERE group_id=? ORDER BY idx`, id)
		if e != nil {
			return nil, e
		}
		for fr.Next() {
			var f File
			if e = fr.Scan(&f.Index, &f.Size, &f.SHA256, &f.RemoteID); e != nil {
				break
			}
			b.Items[i].Files = append(b.Items[i].Files, f)
		}
		if x := fr.Err(); e == nil {
			e = x
		}
		fr.Close()
		if e != nil {
			return nil, e
		}
		if _, err = tx.Exec(`UPDATE uploads SET state='batched',batch_id=? WHERE id=?`, b.ID, id); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(`DELETE FROM dispatch_queue WHERE group_id=?`, id); err != nil {
			return nil, err
		}
	}
	encoded, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(`INSERT INTO batches(id,payload,created_at) VALUES(?,?,?)`, b.ID, string(encoded), time.Now().Unix()); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	slog.Info("batch ready", "batch_id", b.ID, "groups", len(b.Items),
		"window_start", b.WindowStart, "window_end", b.WindowEnd)
	return b, nil
}
