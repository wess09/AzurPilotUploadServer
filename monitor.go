package main

import (
	"context"
	"database/sql"
	"embed"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

//go:embed monitor/index.html monitor/styles.css monitor/app.js
var monitorAssets embed.FS

type monitorQueue struct {
	Pending       int64     `json:"pending"`
	Uploading     int64     `json:"uploading"`
	DirectPending int64     `json:"direct_pending"`
	Retry         int64     `json:"retry"`
	Uploaded      int64     `json:"uploaded"`
	Batches       int64     `json:"unacked_batches"`
	NextDispatch  int64     `json:"next_dispatch"`
	UpdatedAt     time.Time `json:"updated_at"`
	Available     bool      `json:"available"`
	Stale         bool      `json:"stale"`
}

type monitorCache struct {
	mu      sync.Mutex
	expires time.Time
	queue   monitorQueue
}

func (s *Server) queueSummary(ctx context.Context) monitorQueue {
	s.monitor.mu.Lock()
	defer s.monitor.mu.Unlock()
	if time.Now().Before(s.monitor.expires) {
		return s.monitor.queue
	}
	// Use an independent, query-only connection: the upload pool deliberately
	// has just one connection and may be continuously occupied by control calls.
	// WAL permits these reads alongside uploads. Only one refresh per cache TTL
	// opens this connection, and it is closed immediately after the two queries.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var q monitorQueue
	db, err := sql.Open("sqlite", filepath.Join(s.C.DataDir, "state.sqlite"))
	if err == nil {
		defer db.Close()
		db.SetMaxOpenConns(1)
		_, err = db.ExecContext(ctx, `PRAGMA query_only=ON; PRAGMA busy_timeout=1000`)
	}
	if err == nil {
		err = db.QueryRowContext(ctx, `SELECT
 count(*) FILTER(WHERE f.state='pending' AND u.state<>'duplicate'),
 count(*) FILTER(WHERE f.state='uploading'),
 count(*) FILTER(WHERE f.state='direct_pending' AND u.state<>'duplicate'),
 count(*) FILTER(WHERE f.state='pending' AND f.error<>'' AND u.state<>'duplicate'),
 count(*) FILTER(WHERE f.state='uploaded')
 FROM files f JOIN uploads u ON u.id=f.group_id`).Scan(&q.Pending, &q.Uploading, &q.DirectPending, &q.Retry, &q.Uploaded)
	}
	if err == nil {
		err = db.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM batches WHERE acked=0),
 (SELECT next_at FROM dispatch_state WHERE id=1)`).Scan(&q.Batches, &q.NextDispatch)
	}
	if err != nil {
		s.monitor.queue.Stale = true
		s.monitor.expires = time.Now().Add(5 * time.Second)
		return s.monitor.queue
	}
	q.Available, q.UpdatedAt = true, time.Now().UTC()
	s.monitor.queue = q
	s.monitor.expires = time.Now().Add(15 * time.Second)
	return q
}

func (s *Server) monitorSnapshot(w http.ResponseWriter, r *http.Request) {
	var after int64
	if value := r.URL.Query().Get("after"); value != "" {
		var err error
		after, err = strconv.ParseInt(value, 10, 64)
		if err != nil || after < 0 {
			reject(w, 400, strconv.ErrSyntax)
			return
		}
	}
	used, reserved := s.S.Quota.snapshot()
	w.Header().Set("Cache-Control", "no-store")
	respond(w, 200, map[string]any{
		"status": "ok", "spool_used": used, "spool_reserved": reserved, "spool_limit": s.C.SpoolBytes,
		"worker_connected": len(s.workerSlot) > 0, "worker_mode": s.C.WorkerMode,
		"receive_active": len(s.receivers), "receive_limit": s.C.ReceiveConcurrency,
		"direct_control_active": s.controlActive.Load(), "direct_upload_stage": "cloud_completed",
		"upload_limit": s.C.UploadConcurrency, "batch_interval_seconds": int64(s.S.Interval / time.Second),
		"transfers": s.transfers.snapshot(), "history": s.transfers.history(after),
		"history_step_seconds": historyStepSeconds, "history_retention_seconds": historyStepSeconds * historySlots,
		"queue": s.queueSummary(r.Context()),
	})
}

func (s *Server) monitorRoutes(m *http.ServeMux) {
	m.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusFound)
	})
	for route, asset := range map[string]string{
		"GET /dashboard/{$}":        "index.html",
		"GET /dashboard/styles.css": "styles.css",
		"GET /dashboard/app.js":     "app.js",
	} {
		m.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
			body, err := monitorAssets.ReadFile("monitor/" + asset)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			types := map[string]string{"index.html": "text/html; charset=utf-8", "styles.css": "text/css; charset=utf-8", "app.js": "text/javascript; charset=utf-8"}
			w.Header().Set("Content-Type", types[asset])
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Referrer-Policy", "no-referrer")
			w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'self'")
			_, _ = w.Write(body)
		})
	}
	m.HandleFunc("GET /api/v1/monitor", s.monitorSnapshot)
}
