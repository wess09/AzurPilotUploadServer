package main

import (
	"context"
	"log/slog"
	"time"
)

func logGroupReceived(m Metadata, hash string, duplicate bool) {
	var size int64
	for _, n := range m.Sizes {
		size += n
	}
	slog.Info("upload group received", "upload_id", m.UploadID, "project", m.Project,
		"scene", m.Scene, "server", m.Server, "files", len(m.Sizes), "bytes", size,
		"sha256", hash, "duplicate", duplicate)
}

// Keep idle/retrying nodes observable without logging health probes, image
// contents, credentials, submitter identities, or each transfer buffer.
func (s *Server) logStatus(ctx context.Context) {
	var pending, uploading, batches int
	var next int64
	err := s.S.DB.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM files f JOIN uploads u ON u.id=f.group_id WHERE f.state='pending' AND u.state<>'duplicate'),
 (SELECT count(*) FROM files WHERE state='uploading'),
 (SELECT count(*) FROM batches WHERE acked=0),
 (SELECT next_at FROM dispatch_state WHERE id=1)`).Scan(&pending, &uploading, &batches, &next)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("uploader status unavailable", "error", err)
		}
		return
	}
	used, reserved := s.S.Quota.snapshot()
	slog.Info("uploader status", "receiving", len(s.receivers), "pending_files", pending,
		"uploading_files", uploading, "unacked_batches", batches,
		"spool_used", used, "spool_reserved", reserved, "spool_limit", s.C.SpoolBytes,
		"worker_connected", len(s.workerSlot) > 0,
		"next_dispatch_at", time.Unix(next, 0).UTC().Format(time.RFC3339))
}

func (s *Server) statusLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	ticks := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.logTransfers()
			ticks++
			if ticks%6 == 0 {
				s.logStatus(ctx)
			}
		}
	}
}

func (s *Server) logTransfers() {
	r := s.transfers.snapshot()
	slog.Info("transfer rates", "window_seconds", r.WindowSeconds,
		"download_images_per_second", r.Download.ImagesPerSecond,
		"upload_images_per_second", r.Upload.ImagesPerSecond,
		"download_images_per_second_60s", r.Download.ImagesPerSecond60s,
		"upload_images_per_second_60s", r.Upload.ImagesPerSecond60s,
		"download_mib_per_second", rounded(r.Download.BytesPerSecond/(1<<20)),
		"upload_mib_per_second", rounded(r.Upload.BytesPerSecond/(1<<20)),
		"download_images_total", r.Download.Total.Images,
		"upload_images_total", r.Upload.Total.Images)
}
