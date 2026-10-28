package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type cloudThrottleError struct {
	RetryAfter time.Duration
	Endpoint   string
}

func (e *cloudThrottleError) Error() string {
	return fmt.Sprintf("123 rate limited; retry after %ds; endpoint=%s", int(math.Ceil(e.RetryAfter.Seconds())), e.Endpoint)
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

type uploadState struct {
	Active         int    `json:"active"`
	Limit          int    `json:"limit"`
	Minimum        int    `json:"minimum"`
	Maximum        int    `json:"maximum"`
	BackoffSeconds int64  `json:"backoff_seconds"`
	RateLimited    uint64 `json:"rate_limited_total"`
}

type uploadControl struct {
	mu                sync.Mutex
	minimum, maximum  int
	limit, active     int
	successes, streak int
	limited           uint64
	until, lastRaise  time.Time
	now               func() time.Time
}

func newUploadControl(minimum, maximum int) *uploadControl {
	minimum = max(1, minimum)
	maximum = max(minimum, maximum)
	return &uploadControl{minimum: minimum, maximum: maximum, limit: minimum, lastRaise: time.Now(), now: time.Now}
}

func (c *uploadControl) remaining() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return max(0, c.until.Sub(c.now()))
}

func (c *uploadControl) throttle(delay time.Duration) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.limited++
	if !now.Before(c.until) {
		c.streak = min(6, c.streak+1)
		if delay <= 0 {
			delay = min(5*time.Minute, 10*time.Second*time.Duration(1<<(c.streak-1)))
		}
		c.limit = max(c.minimum, c.limit/2)
		c.successes = 0
		c.until = now.Add(delay)
	} else if delay > 0 && now.Add(delay).After(c.until) {
		c.until = now.Add(delay)
	}
	c.lastRaise = c.until
	return c.until.Sub(now)
}

func (c *uploadControl) begin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active >= c.limit || c.now().Before(c.until) {
		return false
	}
	c.active++
	return true
}

func (c *uploadControl) finish(success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	now := c.now()
	if !success || now.Before(c.until) {
		return
	}
	c.successes++
	if c.successes >= c.limit && now.Sub(c.lastRaise) >= 10*time.Second {
		c.limit = min(c.maximum, c.limit+max(1, c.limit/4))
		c.successes, c.streak, c.lastRaise = 0, 0, now
	}
}

func (c *uploadControl) snapshot() uploadState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return uploadState{Active: c.active, Limit: c.limit, Minimum: c.minimum, Maximum: c.maximum,
		BackoffSeconds: int64(math.Ceil(max(0, c.until.Sub(c.now()).Seconds()))), RateLimited: c.limited}
}

// One dispatcher owns claiming. Idle capacity has no goroutines or SQLite
// polling loops; only active, streaming file transfers create a worker.
func (s *Server) runUploads(ctx context.Context) {
	ticker := time.NewTicker(max(s.C.Poll, time.Millisecond))
	defer ticker.Stop()
	wake := make(chan struct{}, 1)
	var workers sync.WaitGroup
	defer workers.Wait()
	for ctx.Err() == nil {
		for ctx.Err() == nil && s.uploads.begin() {
			file, err := s.S.claimFile(ctx)
			if err != nil {
				s.uploads.finish(false)
				if !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
					slog.Warn("claim upload", "error", err)
				}
				break
			}
			workers.Go(func() {
				success := s.uploadOne(ctx, file)
				s.uploads.finish(success)
				select {
				case wake <- struct{}{}:
				default:
				}
			})
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func (s *Server) uploadOne(ctx context.Context, f File) bool {
	started := time.Now()
	uploadCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	id, err := s.Pan.Upload(uploadCtx, f)
	cancel()
	if err == nil && id <= 0 {
		err = fmt.Errorf("missing cloud file ID")
	}
	if err != nil {
		delay := 10 * time.Second
		var limited *cloudThrottleError
		if errors.As(err, &limited) {
			delay = max(delay, limited.RetryAfter)
		}
		_, saveErr := s.S.DB.Exec(`UPDATE files SET state='pending',retry_at=?,error=? WHERE id=?`, time.Now().Add(delay).Unix(), err.Error(), f.ID)
		slog.Warn("upload retry", "upload_id", f.Group, "file", f.ID, "index", f.Index,
			"bytes", f.Size, "retry_in", delay.String(), "error", err)
		if saveErr != nil {
			slog.Error("save upload retry", "file", f.ID, "error", saveErr)
		}
		return false
	}
	if _, err = s.S.DB.Exec(`UPDATE files SET state='uploaded',remote_id=?,error='' WHERE id=?`, id, f.ID); err != nil {
		slog.Error("save upload state", "file", f.ID, "error", err)
		return false
	}
	s.transfers.record(true, f.Size)
	slog.Info("cloud upload completed", "upload_id", f.Group, "file", f.ID, "index", f.Index,
		"remote_id", id, "bytes", f.Size, "duration_ms", time.Since(started).Milliseconds())
	if err = s.S.removeFile(f); err != nil {
		slog.Warn("cleanup completed upload", "file", f.ID, "error", err)
	}
	return true
}
