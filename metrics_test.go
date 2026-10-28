package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTransferRatesExpireWithoutHealthPolling(t *testing.T) {
	now := time.Unix(1800000000, 0)
	m := newTransferMetrics(func() time.Time { return now })
	now = now.Add(time.Minute)
	for range 10 {
		m.record(false, 1024)
	}
	for range 5 {
		m.record(true, 2048)
	}
	s := m.snapshot()
	if s.Download.ImagesPerSecond != 1 || s.Upload.ImagesPerSecond != .5 || s.Download.BytesPerSecond != 1024 || s.Upload.Total.Bytes != 10240 {
		t.Fatal("wrong image or byte rates", s)
	}
	if again := m.snapshot(); again.Download != s.Download || again.Upload != s.Upload {
		t.Fatal("reading metrics changed rate counters", s, again)
	}
	now = now.Add(10 * time.Second)
	s = m.snapshot()
	if s.Download.ImagesPerSecond != 0 || s.Upload.ImagesPerSecond != 0 || s.Download.ImagesPerSecond60s != .167 {
		t.Fatal("idle short-window rates did not expire", s)
	}
	now = now.Add(50 * time.Second)
	s = m.snapshot()
	if s.Download.ImagesPerSecond60s != 0 || s.Upload.ImagesPerSecond60s != 0 || s.Download.Total.Images != 10 || s.Upload.Total.Images != 5 {
		t.Fatal("long window did not expire or totals were lost", s)
	}
	// Reuse the same ring slot after a full minute; old events must not return.
	m.record(false, 8)
	s = m.snapshot()
	if s.Download.ImagesPerSecond != .1 || s.Download.Total.Images != 11 {
		t.Fatal("old ring slot was counted again", s)
	}
}

func TestTransferMetricsConcurrentCompletions(t *testing.T) {
	now := time.Unix(1800000000, 0)
	m := newTransferMetrics(func() time.Time { return now })
	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			for range 100 {
				m.record(false, 8)
				m.record(true, 16)
				_ = m.snapshot()
			}
		})
	}
	wg.Wait()
	s := m.snapshot()
	if s.Download.Total.Images != 3000 || s.Upload.Total.Images != 3000 || s.Download.Total.Bytes != 24000 || s.Upload.Total.Bytes != 48000 {
		t.Fatal("concurrent updates lost counts", s)
	}
	if _, err := json.Marshal(s); err != nil {
		t.Fatal("startup produced invalid rates", err)
	}
}

func TestTransferMetricsCountFilesAndVerifiedRetries(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), nil)
	files := [][]byte{pngData("one"), pngData("two")}
	m := metadata(9101, files...)
	initializeSession(t, s, m)
	path := "/api/v1/uploads/" + m.UploadID
	for _, tc := range []struct {
		index int
		body  []byte
		code  int
	}{
		{0, files[0][:len(files[0])-1], 400},
		{0, files[0], 202},
		{0, pngData("bad"), 409},
		{0, files[0], 202}, // Same file transferred and verified again.
		{1, files[1], 202},
	} {
		w := sessionRequest(s, "PUT", fmt.Sprintf("%s/files/%d", path, tc.index), tc.body)
		if w.Code != tc.code {
			t.Fatalf("file request: %d %s", w.Code, w.Body)
		}
	}
	for range 2 {
		if w := sessionRequest(s, "POST", path+"/complete", nil); w.Code != 202 {
			t.Fatal(w.Code, w.Body)
		}
	}
	stats := s.transfers.snapshot()
	if stats.Download.Total.Images != 3 || stats.Upload.Total.Images != 0 {
		t.Fatal("counted a group, completion call or failed transfer as an image", stats)
	}
	w := sessionRequest(s, "GET", "/healthz", nil)
	var health struct {
		Transfers transferSnapshot `json:"transfers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &health); err != nil || w.Code != 200 || health.Transfers.Download.Total.Images != 3 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("health metrics missing or cacheable", w.Code, w.Body, err)
	}
}

func TestTransferMetricsCoverMultipartAndLegacy(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), nil)
	f := pngData("multipart")
	m := metadata(9102, f)
	for range 2 {
		if w := receive(t, s, m, f); w.Code != 202 {
			t.Fatal(w.Code, w.Body)
		}
	}
	if w := oldUpload(t, s, "/api/upload", nil, pngData("legacy"), "Alas (metrics-test)"); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	if got := s.transfers.snapshot().Download.Total.Images; got != 3 {
		t.Fatalf("multipart, verified retry or legacy transfer was missed: %d", got)
	}
}

func TestTransferMetricsCloudFailuresDoNotCountAsUploads(t *testing.T) {
	c := testConfig(t)
	pan := &testPan{fail: true}
	s := newServer(c, opened(t, c), pan)
	f := pngData("cloud")
	if w := receive(t, s, metadata(9103, f), f); w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.uploadLoop(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	eventually(t, func() bool {
		var retry int
		_ = s.S.DB.QueryRow(`SELECT count(*) FROM files WHERE state='pending' AND retry_at>0`).Scan(&retry)
		return retry == 1
	})
	if got := s.transfers.snapshot().Upload.Total.Images; got != 0 {
		t.Fatalf("failed cloud attempt counted as uploaded: %d", got)
	}
	cancel()
	<-done
	pan.fail = false
	if _, err := s.S.DB.Exec(`UPDATE files SET retry_at=0`); err != nil {
		t.Fatal(err)
	}
	retryCtx, retryCancel := context.WithCancel(context.Background())
	retryDone := make(chan struct{})
	go func() { defer close(retryDone); s.uploadLoop(retryCtx) }()
	t.Cleanup(func() { retryCancel(); <-retryDone })
	eventually(t, func() bool { return s.transfers.snapshot().Upload.Total.Images == 1 })
	if stats := s.transfers.snapshot(); stats.Upload.Total.Bytes != int64(len(f)) || stats.Download.Total.Images != 1 {
		t.Fatal("cloud success did not count exactly one image", stats)
	}
}
