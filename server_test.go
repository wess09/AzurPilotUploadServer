package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testPan struct {
	mu                  sync.Mutex
	calls, active, peak int
	fail                bool
	hold                <-chan struct{}
}

func (p *testPan) Upload(ctx context.Context, f File) (int64, error) {
	p.mu.Lock()
	p.calls++
	id := p.calls
	p.active++
	if p.active > p.peak {
		p.peak = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	if p.hold != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-p.hold:
		}
	}
	b, e := os.ReadFile(f.Path)
	if e != nil {
		return 0, e
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != f.SHA256 {
		return 0, fmt.Errorf("hash changed")
	}
	if p.fail {
		return 0, errors.New("offline")
	}
	return int64(id), nil
}
func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{DataDir: t.TempDir(), ReceiveConcurrency: 30, UploadConcurrency: 2, BatchSize: 30, SpoolBytes: 1 << 30, FileBytes: 10 << 20, GroupBytes: 500 << 20, Poll: time.Millisecond}
}
func pngData(suffix string) []byte {
	return append([]byte{137, 80, 78, 71, 13, 10, 26, 10}, []byte(suffix)...)
}
func metadata(i int, files ...[]byte) Metadata {
	m := Metadata{UploadID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i), DeviceID: strings.Repeat("a", 32), InstanceID: strings.Repeat("b", 16), Project: "drop", Scene: "campaign", Server: "cn", CollectedAt: "2026-10-26T12:00:00Z"}
	for _, f := range files {
		m.Sizes = append(m.Sizes, int64(len(f)))
	}
	return m
}
func receive(t *testing.T, s *Server, m Metadata, files ...[]byte) *httptest.ResponseRecorder {
	t.Helper()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	part, e := w.CreateFormField("metadata")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.NewEncoder(part).Encode(m); e != nil {
		t.Fatal(e)
	}
	for _, f := range files {
		part, e = w.CreateFormFile("file", "image.png")
		if e != nil {
			t.Fatal(e)
		}
		if _, e = part.Write(f); e != nil {
			t.Fatal(e)
		}
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	req := httptest.NewRequest("POST", "/api/v1/uploads", &b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}
func opened(t *testing.T, c Config) *Store {
	t.Helper()
	s, e := openStore(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.DB.Close() })
	return s
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond * 5)
	}
	t.Fatal("condition did not become true")
}

func TestQuotaConcurrentReservations(t *testing.T) {
	q := &Quota{Limit: 64}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if q.reserve(8) == nil {
				u, r := q.snapshot()
				if u+r > 64 {
					t.Errorf("quota exceeded: %d", u+r)
				}
				q.finish(8, 8)
				q.removed(8)
			}
		})
	}
	wg.Wait()
	u, r := q.snapshot()
	if u != 0 || r != 0 {
		t.Fatalf("quota leaked: %d %d", u, r)
	}
}
func TestLimitsAndInterruptedReceive(t *testing.T) {
	c := testConfig(t)
	c.FileBytes = 32
	c.GroupBytes = 48
	s := newServer(c, opened(t, c), &testPan{})
	a := pngData(strings.Repeat("x", 24))
	b := pngData("small")
	for _, tc := range []struct {
		meta  Metadata
		files [][]byte
	}{{metadata(1, pngData(strings.Repeat("x", 25))), [][]byte{pngData(strings.Repeat("x", 25))}}, {metadata(2, a, a), [][]byte{a, a}}} {
		rec := receive(t, s, tc.meta, tc.files...)
		if rec.Code != 413 {
			t.Fatalf("limit not rejected: %d %s", rec.Code, rec.Body.String())
		}
	}
	rec := receive(t, s, metadata(3, b, b), b)
	if rec.Code != 400 {
		t.Fatalf("interrupted group: %d", rec.Code)
	}
	if u, r := s.S.Quota.snapshot(); u != int64(len(b)) || r != 0 {
		t.Fatalf("unexpected quota %d %d", u, r)
	}
}
func TestCompleteFileDrainsBeforeGroupSeal(t *testing.T) {
	c := testConfig(t)
	data := pngData("first")
	c.SpoolBytes, c.FileBytes = int64(len(data)), int64(len(data))
	c.GroupBytes = c.FileBytes * 2
	s := newServer(c, opened(t, c), &testPan{})
	m := metadata(4, data, data)
	if e := s.S.claimGroup(m); e != nil {
		t.Fatal(e)
	}
	if e := s.savePart(context.Background(), m.UploadID, 0, int64(len(data)), bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { s.uploadLoop(ctx) })
	defer func() { cancel(); wg.Wait() }()
	eventually(t, func() bool { f, e := s.S.existing(m.UploadID, 0); return e == nil && f.RemoteID > 0 && f.Path == "" })
	u, r := s.S.Quota.snapshot()
	if u != 0 || r != 0 {
		t.Fatalf("file wasn't freed: %d %d", u, r)
	}
	var state string
	_ = s.S.DB.QueryRow(`SELECT state FROM uploads WHERE id=?`, m.UploadID).Scan(&state)
	if state != "receiving" {
		t.Fatal(state)
	}
}
func TestUploadFailurePreservesSpool(t *testing.T) {
	c := testConfig(t)
	p := &testPan{fail: true}
	s := newServer(c, opened(t, c), p)
	data := pngData("retry")
	m := metadata(5, data)
	if rec := receive(t, s, m, data); rec.Code != 202 {
		t.Fatal(rec.Body.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { s.uploadLoop(ctx) })
	eventually(t, func() bool {
		var e string
		_ = s.S.DB.QueryRow(`SELECT error FROM files WHERE group_id=?`, m.UploadID).Scan(&e)
		return e != ""
	})
	cancel()
	wg.Wait()
	f, e := s.S.existing(m.UploadID, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(f.Path); e != nil {
		t.Fatal("failed upload removed original", e)
	}
}
func TestBatchBoundaryDedupAndDurableOutbox(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	s := newServer(c, st, &testPan{})
	for i := range 29 {
		b := pngData(fmt.Sprintf("sample %d", i))
		if r := receive(t, s, metadata(i, b), b); r.Code != 202 {
			t.Fatalf("receive %s", r.Body.String())
		}
	}
	_, _ = st.DB.Exec(`UPDATE files SET state='uploaded',remote_id=1`)
	if b, e := st.makeBatch(30); e != nil || b != nil {
		t.Fatalf("29 triggered: %+v %v", b, e)
	}
	b := pngData("sample 0")
	r := receive(t, s, metadata(100, b), b)
	if r.Code != 202 || !strings.Contains(r.Body.String(), `"duplicate":true`) {
		t.Fatalf("dedup failed: %s", r.Body.String())
	}
	b = pngData("sample 29")
	if r = receive(t, s, metadata(29, b), b); r.Code != 202 {
		t.Fatal(r.Body.String())
	}
	if premature, e := st.makeBatch(30); e != nil || premature != nil {
		t.Fatal("group dispatched before time boundary", e)
	}
	clock := atNextBoundary(t, st)
	partial, e := st.makeBatch(30)
	if e != nil || partial == nil || len(partial.Items) != 29 {
		t.Fatal("ready groups must dispatch without reaching 30", partial, e)
	}
	_, _ = st.DB.Exec(`UPDATE batches SET acked=1 WHERE id=?`, partial.ID)
	_, _ = st.DB.Exec(`UPDATE files SET state='uploaded',remote_id=1`)
	if premature, e := st.makeBatch(30); e != nil || premature != nil {
		t.Fatal("late cloud completion escaped its next window", premature, e)
	}
	clock.Add(int64(st.Interval / time.Second))
	batch, e := st.makeBatch(30)
	if e != nil || batch == nil || len(batch.Items) != 1 {
		t.Fatalf("batch: %+v %v", batch, e)
	}
	again, e := st.makeBatch(30)
	if e != nil || again.ID != batch.ID {
		t.Fatal("notification wasn't replayed")
	}
	if e = st.DB.Close(); e != nil {
		t.Fatal(e)
	}
	restored, e := openStore(c)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.DB.Close()
	replayed, e := restored.makeBatch(30)
	if e != nil || replayed.ID != batch.ID {
		t.Fatal("restart lost outbox", e)
	}
}
func TestQuotaBackpressureAndRecovery(t *testing.T) {
	c := testConfig(t)
	data := pngData("quota")
	c.SpoolBytes = int64(len(data))
	c.FileBytes = c.SpoolBytes
	c.GroupBytes = 2 * c.SpoolBytes
	s := newServer(c, opened(t, c), &testPan{})
	if r := receive(t, s, metadata(1, data), data); r.Code != 202 {
		t.Fatal(r.Body.String())
	}
	if r := receive(t, s, metadata(2, data), data); r.Code != 503 || r.Header().Get("Retry-After") == "" {
		t.Fatalf("quota did not backpressure: %d", r.Code)
	}
	u, res := s.S.Quota.snapshot()
	if u+res > c.SpoolBytes {
		t.Fatal("over quota")
	}
}
func TestReceiveConcurrency(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), &testPan{})
	for range 30 {
		s.receivers <- struct{}{}
	}
	r := receive(t, s, Metadata{})
	if r.Code != 429 {
		t.Fatalf("31st receive accepted: %d", r.Code)
	}
	for range 30 {
		<-s.receivers
	}
}
func TestRestartRemovesOrphansAndRetainsPending(t *testing.T) {
	c := testConfig(t)
	st, e := openStore(c)
	if e != nil {
		t.Fatal(e)
	}
	s := newServer(c, st, &testPan{})
	b := pngData("retain")
	if r := receive(t, s, metadata(1, b), b); r.Code != 202 {
		t.Fatal(r.Body.String())
	}
	orphan := filepath.Join(st.Dir, "orphan.part")
	if e = os.WriteFile(orphan, []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	st.DB.Close()
	st, e = openStore(c)
	if e != nil {
		t.Fatal(e)
	}
	defer st.DB.Close()
	if _, e = os.Stat(orphan); !os.IsNotExist(e) {
		t.Fatal("orphan retained")
	}
	u, r := st.Quota.snapshot()
	if u != int64(len(b)) || r != 0 {
		t.Fatalf("recovered quota incorrect: %d %d", u, r)
	}
}
