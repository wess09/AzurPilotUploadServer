package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestPanSingleUploadProtocol(t *testing.T) {
	data := pngData(strings.Repeat("image", 4096))
	path := t.TempDir() + "/original.png"
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	sha, md := sha256.Sum256(data), md5.Sum(data)
	f := File{Path: path, SHA256: hex.EncodeToString(sha[:]), MD5: hex.EncodeToString(md[:]), Size: int64(len(data))}
	var completed atomic.Bool
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Platform") != "open_platform" {
			t.Error("missing platform")
		}
		switch r.URL.Path {
		case "/api/v1/access_token":
			respond(w, 200, map[string]any{"code": 0, "data": map[string]any{"accessToken": "test-token"}})
		case "/upload/v2/file/domain":
			respond(w, 200, map[string]any{"code": 0, "data": []string{server.URL}})
		case "/upload/v2/file/single/create":
			if r.Header.Get("Authorization") != "Bearer test-token" || r.ContentLength <= f.Size {
				t.Error("missing auth/exact content length")
			}
			reader, e := r.MultipartReader()
			if e != nil {
				t.Error(e)
				return
			}
			fields := map[string]string{}
			for {
				part, e := reader.NextPart()
				if e == io.EOF {
					break
				}
				if e != nil {
					t.Error(e)
					return
				}
				b, e := io.ReadAll(part)
				if e != nil {
					t.Error(e)
					return
				}
				if part.FormName() == "file" {
					if !bytes.Equal(b, data) {
						t.Error("corrupted file")
					}
				} else {
					fields[part.FormName()] = string(b)
				}
			}
			if fields["etag"] != f.MD5 || fields["filename"] != f.SHA256+".png" {
				t.Error(fields)
			}
			respond(w, 200, map[string]any{"code": 0, "data": map[string]any{"completed": completed.Load(), "fileID": 123}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	p := &PanClient{Client: server.Client(), Base: server.URL, ClientID: "id", Secret: "secret"}
	if _, e := p.Upload(context.Background(), f); e == nil {
		t.Fatal("unconfirmed upload accepted")
	}
	completed.Store(true)
	if id, e := p.Upload(context.Background(), f); e != nil || id != 123 {
		t.Fatalf("%d %v", id, e)
	}
}

func TestNotifierReplaysUnacknowledgedBatch(t *testing.T) {
	c := testConfig(t)
	c.BatchSize = 1
	s := newServer(c, opened(t, c), &testPan{})
	data := pngData("notify")
	if r := receive(t, s, metadata(1, data), data); r.Code != 202 {
		t.Fatal(r.Body.String())
	}
	_, _ = s.S.DB.Exec(`UPDATE files SET state='uploaded',remote_id=1`)
	atNextBoundary(t, s.S)
	var attempts atomic.Int32
	var first atomic.Value
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer worker-secret" {
			t.Error("missing worker auth")
		}
		conn, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer conn.CloseNow()
		var b Batch
		if e = wsjson.Read(r.Context(), conn, &b); e != nil {
			t.Error(e)
			return
		}
		if attempts.Add(1) == 1 {
			first.Store(b.ID)
			return
		}
		if first.Load() != b.ID {
			t.Error("replay changed batch ID")
		}
		_ = wsjson.Write(r.Context(), conn, map[string]string{"type": "batch_accepted", "batch_id": b.ID})
		for {
			var acked int
			_ = s.S.DB.QueryRow(`SELECT acked FROM batches WHERE id=?`, b.ID).Scan(&acked)
			if acked == 1 {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}))
	defer ws.Close()
	s.C.WorkerURL = "ws" + strings.TrimPrefix(ws.URL, "http")
	s.C.WorkerToken = "worker-secret"
	if e := s.notify(ctx); e == nil {
		t.Fatal("missing ACK did not interrupt connection")
	}
	_ = s.notify(ctx)
	if attempts.Load() != 2 || ctx.Err() == nil {
		t.Fatal("replay not acknowledged")
	}
}

type repeatingReader byte

func (r repeatingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

func TestThirtyStreamingReceiversTwoUploadWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("300 MiB streaming resource test")
	}
	c := testConfig(t)
	c.SpoolBytes, c.FileBytes, c.GroupBytes = 1<<30, 10<<20, 500<<20
	p := &streamPan{}
	s := newServer(c, opened(t, c), p)
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() { s.uploadLoop(ctx) })
	}
	defer func() { cancel(); workers.Wait() }()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	stopSample := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-stopSample:
				return
			case <-time.After(5 * time.Millisecond):
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				for v := peak.Load(); m.HeapInuse > v && !peak.CompareAndSwap(v, m.HeapInuse); v = peak.Load() {
				}
			}
		}
	})
	var requests sync.WaitGroup
	for i := range 30 {
		requests.Go(func() {
			reader, writer := io.Pipe()
			form := multipart.NewWriter(writer)
			m := metadata(i, nil)
			m.Sizes = []int64{c.FileBytes}
			go func() {
				defer writer.Close()
				part, e := form.CreateFormField("metadata")
				if e != nil {
					return
				}
				if e = json.NewEncoder(part).Encode(m); e != nil {
					return
				}
				part, e = form.CreateFormFile("file", "sample.png")
				if e != nil {
					return
				}
				_, _ = part.Write([]byte{137, 80, 78, 71, 13, 10, 26, 10})
				_, _ = io.CopyBuffer(part, io.LimitReader(repeatingReader(i+1), c.FileBytes-8), make([]byte, 64<<10))
				_ = form.Close()
			}()
			defer reader.Close()
			r := httptest.NewRequest("POST", "/api/v1/uploads", reader)
			r.Header.Set("Content-Type", form.FormDataContentType())
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 202 {
				t.Errorf("receive %d: %d %s", i, w.Code, w.Body.String())
			}
		})
	}
	requests.Wait()
	eventually(t, func() bool { u, r := s.S.Quota.snapshot(); return u+r == 0 })
	close(stopSample)
	sampler.Wait()
	if p.peak.Load() > 2 {
		t.Fatal("more than two cloud uploads")
	}
	growth := int64(peak.Load()) - int64(base.HeapInuse)
	t.Logf("streamed=300 MiB peak heap growth=%.1f MiB cloud concurrency=%d", float64(growth)/(1<<20), p.peak.Load())
	if growth > 128<<20 {
		t.Fatal("heap grew with image bodies")
	}
}

type streamPan struct {
	active, peak atomic.Int32
	ids          atomic.Int64
}

func (p *streamPan) Upload(ctx context.Context, f File) (int64, error) {
	n := p.active.Add(1)
	defer p.active.Add(-1)
	for v := p.peak.Load(); n > v && !p.peak.CompareAndSwap(v, n); v = p.peak.Load() {
	}
	file, e := os.Open(f.Path)
	if e != nil {
		return 0, e
	}
	defer file.Close()
	sum := sha256.New()
	if _, e = io.CopyBuffer(sum, file, make([]byte, 64<<10)); e != nil {
		return 0, e
	}
	if hex.EncodeToString(sum.Sum(nil)) != f.SHA256 {
		return 0, fmt.Errorf("hash mismatch")
	}
	return p.ids.Add(1), nil
}

func TestActualFileOverflowDoesNotWritePastReservation(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), &testPan{})
	data := pngData("overflow")
	m := metadata(1, data)
	m.Sizes[0]--
	if r := receive(t, s, m, data); r.Code != 413 {
		t.Fatalf("overflow accepted: %d", r.Code)
	}
	if u, r := s.S.Quota.snapshot(); u+r != 0 {
		t.Fatal("partial reservation retained")
	}
}
