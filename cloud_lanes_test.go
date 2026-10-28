package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDirectConfirmationDoesNotShareCreationOrBulkBackoff(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/upload/v1/file/create" {
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"fileID":42,"completed":true}}`))
	}))
	defer upstream.Close()
	throttled := 0
	p := &PanClient{Client: upstream.Client(), Base: upstream.URL, token: "test-token", expires: time.Now().Add(time.Hour),
		backoff: func() time.Duration { return time.Minute }, onThrottle: func(d time.Duration) time.Duration { throttled++; return d }}
	if _, e := p.CreateDirect(context.Background(), File{}); e == nil {
		t.Fatal("creation should receive 429")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, e := p.CompleteDirect(ctx, "ticket", false)
	if e != nil || result.FileID != 42 || throttled != 0 {
		t.Fatalf("confirmation blocked by unrelated endpoint: %+v %v", result, e)
	}
}

func TestRepeatedDirectManifestDoesNotWaitForDatabaseConnection(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	s := newServer(c, st, &directTestPan{})
	data := pngData("idempotent")
	m := metadata(80001, data)
	md, sha := md5.Sum(data), sha256.Sum256(data)
	payload, _ := json.Marshal(directManifest{Version: 2, Metadata: m, Files: []directHash{{MD5: hex.EncodeToString(md[:]), SHA256: hex.EncodeToString(sha[:])}}})
	initialize := func(ctx context.Context) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/api/v2/uploads/init", bytes.NewReader(payload)).WithContext(ctx)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := initialize(context.Background()); rec.Code != 202 {
		t.Fatal(rec.Body.String())
	}
	conn, e := st.DB.Conn(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if rec := initialize(ctx); rec.Code != 202 {
		t.Fatalf("cached manifest hit database: %d %s", rec.Code, rec.Body.String())
	}
}
