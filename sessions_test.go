package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
)

func sessionRequest(s *Server, method, path string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func initializeSession(t *testing.T, s *Server, m Metadata) {
	t.Helper()
	b, _ := json.Marshal(m)
	if w := sessionRequest(s, "POST", "/api/v1/uploads/init", b); w.Code != 202 {
		t.Fatalf("initialize: %d %s", w.Code, w.Body)
	}
}

func TestSessionSurvivesRestartAndRequiresAllFiles(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	s := newServer(c, st, nil)
	files := [][]byte{pngData("one"), pngData("two")}
	m := metadata(501, files...)
	initializeSession(t, s, m)
	path := "/api/v1/uploads/" + m.UploadID
	if w := sessionRequest(s, "PUT", path+"/files/0", files[0]); w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	if w := sessionRequest(s, "POST", path+"/complete", nil); w.Code != 409 {
		t.Fatal("incomplete group sealed", w.Code, w.Body)
	}
	if b, err := st.makeBatch(1); err != nil || b != nil {
		t.Fatal("incomplete group dispatched", b, err)
	}
	if err := st.DB.Close(); err != nil {
		t.Fatal(err)
	}
	st = opened(t, c)
	s = newServer(c, st, nil)
	initializeSession(t, s, m)
	for i, f := range files {
		if w := sessionRequest(s, "PUT", fmt.Sprintf("%s/files/%d", path, i), f); w.Code != 202 {
			t.Fatal(w.Code, w.Body)
		}
	}
	for range 2 {
		if w := sessionRequest(s, "POST", path+"/complete", nil); w.Code != 202 {
			t.Fatal(w.Code, w.Body)
		}
	}
	var count int
	if err := st.DB.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&count); err != nil || count != 2 {
		t.Fatal("retry duplicated files", count, err)
	}
	other := metadata(502, files...)
	w := receive(t, s, other, files...)
	var result struct{ Duplicate bool }
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != 202 || !result.Duplicate {
		t.Fatal("session and multipart dedup differ", w.Code, w.Body, err)
	}
}

func TestSessionLimitsAndConflictingRetries(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	s := newServer(c, st, nil)
	f := pngData("one")
	m := metadata(503, f)
	initializeSession(t, s, m)
	path := "/api/v1/uploads/" + m.UploadID
	for _, tc := range []struct {
		index string
		body  []byte
		code  int
	}{
		{"1", f, 400}, {"0", append(f, 1), 413}, {"0", f[:len(f)-1], 400},
		{"0", f, 202}, {"0", pngData("two"), 409}, {"0", f, 202},
	} {
		if w := sessionRequest(s, "PUT", path+"/files/"+tc.index, tc.body); w.Code != tc.code {
			t.Fatalf("index=%s: %d %s, want %d", tc.index, w.Code, w.Body, tc.code)
		}
	}
	// Unknown Content-Length must still enforce the declared file size and
	// remove partial data and reservations on excess or interruption.
	other := metadata(504, f)
	initializeSession(t, s, other)
	for _, body := range [][]byte{append(f, 1), f[:len(f)-1]} {
		r := httptest.NewRequest("PUT", "/api/v1/uploads/"+other.UploadID+"/files/0", bytes.NewReader(body))
		r.ContentLength = -1
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 400 && w.Code != 413 {
			t.Fatal("accepted invalid stream", w.Code, w.Body)
		}
		used, reserved := st.Quota.snapshot()
		if used != int64(len(f)) || reserved != 0 {
			t.Fatal("partial stream leaked quota", used, reserved)
		}
	}
}

func TestSessionFiveHundredMiBManifestReservesOnlyActualFile(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), nil)
	m := metadata(505)
	for range 50 {
		m.Sizes = append(m.Sizes, 10<<20)
	}
	initializeSession(t, s, m)
	used, reserved := s.S.Quota.snapshot()
	if used != 0 || reserved != 0 {
		t.Fatal("manifest preallocated spool", used, reserved)
	}
	m.UploadID = metadata(506).UploadID
	m.Sizes = append(m.Sizes, 8)
	b, _ := json.Marshal(m)
	if w := sessionRequest(s, "POST", "/api/v1/uploads/init", b); w.Code != 413 {
		t.Fatal("oversize group accepted", w.Code, w.Body)
	}
	m = metadata(507)
	m.Sizes = []int64{(10 << 20) + 1}
	b, _ = json.Marshal(m)
	if w := sessionRequest(s, "POST", "/api/v1/uploads/init", b); w.Code != 413 {
		t.Fatal("oversize file accepted", w.Code, w.Body)
	}
}
