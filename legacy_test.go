package main

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func oldUpload(t *testing.T, s *Server, path string, fields map[string]string, file []byte, agent string) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "1792886400000.png")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(file)
	for k, v := range fields {
		_ = writer.WriteField(k, v)
	}
	_ = writer.Close()
	req := httptest.NewRequest("POST", path, &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("User-Agent", agent)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	return w
}

func TestLegacyAPUploadAndDedup(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	s := newServer(c, st, &testPan{})
	fields := map[string]string{"device_id": strings.Repeat("a", 32), "genre": "opsi_meowfficer_farming", "combat_count": "12"}
	for i, path := range []string{"/api/upload", "/api/upload/"} {
		w := oldUpload(t, s, path, fields, pngData("legacy"), "alas AzurPilot")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body)
		}
		var out struct {
			Status    bool
			Code      int
			Duplicate bool
			ImgID     string
			Data      struct{ MD5 string }
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || !out.Status || out.Code != 200 || out.Duplicate != (i == 1) || len(out.ImgID) != 64 || len(out.Data.MD5) != 32 {
			t.Fatal(w.Body, err)
		}
	}
	var raw string
	if err := st.DB.QueryRow(`SELECT metadata FROM uploads WHERE hash IS NOT NULL LIMIT 1`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var m Metadata
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	if m.CombatCount != 12 || m.Scene != "opsi_meowfficer_farming" || m.CollectedAt != time.UnixMilli(1792886400000).UTC().Format(time.RFC3339Nano) {
		t.Fatal(m)
	}
}

func TestLegacyFileOnlyAndQuotaCleanup(t *testing.T) {
	c := testConfig(t)
	c.FileBytes = 32
	c.GroupBytes = 64
	st := opened(t, c)
	s := newServer(c, st, &testPan{})
	w := oldUpload(t, s, "/api/upload/", nil, pngData("file-only"), "Alas (legacy-user)")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	used, reserved := st.Quota.snapshot()
	w = oldUpload(t, s, "/api/upload", nil, pngData(strings.Repeat("x", 40)), "Alas (legacy-user)")
	if w.Code != 413 {
		t.Fatal(w.Code, w.Body)
	}
	a, b := st.Quota.snapshot()
	if a != used || b != reserved {
		t.Fatal("failed file leaked quota", a, b, used, reserved)
	}
	s.C.StatisticsURL = "https://stats.azurpilot.com"
	r := httptest.NewRequest("POST", "/api/telemetry?x=1", strings.NewReader("unread"))
	out := httptest.NewRecorder()
	s.Handler().ServeHTTP(out, r)
	if out.Code != 307 || out.Header().Get("Location") != "https://stats.azurpilot.com/api/telemetry?x=1" {
		t.Fatal(out.Code, out.Header())
	}
}
