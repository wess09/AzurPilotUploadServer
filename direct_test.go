package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type directTestPan struct {
	mu      sync.Mutex
	files   map[string]File
	ids     map[int64]File
	creates int
	polls   int
	async   bool
	modify  func(*CloudFileInfo)
}

func (p *directTestPan) Upload(context.Context, File) (int64, error) {
	return 0, fmt.Errorf("v2 must not use A's image upload worker")
}
func (p *directTestPan) CreateDirect(_ context.Context, f File) (CloudTicket, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.files == nil {
		p.files, p.ids = map[string]File{}, map[int64]File{}
	}
	p.creates++
	id := fmt.Sprintf("ticket-%d", p.creates)
	p.files[id], p.ids[int64(p.creates)] = f, f
	return CloudTicket{PreuploadID: id, SliceSize: 8}, nil
}
func (p *directTestPan) DirectURL(_ context.Context, id string, n int) (string, error) {
	return fmt.Sprintf("https://uploads.123624.com/%s/%d?signature=secret", id, n), nil
}
func (p *directTestPan) CompleteDirect(_ context.Context, id string, poll bool) (CloudCompletion, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.async && !poll {
		return CloudCompletion{Async: true}, nil
	}
	if poll {
		p.polls++
	}
	f := p.files[id]
	for i, stored := range p.ids {
		if stored.ID == f.ID {
			return CloudCompletion{FileID: i, Completed: true}, nil
		}
	}
	return CloudCompletion{}, fmt.Errorf("missing ticket")
}
func (p *directTestPan) DirectInfo(_ context.Context, id int64) (CloudFileInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f := p.ids[id]
	info := CloudFileInfo{FileID: id, Name: f.SHA256 + ".png", MD5: f.MD5, Size: f.Size}
	if p.modify != nil {
		p.modify(&info)
	}
	return info, nil
}

func directTestManifest(id int, files ...[]byte) directManifest {
	m := directManifest{Version: 2, Metadata: metadata(id, files...)}
	for _, data := range files {
		md, sha := md5.Sum(data), sha256.Sum256(data)
		m.Files = append(m.Files, directHash{MD5: hex.EncodeToString(md[:]), SHA256: hex.EncodeToString(sha[:])})
	}
	return m
}
func directInit(t *testing.T, s *Server, m directManifest, want int) {
	t.Helper()
	b, _ := json.Marshal(m)
	w := sessionRequest(s, "POST", "/api/v2/uploads/init", b)
	if w.Code != want {
		t.Fatalf("init: %d %s, want %d", w.Code, w.Body, want)
	}
}
func directControl(t *testing.T, s *Server, m directManifest, suffix string, want int) map[string]any {
	t.Helper()
	w := sessionRequest(s, "POST", "/api/v2/uploads/"+m.Metadata.UploadID+suffix, nil)
	if w.Code != want {
		t.Fatalf("%s: %d %s, want %d", suffix, w.Code, w.Body, want)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestV2DirectResumeVerifyAndBatchWithoutSpooling(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	pan := &directTestPan{async: true}
	s := newServer(c, st, pan)
	m := directTestManifest(9201, pngData("one"), pngData("two"))
	directInit(t, s, m, 202)
	directInit(t, s, m, 202)
	if _, err := st.claimFile(context.Background()); err != sql.ErrNoRows {
		t.Fatal("v2 file entered the v1 spool worker", err)
	}
	directControl(t, s, m, "/complete", 409)
	first := directControl(t, s, m, "/files/0/prepare", 200)
	if len(first["parts"].([]any)) != 2 {
		t.Fatal("wrong part plan", first)
	}
	if err := st.DB.Close(); err != nil {
		t.Fatal(err)
	}
	st = opened(t, c)
	s = newServer(c, st, pan)
	directControl(t, s, m, "/files/0/prepare", 200)
	if pan.creates != 1 {
		t.Fatal("restart created a new cloud task", pan.creates)
	}
	for i := range m.Files {
		directControl(t, s, m, fmt.Sprintf("/files/%d/prepare", i), 200)
		directControl(t, s, m, fmt.Sprintf("/files/%d/complete", i), 202)
		pending := directControl(t, s, m, fmt.Sprintf("/files/%d/prepare", i), 200)
		if pending["finalizing"] != true {
			t.Fatal("merge progress was not durable", pending)
		}
		directControl(t, s, m, fmt.Sprintf("/files/%d/complete", i), 200)
		directControl(t, s, m, fmt.Sprintf("/files/%d/complete", i), 200)
	}
	if pan.polls != 2 {
		t.Fatal("async completion not polled", pan.polls)
	}
	result := directControl(t, s, m, "/complete", 202)
	if result["schema_version"] != float64(2) || result["duplicate"] != false {
		t.Fatal(result)
	}
	directControl(t, s, m, "/complete", 202)
	if used, reserved := st.Quota.snapshot(); used != 0 || reserved != 0 {
		t.Fatal("direct upload consumed spool quota", used, reserved)
	}
	entries, _ := os.ReadDir(st.Dir)
	if len(entries) != 0 {
		t.Fatal("direct upload left image files", entries)
	}
	if _, err := st.DB.Exec(`UPDATE dispatch_state SET next_at=0`); err != nil {
		t.Fatal(err)
	}
	batch, err := st.makeBatch(30)
	if err != nil || batch == nil || len(batch.Items) != 1 || batch.Version != 1 || batch.Items[0].Metadata.CollectedAt != m.Metadata.CollectedAt || len(batch.Items[0].Files) != 2 {
		t.Fatal("existing B protocol or capture time changed", batch, err)
	}
	for _, f := range batch.Items[0].Files {
		if f.RemoteID <= 0 || f.Path != "" {
			t.Fatal("unverified file reached B", f)
		}
	}
	if stats := s.transfers.snapshot(); stats.DirectUpload.Total.Images != 2 || stats.Download.Total.Images != 0 || stats.Upload.Total.Images != 0 {
		t.Fatal("direct confirmations confused with A image traffic", stats)
	}
	// A distinct report of already verified content reuses cloud IDs and seals
	// as a duplicate, with no extra cloud object or spool reservation.
	m.Metadata.UploadID = metadata(9202).UploadID
	directInit(t, s, m, 202)
	for i := range m.Files {
		r := directControl(t, s, m, fmt.Sprintf("/files/%d/prepare", i), 200)
		if r["completed"] != true {
			t.Fatal("verified content not reused", r)
		}
	}
	if r := directControl(t, s, m, "/complete", 202); r["duplicate"] != true || pan.creates != 2 {
		t.Fatal("duplicate direct upload was recreated", r, pan.creates)
	}
}

func TestV2RequiresCloudMD5SizeAndOwnedDestination(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*CloudFileInfo)
	}{
		{"md5", func(i *CloudFileInfo) { i.MD5 = strings.Repeat("0", 32) }},
		{"size", func(i *CloudFileInfo) { i.Size++ }},
		{"directory", func(i *CloudFileInfo) { i.ParentID++ }},
		{"filename", func(i *CloudFileInfo) { i.Name = "someone-else.png" }},
		{"trashed", func(i *CloudFileInfo) { i.Trashed = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			pan := &directTestPan{modify: tc.modify}
			s := newServer(c, opened(t, c), pan)
			m := directTestManifest(9203, pngData("verify"))
			directInit(t, s, m, 202)
			directControl(t, s, m, "/files/0/prepare", 200)
			directControl(t, s, m, "/files/0/complete", 503)
			directControl(t, s, m, "/complete", 409)
			var n int
			if err := s.S.DB.QueryRow(`SELECT count(*) FROM uploads WHERE hash IS NOT NULL`).Scan(&n); err != nil || n != 0 {
				t.Fatal("failed verification poisoned the dedup index", n, err)
			}
			pan.modify = nil
			directControl(t, s, m, "/files/0/complete", 200)
			directControl(t, s, m, "/complete", 202)
		})
	}
}

func TestV1AndV2UploadIDsCannotBeMixed(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), &directTestPan{})
	m := directTestManifest(9204, pngData("version"))
	directInit(t, s, m, 202)
	if w := receive(t, s, m.Metadata, pngData("version")); w.Code != 409 {
		t.Fatal("v1 multipart reused a v2 ID", w.Code, w.Body)
	}
	if w := sessionRequest(s, "POST", "/api/v1/uploads/"+m.Metadata.UploadID+"/complete", nil); w.Code != 409 {
		t.Fatal("v1 completion bypassed cloud verification", w.Code, w.Body)
	}
	m.Metadata.UploadID = metadata(9205).UploadID
	initializeSession(t, s, m.Metadata)
	directInit(t, s, m, 409)
	m.Metadata.UploadID = metadata(9204).UploadID
	b := []byte(`{"file_id":123}`)
	if w := sessionRequest(s, "POST", "/api/v2/uploads/"+m.Metadata.UploadID+"/files/0/complete", b); w.Code != 400 {
		t.Fatal("accepted a client-supplied cloud ID", w.Code, w.Body)
	}
}

func TestV2UpgradePreservesExistingV1Rows(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	f := pngData("old-spool")
	m := metadata(9206, f)
	s := newServer(c, st, &directTestPan{})
	if w := receive(t, s, m, f); w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	if _, err := st.DB.Exec(`ALTER TABLE uploads DROP COLUMN protocol`); err != nil {
		t.Fatal(err)
	}
	if err := st.DB.Close(); err != nil {
		t.Fatal(err)
	}
	st = opened(t, c)
	var protocol int
	if err := st.DB.QueryRow(`SELECT protocol FROM uploads WHERE id=?`, m.UploadID).Scan(&protocol); err != nil || protocol != 1 {
		t.Fatal("old rows were not migrated as v1", protocol, err)
	}
	if got, err := st.existing(m.UploadID, 0); err != nil || got.Path == "" || got.Size != int64(len(f)) {
		t.Fatal("migration lost the pending v1 image", got, err)
	}
	if used, reserved := st.Quota.snapshot(); used != int64(len(f)) || reserved != 0 {
		t.Fatal("migration changed spool accounting", used, reserved)
	}
}

func TestDirectPanUsesServerCredentialsOnlyForControl(t *testing.T) {
	var urls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer server-secret" || r.Header.Get("Platform") != "open_platform" {
			t.Error("missing server control credentials")
		}
		urls = append(urls, r.URL.Path)
		var data any
		switch r.URL.Path {
		case "/upload/v1/file/create":
			b, _ := io.ReadAll(r.Body)
			if bytes.Contains(b, []byte("image/png")) || !bytes.Contains(b, []byte(`"parentFileID":42`)) {
				t.Error("control call contains image bytes or wrong directory")
			}
			data = CloudTicket{PreuploadID: "ticket", SliceSize: 16 << 20}
		case "/upload/v1/file/get_upload_url":
			data = map[string]any{"presignedURL": "https://uploads.123624.com/part?signature=limited"}
		case "/upload/v1/file/upload_complete", "/upload/v1/file/upload_async_result":
			data = CloudCompletion{FileID: 12, Completed: true}
		case "/api/v1/file/detail":
			data = CloudFileInfo{FileID: 12, ParentID: 42, MD5: "md5", Size: 8}
		default:
			t.Error(r.URL.Path)
		}
		respond(w, 200, map[string]any{"code": 0, "data": data})
	}))
	defer server.Close()
	p := &PanClient{Client: server.Client(), Base: server.URL, ParentID: 42, token: "server-secret", expires: time.Now().Add(time.Hour)}
	ctx := context.Background()
	f := File{SHA256: strings.Repeat("a", 64), MD5: strings.Repeat("b", 32), Size: 8}
	if _, err := p.CreateDirect(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DirectURL(ctx, "ticket", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CompleteDirect(ctx, "ticket", false); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CompleteDirect(ctx, "ticket", true); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DirectInfo(ctx, 12); err != nil {
		t.Fatal(err)
	}
	if len(urls) != 5 {
		t.Fatal(urls)
	}
}
