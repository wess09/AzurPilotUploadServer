package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type CloudTicket struct {
	PreuploadID string `json:"preuploadID"`
	SliceSize   int64  `json:"sliceSize"`
	FileID      int64  `json:"fileID"`
	Reuse       bool   `json:"reuse"`
}

type CloudCompletion struct {
	FileID    int64 `json:"fileID"`
	Async     bool  `json:"async"`
	Completed bool  `json:"completed"`
}

type CloudFileInfo struct {
	FileID   int64  `json:"fileID"`
	ParentID int64  `json:"parentFileID"`
	Name     string `json:"filename"`
	MD5      string `json:"etag"`
	Size     int64  `json:"size"`
	Type     int    `json:"type"`
	Status   int    `json:"status"`
	Trashed  int    `json:"trashed"`
}

type DirectCloud interface {
	CreateDirect(context.Context, File) (CloudTicket, error)
	DirectURL(context.Context, string, int) (string, error)
	CompleteDirect(context.Context, string, bool) (CloudCompletion, error)
	DirectInfo(context.Context, int64) (CloudFileInfo, error)
}

func (p *PanClient) directAPI(ctx context.Context, path string, body any, result any) error {
	token, err := p.access(ctx)
	if err != nil {
		return err
	}
	method := http.MethodGet
	var b []byte
	if body != nil {
		method = http.MethodPost
		b, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, p.Base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Platform", "open_platform")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	var out struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err = p.do(req, &out); err != nil {
		return err
	}
	if out.Code == 401 {
		p.mu.Lock()
		p.token = ""
		p.mu.Unlock()
	}
	if out.Code != 0 {
		return fmt.Errorf("123 direct API %d: %s", out.Code, out.Message)
	}
	return json.Unmarshal(out.Data, result)
}

func (p *PanClient) CreateDirect(ctx context.Context, f File) (CloudTicket, error) {
	var out CloudTicket
	err := p.directAPI(ctx, "/upload/v1/file/create", map[string]any{
		"parentFileID": p.ParentID, "filename": f.SHA256 + ".png", "etag": f.MD5, "size": f.Size, "duplicate": 1,
	}, &out)
	return out, err
}

func validDirectURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}

func (p *PanClient) DirectURL(ctx context.Context, id string, part int) (string, error) {
	var out struct {
		URL string `json:"presignedURL"`
	}
	err := p.directAPI(ctx, "/upload/v1/file/get_upload_url", map[string]any{"preuploadID": id, "sliceNo": part}, &out)
	if err == nil && !validDirectURL(out.URL) {
		err = fmt.Errorf("123 returned an invalid presigned URL")
	}
	return out.URL, err
}

func (p *PanClient) CompleteDirect(ctx context.Context, id string, poll bool) (CloudCompletion, error) {
	path := "/upload/v1/file/upload_complete"
	if poll {
		path = "/upload/v1/file/upload_async_result"
	}
	var out CloudCompletion
	err := p.directAPI(ctx, path, map[string]any{"preuploadID": id}, &out)
	return out, err
}

func (p *PanClient) DirectInfo(ctx context.Context, id int64) (CloudFileInfo, error) {
	var out CloudFileInfo
	err := p.directAPI(ctx, fmt.Sprintf("/api/v1/file/detail?fileID=%d", id), nil, &out)
	return out, err
}
