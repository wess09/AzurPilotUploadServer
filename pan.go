package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PanClient struct {
	Client                 *http.Client
	Base, ClientID, Secret string
	ParentID               int64
	mu                     sync.Mutex
	token                  string
	expires                time.Time
	onThrottle             func(time.Duration) time.Duration
	backoff                func() time.Duration
	domainMu               sync.Mutex
	domain                 string
	domainUntil            time.Time
}

func (p *PanClient) access(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && time.Until(p.expires) > time.Minute {
		return p.token, nil
	}
	b, _ := json.Marshal(map[string]string{"clientID": p.ClientID, "clientSecret": p.Secret})
	req, err := http.NewRequestWithContext(ctx, "POST", p.Base+"/api/v1/access_token", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Platform", "open_platform")
	var out struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Token   string `json:"accessToken"`
			Expires string `json:"expiredAt"`
		} `json:"data"`
	}
	if err = p.do(req, &out); err != nil {
		return "", err
	}
	if out.Code == 429 {
		return "", p.rateLimited(0)
	}
	if out.Code != 0 || out.Data.Token == "" {
		return "", fmt.Errorf("123 token: %s", out.Message)
	}
	p.token = out.Data.Token
	p.expires, err = time.Parse(time.RFC3339, out.Data.Expires)
	if err != nil {
		p.expires = time.Now().Add(30 * time.Minute)
	}
	return p.token, nil
}
func (p *PanClient) do(req *http.Request, out any) error {
	if p.backoff != nil {
		if delay := p.backoff(); delay > 0 {
			return &cloudThrottleError{RetryAfter: delay}
		}
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return p.rateLimited(retryAfter(resp.Header.Get("Retry-After"), time.Now()))
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 401 && !strings.HasSuffix(req.URL.Path, "/access_token") {
			p.mu.Lock()
			p.token = ""
			p.mu.Unlock()
		}
		return fmt.Errorf("123 HTTP %d", resp.StatusCode)
	}
	// Bound a stalled JSON response after the image has already been sent.
	timer := time.AfterFunc(30*time.Second, func() { _ = resp.Body.Close() })
	defer timer.Stop()
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

func (p *PanClient) rateLimited(delay time.Duration) error {
	if p.onThrottle != nil {
		delay = p.onThrottle(delay)
	}
	if delay <= 0 {
		delay = 10 * time.Second
	}
	return &cloudThrottleError{RetryAfter: delay}
}

func (p *PanClient) uploadDomain(ctx context.Context, token string) (string, error) {
	p.domainMu.Lock()
	defer p.domainMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p.domain != "" && time.Now().Before(p.domainUntil) {
		return p.domain, nil
	}
	req, err := http.NewRequestWithContext(ctx, "GET", p.Base+"/upload/v2/file/domain", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Platform", "open_platform")
	var domain struct {
		Code    int      `json:"code"`
		Message string   `json:"message"`
		Data    []string `json:"data"`
	}
	if err = p.do(req, &domain); err != nil {
		return "", err
	}
	if domain.Code == 429 {
		return "", p.rateLimited(0)
	}
	if domain.Code == 401 {
		p.mu.Lock()
		p.token = ""
		p.mu.Unlock()
	}
	if domain.Code != 0 || len(domain.Data) == 0 || !strings.HasPrefix(domain.Data[0], "https://") {
		return "", fmt.Errorf("123 upload domain unavailable: %d %s", domain.Code, domain.Message)
	}
	p.domain, p.domainUntil = domain.Data[0], time.Now().Add(time.Minute)
	return p.domain, nil
}

func (p *PanClient) Upload(ctx context.Context, f File) (int64, error) {
	token, err := p.access(ctx)
	if err != nil {
		return 0, err
	}
	domain, err := p.uploadDomain(ctx, token)
	if err != nil {
		return 0, err
	}
	file, err := os.Open(f.Path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	// Only multipart headers live in memory. The image is read from disk by the
	// HTTP transport with an exact Content-Length, including multipart overhead.
	var headers bytes.Buffer
	mw := multipart.NewWriter(&headers)
	for _, field := range [][2]string{{"parentFileID", strconv.FormatInt(p.ParentID, 10)}, {"filename", f.SHA256 + ".png"}, {"etag", f.MD5}, {"size", strconv.FormatInt(f.Size, 10)}, {"duplicate", "1"}} {
		if err = mw.WriteField(field[0], field[1]); err != nil {
			return 0, err
		}
	}
	if _, err = mw.CreateFormFile("file", f.SHA256+".png"); err != nil {
		return 0, err
	}
	headN := headers.Len()
	if err = mw.Close(); err != nil {
		return 0, err
	}
	raw := headers.Bytes()
	body := io.MultiReader(bytes.NewReader(raw[:headN]), file, bytes.NewReader(raw[headN:]))
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(domain, "/")+"/upload/v2/file/single/create", body)
	if err != nil {
		return 0, err
	}
	req.ContentLength = int64(len(raw)) + f.Size
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Platform", "open_platform")
	var out struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Completed bool  `json:"completed"`
			ID        int64 `json:"fileID"`
		} `json:"data"`
	}
	if err = p.do(req, &out); err != nil {
		return 0, err
	}
	if out.Code == 429 {
		return 0, p.rateLimited(0)
	}
	if out.Code == 401 {
		p.mu.Lock()
		p.token = ""
		p.mu.Unlock()
	}
	if out.Code != 0 || !out.Data.Completed || out.Data.ID <= 0 {
		return 0, fmt.Errorf("123 upload not completed: %d %s", out.Code, out.Message)
	}
	return out.Data.ID, nil
}
