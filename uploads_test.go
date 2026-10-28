package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdaptiveUploadsBoundConcurrencyAndRespectCooldown(t *testing.T) {
	now := time.Now()
	c := newUploadControl(10, 500)
	c.now = func() time.Time { return now }
	c.lastRaise = now
	for range 10 {
		if !c.begin() {
			t.Fatal("initial concurrency was below ten")
		}
	}
	if c.begin() {
		t.Fatal("initial concurrency exceeded ten")
	}
	now = now.Add(11 * time.Second)
	for range 10 {
		c.finish(true)
	}
	if state := c.snapshot(); state.Limit <= 10 || state.Limit > 500 || state.Active != 0 {
		t.Fatal("successful uploads did not grow within bounds", state)
	}
	before := c.snapshot().Limit
	if wait := c.throttle(7 * time.Second); wait != 7*time.Second || c.begin() {
		t.Fatal("429 cooldown ignored", wait)
	}
	if state := c.snapshot(); state.Limit > before || state.Limit < 10 || state.RateLimited != 1 {
		t.Fatal("429 did not decrease concurrency within bounds", state)
	}
	now = now.Add(6 * time.Second)
	if c.begin() {
		t.Fatal("work started before Retry-After expired")
	}
	now = now.Add(time.Second)
	if !c.begin() {
		t.Fatal("work did not resume after cooldown")
	}
	c.finish(false)
	if wait := c.throttle(0); wait != 20*time.Second {
		t.Fatal("repeated 429 did not back off exponentially", wait)
	}
}

func TestCloud429IsTypedAndSuppressesRequestsDuringBackoff(t *testing.T) {
	calls := 0
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer endpoint.Close()
	c := newUploadControl(10, 500)
	p := &PanClient{Client: endpoint.Client(), onThrottle: c.throttle, backoff: c.remaining}
	for range 2 {
		req, _ := http.NewRequest("GET", endpoint.URL, nil)
		var response any
		err := p.do(req, &response)
		var limited *cloudThrottleError
		if !errors.As(err, &limited) || limited.RetryAfter <= 0 || limited.RetryAfter > 7*time.Second {
			t.Fatal("429 Retry-After was not preserved", err)
		}
	}
	if calls != 1 {
		t.Fatal("request reached cloud during cooldown", calls)
	}
}
