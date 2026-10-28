package main

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"
)

func (s *Server) cloudStatus() map[string]any {
	p, ok := s.Pan.(*PanClient)
	if !ok {
		return nil
	}
	p.lanesMu.Lock()
	defer p.lanesMu.Unlock()
	out := make(map[string]any, len(p.lanes))
	for path, lane := range p.lanes {
		lane.mu.Lock()
		out[path] = map[string]any{"requests_per_second": math.Round(float64(time.Second)/float64(lane.gap)*100) / 100,
			"backoff_seconds": math.Ceil(max(0, time.Until(lane.until).Seconds())), "rate_limited": lane.limited}
		lane.mu.Unlock()
	}
	return out
}

// Cloud quotas belong to an endpoint, not the whole account pipeline. Creation
// cannot stop confirmation of images clients have already sent to storage.
type cloudLane struct {
	mu                     sync.Mutex
	gap, minimum           time.Duration
	next, until, lastRaise time.Time
	generation             uint64
	limited, successes     int
}

func (p *PanClient) lane(path string) *cloudLane {
	path = strings.Split(path, "?")[0]
	p.lanesMu.Lock()
	defer p.lanesMu.Unlock()
	if p.lanes == nil {
		p.lanes = make(map[string]*cloudLane)
	}
	if lane := p.lanes[path]; lane != nil {
		return lane
	}
	gap := 20 * time.Millisecond
	minimum := gap
	if path == "/upload/v1/file/create" {
		gap = 250 * time.Millisecond
	}
	if path == "/api/v1/access_token" {
		gap = time.Second
		minimum = gap
	}
	lane := &cloudLane{gap: gap, minimum: minimum, lastRaise: time.Now()}
	p.lanes[path] = lane
	return lane
}

func (l *cloudLane) wait(ctx context.Context, path string) error {
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		l.mu.Lock()
		now := time.Now()
		if now.Before(l.until) {
			delay := l.until.Sub(now)
			l.mu.Unlock()
			return &cloudThrottleError{RetryAfter: delay, Endpoint: path}
		}
		slot := maxTime(now, maxTime(l.next, l.until))
		generation := l.generation
		if deadline, ok := ctx.Deadline(); ok && slot.Add(50*time.Millisecond).After(deadline) {
			delay := max(time.Second, slot.Sub(now))
			l.mu.Unlock()
			return &cloudThrottleError{RetryAfter: delay, Endpoint: path}
		}
		l.next = slot.Add(l.gap)
		l.mu.Unlock()
		timer := time.NewTimer(max(0, time.Until(slot)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		l.mu.Lock()
		valid := generation == l.generation && !time.Now().Before(l.until)
		l.mu.Unlock()
		if valid {
			return nil
		}
	}
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (p *PanClient) limitEndpoint(path string, delay time.Duration) error {
	lane := p.lane(path)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	now := time.Now()
	if !now.Before(lane.until) {
		lane.gap = min(time.Second, lane.gap*3/2)
		lane.generation++
		lane.successes = 0
		lane.limited++
		lane.lastRaise = now
		if delay <= 0 {
			delay = 2 * time.Second
		}
		lane.until = now.Add(delay)
		lane.next = lane.until
	} else if delay > 0 && now.Add(delay).After(lane.until) {
		lane.until = now.Add(delay)
	}
	return &cloudThrottleError{RetryAfter: time.Until(lane.until), Endpoint: path}
}
func (p *PanClient) endpointSuccess(path string) {
	lane := p.lane(path)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.successes++
	if lane.successes >= 50 && time.Since(lane.lastRaise) >= 10*time.Second {
		lane.gap = max(lane.minimum, lane.gap*9/10)
		lane.lastRaise = time.Now()
		lane.successes = 0
	}
}
