package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMonitorSeparatesRetransfersAndBoundsHistory(t *testing.T) {
	now := time.Unix(1800000000, 0)
	m := newTransferMetrics(func() time.Time { return now })
	m.record(false, 100)
	m.recordReuse(200)
	m.record(true, 300)
	m.recordDirect(400)
	r := m.snapshot()
	if r.Download.Total.Images != 2 || r.Received.Total.Images != 1 || r.Retransmit.Total.Bytes != 200 {
		t.Fatal("retransfers were presented as newly received images", r)
	}
	first := m.history(0)
	if len(first) != 1 || first[0].ImageRates != [4]float64{1, 1, 1, 1} {
		t.Fatal("wrong initial history", first)
	}
	now = now.Add(10 * time.Second)
	m.recordDirect(500)
	if h := m.history(first[0].Time + 5); len(h) != 2 || h[0].ImageRates != [4]float64{} || h[1].ByteRates[3] != 500 {
		t.Fatal("incremental history lost empty buckets or current activity", h)
	}
	now = now.Add(2 * time.Hour)
	h := m.history(0)
	if len(h) != historySlots || h[0].Time != now.Unix()-(historySlots-1)*historyStepSeconds {
		t.Fatal("history was not bounded to one hour", len(h))
	}
	for _, p := range h {
		if p.ImageRates != [4]float64{} {
			t.Fatal("expired ring slot returned as new traffic", p)
		}
	}
}

func TestMonitorIsReadOnlyAndIndependentOfUploadSlots(t *testing.T) {
	c := testConfig(t)
	s := newServer(c, opened(t, c), nil)
	for range cap(s.receivers) {
		s.receivers <- struct{}{}
	}
	for _, path := range []string{"/dashboard/", "/dashboard/app.js", "/dashboard/styles.css", "/api/v1/monitor"} {
		w := sessionRequest(s, "GET", path, nil)
		if w.Code != 200 || w.Body.Len() == 0 {
			t.Fatal("dashboard blocked by occupied upload slots", path, w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), "PAN_CLIENT_SECRET") || strings.Contains(w.Body.String(), "submitter") {
			t.Fatal("monitor leaked private configuration or metadata", path)
		}
	}
	w := sessionRequest(s, "GET", "/api/v1/monitor", nil)
	var snapshot struct {
		Queue       monitorQueue `json:"queue"`
		SpoolLimit  int64        `json:"spool_limit"`
		ReceiveUsed int          `json:"receive_active"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil || !snapshot.Queue.Available || snapshot.SpoolLimit != c.SpoolBytes || snapshot.ReceiveUsed != 30 {
		t.Fatal("incomplete monitor snapshot", w.Body, err)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("monitor snapshot was cacheable")
	}
	for _, after := range []string{"-1", "bad", "9999999999999999999999"} {
		if w := sessionRequest(s, "GET", "/api/v1/monitor?after="+after, nil); w.Code != 400 {
			t.Fatal("invalid cursor accepted", after, w.Code)
		}
	}
	if w := sessionRequest(s, "POST", "/api/v1/monitor", nil); w.Code != 405 {
		t.Fatal("monitor allowed a mutation request", w.Code)
	}
}
