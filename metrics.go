package main

import (
	"math"
	"sync"
	"time"
)

const transferWindow = 10
const transferAverageWindow = 60
const historyStepSeconds = 5
const historySlots = 720 // One hour; fixed memory independent of transfers or viewers.

type transferCounts struct {
	Images int64 `json:"images"`
	Bytes  int64 `json:"bytes"`
}

type transferBucket struct {
	second                           int64
	download, upload, direct, reused transferCounts
}

type transferRate struct {
	ImagesPerSecond    float64        `json:"images_per_second"`
	BytesPerSecond     float64        `json:"bytes_per_second"`
	ImagesPerSecond60s float64        `json:"images_per_second_60s"`
	BytesPerSecond60s  float64        `json:"bytes_per_second_60s"`
	Total              transferCounts `json:"total"`
}

type transferSnapshot struct {
	Since                time.Time    `json:"since"`
	SampledAt            time.Time    `json:"sampled_at"`
	WindowSeconds        int          `json:"window_seconds"`
	AverageWindowSeconds int          `json:"average_window_seconds"`
	Download             transferRate `json:"download"`
	Upload               transferRate `json:"upload"`
	DirectUpload         transferRate `json:"direct_upload"`
	Received             transferRate `json:"received"`
	Retransmit           transferRate `json:"retransmit"`
}

// Track successful file completions, not groups. A fixed ring keeps memory
// independent of file size, request count and process lifetime.
type transferMetrics struct {
	mu                               sync.Mutex
	now                              func() time.Time
	since                            time.Time
	buckets                          [transferAverageWindow]transferBucket
	historyBuckets                   [historySlots]transferBucket
	download, upload, direct, reused transferCounts
}

func newTransferMetrics(now func() time.Time) *transferMetrics {
	return &transferMetrics{now: now, since: now()}
}

func (m *transferMetrics) record(upload bool, size int64) {
	direction := 0
	if upload {
		direction = 1
	}
	m.recordDirection(direction, size)
}

func (m *transferMetrics) recordDirect(size int64) { m.recordDirection(2, size) }
func (m *transferMetrics) recordReuse(size int64)  { m.recordDirection(3, size) }

func (m *transferMetrics) recordDirection(direction int, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	second := m.now().Unix()
	b := &m.buckets[(second%transferAverageWindow+transferAverageWindow)%transferAverageWindow]
	if b.second != second {
		*b = transferBucket{second: second}
	}
	counts, total := &b.download, &m.download
	if direction == 1 {
		counts, total = &b.upload, &m.upload
	} else if direction == 2 {
		counts, total = &b.direct, &m.direct
	}
	counts.Images++
	counts.Bytes += size
	total.Images++
	total.Bytes += size
	if direction == 3 {
		b.reused.Images++
		b.reused.Bytes += size
		m.reused.Images++
		m.reused.Bytes += size
	}
	slot := second / historyStepSeconds
	h := &m.historyBuckets[(slot%historySlots+historySlots)%historySlots]
	if h.second != slot {
		*h = transferBucket{second: slot}
	}
	counts = &h.download
	if direction == 1 {
		counts = &h.upload
	} else if direction == 2 {
		counts = &h.direct
	}
	counts.Images++
	counts.Bytes += size
	if direction == 3 {
		h.reused.Images++
		h.reused.Bytes += size
	}
}

func (m *transferMetrics) snapshot() transferSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	second := now.Unix()
	var down, up, down60, up60 transferCounts
	var direct, direct60 transferCounts
	var reused, reused60 transferCounts
	for _, b := range m.buckets {
		age := second - b.second
		if age < 0 || age >= transferAverageWindow {
			continue
		}
		down60.Images += b.download.Images
		down60.Bytes += b.download.Bytes
		up60.Images += b.upload.Images
		up60.Bytes += b.upload.Bytes
		direct60.Images += b.direct.Images
		direct60.Bytes += b.direct.Bytes
		reused60.Images += b.reused.Images
		reused60.Bytes += b.reused.Bytes
		if age < transferWindow {
			down.Images += b.download.Images
			down.Bytes += b.download.Bytes
			up.Images += b.upload.Images
			up.Bytes += b.upload.Bytes
			direct.Images += b.direct.Images
			direct.Bytes += b.direct.Bytes
			reused.Images += b.reused.Images
			reused.Bytes += b.reused.Bytes
		}
	}
	seconds := max(1, min(float64(transferWindow), now.Sub(m.since).Seconds()))
	seconds60 := max(1, min(float64(transferAverageWindow), now.Sub(m.since).Seconds()))
	rate := func(short, average, total transferCounts) transferRate {
		return transferRate{
			ImagesPerSecond:    rounded(float64(short.Images) / seconds),
			BytesPerSecond:     rounded(float64(short.Bytes) / seconds),
			ImagesPerSecond60s: rounded(float64(average.Images) / seconds60),
			BytesPerSecond60s:  rounded(float64(average.Bytes) / seconds60),
			Total:              total,
		}
	}
	return transferSnapshot{
		Since: m.since.UTC(), SampledAt: now.UTC(), WindowSeconds: transferWindow, AverageWindowSeconds: transferAverageWindow,
		Download: rate(down, down60, m.download), Upload: rate(up, up60, m.upload),
		DirectUpload: rate(direct, direct60, m.direct),
		Received:     rate(subtractCounts(down, reused), subtractCounts(down60, reused60), subtractCounts(m.download, m.reused)),
		Retransmit:   rate(reused, reused60, m.reused),
	}
}

func subtractCounts(all, reused transferCounts) transferCounts {
	return transferCounts{Images: all.Images - reused.Images, Bytes: all.Bytes - reused.Bytes}
}

type transferPoint struct {
	Time       int64      `json:"time"`
	ImageRates [4]float64 `json:"images"` // New receive, retransmit, A upload, verified direct.
	ByteRates  [4]float64 `json:"bytes"`
}

func (m *transferMetrics) history(after int64) []transferPoint {
	m.mu.Lock()
	now, since, buckets := m.now(), m.since, m.historyBuckets
	m.mu.Unlock()
	last := now.Unix() / historyStepSeconds
	first := max(since.Unix()/historyStepSeconds, last-historySlots+1)
	points := make([]transferPoint, 0, historySlots)
	for slot := first; slot <= last; slot++ {
		at := slot * historyStepSeconds
		if at < after {
			continue
		}
		p := transferPoint{Time: at}
		b := buckets[(slot%historySlots+historySlots)%historySlots]
		if b.second == slot {
			start := max(float64(at), float64(since.UnixNano())/1e9)
			seconds := max(1, min(float64(at+historyStepSeconds), float64(now.UnixNano())/1e9)-start)
			for i, n := range [4]transferCounts{subtractCounts(b.download, b.reused), b.reused, b.upload, b.direct} {
				p.ImageRates[i] = rounded(float64(n.Images) / seconds)
				p.ByteRates[i] = rounded(float64(n.Bytes) / seconds)
			}
		}
		points = append(points, p)
	}
	return points
}

func rounded(v float64) float64 { return math.Round(v*1000) / 1000 }
