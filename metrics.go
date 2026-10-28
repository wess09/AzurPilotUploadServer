package main

import (
	"math"
	"sync"
	"time"
)

const transferWindow = 10
const transferAverageWindow = 60

type transferCounts struct {
	Images int64 `json:"images"`
	Bytes  int64 `json:"bytes"`
}

type transferBucket struct {
	second           int64
	download, upload transferCounts
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
}

// Track successful file completions, not groups. A fixed ring keeps memory
// independent of file size, request count and process lifetime.
type transferMetrics struct {
	mu               sync.Mutex
	now              func() time.Time
	since            time.Time
	buckets          [transferAverageWindow]transferBucket
	download, upload transferCounts
}

func newTransferMetrics(now func() time.Time) *transferMetrics {
	return &transferMetrics{now: now, since: now()}
}

func (m *transferMetrics) record(upload bool, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	second := m.now().Unix()
	b := &m.buckets[(second%transferAverageWindow+transferAverageWindow)%transferAverageWindow]
	if b.second != second {
		*b = transferBucket{second: second}
	}
	counts, total := &b.download, &m.download
	if upload {
		counts, total = &b.upload, &m.upload
	}
	counts.Images++
	counts.Bytes += size
	total.Images++
	total.Bytes += size
}

func (m *transferMetrics) snapshot() transferSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	second := now.Unix()
	var down, up, down60, up60 transferCounts
	for _, b := range m.buckets {
		age := second - b.second
		if age < 0 || age >= transferAverageWindow {
			continue
		}
		down60.Images += b.download.Images
		down60.Bytes += b.download.Bytes
		up60.Images += b.upload.Images
		up60.Bytes += b.upload.Bytes
		if age < transferWindow {
			down.Images += b.download.Images
			down.Bytes += b.download.Bytes
			up.Images += b.upload.Images
			up.Bytes += b.upload.Bytes
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
	}
}

func rounded(v float64) float64 { return math.Round(v*1000) / 1000 }
