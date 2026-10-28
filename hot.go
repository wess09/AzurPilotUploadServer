package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const hotTTL = 5 * time.Minute

type hotEntry struct {
	id    int64
	until time.Time
}
type hotCache struct {
	client *redis.Client
	local  *lruCache[contentKey, hotEntry]
}

func newHotCache(url string) (*hotCache, error) {
	h := &hotCache{}
	if url == "" {
		h.local = newLRU[contentKey, hotEntry](8192)
		return h, nil
	}
	opts, e := redis.ParseURL(url)
	if e != nil {
		return nil, fmt.Errorf("invalid Redis configuration")
	}
	opts.PoolSize = 8
	opts.MinIdleConns = 1
	opts.DialTimeout = time.Second
	opts.ReadTimeout = 500 * time.Millisecond
	opts.WriteTimeout = 500 * time.Millisecond
	opts.MaxRetries = 0
	h.client = redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e = h.client.Ping(ctx).Err(); e != nil {
		h.client.Close()
		return nil, fmt.Errorf("Redis unavailable")
	}
	return h, nil
}
func (h *hotCache) get(k contentKey) (int64, bool) {
	if h == nil {
		return 0, false
	}
	if h.client == nil {
		entry, ok := h.local.get(k)
		return entry.id, ok && time.Now().Before(entry.until)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	id, e := h.client.Get(ctx, hotKey(k)).Int64()
	return id, e == nil && id > 0
}
func (h *hotCache) put(k contentKey, id int64) {
	if h == nil {
		return
	}
	if h.client == nil {
		h.local.put(k, hotEntry{id, time.Now().Add(hotTTL)})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = h.client.Set(ctx, hotKey(k), id, hotTTL).Err()
}
func hotKey(k contentKey) string {
	return "uploader:hot:" + k.sha + ":" + k.md5 + ":" + strconv.FormatInt(k.size, 10)
}
