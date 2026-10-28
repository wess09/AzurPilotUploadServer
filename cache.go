package main

import (
	"container/list"
	"sync"
)

// Bounded O(1) caches hold only committed metadata and cloud IDs, never images.
type cacheEntry[K comparable, V any] struct {
	key   K
	value V
}

type lruCache[K comparable, V any] struct {
	mu    sync.Mutex
	limit int
	items map[K]*list.Element
	order *list.List
}

func newLRU[K comparable, V any](limit int) *lruCache[K, V] {
	return &lruCache[K, V]{limit: limit, items: make(map[K]*list.Element), order: list.New()}
}

func (c *lruCache[K, V]) get(key K) (value V, ok bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if item := c.items[key]; item != nil {
		c.order.MoveToFront(item)
		return item.Value.(cacheEntry[K, V]).value, true
	}
	return
}

func (c *lruCache[K, V]) put(key K, value V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if item := c.items[key]; item != nil {
		item.Value = cacheEntry[K, V]{key, value}
		c.order.MoveToFront(item)
		return
	}
	c.items[key] = c.order.PushFront(cacheEntry[K, V]{key, value})
	if len(c.items) > c.limit {
		last := c.order.Back()
		delete(c.items, last.Value.(cacheEntry[K, V]).key)
		c.order.Remove(last)
	}
}

type groupMetadata struct {
	raw      string
	metadata Metadata
	protocol int
}

type contentKey struct {
	sha, md5 string
	size     int64
}

func (s *Store) lockDirect(id string) bool {
	s.directMu.Lock()
	defer s.directMu.Unlock()
	if s.directBusy == nil {
		s.directBusy = make(map[string]bool)
	}
	if s.directBusy[id] {
		return false
	}
	s.directBusy[id] = true
	return true
}

func (s *Store) unlockDirect(id string) bool {
	s.directMu.Lock()
	defer s.directMu.Unlock()
	busy := s.directBusy[id]
	delete(s.directBusy, id)
	return busy
}
