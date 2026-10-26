package main

import (
	"errors"
	"sync"
)

var errQuota = errors.New("spool quota or free space exhausted; retry later")

// One reservation per file guarantees an in-progress file can finish even when
// other requests fill the spool. Uploaders drain complete files independently.
type Quota struct {
	mu                             sync.Mutex
	Used, Reserved, Limit, MinFree int64
	Free                           func() (int64, error)
}

func (q *Quota) reserve(n int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if n < 0 || n > q.Limit-q.Used-q.Reserved {
		return errQuota
	}
	if q.Free != nil {
		free, err := q.Free()
		if err != nil {
			return err
		}
		if free-q.Reserved-n < q.MinFree {
			return errQuota
		}
	}
	q.Reserved += n
	return nil
}
func (q *Quota) finish(reserved, actual int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.Reserved -= reserved
	q.Used += actual
}
func (q *Quota) removed(n int64) { q.mu.Lock(); defer q.mu.Unlock(); q.Used -= n }
func (q *Quota) snapshot() (int64, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.Used, q.Reserved
}
