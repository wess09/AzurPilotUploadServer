package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
)

// Opt-in contract check against a disposable B service with recognition stopped.
// This exercises real WebSocket transport and B's durable acceptance, while
// cloud uploads use the same disk-verifying fake as the resource tests.
func TestRealWorkerWebSocketContract(t *testing.T) {
	url := os.Getenv("WORKER_TEST_WS_URL")
	if url == "" {
		t.Skip("WORKER_TEST_WS_URL is not configured")
	}
	c := testConfig(t)
	c.WorkerURL, c.WorkerToken = url, os.Getenv("WORKER_TEST_TOKEN")
	s := newServer(c, opened(t, c), &testPan{})
	ctx, cancel := context.WithCancel(context.Background())
	var work sync.WaitGroup
	for range 2 {
		work.Go(func() { s.uploadLoop(ctx) })
	}
	work.Go(func() { _ = s.notify(ctx) })
	defer func() { cancel(); work.Wait() }()
	for i := range 30 {
		data := pngData(fmt.Sprintf("contract-sample-%d", i))
		if r := receive(t, s, metadata(i, data), data); r.Code != 202 {
			t.Fatal(r.Code, r.Body.String())
		}
	}
	eventually(t, func() bool {
		var count int
		_ = s.S.DB.QueryRow(`SELECT count(*) FROM batches WHERE acked=1`).Scan(&count)
		return count == 1
	})
}
