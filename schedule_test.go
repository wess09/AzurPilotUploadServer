package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func atNextBoundary(t *testing.T, st *Store) *atomic.Int64 {
	t.Helper()
	var next int64
	if err := st.DB.QueryRow(`SELECT next_at FROM dispatch_state WHERE id=1`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	clock := new(atomic.Int64)
	clock.Store(next)
	st.Now = func() time.Time { return time.Unix(clock.Load(), 0) }
	return clock
}

func TestWindowSplitsOverflowAndSurvivesRestart(t *testing.T) {
	c := testConfig(t)
	st := opened(t, c)
	s := newServer(c, st, nil)
	for i := range 61 {
		f := pngData(fmt.Sprintf("window-%d", i))
		if w := receive(t, s, metadata(i, f), f); w.Code != 202 {
			t.Fatal(w.Code, w.Body)
		}
	}
	_, _ = st.DB.Exec(`UPDATE files SET state='uploaded',remote_id=1`)
	clock := atNextBoundary(t, st)
	clock.Add(-1)
	if b, e := st.makeBatch(30); e != nil || b != nil {
		t.Fatal("quantity triggered before interval", b, e)
	}
	clock.Add(1)
	first, e := st.makeBatch(30)
	if e != nil || first == nil || len(first.Items) != 30 {
		t.Fatal(first, e)
	}
	_, _ = st.DB.Exec(`UPDATE batches SET acked=1 WHERE id=?`, first.ID)
	if e = st.DB.Close(); e != nil {
		t.Fatal(e)
	}
	st = opened(t, c)
	for _, want := range []int{30, 1} {
		b, e := st.makeBatch(30)
		if e != nil || b == nil || len(b.Items) != want || b.WindowEnd != first.WindowEnd {
			t.Fatal("lost scheduled overflow", b, e)
		}
		_, _ = st.DB.Exec(`UPDATE batches SET acked=1 WHERE id=?`, b.ID)
	}
	if b, e := st.makeBatch(30); e != nil || b != nil {
		t.Fatal("empty window or duplicate dispatch", b, e)
	}
}

func TestReverseWorkerAuthAndReplay(t *testing.T) {
	c := testConfig(t)
	c.WorkerMode, c.WorkerToken = "reverse", "reverse-secret"
	s := newServer(c, opened(t, c), nil)
	f := pngData("reverse")
	if w := receive(t, s, metadata(801, f), f); w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	_, _ = s.S.DB.Exec(`UPDATE files SET state='uploaded',remote_id=1`)
	atNextBoundary(t, s.S)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.workerContext = ctx
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	url := "ws" + strings.TrimPrefix(host.URL, "http") + "/internal/v1/worker/connect"
	if conn, r, e := websocket.Dial(ctx, url, nil); e == nil || r.StatusCode != 401 {
		if conn != nil {
			conn.CloseNow()
		}
		t.Fatal("accepted unauthenticated worker", e)
	}
	options := &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer reverse-secret"}}}
	conn, _, e := websocket.Dial(ctx, url, options)
	if e != nil {
		t.Fatal(e)
	}
	var first Batch
	if e = wsjson.Read(ctx, conn, &first); e != nil {
		t.Fatal(e)
	}
	if len(first.Items) != 1 {
		t.Fatal("single sample didn't dispatch", first)
	}
	if extra, r, e := websocket.Dial(ctx, url, options); e == nil || r.StatusCode != 409 {
		if extra != nil {
			extra.CloseNow()
		}
		t.Fatal("second worker accepted", e)
	}
	conn.CloseNow()
	eventually(t, func() bool { return len(s.workerSlot) == 0 })
	conn, _, e = websocket.Dial(ctx, url, options)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.CloseNow()
	var replay Batch
	if e = wsjson.Read(ctx, conn, &replay); e != nil || replay.ID != first.ID {
		t.Fatal("reconnect lost batch identity", e, replay)
	}
	if e = wsjson.Write(ctx, conn, map[string]string{"type": "batch_accepted", "batch_id": replay.ID}); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool {
		var ack int
		_ = s.S.DB.QueryRow(`SELECT acked FROM batches WHERE id=?`, replay.ID).Scan(&ack)
		return ack == 1
	})
}
