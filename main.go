package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func (s *Server) notify(ctx context.Context) error {
	select {
	case s.workerSlot <- struct{}{}:
		defer func() { <-s.workerSlot }()
	default:
		return errors.New("worker already connected")
	}
	header := http.Header{"Authorization": []string{"Bearer " + s.C.WorkerToken}}
	if s.C.AccessID != "" {
		header.Set("CF-Access-Client-Id", s.C.AccessID)
		header.Set("CF-Access-Client-Secret", s.C.AccessSecret)
	}
	c, _, err := websocket.Dial(ctx, s.C.WorkerURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return err
	}
	defer c.CloseNow()
	slog.Info("worker connected", "mode", "forward")
	return s.sendBatches(ctx, c)
}

func (s *Server) acceptWorker(w http.ResponseWriter, r *http.Request) {
	if s.C.WorkerMode != "reverse" {
		http.NotFound(w, r)
		return
	}
	if s.C.WorkerToken == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.C.WorkerToken)) != 1 {
		reject(w, 401, errors.New("invalid worker credential"))
		return
	}
	select {
	case s.workerSlot <- struct{}{}:
		defer func() { <-s.workerSlot }()
	default:
		reject(w, 409, errors.New("worker already connected"))
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.workerContext, cancel)
	defer stop()
	slog.Info("worker connected", "mode", "reverse")
	err = s.sendBatches(ctx, c)
	if err != nil && ctx.Err() == nil {
		slog.Warn("worker disconnected", "error", err)
	}
}

func (s *Server) sendBatches(ctx context.Context, c *websocket.Conn) error {
	c.SetReadLimit(4096)
	connectionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	acks := make(chan string, 1)
	readErr := make(chan error, 1)
	readDone := make(chan struct{})
	defer func() { cancel(); c.CloseNow(); <-readDone }()
	go func() {
		defer close(readDone)
		defer cancel()
		for {
			var ack struct {
				Type string `json:"type"`
				ID   string `json:"batch_id"`
			}
			e := wsjson.Read(connectionCtx, c, &ack)
			if e != nil {
				readErr <- e
				return
			}
			if ack.Type == "batch_accepted" {
				select {
				case acks <- ack.ID:
				case <-connectionCtx.Done():
					return
				}
			}
		}
	}()
	t := time.NewTicker(s.C.Poll)
	defer t.Stop()
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		b, e := s.S.makeBatch(s.C.BatchSize)
		if e != nil {
			return e
		}
		if b != nil {
			started := time.Now()
			writeCtx, stop := context.WithTimeout(connectionCtx, 30*time.Second)
			e = wsjson.Write(writeCtx, c, b)
			stop()
			if e != nil {
				return e
			}
			slog.Info("batch sent", "batch_id", b.ID, "groups", len(b.Items), "window_end", b.WindowEnd)
			ackTimer := time.NewTimer(30 * time.Second)
			select {
			case id := <-acks:
				ackTimer.Stop()
				if id != b.ID {
					return errors.New("unexpected batch acknowledgement")
				}
				if _, e = s.S.DB.Exec(`UPDATE batches SET acked=1 WHERE id=?`, id); e != nil {
					return e
				}
				slog.Info("batch accepted", "batch_id", id, "groups", len(b.Items),
					"duration_ms", time.Since(started).Milliseconds())
			case e := <-readErr:
				ackTimer.Stop()
				return e
			case <-ackTimer.C:
				return errors.New("worker acknowledgement timeout")
			case <-connectionCtx.Done():
				ackTimer.Stop()
				return connectionCtx.Err()
			}
			continue
		}
		select {
		case <-connectionCtx.Done():
			return connectionCtx.Err()
		case <-t.C:
		case <-ping.C:
			pctx, stop := context.WithTimeout(connectionCtx, 5*time.Second)
			e = c.Ping(pctx)
			stop()
			if e != nil {
				return e
			}
		}
	}
}
func main() {
	if err := run(); err != nil {
		slog.Error("uploader stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, err := loadConfig()
	if err != nil {
		return err
	}
	st, err := openStore(c)
	if err != nil {
		return err
	}
	defer st.DB.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cloudTransport := http.DefaultTransport.(*http.Transport).Clone()
	// Bound the cloud's response wait separately from sending the image. A
	// completed request body must not occupy either upload worker for minutes.
	// The overall deadline still allows large files to cross a slow uplink.
	cloudTransport.ResponseHeaderTimeout = 20 * time.Second
	s := newServer(c, st, &PanClient{Client: &http.Client{Timeout: 5 * time.Minute, Transport: cloudTransport}, Base: c.PanBase, ClientID: c.PanID, Secret: c.PanSecret, ParentID: c.PanParentID})
	s.workerContext = ctx
	slog.Info("uploader starting", "listen", c.Addr, "receive_concurrency", c.ReceiveConcurrency,
		"upload_concurrency_min", c.UploadConcurrency, "upload_concurrency_max", c.UploadMaxConcurrency, "file_limit", c.FileBytes, "group_limit", c.GroupBytes,
		"spool_limit", c.SpoolBytes, "batch_interval", st.Interval.String(), "worker_mode", c.WorkerMode)
	s.logStatus(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { s.runUploads(ctx) })
	wg.Go(func() { s.cleanupLoop(ctx) })
	wg.Go(func() { s.statusLoop(ctx) })
	if c.WorkerMode == "forward" {
		wg.Go(func() {
			for ctx.Err() == nil {
				if e := s.notify(ctx); e != nil && ctx.Err() == nil {
					slog.Warn("worker reconnect", "error", e)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		})
	}
	server := &http.Server{Addr: c.Addr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err = <-done:
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	slog.Info("uploader stopping")
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	stop()
	wg.Wait()
	slog.Info("uploader stopped")
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
