package main

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

type accessWriter struct {
	http.ResponseWriter
	status, bytes int
	reason        string
}

func (w *accessWriter) setError(reason string) {
	if len(reason) > 256 {
		reason = reason[:256]
	}
	w.reason = reason
}

func controlError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, context.Canceled):
		status = 499
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case strings.Contains(err.Error(), "metadata conflict"):
		status = http.StatusConflict
	case strings.Contains(err.Error(), "busy") || strings.Contains(err.Error(), "already being received"):
		status = http.StatusLocked
	case strings.Contains(err.Error(), "too many") || strings.Contains(err.Error(), "connection limit"):
		status = http.StatusServiceUnavailable
	}
	reject(w, status, err)
}

func (w *accessWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *accessWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *accessWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	n, e := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, e
}
func (w *accessWriter) Flush() { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (w *accessWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.status = 101
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		writer := &accessWriter{ResponseWriter: w}
		next.ServeHTTP(writer, r)
		ip, _, e := net.SplitHostPort(r.RemoteAddr)
		if e != nil {
			ip = r.RemoteAddr
		}
		status := writer.status
		if status == 0 {
			status = 200
		}
		agent := r.UserAgent()
		if len(agent) > 200 {
			agent = agent[:200]
		}
		// A is reached directly through NAT. Forwarded headers are untrusted.
		slog.Info("http request", "request_ip", ip, "method", r.Method, "path", r.URL.Path, "reason", writer.reason,
			"status", status, "duration_ms", time.Since(started).Milliseconds(), "bytes", writer.bytes, "user_agent", agent)
	})
}

func directDeadline(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
		defer cancel()
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(12 * time.Second))
		defer controller.SetReadDeadline(time.Time{})
		next(w, r.WithContext(ctx))
	}
}
