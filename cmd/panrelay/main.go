package main

import (
	"context"
	"crypto/subtle"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

// Private control-plane relay. Credentials travel through the SSH reverse
// tunnel; no image bodies or arbitrary destinations are accepted.
func main() {
	token := os.Getenv("RELAY_TOKEN")
	if token == "" {
		log.Fatal("RELAY_TOKEN required")
	}
	upstreamURL := os.Getenv("RELAY_UPSTREAM")
	if upstreamURL == "" {
		upstreamURL = "https://open-api.123pan.com"
	}
	upstream, _ := url.Parse(upstreamURL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return dialer.DialContext(ctx, "tcp4", address)
	}
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 128
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = transport
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		r.Host = upstream.Host
		if upstream.Host == "open-api.123pan.com" {
			r.Header.Del("X-AzurPilot-Relay-Token")
		}
	}
	allowed := map[string]bool{"/upload/v1/file/create": true, "/upload/v1/file/get_upload_url": true, "/upload/v1/file/upload_complete": true, "/upload/v1/file/upload_async_result": true, "/upload/v2/file/create": true, "/upload/v2/file/upload_complete": true, "/upload/v2/file/domain": true, "/api/v1/file/detail": true}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-AzurPilot-Relay-Token")), []byte(token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		if !allowed[r.URL.Path] {
			http.NotFound(w, r)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		proxy.ServeHTTP(w, r)
	})
	listen := os.Getenv("RELAY_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:18093"
	}
	server := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}
