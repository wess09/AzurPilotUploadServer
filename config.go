package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr, DataDir, WorkerURL, WorkerToken, PanID, PanSecret, PanBase, AccessID, AccessSecret string
	ReceiveConcurrency, UploadConcurrency, BatchSize                                         int
	SpoolBytes, FileBytes, GroupBytes, MinFreeBytes                                          int64
	Poll                                                                                     time.Duration
	BatchInterval                                                                            time.Duration
	WorkerMode                                                                               string
}

func loadConfig() (Config, error) {
	c := Config{Addr: env("LISTEN_ADDR", ":8090"), DataDir: env("DATA_DIR", "data"), WorkerURL: os.Getenv("WORKER_WS_URL"), WorkerToken: os.Getenv("WORKER_TOKEN"), PanID: os.Getenv("PAN_CLIENT_ID"), PanSecret: os.Getenv("PAN_CLIENT_SECRET"), PanBase: env("PAN_API_BASE", "https://open-api.123pan.com"), AccessID: os.Getenv("CF_ACCESS_CLIENT_ID"), AccessSecret: os.Getenv("CF_ACCESS_CLIENT_SECRET"), Poll: time.Second}
	c.WorkerMode = env("WORKER_MODE", "reverse")
	var err error
	c.BatchInterval, err = time.ParseDuration(env("BATCH_INTERVAL", "30m"))
	if err != nil || c.BatchInterval < time.Second || c.BatchInterval > 24*time.Hour || c.BatchInterval%time.Second != 0 {
		return c, fmt.Errorf("BATCH_INTERVAL must be whole seconds between 1s and 24h")
	}
	if c.WorkerMode != "reverse" && c.WorkerMode != "forward" {
		return c, fmt.Errorf("WORKER_MODE must be reverse or forward")
	}
	for _, v := range []struct {
		name     string
		dst      *int
		fallback int
	}{{"RECEIVE_CONCURRENCY", &c.ReceiveConcurrency, 30}, {"UPLOAD_CONCURRENCY", &c.UploadConcurrency, 2}, {"BATCH_SIZE", &c.BatchSize, 30}} {
		n, e := strconv.Atoi(env(v.name, strconv.Itoa(v.fallback)))
		if e != nil || n < 1 {
			return c, fmt.Errorf("invalid %s", v.name)
		}
		*v.dst = n
	}
	for _, v := range []struct {
		name     string
		dst      *int64
		fallback int64
	}{{"SPOOL_MAX_BYTES", &c.SpoolBytes, 1 << 30}, {"MAX_FILE_BYTES", &c.FileBytes, 10 << 20}, {"MAX_GROUP_BYTES", &c.GroupBytes, 500 << 20}, {"MIN_FREE_BYTES", &c.MinFreeBytes, 256 << 20}} {
		n, e := strconv.ParseInt(env(v.name, strconv.FormatInt(v.fallback, 10)), 10, 64)
		if e != nil || n < 0 {
			return c, fmt.Errorf("invalid %s", v.name)
		}
		*v.dst = n
	}
	if c.FileBytes < 1 || c.GroupBytes < c.FileBytes || c.SpoolBytes < c.FileBytes {
		return c, fmt.Errorf("invalid file/group/spool limits")
	}
	if c.BatchSize > 30 {
		return c, fmt.Errorf("BATCH_SIZE must be 1..30")
	}
	if c.PanID == "" || c.PanSecret == "" || c.WorkerToken == "" {
		return c, fmt.Errorf("PAN_CLIENT_ID, PAN_CLIENT_SECRET and WORKER_TOKEN are required")
	}
	if c.WorkerMode == "forward" && c.WorkerURL == "" {
		return c, fmt.Errorf("WORKER_WS_URL is required in forward mode")
	}
	return c, nil
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
