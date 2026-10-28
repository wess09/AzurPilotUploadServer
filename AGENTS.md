# Repository Guidelines

## Project Structure & Module Organization

This repository implements node A of the AzurPilot screenshot pipeline in Go 1.26. Root files deliberately form one small executable: `server.go` handles streaming reception and upload workers, `store.go` owns durable state, `database.go` supports PostgreSQL and offline SQLite migration, `hot.go` implements the five-minute Redis hash cache, `access.go` logs request sources, `quota.go` tracks disk reservations, `pan.go` implements 123Pan, and `main.go` manages shutdown and WebSocket delivery. Platform disk checks live in `disk_windows.go` and `disk_unix.go`. Runtime data belongs in the ignored `data/` directory.

## Build, Test, and Development Commands

- `go run .`: start with environment variables loaded explicitly.
- `go build -o bin/upload-server .`: build a native executable.
- `go test -count=1 ./...`: verify persistence, resource limits, and transport protocols.
- `go test -race -count=1 ./...`: check concurrent reservations and workers; Windows requires a C compiler.
- `go vet ./...`: run static checks.
- `docker compose up -d --build`: build and start using `.env` and persistent state storage.

## Coding Style & Naming Conventions

Run `gofmt`; use standard-library HTTP handlers and explicit error handling. Keep files named for their responsibility and tests beside implementations as `*_test.go`. Preserve 64 KiB streaming buffers. Never replace `MultipartReader` with helpers that buffer complete upload bodies.

## Testing Guidelines

Use `testing`, `httptest`, temporary directories, and fake cloud endpoints. Tests must cover retry/restart behavior, exact quota accounting, and idempotent delivery. The normal suite includes a 300 MiB streaming resource test; `go test -short ./...` skips it. Do not require real cloud credentials in tests.

## Commit & Pull Request Guidelines

Use focused `feat:`, `fix:`, or `docs:` commits with concise descriptions. Include validation results and configuration/protocol changes in review notes. After completing and validating changes, create a local commit unless instructed otherwise.

## Security & Configuration

Keep `.env`, credentials, images, and SQLite files out of Git. Never release spool space before successful unlink. Retain durable cloud IDs and batch IDs until acknowledgement; never delete cloud originals automatically.

Production uses PostgreSQL with an eight-connection pool and volatile Redis hashes (300-second TTL). SQLite remains for offline migration and isolated unit tests. Preserve the original SQLite snapshot and spool during migration; never clear live queues. Redis misses use cloud MD5 reuse without scanning cold history. Keep caches bounded and use TCP peer IPs unless a trusted proxy is explicitly configured.
