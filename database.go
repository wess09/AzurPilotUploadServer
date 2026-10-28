package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Keep the legacy dialect only for offline migration and existing unit tests.
// Production PostgreSQL uses a bounded pool rather than a global writer queue.
type database struct {
	*sql.DB
	postgres bool
}
type transaction struct {
	*sql.Tx
	postgres bool
}

func (s *Store) storageStatus() map[string]any {
	backend := "sqlite"
	if s.DB.postgres {
		backend = "postgres"
	}
	stats := s.DB.Stats()
	return map[string]any{"backend": backend, "redis": s.contentCache != nil && s.contentCache.client != nil,
		"hot_hash_ttl_seconds": int(hotTTL.Seconds()), "db_connections": stats.OpenConnections, "db_pool_limit": stats.MaxOpenConnections,
		"db_wait_count": stats.WaitCount, "db_wait_ms": stats.WaitDuration.Milliseconds()}
}

func bind(q string, pg bool) string {
	if !pg {
		return q
	}
	q = strings.ReplaceAll(q, " INDEXED BY files_group_state", "")
	q = strings.ReplaceAll(q, "ORDER BY rowid", "ORDER BY id")
	if strings.Contains(q, "INSERT OR IGNORE INTO") {
		q = strings.ReplaceAll(q, "INSERT OR IGNORE INTO", "INSERT INTO")
		q = strings.TrimSuffix(strings.TrimSpace(q), ";") + " ON CONFLICT DO NOTHING"
	}
	var b strings.Builder
	index := 1
	for _, ch := range q {
		if ch == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(index))
			index++
		} else {
			b.WriteRune(ch)
		}
	}
	return b.String()
}
func (d *database) Exec(q string, a ...any) (sql.Result, error) {
	return d.ExecContext(context.Background(), q, a...)
}
func (d *database) ExecContext(c context.Context, q string, a ...any) (sql.Result, error) {
	return d.DB.ExecContext(c, bind(q, d.postgres), a...)
}
func (d *database) Query(q string, a ...any) (*sql.Rows, error) {
	return d.QueryContext(context.Background(), q, a...)
}
func (d *database) QueryContext(c context.Context, q string, a ...any) (*sql.Rows, error) {
	return d.DB.QueryContext(c, bind(q, d.postgres), a...)
}
func (d *database) QueryRow(q string, a ...any) *sql.Row {
	return d.QueryRowContext(context.Background(), q, a...)
}
func (d *database) QueryRowContext(c context.Context, q string, a ...any) *sql.Row {
	return d.DB.QueryRowContext(c, bind(q, d.postgres), a...)
}
func (d *database) Begin() (*transaction, error) { return d.BeginTx(context.Background(), nil) }
func (d *database) BeginTx(c context.Context, o *sql.TxOptions) (*transaction, error) {
	t, e := d.DB.BeginTx(c, o)
	if e != nil {
		return nil, e
	}
	return &transaction{t, d.postgres}, nil
}
func (t *transaction) Exec(q string, a ...any) (sql.Result, error) {
	return t.ExecContext(context.Background(), q, a...)
}
func (t *transaction) ExecContext(c context.Context, q string, a ...any) (sql.Result, error) {
	return t.Tx.ExecContext(c, bind(q, t.postgres), a...)
}
func (t *transaction) Query(q string, a ...any) (*sql.Rows, error) {
	return t.Tx.QueryContext(context.Background(), bind(q, t.postgres), a...)
}
func (t *transaction) QueryRow(q string, a ...any) *sql.Row {
	return t.Tx.QueryRowContext(context.Background(), bind(q, t.postgres), a...)
}

func openDatabase(c Config) (*database, error) {
	driver, dsn := "sqlite", filepath.Join(c.DataDir, "state.sqlite")
	pg := c.DatabaseURL != ""
	if pg {
		driver, dsn = "pgx", c.DatabaseURL
	}
	raw, e := sql.Open(driver, dsn)
	if e != nil {
		return nil, fmt.Errorf("state database configuration invalid")
	}
	d := &database{raw, pg}
	if pg {
		d.SetMaxOpenConns(8)
		d.SetMaxIdleConns(4)
		d.SetConnMaxIdleTime(time.Minute)
	} else {
		d.SetMaxOpenConns(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e = d.PingContext(ctx); e != nil {
		raw.Close()
		return nil, fmt.Errorf("state database unavailable")
	}
	if !pg {
		if _, e = d.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000"); e != nil {
			d.Close()
			return nil, e
		}
	}
	schema := stateSchema
	if pg {
		schema = strings.ReplaceAll(schema, "INTEGER", "BIGINT")
	}
	if _, e = d.Exec(schema); e != nil {
		d.Close()
		return nil, e
	}
	return d, nil
}
