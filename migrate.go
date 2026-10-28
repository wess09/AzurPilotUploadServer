package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

type sqliteCopy struct {
	rows  *sql.Rows
	count int
}

func (s *sqliteCopy) Next() bool { return s.rows.Next() }
func (s *sqliteCopy) Err() error { return s.rows.Err() }
func (s *sqliteCopy) Values() ([]any, error) {
	values := make([]any, s.count)
	targets := make([]any, s.count)
	for i := range values {
		targets[i] = &values[i]
	}
	e := s.rows.Scan(targets...)
	return values, e
}

// Run only while the old uploader is stopped. COPY and the migration marker
// commit together; the original SQLite file and all image paths are retained.
func migrateSQLite(c Config, path string) error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL required for migration")
	}
	target, e := openDatabase(c)
	if e != nil {
		return e
	}
	defer target.Close()
	source, e := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if e != nil {
		return e
	}
	defer source.Close()
	source.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	old, e := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return e
	}
	defer old.Rollback()
	pg, e := pgx.Connect(ctx, c.DatabaseURL)
	if e != nil {
		return fmt.Errorf("migration database connection failed")
	}
	defer pg.Close(ctx)
	tx, e := pg.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SET LOCAL statement_timeout='10min'"); e != nil {
		return e
	}
	var existing int
	if e = tx.QueryRow(ctx, "SELECT (SELECT count(*) FROM uploads)+(SELECT count(*) FROM files)+(SELECT count(*) FROM batches)").Scan(&existing); e != nil {
		return e
	}
	if existing != 0 {
		return fmt.Errorf("migration target is not empty; refusing to overwrite")
	}
	for _, table := range []string{"uploads", "files", "direct_files", "batches", "dispatch_state", "dispatch_queue"} {
		query := "SELECT * FROM " + table
		if table == "direct_files" {
			query = "SELECT d.* FROM direct_files d JOIN files f ON f.id=d.file_id"
		}
		rows, x := old.QueryContext(ctx, query)
		if x != nil {
			return x
		}
		columns, x := rows.Columns()
		if x != nil {
			rows.Close()
			return x
		}
		copied, x := tx.CopyFrom(ctx, pgx.Identifier{table}, columns, &sqliteCopy{rows, len(columns)})
		rows.Close()
		if x != nil {
			return fmt.Errorf("copy %s: %w", table, x)
		}
		var actual int64
		if x = tx.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&actual); x != nil {
			return x
		}
		if actual != copied {
			return fmt.Errorf("migration count mismatch: %s", table)
		}
		slog.Info("state migration copied", "table", table, "rows", copied)
	}
	if _, e = tx.Exec(ctx, "ANALYZE uploads,files,direct_files,batches,dispatch_state,dispatch_queue"); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	slog.Info("state migration committed", "backend", "postgres", "sqlite_retained", true)
	return nil
}
