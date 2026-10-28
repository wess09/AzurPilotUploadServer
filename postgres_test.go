package main

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPostgresMigrationAndRedisHotTTL(t *testing.T) {
	dsn, redisURL := os.Getenv("TEST_POSTGRES_URL"), os.Getenv("TEST_REDIS_URL")
	if dsn == "" || redisURL == "" {
		t.Skip("requires dedicated PostgreSQL and Redis test services")
	}
	admin, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	schema := "migration_" + newID()
	if _, e = admin.Exec("CREATE SCHEMA " + schema); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec("DROP SCHEMA " + schema + " CASCADE")
	parsed, e := url.Parse(dsn)
	if e != nil {
		t.Fatal(e)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	c := testConfig(t)
	old, e := openStore(c)
	if e != nil {
		t.Fatal(e)
	}
	data := pngData("migration")
	m := metadata(70001, data)
	if e = old.claimGroup(m); e != nil {
		t.Fatal(e)
	}
	sha := strings.Repeat("a", 64)
	md5 := strings.Repeat("b", 32)
	if _, e = old.DB.Exec(`INSERT INTO files(id,group_id,idx,path,size,md5,sha,remote_id,state) VALUES(?,?,0,'',?,?,?,9,'uploaded')`, newID(), m.UploadID, len(data), md5, sha); e != nil {
		t.Fatal(e)
	}
	if _, _, e = old.seal(m.UploadID); e != nil {
		t.Fatal(e)
	}
	old.DB.Close()
	c.DatabaseURL, c.RedisURL = parsed.String(), redisURL
	if e = migrateSQLite(c, filepath.Join(c.DataDir, "state.sqlite")); e != nil {
		t.Fatal(e)
	}
	st, e := openStore(c)
	if e != nil {
		t.Fatal(e)
	}
	defer st.DB.Close()
	defer st.contentCache.client.Close()
	if !st.DB.postgres || st.DB.Stats().MaxOpenConnections != 0 {
		t.Fatal("PostgreSQL pool not enabled")
	}
	var count, busy int
	if e = st.DB.QueryRow(`SELECT count(*) FROM files WHERE remote_id=?`, 9).Scan(&count); e != nil || count != 1 {
		t.Fatalf("files=%d err=%v", count, e)
	}
	if e = st.DB.QueryRow(`SELECT busy FROM uploads WHERE id=?`, m.UploadID).Scan(&busy); e != nil || busy != 0 {
		t.Fatalf("busy=%d err=%v", busy, e)
	}
	key := contentKey{sha, md5, int64(len(data))}
	st.contentCache.put(key, 9)
	defer st.contentCache.client.Del(context.Background(), hotKey(key))
	if id, ok := st.contentCache.get(key); !ok || id != 9 {
		t.Fatal("hot hash lookup failed")
	}
	ttl, e := st.contentCache.client.TTL(context.Background(), hotKey(key)).Result()
	if e != nil || ttl <= 4*time.Minute || ttl > 5*time.Minute {
		t.Fatalf("unexpected TTL: %s %v", ttl, e)
	}
	if _, e = st.DB.Exec(`UPDATE dispatch_state SET next_at=0`); e != nil {
		t.Fatal(e)
	}
	first, e := st.makeBatch(30)
	if e != nil || first == nil || len(first.Items) != 1 {
		t.Fatalf("batch=%v error=%v", first, e)
	}
	second, e := st.makeBatch(30)
	if e != nil || second.ID != first.ID {
		t.Fatal("batch replay changed identity")
	}
	if _, e = os.Stat(filepath.Join(c.DataDir, "state.sqlite")); e != nil {
		t.Fatal("migration removed SQLite recovery file")
	}
}
