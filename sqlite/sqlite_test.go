package sqliteds

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dronm/ds"
)

const DB_FILE = "test_db.db"

func getTestVar(t *testing.T, n string) string {
	v := os.Getenv(n)
	if v == "" {
		t.Fatalf("getTestVar() failed: %s environment variable is not set", n)
	}
	return v
}

func TestPrimary(t *testing.T) {
	//get provider interface
	t.Logf("Testing primary server to sqlite database connection")

	prov, err := ds.NewProvider("sqlite", DB_FILE)
	if err != nil {
		t.Fatalf("NewProvider() failed: %v", err)
	}
	//cast to sqlite
	sqlite := prov.(*SQLiteProvider)
	//aquire connection
	pool_conn, conn_id, err := sqlite.GetPrimary()
	if err != nil {
		t.Fatalf("sqlite.GetPrimary() failed: %v", err)
	}
	defer sqlite.Release(pool_conn, conn_id)
	conn := pool_conn.Conn()

	want_v := time.Now()
	//table creation
	if _, err := conn.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS test_ds (
			id integer NOT NULL PRIMARY KEY,
			ts DATETIME NOT NULL)`,
		want_v); err != nil {
		t.Errorf("sqlite.Exec() failed creating table: %v", err)
	}

	t.Logf("Testing INSERT")
	want_v = want_v.Round(time.Microsecond)
	if _, err := conn.Exec(context.Background(), "INSERT INTO test_ds (ts) VALUES($1)", want_v); err != nil {
		t.Errorf("sqlite.Exec() failed: %v", err)
	}

	t.Logf("Testing SELECT")
	var got_v time.Time
	if err := conn.QueryRow(context.Background(), "SELECT ts FROM test_ds WHERE ts = $1 LIMIT 1", want_v).Scan(&got_v); err != nil {
		t.Errorf("sqlite.Scan() failed: %v", err)
	}
	if got_v.Compare(want_v) != 0 {
		t.Errorf("Expected %v, got %v", want_v, got_v)
	}
}
