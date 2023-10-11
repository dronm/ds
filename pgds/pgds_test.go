package pgds

import (
	"context"
	"testing"
	"time"

	"github.com/dronm/ds"
	//"github.com/jackc/pgx/v4"
)

func TestPrimary(t *testing.T) {
	//get provider interface
	t.Logf("Testing primary server to pg database connection")

	prov, err := ds.NewProvider("pg", "postgresql://postgres@:5432/test_proj", nil, nil)
	if err != nil {
		t.Fatalf("NewProvider() failed: %v", err)
	}
	//cast to pg
	pg := prov.(*PgProvider)
	//aquire connection
	pool_conn, conn_id, err := pg.GetPrimary()
	if err != nil {
		t.Fatalf("pg.GetPrimary() failed: %v", err)
	}
	defer pg.Release(pool_conn, conn_id)

	t.Logf("Testing INSERT")
	want_v := time.Now()
	want_v = want_v.Round(time.Microsecond)
	if _, err := pool_conn.Conn().Exec(context.Background(), "INSERT INTO test_ds (ts) VALUES($1)", want_v); err != nil {
		t.Errorf("pg.Conn() failed: %v", err)
	}

	t.Logf("Testing SELECT")
	var got_v time.Time
	if err := pool_conn.Conn().QueryRow(context.Background(), "SELECT ts FROM test_ds WHERE ts = $1 LIMIT 1", want_v).Scan(&got_v); err != nil {
		t.Errorf("pg.Scan() failed: %v", err)
	}
	if got_v != want_v {
		t.Errorf("Expected %v, got %v", want_v, got_v)
	}
}

func TestSecondary(t *testing.T) {
	//get provider interface
	t.Logf("Testing secondary server to pg database connection")

	prov, err := ds.NewProvider("pg", "", nil, map[string]string{"sec1": "postgresql://postgres@:5432/test_proj"})
	if err != nil {
		t.Fatalf("NewProvider() failed: %v", err)
	}
	//cast to pg
	pg := prov.(*PgProvider)
	//aquire connection
	pool_conn, conn_id, err := pg.GetSecondary("")
	if err != nil {
		t.Fatalf("pg.GetSecondary() failed: %v", err)
	}
	defer pg.Release(pool_conn, conn_id)

	t.Logf("Testing SELECT")
	var got_v time.Time
	if err := pool_conn.Conn().QueryRow(context.Background(), "SELECT ts FROM test_ds LIMIT 1").Scan(&got_v); err != nil {
		t.Fatalf("pg.Scan() failed: %v", err)
	}
	t.Logf("Selected value: %v", got_v)
}
