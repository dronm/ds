// ds_test provides test for ds/pgds
package ds_test

import (
	"testing"
	"context"
	"time"
	"fmt"
	
	"github.com/dronm/ds"
	"github.com/dronm/ds/pgds"
	
	"github.com/jackc/pgx/v4"
)

func TestPrimary(t *testing.T) {
	//get provider interface
	prov, err := ds.NewProvider("pg", "postgresql://postgres@:5432/test_proj", nil, nil)
	if err != nil {
		panic(fmt.Sprintf("NewProvider() failed: %v", err))
	}	
	//cast to pg
	pg := prov.(*pgds.PgProvider)
	//aquire connection
	pool_conn, conn_id, err := pg.GetPrimary()
	if err != nil {
		panic(fmt.Sprintf("GetPrimary() failed: %v", err))
	}
	defer pg.Release(pool_conn, conn_id)
	
	if _, err := pool_conn.Conn().Exec(context.Background(), "INSERT INTO test_ds (ts) VALUES(now())"); err != nil {
		panic(fmt.Sprintf("Exec() failed: %v", err))
	}
}

func TestSecondary(t *testing.T) {
	//get provider interface
	prov, err := ds.NewProvider("pg", "postgresql://postgres@:5432/test_proj", nil, nil)
	if err != nil {
		panic(fmt.Sprintf("NewProvider() failed: %v", err))
	}	
	//cast to pg
	pg := prov.(*pgds.PgProvider)
	//aquire connection
	pool_conn, conn_id, err := pg.GetSecondary("")
	if err != nil {
		panic(fmt.Sprintf("GetSecondary() failed: %v", err))
	}
	defer pg.Release(pool_conn, conn_id)
	
	q_res := pool_conn.Conn().QueryRow(context.Background(), "SELECT id, ts FROM test_ds LIMIT 1")
	res := struct {
		Id int `json:"id"`
		Ts time.Time `json:"ts"`
	}{}
	if err := q_res.Scan(&res.Id, &res.Ts); err != nil && err != pgx.ErrNoRows {
		panic(fmt.Sprintf("Scan() failed: %v", err))
	}
	fmt.Println(res)
}

