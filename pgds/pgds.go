// pgds package implements postgresql data storage based on pgx driver.
// It supports schema with one primary and several secondaty servers.
// Primary server is retrieved by GetPrimary() method.
// Primary server is used for write queries (INSER/UPDATE/DELETE)
// The list used secondary server is returned by GetSecondary() function.
// Secondary servers are read-only, used for SELECT queries.
package pgds

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dronm/ds"
)

var pder = &PgProvider{}

type ServerID string

const PRIMARY_ID ServerID = "primary" //primary server ID

// OnDbNotificationProto pg notification callback function.
type OnDbNotificationProto = func(*pgconn.PgConn, *pgconn.Notification)

// Db holds db instances.
type Db struct {
	connStr        string
	onNotification OnDbNotificationProto
	Pool           *pgxpool.Pool
	mx             sync.RWMutex
	ref            int
}

// GetRefCount returns active db instancie counter.
func (d *Db) GetRefCount() int {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.ref
}

// Connect opens connection with data base.
func (d *Db) Connect() error {
	conn_conf, err := pgxpool.ParseConfig(d.connStr)
	if err != nil {
		return err
	}
	conn_conf.ConnConfig.OnNotification = d.onNotification
	d.Pool, err = pgxpool.NewWithConfig(context.Background(), conn_conf)
	return err
}

// addRef adds intenal data base instance counter.
func (d *Db) addRef() error {
	d.mx.Lock()
	defer d.mx.Unlock()

	if d.Pool == nil {
		if err := d.Connect(); err != nil {
			return err
		}
	}
	d.ref++

	return nil
}

// release decreases data base instance counter.
func (d *Db) release() {
	d.mx.Lock()
	if d.ref > 0 {
		d.ref--
	}
	d.mx.Unlock()
}

// PgProvider holds one primary and array of secondary instances.
type PgProvider struct {
	Primary     *Db
	Secondaries map[ServerID]*Db
}

// Release releases database connection by its ID (primary or secondary).
func (p *PgProvider) Release(poolConn *pgxpool.Conn, id ServerID) {
	if id == PRIMARY_ID {
		p.Primary.release()
	} else {
		if sec, ok := p.Secondaries[id]; ok {
			sec.release()
		}
	}
	poolConn.Release()
}

func (p *PgProvider) ReleasePrimary(poolConn *pgxpool.Conn) {
	p.Release(poolConn, PRIMARY_ID)
}

// GetPrimary returns primary connection with its ID.
// ID is necessary for releasing.
func (p *PgProvider) GetPrimary() (*pgxpool.Conn, ServerID, error) {
	err := p.Primary.addRef()
	if err != nil {
		return nil, "", err
	} else {
		conn, err := p.Primary.Pool.Acquire(context.Background())
		return conn, PRIMARY_ID, err
	}
}

// ReleaseSecondary releases secondary connection by its ID.
// Deprecated: use common Release()
func (p *PgProvider) ReleaseSecondary(poolConn *pgxpool.Conn, connID ServerID) {
	// p.Primary.release()
	p.Release(poolConn, connID)
}

// GetSecondary looks for an avalable secondary with less ref count.
// srvLsn is a pg replication log position. If empty the list busy server will be returned.
// Otherwise server which position is higher then given lsn.
// If nothing found returns primary.
func (p *PgProvider) GetSecondary(srvLsn string) (*pgxpool.Conn, ServerID, error) {
	if p.Secondaries == nil {
		//no secondary available
		return p.GetPrimary()
	}
	if len(srvLsn) == 0 {
		//find less busy server
		var excluded_ids map[ServerID]bool
	srv_loop:
		var min_db *Db
		var min_id ServerID
		var min_cnt int = 9999999
		for sec_id, sec := range p.Secondaries {
			if _, ok := excluded_ids[sec_id]; ok {
				continue
			}
			cnt := sec.GetRefCount()
			if cnt < min_cnt {
				min_cnt = cnt
				min_db = sec
				min_id = sec_id
			}
		}
		if min_db == nil {
			//no secondary available
			return p.GetPrimary()
		}
		if err := min_db.addRef(); err == nil {
			conn, err := min_db.Pool.Acquire(context.Background())
			if err == nil {
				return conn, min_id, nil
			}
		}
		if excluded_ids == nil {
			excluded_ids = make(map[ServerID]bool)
		}
		excluded_ids[min_id] = true
		goto srv_loop

	} else {
		//got minimum required wal position
		var pool_conn *pgxpool.Conn
		var err error
		var srv_id ServerID
		for sec_id, sec := range p.Secondaries {
			pool_conn, err = sec.Pool.Acquire(context.Background())
			if err == nil {
				continue
			}
			conn := pool_conn.Conn()
			if _, err := conn.Prepare(context.Background(), "LSN_CHECK",
				`SELECT coalesce(pg_wal_lsn_diff(
						(SELECT pg_last_wal_receive_lsn()),
						$1
					),0::numeric) >= 0`); err != nil {
				pool_conn.Release()
				continue
			}
			srv_fits := false
			tries := 2
		wt_loop:
			if err := conn.QueryRow(context.Background(), "LSN_CHECK", srvLsn).Scan(&srv_fits); err != nil {
				pool_conn.Release()
				continue
			}
			if !srv_fits && tries > 0 {
				time.Sleep(time.Duration(100) * time.Millisecond)
				tries--
				goto wt_loop

			} else if !srv_fits {
				pool_conn.Release()
				continue
			} else {
				srv_id = sec_id
				break
			}
		}
		if pool_conn == nil {
			return p.GetPrimary()
		}
		return pool_conn, srv_id, err
	}
}

// InitProvider initializes provider.
// Expects parameters:
//
//	primaryConnStr string containing connection to data base.
//	onDbNotification of type OnDbNotificationProto. Callback function to be used for database notifications.
//	secondaries map[string]string of IDs with connection strings. Key is the server ID and value is a connection string.
func (p *PgProvider) InitProvider(provParams []interface{}) error {
	if len(provParams) < 3 {
		return errors.New("InitProvider parameters: primaryConnStr(string), onDbNotification(OnDbNotificationProto), secondaries(map[string]string)")
	}
	primaryConnStr, ok := provParams[0].(string)
	if !ok {
		return errors.New("InitProvider parameter primaryConnStr must be of type string")
	}
	var onDbNotification OnDbNotificationProto
	if provParams[1] != nil {
		onDbNotification, ok = provParams[1].(OnDbNotificationProto)
		if !ok {
			return errors.New("InitProvider parameter onDbNotification must be of type OnDbNotificationProto")
		}
	}
	p.Primary = &Db{connStr: primaryConnStr, onNotification: onDbNotification}

	if secondaries, ok := provParams[2].(map[string]string); ok {
		p.Secondaries = make(map[ServerID]*Db, 0)
		for id, conn_s := range secondaries {
			p.Secondaries[ServerID(id)] = &Db{connStr: conn_s}
		}
	}

	return nil
}

// init registers pg provider.
func init() {
	ds.Register("pg", pder)
}
