// Package pgds implements postgresql data storage based on pgx driver.
// It supports schema with one primary and several secondaty servers.
// Primary server is retrieved by GetPrimary() method.
// Primary server is used for write queries (INSER/UPDATE/DELETE)
// The list used secondary server is returned by GetSecondary() function.
package pgds

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dronm/ds/v2"
)

var pgProv = &PgProvider{}

const (
	PRIMARY_ID  ds.ServerID = "primary" //primary server ID
	PROVIDER_ID             = "pg"
)

// OnDbNotificationProto pg notification callback function.
type OnDbNotification = func(*pgconn.PgConn, *pgconn.Notification)

// PGXConn implements ds.Conn interface: Exec, Query, QueryRow, etc.
type PGXConn struct {
	Conn *pgx.Conn
}

func (c *PGXConn) Exec(ctx context.Context, sql string, args ...any) (ds.ExecResult, error) {
	return c.Conn.Exec(ctx, sql, args...)
}

func (c *PGXConn) QueryRow(ctx context.Context, sql string, args ...any) ds.Row {
	return c.Conn.QueryRow(ctx, sql, args...)
}

func (c *PGXConn) Query(ctx context.Context, sql string, args ...any) (ds.Rows, error) {
	return c.Conn.Query(ctx, sql, args...)
}

func (c *PGXConn) Prepare(ctx context.Context, name, sql string) (ds.PrepStatementDescr, error) {
	sDescr, err := c.Conn.Prepare(ctx, name, sql)
	if err != nil {
		return nil, err
	}
	return &PGXPrepStatementDescr{sDescr}, nil
}

func (c *PGXConn) ErrNoRows(err error) bool {
	return err == pgx.ErrNoRows
}

func (c *PGXConn) ErrConstraintViolation(err error) bool {
	if pgerr, ok := err.(*pgconn.PgError); ok && pgerr.Code == "23514" {
		return true
	}
	return false
}

func (c *PGXConn) ErrForeignKeyViolation(err error) bool {
	if pgerr, ok := err.(*pgconn.PgError); ok && pgerr.Code == "23503" {
		return true
	}
	return false
}

type PGXPrepStatementDescr struct {
	*pgconn.StatementDescription
}

func (s *PGXPrepStatementDescr) GetName() string {
	return s.Name
}

func (s *PGXPrepStatementDescr) GetSQL() string {
	return s.SQL
}

// PGXPool implements ds.PoolConn interface.
type PGXPoolConn struct {
	PGXConn *pgxpool.Conn
}

func (c *PGXPoolConn) Release() {
	c.PGXConn.Release()
}

func (c *PGXPoolConn) Conn() ds.Conn {
	conn := c.PGXConn.Conn()
	return &PGXConn{conn}
}

// PGXRows wraps pgx.Rows to implement the Rows interface
type PGXRows struct {
	pgx.Rows
}

func (r *PGXRows) Scan(dest ...interface{}) error {
	return r.Rows.Scan(dest...)
}

func (r *PGXRows) Err() error {
	return r.Err()
}

func (r *PGXRows) Next() bool {
	return r.Next()
}

type PGXRow struct {
	pgx.Row
}

func (r *PGXRow) Scan(dest ...interface{}) error {
	return r.Row.Scan(dest...)
}

// Db holds db instance.
type Db struct {
	connStr        string
	onNotification OnDbNotification
	pool           *pgxpool.Pool
	mx             sync.RWMutex
	refCount       int
}

// GetRefCount returns active db instancie counter.
func (d *Db) RefCount() int {
	d.mx.RLock()
	defer d.mx.RUnlock()
	return d.refCount
}

// Connect opens connection with data base.
func (d *Db) Connect() error {
	connConf, err := pgxpool.ParseConfig(d.connStr)
	if err != nil {
		return err
	}
	connConf.ConnConfig.OnNotification = d.onNotification
	d.pool, err = pgxpool.NewWithConfig(context.Background(), connConf)
	return err
}

// addRef adds intenal data base instance counter.
func (d *Db) addRef() error {
	d.mx.Lock()
	defer d.mx.Unlock()

	if d.pool == nil {
		if err := d.Connect(); err != nil {
			return err
		}
	}
	d.refCount++

	return nil
}

// release decreases data base instance counter.
func (d *Db) release() {
	d.mx.Lock()
	d.refCount--
	if d.refCount < 0 {
		d.refCount = 0
	}
	d.mx.Unlock()
}

// PgProvider holds one primary and array of secondary instances.
type PgProvider struct {
	Primary     *Db
	Secondaries map[ds.ServerID]*Db
}

// GetPrimary returns primary connection with its ID.
// ID is necessary for releasing.
func (p *PgProvider) GetPrimary() (ds.PoolConn, ds.ServerID, error) {
	err := p.Primary.addRef()
	if err != nil {
		return nil, "", err
	} else {
		conn, err := p.Primary.pool.Acquire(context.Background())
		return &PGXPoolConn{conn}, PRIMARY_ID, err
	}
}

// Release releases database connection by its ID (primary or secondary).
func (p *PgProvider) Release(poolConn ds.PoolConn, id ds.ServerID) {
	if id == PRIMARY_ID {
		p.Primary.release()
	} else {
		if sec, ok := p.Secondaries[id]; ok {
			sec.release()
		}
	}
	poolConn.Release()
}

func (p *PgProvider) ReleasePrimary(poolConn ds.PoolConn) {
	p.Release(poolConn, PRIMARY_ID)
}

// GetSecondary looks for an avalable secondary with less ref count.
// srvLsn is a pg replication log position. If empty the list busy server will be returned.
// Otherwise server which position is higher then given lsn.
// If nothing found returns primary.
func (p *PgProvider) GetSecondary(srvLsn string) (ds.PoolConn, ds.ServerID, error) {
	if p.Secondaries == nil {
		//no secondary available
		return p.GetPrimary()
	}
	if len(srvLsn) == 0 {
		//find less busy server
		var excluded_ids map[ds.ServerID]bool
	srv_loop:
		var min_db *Db
		var min_id ds.ServerID
		var min_cnt int = 9999999
		for sec_id, sec := range p.Secondaries {
			if _, ok := excluded_ids[sec_id]; ok {
				continue
			}
			cnt := sec.RefCount()
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
			conn, err := min_db.pool.Acquire(context.Background())
			if err == nil {
				return &PGXPoolConn{conn}, min_id, nil
			}
		}
		if excluded_ids == nil {
			excluded_ids = make(map[ds.ServerID]bool)
		}
		excluded_ids[min_id] = true
		goto srv_loop

	} else {
		//got minimum required wal position
		var pool_conn *pgxpool.Conn
		var err error
		var srv_id ds.ServerID
		for sec_id, sec := range p.Secondaries {
			pool_conn, err = sec.pool.Acquire(context.Background())
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
		return &PGXPoolConn{pool_conn}, srv_id, err
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
	var onDbNotification OnDbNotification
	if provParams[1] != nil {
		onDbNotification, ok = provParams[1].(OnDbNotification)
		if !ok {
			return errors.New("InitProvider parameter onDbNotification must be of type OnDbNotification")
		}
	}
	p.Primary = &Db{connStr: primaryConnStr, onNotification: onDbNotification}

	if secondaries, ok := provParams[2].(map[string]string); ok {
		p.Secondaries = make(map[ds.ServerID]*Db, 0)
		for id, conn_s := range secondaries {
			p.Secondaries[ds.ServerID(id)] = &Db{connStr: conn_s}
		}
	}

	return nil
}

// init registers pg provider.
func init() {
	ds.Register(PROVIDER_ID, pgProv)
}
