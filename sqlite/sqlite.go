package sqliteds

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/dronm/ds"
)

type ServerID string

const PRIMARY_ID ServerID = "primary" //primary server ID

const (
	DEF_DRIVER_NAME = "sqlite3"
)

var pder = &SQLiteProvider{}

type DbPool struct {
	DbConn *DbConn
}

func (p *DbPool) Conn() *DbConn {
	return p.DbConn
}

type DbConn struct {
	Conn *sql.DB
}

func (c *DbConn) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	//if args is not a slice: wrap to a new slice
	return c.Conn.ExecContext(ctx, query, args...)
}

func (c *DbConn) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return c.Conn.QueryRowContext(ctx, query, args...)
}

func (c *DbConn) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.Conn.QueryContext(ctx, query, args...)
}

func (c *DbConn) IsErrNoRows(err error) bool {
	return err == sql.ErrNoRows
}

// Db holds db instances.
type Db struct {
	Pool       *DbPool
	fileName   string
	driverName string
	// Pool    *pgxpool.Pool
	mx  sync.RWMutex
	ref int
}

// GetRefCount returns active db instancie counter.
func (d *Db) GetRefCount() int {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.ref
}

// Connect opens connection with data base.
func (d *Db) Connect() error {
	var err error

	d.Pool.DbConn.Conn, err = sql.Open(d.driverName, d.fileName)
	if err != nil {
		return err
	}

	return nil
}

// addRef adds intenal data base instance counter.
func (d *Db) addRef() error {
	d.mx.Lock()
	defer d.mx.Unlock()

	if d.Pool.DbConn.Conn == nil {
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

// SQLiteProvider holds one primary and array of secondary instances.
type SQLiteProvider struct {
	Database *Db
}

// Release releases database connection by its ID (primary or secondary).
func (p *SQLiteProvider) Release(pool *DbPool, id ServerID) {
	p.Database.release()
}

// alias
func (p *SQLiteProvider) ReleasePrimary(pool *DbPool) {
	p.Release(pool, PRIMARY_ID)
}

func (p *SQLiteProvider) GetPrimary() (*DbPool, ServerID, error) {
	if err := p.Database.addRef(); err != nil {
		return nil, "", err
	}
	return p.Database.Pool, "", nil
}

func (p *SQLiteProvider) GetSecondary(srvLsn string) (*DbPool, ServerID, error) {
	return p.GetPrimary()
}

// InitProvider initializes provider.
// Expects parameters:
//
//	database file string containing database
//
// sqlite driver name, default is DEF_DRIVER_NAME
func (p *SQLiteProvider) InitProvider(provParams []interface{}) error {
	if len(provParams) < 1 {
		return errors.New("InitProvider parameters: database file name, sqlite driver name (default is sqlite3)")
	}
	p.Database = &Db{Pool: &DbPool{DbConn: &DbConn{}}}

	var ok bool
	p.Database.fileName, ok = provParams[0].(string)
	if !ok {
		return errors.New("InitProvider parameter database file name must be of type string")
	}
	if len(provParams) > 1 {
		p.Database.driverName, ok = provParams[1].(string)
		if !ok {
			return errors.New("InitProvider parameter driver name must be of type string")
		}
	} else {
		p.Database.driverName = DEF_DRIVER_NAME
	}

	return nil
}

// init registers pg provider.
func init() {
	ds.Register("sqlite", pder)
}
