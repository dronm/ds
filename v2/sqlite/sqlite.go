package sqliteds

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/dronm/ds/v2"
	_ "github.com/mattn/go-sqlite3"
)

const (
	PRIMARY_ID  ds.ServerID = "primary" //primary server ID
	PROVIDER_ID             = "sqlite3"
)

var pder = &SQLiteProvider{}

type SQLitePoolConn struct {
	DbConn *SQLiteConn
}

func (p *SQLitePoolConn) Conn() ds.Conn {
	return p.DbConn
}

// stub
func (c *SQLitePoolConn) Release() {
}

type SQLiteExecResult struct {
	Res sql.Result
}

func (r *SQLiteExecResult) RowsAffected() int64 {
	rowsAff, _ := r.Res.RowsAffected()
	return rowsAff
}

type SQLiteConn struct {
	Conn       *sql.DB
	PreparedSt sync.Map
}

func (c *SQLiteConn) Exec(ctx context.Context, query string, args ...any) (ds.ExecResult, error) {
	var err error
	var res sql.Result
	if st, ok := c.PreparedSt.Load(query); ok {
		res, err = st.(*sql.Stmt).ExecContext(ctx, args...)
	} else {
		res, err = c.Conn.ExecContext(ctx, query, args...)
	}
	if err != nil {
		return nil, err
	}
	return &SQLiteExecResult{res}, nil
}

func (c *SQLiteConn) QueryRow(ctx context.Context, query string, args ...any) ds.Row {
	var row *sql.Row
	if st, ok := c.PreparedSt.Load(query); ok {
		row = st.(*sql.Stmt).QueryRowContext(ctx, args...)
	} else {
		row = c.Conn.QueryRowContext(ctx, query, args...)
	}
	return row
}

func (c *SQLiteConn) Query(ctx context.Context, query string, args ...any) (ds.Rows, error) {
	var err error
	var rows *sql.Rows
	if st, ok := c.PreparedSt.Load(query); ok {
		rows, err = st.(*sql.Stmt).QueryContext(ctx, args...)
	} else {
		rows, err = c.Conn.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return nil, err
	}
	return &SQLiteRows{rows}, nil
}

// Prepare is a stub.
func (c *SQLiteConn) Prepare(ctx context.Context, name, sql string) (ds.PrepStatementDescr, error) {
	st, err := c.Conn.PrepareContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	c.PreparedSt.Store(name, st)
	return nil, nil
}

func (c *SQLiteConn) ErrNoRows(err error) bool {
	return err == sql.ErrNoRows
}

func (c *SQLiteConn) ErrConstraintViolation(err error) bool {
	/*if pgerr, ok := err.(*pgconn.PgError); ok && pgerr.Code == "23514" {
		return true
	}*/
	return false
}
func (c *SQLiteConn) ErrForeignKeyViolation(err error) bool {
	return false
}

// SQLiteRows wraps pgx.Rows to implement the Rows interface
type SQLiteRows struct {
	Rows *sql.Rows
}

func (r *SQLiteRows) Scan(dest ...interface{}) error {
	return r.Rows.Scan(dest...)
}

func (r *SQLiteRows) Close() {
	_ = r.Rows.Close()
}

func (r *SQLiteRows) Err() error {
	return r.Rows.Err()
}

func (r *SQLiteRows) Next() bool {
	return r.Rows.Next()
}

// Db holds db instances.
type Db struct {
	pool *SQLitePoolConn
	mx   sync.RWMutex
	ref  int
}

// GetRefCount returns active db instancie counter.
func (d *Db) GetRefCount() int {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.ref
}

// addRef adds intenal data base instance counter.
func (d *Db) addRef() error {
	d.mx.Lock()
	defer d.mx.Unlock()

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
func (p *SQLiteProvider) Release(pool ds.PoolConn, id ds.ServerID) {
	p.Database.release()
}

func (p *SQLiteProvider) ReleasePrimary(poolConn ds.PoolConn) {
	p.Release(poolConn, PRIMARY_ID)
}

func (p *SQLiteProvider) GetPrimary() (ds.PoolConn, ds.ServerID, error) {
	if err := p.Database.addRef(); err != nil {
		return nil, "", err
	}
	return &SQLitePoolConn{DbConn: p.Database.pool.DbConn}, "", nil
}

func (p *SQLiteProvider) GetSecondary(srvLsn string) (ds.PoolConn, ds.ServerID, error) {
	return p.GetPrimary()
}

// InitProvider initializes provider.
// Expects parameters:
//
//	*sql.DB
//
// sqlite driver name, default is DEF_DRIVER_NAME
func (p *SQLiteProvider) InitProvider(provParams []interface{}) error {
	if len(provParams) < 1 {
		return errors.New("InitProvider parameters: database file name, sqlite driver name (default is sqlite3)")
	}
	p.Database = &Db{pool: &SQLitePoolConn{DbConn: &SQLiteConn{}}}

	var ok bool
	p.Database.pool.DbConn.Conn, ok = provParams[0].(*sql.DB)
	if !ok {
		return errors.New("InitProvider parameter must be of type *sql.DB")
	}
	return nil
}

// init registers pg provider.
func init() {
	ds.Register(PROVIDER_ID, pder)
}
