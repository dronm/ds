// Package pgds implements a PostgreSQL data storage provider
// based on pgx/pgxpool. It supports one primary and optional
// read-only secondary replicas with LSN-based selection.
package pgds

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dronm/ds/v3"
)

var replicaHasLSNFn = replicaHasLSN

const (
	ProviderID = "pg"
	PrimaryID  = ds.ServerID("primary")
)

const (
	lsnCheckQuery = `
SELECT coalesce(
	pg_wal_lsn_diff(
		(SELECT pg_last_wal_receive_lsn()),
		$1
	),
	0
) >= 0
`
	maxLSNWait  = 300 * time.Millisecond
	lsnPollStep = 50 * time.Millisecond
)

type dbHandle interface {
	acquire(ctx context.Context) (*pgxpool.Conn, error)
	close() error
}

// OnDBNotification is a callback for PostgreSQL LISTEN/NOTIFY.
type OnDBNotification = pgconn.NotificationHandler

//
// ---------- Config ----------
//

type Config struct {
	PrimaryConnStr string
	Secondaries    map[ds.ServerID]string
	OnNotification OnDBNotification
}

//
// ---------- Provider registration ----------
//

func init() {
	ds.Register(ProviderID, New)
}

func New(cfg any) (ds.Provider, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("pgds: config must be *pgds.Config")
	}
	if c.PrimaryConnStr == "" {
		return nil, errors.New("pgds: PrimaryConnStr is required")
	}

	p := &Provider{
		primary: newDB(c.PrimaryConnStr, c.OnNotification),
	}

	if len(c.Secondaries) > 0 {
		p.secondaries = make(map[ds.ServerID]dbHandle, len(c.Secondaries))
		for id, connStr := range c.Secondaries {
			p.secondaries[id] = newDB(connStr, nil)
		}
	}

	return p, nil
}

//
// ---------- Provider ----------
//

type Provider struct {
	primary     dbHandle
	secondaries map[ds.ServerID]dbHandle
}

func (p *Provider) GetPrimary(
	ctx context.Context,
) (ds.PoolConn, ds.ServerID, error) {
	c, err := p.primary.acquire(ctx)
	if err != nil {
		return nil, "", err
	}
	return wrapPoolConn(c), PrimaryID, nil
}

// GetSecondary returns a secondary whose replay LSN
// is >= minLSN. If minLSN is empty, returns any secondary.
// Falls back to primary if no suitable replica is found.
func (p *Provider) GetSecondary(
	ctx context.Context,
	minLSN string,
) (ds.PoolConn, ds.ServerID, error) {
	if len(p.secondaries) == 0 {
		return p.GetPrimary(ctx)
	}

	// No LSN constraint: return first available replica
	if minLSN == "" {
		for id, db := range p.secondaries {
			c, err := db.acquire(ctx)
			if err == nil {
				return wrapPoolConn(c), id, nil
			}
		}
		return p.GetPrimary(ctx)
	}

	deadline := time.Now().Add(maxLSNWait)

	for time.Now().Before(deadline) {
		for id, db := range p.secondaries {
			pc, err := db.acquire(ctx)
			if err != nil {
				continue
			}

			ok, err := replicaHasLSN(ctx, pc.Conn(), minLSN)
			if err == nil && ok {
				return wrapPoolConn(pc), id, nil
			}

			pc.Release()
		}
		time.Sleep(lsnPollStep)
	}

	return p.GetPrimary(ctx)
}

func (p *Provider) Release(pc ds.PoolConn, _ ds.ServerID) {
	pc.Release()
}

func (p *Provider) Close() error {
	if p.primary != nil {
		_ = p.primary.close()
	}
	for _, s := range p.secondaries {
		_ = s.close()
	}
	return nil
}

//
// ---------- db ----------
//

type db struct {
	connStr string
	onNotif OnDBNotification

	mu   sync.Mutex
	pool *pgxpool.Pool
}

func newDB(connStr string, onNotif OnDBNotification) *db {
	return &db{
		connStr: connStr,
		onNotif: onNotif,
	}
}

func (d *db) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.pool == nil {
		cfg, err := pgxpool.ParseConfig(d.connStr)
		if err != nil {
			return nil, err
		}
		if d.onNotif != nil {
			cfg.ConnConfig.OnNotification = d.onNotif
		}
		d.pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}

	return d.pool.Acquire(ctx)
}

func (d *db) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.pool != nil {
		d.pool.Close()
		d.pool = nil
	}
	return nil
}

//
// ---------- PoolConn ----------
//

type poolConn struct {
	c *pgxpool.Conn
}

func wrapPoolConn(c *pgxpool.Conn) ds.PoolConn {
	return &poolConn{c: c}
}

func (p *poolConn) Conn() ds.Conn {
	return &pgConn{conn: p.c.Conn()}
}

func (p *poolConn) Release() {
	p.c.Release()
}

//
// ---------- Conn ----------
//

type pgConn struct {
	conn *pgx.Conn
}

func (c *pgConn) Exec(
	ctx context.Context,
	sql string,
	args ...any,
) (ds.ExecResult, error) {
	return c.conn.Exec(ctx, sql, args...)
}

func (c *pgConn) Query(
	ctx context.Context,
	sql string,
	args ...any,
) (ds.Rows, error) {
	rows, err := c.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgRows{rows: rows}, nil
}

func (c *pgConn) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) ds.Row {
	return &pgRow{row: c.conn.QueryRow(ctx, sql, args...)}
}

//
// ---------- Rows / Row ----------
//

type pgRows struct {
	rows pgx.Rows
}

func (r *pgRows) Close() error {
	r.rows.Close()
	return r.rows.Err()
}

func (r *pgRows) Err() error {
	return r.rows.Err()
}

func (r *pgRows) Next() bool {
	return r.rows.Next()
}

func (r *pgRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

type pgRow struct {
	row pgx.Row
}

func (r *pgRow) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

//
// ---------- LSN helper ----------
//
func replicaHasLSN(
	ctx context.Context,
	conn *pgx.Conn,
	minLSN string,
) (bool, error) {
	var ok bool
	err := conn.QueryRow(ctx, lsnCheckQuery, minLSN).Scan(&ok)
	return ok, err
}
