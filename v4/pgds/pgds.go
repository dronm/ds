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

	"github.com/dronm/ds/v4"
)

var replicaHasLSNFn = replicaHasLSN

var ErrNoPrimaryPool = errors.New("primary pool is not available")

const (
	ProviderID = "pg"
	PrimaryID  = ds.ServerID("primary")
)

const (
	lsnCheckQuery = `
SELECT coalesce(
	pg_wal_lsn_diff(
		pg_last_wal_replay_lsn(),
		$1
	),
	0
) >= 0
`
	maxLSNWait  = 300 * time.Millisecond
	lsnPollStep = 50 * time.Millisecond
)

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
		p.secondaries = make(map[ds.ServerID]*db, len(c.Secondaries))
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
	primary     *db
	secondaries map[ds.ServerID]*db
}

func (p *Provider) PrimaryPool() (*pgxpool.Pool, error) {
	if p == nil || p.primary == nil {
		return nil, ErrNoPrimaryPool
	}

	p.primary.mu.Lock()
	defer p.primary.mu.Unlock()

	if p.primary.pool == nil {
		return nil, ErrNoPrimaryPool
	}

	return p.primary.pool, nil
}

func (p *Provider) GetPrimary(
	ctx context.Context,
) (ds.PoolConn, ds.ServerID, error) {
	c, err := p.primary.acquire(ctx)
	if err != nil {
		return nil, "", err
	}

	return c, PrimaryID, nil
}

// GetSecondary returns a secondary whose replay LSN
// is >= minLSN. If minLSN is empty, returns any secondary.
// Falls back to primary if no suitable replica is found.
func (p *Provider) GetSecondary(
	ctx context.Context,
	minLSN string,
) (ds.PoolConn, ds.ServerID, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	if len(p.secondaries) == 0 {
		return p.GetPrimary(ctx)
	}

	// No LSN constraint: return first available replica.
	if minLSN == "" {
		for id, db := range p.secondaries {
			c, err := db.acquire(ctx)
			if err == nil {
				return c, id, nil
			}
		}

		return p.GetPrimary(ctx)
	}

	deadline := time.Now().Add(maxLSNWait)

	for {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if !time.Now().Before(deadline) {
			break
		}

		for id, db := range p.secondaries {
			if err := ctx.Err(); err != nil {
				return nil, "", err
			}

			pc, err := db.acquire(ctx)
			if err != nil {
				continue
			}

			ok, err := replicaHasLSNFn(ctx, pc.Conn(), minLSN)
			if err == nil && ok {
				return pc, id, nil
			}

			pc.Release()
		}

		if !sleepWithContext(ctx, time.Until(deadline)) {
			return nil, "", ctx.Err()
		}
	}

	return p.GetPrimary(ctx)
}

func (p *Provider) Release(pc ds.PoolConn, _ ds.ServerID) {
	if pc != nil {
		pc.Release()
	}
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

func sleepWithContext(ctx context.Context, remaining time.Duration) bool {
	if remaining <= 0 {
		return true
	}

	wait := lsnPollStep
	if remaining < wait {
		wait = remaining
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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

func (d *db) acquire(ctx context.Context) (ds.PoolConn, error) {
	pool, err := d.getPool(ctx)
	if err != nil {
		return nil, err
	}

	c, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return wrapPoolConn(c), nil
}

func (d *db) getPool(ctx context.Context) (*pgxpool.Pool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.pool != nil {
		return d.pool, nil
	}

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

	return d.pool, nil
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
	if p == nil || p.c == nil {
		return
	}

	p.c.Release()
	p.c = nil
}

//
// ---------- Conn ----------
//

type pgConn struct {
	conn *pgx.Conn
}

var _ ds.Conn = (*pgConn)(nil)

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

func (c *pgConn) Prepare(
	ctx context.Context,
	name string,
	sql string,
) (ds.PreparedStatement, error) {
	_, err := c.conn.Prepare(ctx, name, sql)
	if err != nil {
		return nil, err
	}

	return &pgPreparedStatement{
		queryer: c,
		name:    name,
	}, nil
}

func (c *pgConn) Begin(ctx context.Context) (ds.Tx, error) {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &pgTx{
		tx: tx,
	}, nil
}

//
// ---------- Prepared statement ----------
//

type pgPreparedStatement struct {
	queryer ds.Querier
	name    string
}

var _ ds.PreparedStatement = (*pgPreparedStatement)(nil)

func (s *pgPreparedStatement) Exec(
	ctx context.Context,
	args ...any,
) (ds.ExecResult, error) {
	return s.queryer.Exec(ctx, s.name, args...)
}

func (s *pgPreparedStatement) Query(
	ctx context.Context,
	args ...any,
) (ds.Rows, error) {
	return s.queryer.Query(ctx, s.name, args...)
}

func (s *pgPreparedStatement) QueryRow(
	ctx context.Context,
	args ...any,
) ds.Row {
	return s.queryer.QueryRow(ctx, s.name, args...)
}

func (s *pgPreparedStatement) Name() string {
	return s.name
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
	err := r.rows.Scan(dest...)
	if err == nil {
		return nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return ds.ErrNoRows
	}
	return err
}

type pgRow struct {
	row pgx.Row
}

func (r *pgRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if err == nil {
		return nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return ds.ErrNoRows
	}
	return err
}

//

type pgTx struct {
	tx pgx.Tx
}

var _ ds.Tx = (*pgTx)(nil)

func (t *pgTx) Exec(
	ctx context.Context,
	sql string,
	args ...any,
) (ds.ExecResult, error) {
	return t.tx.Exec(ctx, sql, args...)
}

func (t *pgTx) Query(
	ctx context.Context,
	sql string,
	args ...any,
) (ds.Rows, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &pgRows{
		rows: rows,
	}, nil
}

func (t *pgTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) ds.Row {
	return &pgRow{
		row: t.tx.QueryRow(ctx, sql, args...),
	}
}

func (t *pgTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *pgTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// ---------- LSN helper ----------
func replicaHasLSN(
	ctx context.Context,
	conn ds.Conn,
	minLSN string,
) (bool, error) {
	var ok bool
	err := conn.QueryRow(ctx, lsnCheckQuery, minLSN).Scan(&ok)
	return ok, err
}
