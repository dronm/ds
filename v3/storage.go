package ds

import "context"

type ServerID string

// ---------- Query results ----------

type Rows interface {
	Close() error
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type Row interface {
	Scan(dest ...any) error
}

type ExecResult interface {
	RowsAffected() int64
}

// ---------- Connections ----------

type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// PoolConn represents a leased connection from a pool.
type PoolConn interface {
	Conn() Conn
	Release()
}

// ---------- Storage ----------

type Storage interface {
	GetPrimary(ctx context.Context) (PoolConn, ServerID, error)
	GetSecondary(ctx context.Context, minLSN string) (PoolConn, ServerID, error)
	Release(PoolConn, ServerID)
}

