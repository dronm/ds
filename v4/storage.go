package ds

import (
	"context"
	"errors"
)

var ErrNoRows = errors.New("no rows in result set")

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

type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

type PreparedStatement interface {
	Exec(ctx context.Context, args ...any) (ExecResult, error)
	Query(ctx context.Context, args ...any) (Rows, error)
	QueryRow(ctx context.Context, args ...any) Row
	Name() string
}

type Tx interface {
	Querier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Conn interface {
	Querier
	Prepare(ctx context.Context, name string, sql string) (PreparedStatement, error)
	Begin(ctx context.Context) (Tx, error)
}

// WithTx starts a transaction, executes fn, and commits when fn returns nil.
//
// If fn returns an error, the transaction is rolled back and the original error
// is returned. If fn panics, the transaction is rolled back and the panic is
// re-thrown.
func WithTx(
	ctx context.Context,
	conn Conn,
	fn func(ctx context.Context, tx Tx) error,
) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}

		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err = fn(ctx, tx); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	return nil
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
