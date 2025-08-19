package ds

import (
	"context"
)

type ServerID string

type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
	//Values() ([]any, error)
}

type Row interface {
	Scan(dest ...any) error
}

// CommandTag represents the result of an Exec command.
type ExecResult interface {
	RowsAffected() int64
	// Add more methods as needed
}

type PrepStatementDescr interface {
	GetName() string
	GetSQL() string
}

type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	Prepare(ctx context.Context, name, sql string) (PrepStatementDescr, error)
	ErrNoRows(err error) bool
	ErrConstraintViolation(err error) bool
	ErrForeignKeyViolation(err error) bool
}

type PoolConn interface {
	Conn() Conn
	Release()
}

type Storage interface {
	GetPrimary() (PoolConn, ServerID, error)
	Release(PoolConn, ServerID)
	ReleasePrimary(PoolConn)
	GetSecondary(string) (PoolConn, ServerID, error)
}
