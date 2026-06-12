package ds_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dronm/ds/v4"
)

type withTxExecResult struct{}

func (r withTxExecResult) RowsAffected() int64 { return 0 }

type withTxRows struct{}

func (r withTxRows) Close() error           { return nil }
func (r withTxRows) Err() error             { return nil }
func (r withTxRows) Next() bool             { return false }
func (r withTxRows) Scan(dest ...any) error { return nil }

type withTxRow struct{}

func (r withTxRow) Scan(dest ...any) error { return nil }

type withTxPreparedStatement struct {
	name string
}

func (s withTxPreparedStatement) Exec(context.Context, ...any) (ds.ExecResult, error) {
	return withTxExecResult{}, nil
}

func (s withTxPreparedStatement) Query(context.Context, ...any) (ds.Rows, error) {
	return withTxRows{}, nil
}

func (s withTxPreparedStatement) QueryRow(context.Context, ...any) ds.Row {
	return withTxRow{}
}

func (s withTxPreparedStatement) Name() string {
	return s.name
}

type withTxConn struct {
	tx       *withTxTx
	beginErr error
}

func (c *withTxConn) Exec(context.Context, string, ...any) (ds.ExecResult, error) {
	return withTxExecResult{}, nil
}

func (c *withTxConn) Query(context.Context, string, ...any) (ds.Rows, error) {
	return withTxRows{}, nil
}

func (c *withTxConn) QueryRow(context.Context, string, ...any) ds.Row {
	return withTxRow{}
}

func (c *withTxConn) Prepare(
	context.Context,
	string,
	string,
) (ds.PreparedStatement, error) {
	return withTxPreparedStatement{name: "stmt"}, nil
}

func (c *withTxConn) Begin(context.Context) (ds.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}

	return c.tx, nil
}

type withTxTx struct {
	commitErr  error
	committed  bool
	rolledBack bool
}

func (t *withTxTx) Exec(context.Context, string, ...any) (ds.ExecResult, error) {
	return withTxExecResult{}, nil
}

func (t *withTxTx) Query(context.Context, string, ...any) (ds.Rows, error) {
	return withTxRows{}, nil
}

func (t *withTxTx) QueryRow(context.Context, string, ...any) ds.Row {
	return withTxRow{}
}

func (t *withTxTx) Commit(context.Context) error {
	t.committed = true

	return t.commitErr
}

func (t *withTxTx) Rollback(context.Context) error {
	t.rolledBack = true

	return nil
}

func TestWithTxCommitsOnSuccess(t *testing.T) {
	tx := &withTxTx{}
	conn := &withTxConn{tx: tx}

	err := ds.WithTx(context.Background(), conn, func(ctx context.Context, tx ds.Tx) error {
		_, err := tx.Exec(ctx, "SELECT 1")

		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tx.committed {
		t.Fatal("expected commit")
	}
	if tx.rolledBack {
		t.Fatal("did not expect rollback")
	}
}

func TestWithTxRollsBackOnCallbackError(t *testing.T) {
	tx := &withTxTx{}
	conn := &withTxConn{tx: tx}
	expectedErr := errors.New("callback failed")

	err := ds.WithTx(context.Background(), conn, func(context.Context, ds.Tx) error {
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
	if tx.committed {
		t.Fatal("did not expect commit")
	}
	if !tx.rolledBack {
		t.Fatal("expected rollback")
	}
}

func TestWithTxReturnsCommitError(t *testing.T) {
	expectedErr := errors.New("commit failed")
	tx := &withTxTx{commitErr: expectedErr}
	conn := &withTxConn{tx: tx}

	err := ds.WithTx(context.Background(), conn, func(context.Context, ds.Tx) error {
		return nil
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected commit error, got %v", err)
	}
	if !tx.committed {
		t.Fatal("expected commit attempt")
	}
	if !tx.rolledBack {
		t.Fatal("expected rollback after commit error")
	}
}
