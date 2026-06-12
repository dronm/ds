package pgds

import (
	"context"
	"errors"
	"testing"

	"github.com/dronm/ds/v4"
)

type fakeExecResult struct{}

func (r fakeExecResult) RowsAffected() int64 { return 0 }

type fakeRows struct{}

func (r fakeRows) Close() error           { return nil }
func (r fakeRows) Err() error             { return nil }
func (r fakeRows) Next() bool             { return false }
func (r fakeRows) Scan(dest ...any) error { return nil }

type fakeRow struct {
	value bool
	err   error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) > 0 {
		if b, ok := dest[0].(*bool); ok {
			*b = r.value
		}
	}

	return nil
}

type fakeTx struct{}

func (t *fakeTx) Exec(context.Context, string, ...any) (ds.ExecResult, error) {
	return fakeExecResult{}, nil
}

func (t *fakeTx) Query(context.Context, string, ...any) (ds.Rows, error) {
	return fakeRows{}, nil
}

func (t *fakeTx) QueryRow(context.Context, string, ...any) ds.Row {
	return fakeRow{}
}

func (t *fakeTx) Commit(context.Context) error   { return nil }
func (t *fakeTx) Rollback(context.Context) error { return nil }

type fakePreparedStatement struct {
	name string
}

func (s fakePreparedStatement) Exec(context.Context, ...any) (ds.ExecResult, error) {
	return fakeExecResult{}, nil
}

func (s fakePreparedStatement) Query(context.Context, ...any) (ds.Rows, error) {
	return fakeRows{}, nil
}

func (s fakePreparedStatement) QueryRow(context.Context, ...any) ds.Row {
	return fakeRow{}
}

func (s fakePreparedStatement) Name() string {
	return s.name
}

type fakeConn struct{}

func (c *fakeConn) Exec(context.Context, string, ...any) (ds.ExecResult, error) {
	return fakeExecResult{}, nil
}

func (c *fakeConn) Query(context.Context, string, ...any) (ds.Rows, error) {
	return fakeRows{}, nil
}

func (c *fakeConn) QueryRow(context.Context, string, ...any) ds.Row {
	return fakeRow{}
}

func (c *fakeConn) Prepare(
	context.Context,
	string,
	string,
) (ds.PreparedStatement, error) {
	return fakePreparedStatement{name: "stmt"}, nil
}

func (c *fakeConn) Begin(context.Context) (ds.Tx, error) {
	return &fakeTx{}, nil
}

type fakePoolConn struct {
	conn         ds.Conn
	releaseCount int
}

func (f *fakePoolConn) Release() {
	f.releaseCount++
}

func (f *fakePoolConn) Conn() ds.Conn {
	if f.conn != nil {
		return f.conn
	}

	return &fakeConn{}
}

type fakeDB struct {
	pc         ds.PoolConn
	acquireErr error
}

func (f *fakeDB) acquire(context.Context) (ds.PoolConn, error) {
	if f.acquireErr != nil {
		return nil, f.acquireErr
	}
	if f.pc != nil {
		return f.pc, nil
	}

	return &fakePoolConn{}, nil
}

func (f *fakeDB) close() error { return nil }

func TestGetSecondaryFallsBackToPrimary(t *testing.T) {
	orig := replicaHasLSNFn
	defer func() { replicaHasLSNFn = orig }()

	replicaHasLSNFn = func(
		_ context.Context,
		_ ds.Conn,
		_ string,
	) (bool, error) {
		return false, nil
	}

	primaryPC := &fakePoolConn{}

	p := &Provider{
		primary: &fakeDB{pc: primaryPC},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{pc: &fakePoolConn{}},
		},
	}

	pc, id, err := p.GetSecondary(context.Background(), "0/FFFFFFFF")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pc != primaryPC {
		t.Fatal("expected primary pool connection")
	}
	if id != PrimaryID {
		t.Fatalf("expected fallback to primary, got %s", id)
	}
}

func TestGetSecondaryWithNoLSN(t *testing.T) {
	p := &Provider{
		primary: &fakeDB{},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{},
			"replica2": &fakeDB{},
			"replica3": &fakeDB{},
		},
	}

	pc, id, err := p.GetSecondary(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pc == nil {
		t.Fatal("expected pool connection, got nil")
	}

	if id == "" || id == PrimaryID {
		t.Fatalf("expected a replica id, got %s", id)
	}
}

func TestGetSecondaryFallsBackToPrimaryWithoutDBAccess(t *testing.T) {
	orig := replicaHasLSNFn
	defer func() { replicaHasLSNFn = orig }()

	replicaHasLSNFn = func(
		_ context.Context,
		_ ds.Conn,
		_ string,
	) (bool, error) {
		return false, nil
	}

	p := &Provider{
		primary: &fakeDB{acquireErr: errors.New("no db")},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{acquireErr: errors.New("no db")},
		},
	}

	_, id, err := p.GetSecondary(context.Background(), "0/FFFFFFFF")
	if err == nil {
		t.Fatal("expected error")
	}
	if id != "" {
		t.Fatalf("expected no server id, got %s", id)
	}
}

func TestGetSecondaryReturnsCaughtUpReplica(t *testing.T) {
	orig := replicaHasLSNFn
	defer func() { replicaHasLSNFn = orig }()

	replicaHasLSNFn = func(
		_ context.Context,
		_ ds.Conn,
		_ string,
	) (bool, error) {
		return true, nil
	}

	replicaPC := &fakePoolConn{}

	p := &Provider{
		primary: &fakeDB{},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{pc: replicaPC},
		},
	}

	pc, id, err := p.GetSecondary(context.Background(), "0/FFFFFFFF")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pc != replicaPC {
		t.Fatal("expected replica pool connection")
	}
	if id != "replica1" {
		t.Fatalf("expected replica1, got %s", id)
	}
}

func TestGetSecondaryFallsBackWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &Provider{
		primary: &fakeDB{},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{},
		},
	}

	_, _, err := p.GetSecondary(ctx, "0/FFFFFFFF")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
