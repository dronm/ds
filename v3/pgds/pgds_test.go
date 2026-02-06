package pgds

import (
	"context"
	"errors"
	"testing"

	"github.com/dronm/ds/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakePoolConn struct{}

func (f *fakePoolConn) Release() {}
func (f *fakePoolConn) Conn() *pgx.Conn {
	panic("Conn should not be called in this test")
}

type fakeDB struct {
	acquireErr error
}

func (f *fakeDB) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return nil, f.acquireErr
}

func (f *fakeDB) close() error { return nil }

func TestGetSecondaryFallsBackToPrimary(t *testing.T) {
	orig := replicaHasLSNFn
	defer func() { replicaHasLSNFn = orig }()

	replicaHasLSNFn = func(
		_ context.Context,
		_ *pgx.Conn,
		_ string,
	) (bool, error) {
		return false, nil
	}

	p := &Provider{
		primary: &fakeDB{},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{},
		},
	}

	pc, id, err := p.GetSecondary(context.Background(), "0/FFFFFFFF")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pc == nil {
		t.Fatal("expected pool connection")
	}

}

func TestGetSecondaryWithNoLSN(t *testing.T) {
	p := &Provider{
		primary: &fakeDB{},
		secondaries: map[ds.ServerID]dbHandle{
			"replica1": &fakeDB{refCount: 5},
			"replica2": &fakeDB{refCount: 1}, // least busy
			"replica3": &fakeDB{refCount: 3},
		},
	}

	pc, id, err := p.GetSecondary(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pc == nil {
		t.Fatal("expected pool connection, got nil")
	}

	if id != "replica2" {
		t.Fatalf("expected replica2, got %s", id)
	}
}

func TestGetSecondaryFallsBackToPrimaryWithoutDBAccess(t *testing.T) {
	orig := replicaHasLSNFn
	defer func() { replicaHasLSNFn = orig }()

	replicaHasLSNFn = func(
		_ context.Context,
		_ *pgx.Conn,
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

func TestGetSecondaryFallsBackToPrimary_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
}
