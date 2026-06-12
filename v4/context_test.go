package ds_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dronm/ds/v4"
)

type contextFakeProvider struct{}

func (f *contextFakeProvider) GetPrimary(ctx context.Context) (ds.PoolConn, ds.ServerID, error) {
	return nil, "primary", nil
}
func (f *contextFakeProvider) GetSecondary(ctx context.Context, _ string) (ds.PoolConn, ds.ServerID, error) {
	return nil, "secondary", nil
}
func (f *contextFakeProvider) Release(ds.PoolConn, ds.ServerID) {}
func (f *contextFakeProvider) Close() error                     { return nil }

func TestContextAccessors(t *testing.T) {
	provider := &contextFakeProvider{}
	ctx := ds.ContextWithProvider(context.Background(), provider)

	gotProvider, ok := ds.ProviderFromContext(ctx)
	if !ok {
		t.Fatal("expected provider in context")
	}
	if gotProvider != provider {
		t.Fatal("unexpected provider from context")
	}

	_, primaryID, err := ds.GetPrimary(ctx)
	if err != nil {
		t.Fatalf("unexpected primary error: %v", err)
	}
	if primaryID != "primary" {
		t.Fatalf("expected primary, got %s", primaryID)
	}

	_, secondaryID, err := ds.GetSecondary(ctx, "0/1")
	if err != nil {
		t.Fatalf("unexpected secondary error: %v", err)
	}
	if secondaryID != "secondary" {
		t.Fatalf("expected secondary, got %s", secondaryID)
	}
}

func TestContextAccessorsWithoutProvider(t *testing.T) {
	_, _, err := ds.GetPrimary(context.Background())
	if !errors.Is(err, ds.ErrNoProviderInContext) {
		t.Fatalf("expected ErrNoProviderInContext, got %v", err)
	}
}
