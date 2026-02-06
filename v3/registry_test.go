package ds_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dronm/ds/v3"
)

type fakeProvider struct{}

func (f *fakeProvider) GetPrimary(ctx context.Context) (ds.PoolConn, ds.ServerID, error) {
	return nil, "primary", nil
}
func (f *fakeProvider) GetSecondary(ctx context.Context, _ string) (ds.PoolConn, ds.ServerID, error) {
	return nil, "secondary", nil
}
func (f *fakeProvider) Release(ds.PoolConn, ds.ServerID) {}
func (f *fakeProvider) Close() error                     { return nil }

func TestProviderRegistry(t *testing.T) {
	const name = "fake"

	ds.Register(name, func(cfg any) (ds.Provider, error) {
		if cfg != "ok" {
			return nil, errors.New("bad cfg")
		}
		return &fakeProvider{}, nil
	})

	p, err := ds.NewProvider(name, "ok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected provider")
	}

	_, err = ds.NewProvider(name, "bad")
	if err == nil {
		t.Fatal("expected error for bad config")
	}
}

func TestUnknownProvider(t *testing.T) {
	_, err := ds.NewProvider("does-not-exist", nil)
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

