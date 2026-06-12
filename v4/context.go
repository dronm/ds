package ds

import (
	"context"
	"errors"
)

var ErrNoProviderInContext = errors.New("ds: provider not found in context")

type providerContextKey struct{}

var providerKey providerContextKey

// ContextWithProvider returns a child context containing provider.
//
// It is intended for application roots and HTTP/RPC middleware where passing the
// provider explicitly through every call would be noisy.
func ContextWithProvider(ctx context.Context, provider Provider) context.Context {
	if provider == nil {
		panic("ds: provider is nil")
	}

	return context.WithValue(ctx, providerKey, provider)
}

// ProviderFromContext returns the Provider stored in ctx.
func ProviderFromContext(ctx context.Context) (Provider, bool) {
	if ctx == nil {
		return nil, false
	}

	provider, ok := ctx.Value(providerKey).(Provider)
	return provider, ok
}

// GetPrimary acquires a primary connection from the Provider stored in ctx.
func GetPrimary(ctx context.Context) (PoolConn, ServerID, error) {
	provider, ok := ProviderFromContext(ctx)
	if !ok {
		return nil, "", ErrNoProviderInContext
	}

	return provider.GetPrimary(ctx)
}

// GetSecondary acquires a secondary connection from the Provider stored in ctx.
//
// If minLSN is not empty, the provider should return a replica that has replayed
// at least that LSN, or fall back to the primary according to provider policy.
func GetSecondary(ctx context.Context, minLSN string) (PoolConn, ServerID, error) {
	provider, ok := ProviderFromContext(ctx)
	if !ok {
		return nil, "", ErrNoProviderInContext
	}

	return provider.GetSecondary(ctx, minLSN)
}
