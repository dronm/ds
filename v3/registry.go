// Package ds provides a pluggable data storage abstraction with
// support for primary and secondary servers.
package ds

import (
	"fmt"
	"sync"
)

type ProviderFactory func(cfg any) (Provider, error)

var (
	mu        sync.RWMutex
	factories = map[string]ProviderFactory{}
)

func Register(name string, factory ProviderFactory) {
	if factory == nil {
		panic("ds: provider factory is nil")
	}
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		panic("ds: provider already registered: " + name)
	}
	factories[name] = factory
}

// Provider creates Storage instances.
// It is configured once and then used by the application.
type Provider interface {
	Storage
	Close() error
}

func NewProvider(name string, cfg any) (Provider, error) {
	mu.RLock()
	factory, ok := factories[name]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("ds: unknown provider %q (forgotten import?)", name)
	}
	return factory(cfg)
}

