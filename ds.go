// Package ds (data storage) implements data storage privider for an application.
// Required provider must be imported with _ ds/... directive before usage.
// Provider costructor parameters must be supplied to NewProvider() function.
package ds

import (
	"fmt"
)

type Provider interface {
	InitProvider(provParams []any) error
}

var provides = make(map[string]Provider)

// Register makes data storage provider available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, provide Provider) {
	if provide == nil {
		panic("Register provider is nil")
	}
	if _, dup := provides[name]; dup {
		panic("Register called twice for provider " + name)
	}
	provides[name] = provide
}

// NewProvider is a provider construction
func NewProvider(providerName string, provParams ...any) (Provider, error) {
	provider, ok := provides[providerName]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q (forgotten import?)", providerName)
	}
	if len(provParams) > 0 {
		er := provider.InitProvider(provParams)
		if er != nil {
			return nil, er
		}
	}
	return provider, nil
}
