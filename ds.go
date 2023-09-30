// Package ds (data storage) implements data storage privider schema.
// Required provider must be imported with _ ds/... directive before usage.
// Provider costructor parameters must be supplied to NewProvider() function.
package ds

import (
	"fmt"
)

type Provider interface {
	InitProvider(provParams []interface{}) error
}

var provides = make(map[string]Provider)


// Register makes dataStorage provider available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, provide Provider) {
	if provide == nil {
		panic("dataStorage: Register provide is nil")
	}
	if _, dup := provides[name]; dup {
		panic("dataStorage: Register called twice for provide " + name)
	}
	provides[name] = provide
}

// NewProvider
func NewProvider(provideName string, provParams ...interface{}) (Provider, error) {
	provider, ok := provides[provideName]
	if !ok {
		return nil, fmt.Errorf("dataStorage: unknown provide %q (forgotten import?)", provideName)
	}
	if len(provParams) > 0 {
		er := provider.InitProvider(provParams)
		if er != nil {
			return nil,er
		}
	}
	return provider, nil
}

