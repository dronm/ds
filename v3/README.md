# ds

`ds` is a small, pluggable data storage abstraction for Go applications.
It provides a common interface for working with a **primary** database and
optional **secondary (replica)** databases.

The package is designed for applications that need:
- clear separation between write and read connections
- optional read scaling via replicas
- provider-based storage backends (e.g. PostgreSQL)

---

## Design goals

- Explicit primary / secondary access
- Provider-based architecture (no global singletons)
- Typed configuration
- Context-aware connection handling
- Minimal abstraction over real drivers
- Safe resource lifecycle management

`ds` is not an ORM and does not attempt to hide SQL or driver behavior.

---

## Architecture overview

- **Provider**  
  A configured storage backend instance (e.g. PostgreSQL).

- **Storage**  
  Exposes methods to acquire connections to primary or secondary servers.

- **PoolConn**  
  A leased connection from a pool that must be released explicitly.

- **Conn**  
  A minimal interface for executing queries.

Providers are registered via a factory and instantiated explicitly.

---

## Core packages

### `ds`

Defines:
- storage interfaces
- provider registry
- provider lifecycle

This package contains no database-specific code.

### `ds/pgds`

PostgreSQL implementation based on `pgx` / `pgxpool`.

Features:
- primary + replica topology
- LSN-aware replica selection
- safe fallback to primary
- LISTEN / NOTIFY support

---

## Installation

```sh
go get github.com/dronm/ds/v3

```
Import the provider you want to use:
```
import (
    "github.com/dronm/ds/v3"
    _ "github.com/dronm/ds/v3/pgds"
)
```
**PostgreSQL example**
```
prov, err := ds.NewProvider("pg", &pgds.Config{
    PrimaryConnStr: "postgres://user:pass@primary/db",
    Secondaries: map[ds.ServerID]string{
        "replica1": "postgres://user:pass@replica1/db",
    },
})
if err != nil {
    log.Fatal(err)
}
defer prov.Close()
```

**Write query (primary)**
```
import ds "github.com/dronm/ds/v3"

pc, id, err := ds.GetPrimary(ctx)
if err != nil {
    log.Fatal(err)
}
defer ds.Release(pc, id)

_, err = pc.Conn().Exec(ctx,
    "INSERT INTO users(name) VALUES($1)",
    "alice",
)
```

**Read query (LSN-aware replica)**
```
import ds "github.com/dronm/ds/v3"

pc, id, err := ds.GetSecondary(ctx, lastKnownLSN)
if err != nil {
    log.Fatal(err)
}
defer ds.Release(pc, id)

row := pc.Conn().QueryRow(ctx,
    "SELECT name FROM users WHERE id=$1",
    1,
)
```
If no replica has caught up to the requested LSN, the provider
automatically falls back to the primary.

---

## LSN-based replica selection

When a minimum LSN is provided:

* replicas are checked for pg_last_wal_receive_lsn() >= minLSN
* the provider waits briefly for replicas to catch up
* if none qualify, the primary is returned

This allows safe read-after-write consistency when needed.

---

## Non-goals

* ORM or query builder
* automatic transaction routing
* hiding SQL or database behavior
* cross-database query compatibility

---

## License

**MIT**

