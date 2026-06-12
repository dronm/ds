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

Transactions are started from an acquired primary connection and exposed through the same query interface as a normal connection.

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
go get github.com/dronm/ds/v4

```
Import the provider you want to use:
```
import (
    "github.com/dronm/ds/v4"
    _ "github.com/dronm/ds/v4/pgds"
)
```
**PostgreSQL example**
```go
import (
    "context"
    "log"

    "github.com/dronm/ds/v4"
    "github.com/dronm/ds/v4/pgds"
)

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

ctx := ds.ContextWithProvider(context.Background(), prov)
```

**Write query (primary)**
```go
pc, id, err := ds.GetPrimary(ctx)
if err != nil {
    log.Fatal(err)
}
defer pc.Release()

_, err = pc.Conn().Exec(ctx,
    "INSERT INTO users(name) VALUES($1)",
    "alice",
)
_ = id
```


**Prepared statement**
```go
pc, _, err := ds.GetPrimary(ctx)
if err != nil {
	log.Fatal(err)
}
defer pc.Release()

stmt, err := pc.Conn().Prepare(ctx,
	"select_user_by_id",
	"SELECT name FROM users WHERE id=$1",
)
if err != nil {
	log.Fatal(err)
}

row := stmt.QueryRow(ctx, 1)
_ = row
```
Prepared statements are connection-scoped. Prepare and execute the statement before releasing the leased connection.

**Transaction**
```go
pc, _, err := ds.GetPrimary(ctx)
if err != nil {
    log.Fatal(err)
}
defer pc.Release()

err = ds.WithTx(ctx, pc.Conn(), func(ctx context.Context, tx ds.Tx) error {
    _, err := tx.Exec(ctx,
        "INSERT INTO users(name) VALUES($1)",
        "alice",
    )
    if err != nil {
        return err
    }

    _, err = tx.Exec(ctx,
        "INSERT INTO user_logs(user_name, action) VALUES($1, $2)",
        "alice",
        "created",
    )
    return err
})
if err != nil {
    log.Fatal(err)
}
```

**Read query (LSN-aware replica)**
```go
pc, id, err := ds.GetSecondary(ctx, lastKnownLSN)
if err != nil {
    log.Fatal(err)
}
defer pc.Release()

row := pc.Conn().QueryRow(ctx,
    "SELECT name FROM users WHERE id=$1",
    1,
)
_ = row
_ = id
```
If no replica has caught up to the requested LSN, the provider
automatically falls back to the primary.


### Context-bound provider

For HTTP applications, attach the provider once in middleware and use the package-level helpers deeper in the call tree:

```go
func WithDataSource(provider ds.Provider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := ds.ContextWithProvider(r.Context(), provider)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
```

Then service code can acquire connections without receiving the provider explicitly:

```go
func CreateUser(ctx context.Context, name string) error {
	pc, _, err := ds.GetPrimary(ctx)
	if err != nil {
		return err
	}
	defer pc.Release()

	_, err = pc.Conn().Exec(ctx,
		"INSERT INTO users(name) VALUES($1)",
		name,
	)
	return err
}
```

---

## LSN-based replica selection

When a minimum LSN is provided:

* replicas are checked for pg_last_wal_replay_lsn() >= minLSN
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

