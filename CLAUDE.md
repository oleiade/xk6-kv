# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

`xk6-kv` is a [k6](https://k6.io) JS extension that exposes a thread-safe key-value store (`k6/x/kv`) for sharing state across Virtual Users during a load test. It plugs into k6 via xk6 and ships two backends — in-memory and BoltDB-on-disk — behind a common interface.

## Common commands

```bash
# Run the full test suite (matches CI invocation)
go test -race -timeout 800s ./...

# Run a single test
go test ./kv/store -run TestDiskStore_Get -race

# Run benchmarks
go test ./kv/store -bench=. -benchmem -run=^$

# Lint (CI pins golangci-lint v1.64; see .golangci.yml)
golangci-lint run

# Vet / build
go vet ./...
go build ./...

# Build a k6 binary with this extension wired in (what end users run)
go install go.k6.io/xk6/cmd/xk6@master
xk6 build --with github.com/oleiade/xk6-kv="."

# Run a JS example against the built binary
./k6 run examples/in-memory.js
./k6 run examples/disk.js
```

Note: `kv/store/disk_test.go` is guarded by `//go:build !windows` because BoltDB file-lock semantics differ on Windows — Windows CI only runs the non-disk tests.

## Architecture

The extension is registered once at process start by `register.go`, which calls `modules.Register("k6/x/kv", kv.New())`. Everything below hangs off that root module.

**Layered design (read these in order):**

1. **`kv/module.go` — `RootModule` / `ModuleInstance`.** The `RootModule` is process-wide and holds a single `store.Store` shared across all VUs. Each VU gets a `ModuleInstance`. The store is created lazily on the first `openKv()` call from JS — the first caller's `backend` and `serialization` options win; subsequent calls reuse the same store (this is intentional, since VUs need to see each other's writes).

2. **`kv/kv.go` — JS-facing `KV` type.** Each method (`Set`, `Get`, `List`, …) wraps the underlying `store.Store` call in a goroutine that resolves a `sobek.Promise`, so JS sees an async API. Values cross the JS/Go boundary via `sobek.Value.Export()` / `runtime.ToValue()`.

3. **`kv/store/store.go` — `Store` interface.** The contract every backend must satisfy: `Get/Set/Delete/Exists/Clear/Size/List/Close`. `List` takes `(prefix, limit)` and returns `[]Entry` ordered lexicographically.

4. **`kv/store/serialized_store.go` — `SerializedStore`.** Decorator that wraps a raw `Store` and a `Serializer`. This is what the module actually hands to `KV`; the raw backends only deal in `[]byte` / `string`, and `SerializedStore` handles `json.Marshal`/`Unmarshal` (or string passthrough) on the way in and out. When adding a new backend, return raw bytes from `Get`/`List` — do **not** deserialize inside the backend.

5. **Backends — `kv/store/memory.go` and `kv/store/disk.go`.**
   - `MemoryStore`: `map[string][]byte` behind a `sync.RWMutex`. `Close` is a no-op.
   - `DiskStore`: BoltDB at `.k6.kv` (constant `DefaultDiskStorePath`), single bucket `"k6"` (note: `bucket` is actually set to `DefaultDiskStorePath`, not `DefaultKvBucket` — see `disk.go:64`). Uses an `atomic.Bool` opened-flag, an `atomic.Int64` refcount, and a `sync.Mutex` to make `open()` idempotent and `Close()` reference-counted. The DB is opened lazily on the first operation, not at construction.

6. **`kv/store/serializer.go`.** Two implementations: `JSONSerializer` (default) round-trips through `encoding/json`; `StringSerializer` does raw string passthrough. Pick via the `serialization: "json" | "string"` option on `openKv()`.

**Adding a new backend:** implement `store.Store`, wire it into the `switch options.Backend` block in `kv/module.go:OpenKv`, and add it to the validation list in `NewOptionsFrom`. The `SerializedStore` decorator handles serialization for you.

**Error model.** `kv/errors.go` defines a small set of named errors (`DatabaseNotOpenError`, `KeyNotFoundError`, …) wrapped in a custom `Error` type that surfaces `{name, message}` to JS. Backend-level errors from BoltDB are wrapped with `fmt.Errorf("...: %w", err)` rather than being mapped to these names — only the JS-facing layer in `kv/kv.go` uses `NewError`.

## Conventions

- Go 1.23+; CI matrix tests 1.23.x and 1.24.x on Ubuntu and Windows.
- Lint config (`.golangci.yml`) is strict — `gochecknoglobals`, `forbidigo` (no `fmt.Print*`, no most `os.*`), `cyclop`, `funlen`, etc. Test files are exempted from several of these via `path: _(test|gen)\.go`.
- License is AGPL-3.0; module path is `github.com/oleiade/xk6-kv`.

## User preferences (from global CLAUDE.md)

- Prefer `jj` over `git` when available.
- Use conventional commit format.
- After implementing fixes, run build + tests + vet + lint before declaring done.
- Investigate and propose fixes rather than asking mid-flow; only stop on truly blocking decisions.
