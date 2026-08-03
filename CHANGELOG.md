# Changelog

## Unreleased

## v1.4.0 - 2026-07-27

### Changed

- **Requires k6 v2.** The extension now imports `go.k6.io/k6/v2` and builds against k6 v2.x. Binaries built with k6 v1.x or v0.5x are no longer supported: `xk6 build --with github.com/oleiade/xk6-kv@v1.4.0` will pull k6 v2. Stay on `v1.3.0` if you need to build against an older k6.

- **Minimum Go version raised to 1.25.**

- Dependencies bumped to latest: `go.etcd.io/bbolt` v1.5.0, `go.k6.io/k6/v2` v2.1.0, `github.com/grafana/sobek`.

### Fixed

- **Disk backend hangs when the database file is locked by another process.** `DiskStore.open()` now passes a 5-second lock timeout to `bolt.Open()` instead of waiting indefinitely. When another k6 process already holds the exclusive lock on the database file (e.g. the shared `.k6.kv`), opening now fails fast with an actionable error (`failed to open kv store ".k6.kv": timeout waiting for file lock (another k6 process may be using the same database file)`) so k6 exits with a non-zero code instead of hanging.

- **Disk backend bucket name regression.** The BoltDB bucket name has been corrected to `k6` (the original value used prior to the in-memory backend refactor). Versions that shipped after the regression used the database file path (`.k6.kv`) as the bucket name, which made data inaccessible to clients expecting the documented `k6` bucket.

  If you have an existing `.k6.kv` file created with a regressed version, its contents are stored in a bucket named `.k6.kv` and will not be visible to this release. Delete the `.k6.kv` file to start fresh.
