# Changelog

## Unreleased

### Fixed

- **Disk backend bucket name regression.** The BoltDB bucket name has been corrected to `k6` (the original value used prior to the in-memory backend refactor). Versions that shipped after the regression used the database file path (`.k6.kv`) as the bucket name, which made data inaccessible to clients expecting the documented `k6` bucket.

  If you have an existing `.k6.kv` file created with a regressed version, its contents are stored in a bucket named `.k6.kv` and will not be visible to this release. Delete the `.k6.kv` file to start fresh.
