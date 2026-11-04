# Create A Release

Use `firecube release create` to name one state of the metadata of a product.

A release is a named record. It binds a version string to a coherent snapshot of
the metadata. It also lists the runs since the previous release and the producer
of the latest run that recorded one. `release create` stamps the root attribute
`firecube_release` with the version.

A release names metadata only. It does not cover chunk bytes. To compare the
metadata with a release later, see [Verify A Release](verify-a-release.md).

## Prerequisites

- Stop every writer of the product. See
  [Maintenance Operations](index.md#prerequisites).
- Make sure that the product has a control plane. If it has none, run
  `firecube migrate adopt` first. See
  [Adopt A Foreign Store](adopt-a-foreign-store.md).

## Create A Release

```bash
firecube release create 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT \
  --note "first release"
```

Expected output for a Zarr v2 store with a consolidated `.zmetadata`:

```text
wrote .zattrs, .zmetadata
snapshot dadfa2e8c110e0fd…
recorded release '1.0.0' at snapshot dadfa2e8c110e0fd…
release '1.0.0' is current
release '1.0.0' created in run release-bb4fa223-…
Release:      1.0.0
Created:      2026-11-04T00:31:51+00:00
Previous:     -
Snapshot:     dadfa2e8c110e0fd…
Run:          release-bb4fa223-…
Producer:     unknown
Note:         first release
Runs since:   3
  adopt-345bfd72-…
  migrate-2e97c56b-…
  release-bb4fa223-…
```

On a Zarr v3 store, the first line is `wrote zarr.json`. The other lines keep
their shape. The `Runs since` list holds the runs of your store.

`Producer: unknown` means that no run of the product recorded a producer.

Firecube keeps the version as you give it, and each version is used once. If
you repeat `release create` for an existing version and the metadata still
equals its snapshot, the command prints `release '1.0.0' is current already`.
It exits 0 and writes nothing.

## List And Show Releases

```bash
firecube release list \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

The command prints one line per release, oldest first. A `*` marks the current
release:

```text
* 1.0.0  2026-11-04T00:31:51+00:00  snapshot=dadfa2e8c110e0fd…  producer=unknown
```

A product with no release prints `No releases` and exits 0.

Show one release with its note and the runs since the previous release:

```bash
firecube release show 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

Without a version, the command shows the current release. It exits 1 when the
release does not exist. If a migration step created the release, the list of
runs includes the migrate run.

## Recover From An Interrupted Create

Run the same `release create` command again. The rerun resumes from the last
completed step. If the first run stamped the root attribute but did not record
the release, the rerun continues from the snapshot step:

```text
run release-cec9a4c8-… stamped release '1.0' before; resuming from the snapshot
```

If a record exists but the release is not current, the rerun completes the
release. It does so only while the live metadata still equals the snapshot of
the record.

A release process that is killed outright leaves its run in the status
`started` and the exclusive claim in place. The next `release create` is
refused until you retire that run. This differs from an interrupted
[migration](migrate.md#recover-from-an-interrupted-run), which you resume. A
new `release create` always starts a new run, so abandon the old run.

1. Make sure that no process of the run is still running.
2. Abandon the run. This also releases the claim that the run owns:

   ```bash
   firecube chunks runs abandon \
     --product-name "file:///data/products/MY_PRODUCT.zarr" \
     --run-id <run-id> \
     --reason "release process killed" \
     --yes-i-really-mean-it
   ```

3. If the claim names no recorded run, clear it:

   ```bash
   firecube chunks claims clear \
     --product-name "file:///data/products/MY_PRODUCT.zarr" \
     --domain MY_PRODUCT:maintenance:exclusive \
     --force --yes-i-really-mean-it
   ```

4. Run the same `release create` command again.

See [Recover Runs And Claims](../chunk-manager/recover.md) for more detail.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `release '…' exists and names snapshot …, and the store's metadata no longer matches it` | The version exists, and the metadata changed since. | Choose another version. |
| `release version '…' cannot be stored` | The version has an ASCII control character, or the record of the release does not fit the storage limits. | Use a shorter version without control characters. See the limits below this table. |
| `Write-domain claim conflict for …:maintenance:exclusive` | Another run or a crash holds the claim. | Follow [Recover From An Interrupted Create](#recover-from-an-interrupted-create). |
| `stamped release '…' and did not complete; run that step again` | A migration step that creates a release stopped before it recorded the release. | Run the same `migrate run` command again. |
| `product … has no release '…'` | The version does not exist. | Run `firecube release list`. |

The storage limits on a version string are:

- On a local file system, the record name must fit the name limit of the file
  system minus 21 bytes. With the usual limit of 255 bytes, the version takes
  at most 229 bytes after Firecube escapes `%`, `/` and a leading `.`.
- On a local file system, the full path of the record plus 21 bytes must stay
  below the path limit. A long path to the product lowers the version limit.
- On S3, the whole object key must fit 1024 bytes. The key must not contain
  `?versionId=`.

## Next Steps

- **[Verify A Release](verify-a-release.md)** — detect a change of the metadata since the release
- **[Migrate Metadata](migrate.md)** — change attributes before a new release
- **[Maintenance Operations](index.md)** — overview of the command groups
