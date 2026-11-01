# Delete And Reingest

Use deletion commands when you need to remove old data, retry a failed range, or
replace a written span.

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
PRODUCT_NAME="MY_PRODUCT"
```

## Preflight

The full URI locates the product and the storage objects to delete. If you pass
it to `--product-name`, `chunks delete` needs no storage type and no storage
driver. It needs no `[storage]` section either. This holds for a dry run and a
real run.

The URI does not supply S3 access. For an `s3://` product, make working
credentials available through the normal credential chain of the default driver.
Anonymous access is off. If you use a custom endpoint, set it in the storage
configuration.

A bare product name and `--all-products` still need the storage type and driver.
For a local product:

```bash
export FIRECUBE_STORAGE_TYPE=local
export FIRECUBE_STORAGE_DRIVER=fsspec
```

`--manifest-only` does not need this storage configuration because it keeps the
product data.

Confirm no writer is using the product, then inspect the tracked records:

```bash
firecube chunks list --product-name "$PRODUCT_URI" --include-span
```

## Delete Tracked Records And Storage

Preview first:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_URI" \
  --dry-run
```

Expected output:

```text
DRY RUN - Would delete:
  Chunks: 2
  Products: MY_PRODUCT.zarr
Records without storage objects: 2 (their data stays in storage; use firecube chunks delete-span to delete span data)
```

Span, run, and schema records have no storage objects of their own: `chunks
delete` removes the records and leaves the data they describe in place. To
delete span data, use [`delete-span`](#delete-storage-chunks-from-spans).

Delete after the dry-run matches your intent:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_URI" \
  --yes-i-really-mean-it
```

Expected output:

```text
Deleted 2 chunks
Records without storage objects: 2 (their data stays in storage; use firecube chunks delete-span to delete span data)
```

Verify:

```bash
firecube chunks list --product-name "$PRODUCT_URI"
```

Expected output:

```text
No chunks found matching criteria.
```

## Delete Records Only

Use `--manifest-only` when the storage data should remain but Firecube should
forget the tracked records:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_URI" \
  --manifest-only \
  --yes-i-really-mean-it
```

Expected output:

```text
Deleted 2 chunks
```

## Delete By Date Range

Preview a date range:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_URI" \
  --range 2024-03-01,2024-03-02 \
  --dry-run
```

Delete it:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_URI" \
  --range 2024-03-01,2024-03-02 \
  --yes-i-really-mean-it
```

`--start-date`, `--end-date`, and `--range` filter by ChunkManager record
timestamps. Use `chunks list --include-span` first when you need to confirm
semantic coverage.

To filter by the data time range covered by each span (rather than the record
timestamp), use `--time-range` instead:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_URI" \
  --time-range 2024-03-01:2024-03-02 \
  --dry-run
```

`--time-range` deletes spans whose data-time window overlaps the given range.
Use it when you know the data timestamps you want to remove but not the exact
record timestamps stored in the ChunkManager.

## Delete Storage Chunks From Spans

Use `delete-span` to delete the data a span wrote. On a cube with a
timestamp-state array, as written by `GenericZarrIngestor`, it fills the span's
time steps with NaN and marks them deleted (state=2). On other cubes it deletes
the span's chunk keys.

Preview:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_URI" \
  --run-id docs-span-run \
  --dry-run
```

Expected output on a cube with a timestamp-state array:

```text
DRY RUN: would NaN-fill 1 spans and delete 0 chunk keys from storage
```

Delete:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_URI" \
  --run-id docs-span-run \
  --yes-i-really-mean-it
```

Expected output on a cube with a timestamp-state array:

```text
NaN-filled 1 spans and deleted 0 chunk keys from storage
```

On other cubes the output counts the deleted chunk keys instead, for example
`Deleted 1 chunk keys from storage across 1 spans`.

Arrays without the span's time dimension, such as static coordinate bounds
(`lat_bnds`, `lon_bnds`), are not touched. The command lists them under
`Warnings`. The command exits 1 when it reports `Errors`.

When chunk keys are deleted, use `--force` only when you intentionally want to
delete spans that are not time-chunk aligned:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_URI" \
  --run-id docs-span-run \
  --force \
  --yes-i-really-mean-it
```

## Reingest A Range

Two approaches are available. Choose based on whether the old values may stay
readable until the replacement is written.

**Delete then reingest**: clears the old values first. Use this when they must
not be read again, for example while replacement data is still being prepared.

Find the spans that cover the data time range. Each record's `meta` holds its
`run_id` and `batch_id`:

```bash
firecube chunks list \
  --product-name "$PRODUCT_URI" \
  --time-range 2024-03-01T00:00:00:2024-03-01T23:59:59 \
  --format json
```

Clear each span, then ingest its source data again:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_URI" \
  --run-id <run-id> \
  --batch-id <batch-id> \
  --dry-run

firecube chunks delete-span \
  --product-name "$PRODUCT_URI" \
  --run-id <run-id> \
  --batch-id <batch-id> \
  --yes-i-really-mean-it
```

Expected output:

```text
NaN-filled 1 spans and deleted 0 chunk keys from storage
```

```bash
firecube ingest <plugin> \
  --input-data file:///data/source/2024-03-01 \
  --target "$PRODUCT_URI" \
  --product-name "$PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option force_reingest=true
```

On a cube with a timestamp-state array, as written by `GenericZarrIngestor`,
`delete-span` fills the span's time steps with NaN and marks them deleted
(state=2); the ingest writes them again at the same timestamps. On other cubes
it deletes chunk keys instead, as in
[Delete Storage Chunks From Spans](#delete-storage-chunks-from-spans).

A span matches by its original batch window, and clearing it clears every time
step it still covers, so supply the source data for all of them.

**Reingest in place**: overwrites overlapping timestamps
in-place (region overwrite) and appends any new tail timestamps. No separate
delete step is required.

```bash
firecube ingest <plugin> \
  --input-data "file://$PWD/replacement-data" \
  --target "$PRODUCT_URI" \
  --product-name "$PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option force_reingest=true
```

Reingesting in place refuses insert attempts (timestamps that would land in the middle of
existing data), incoming or existing duplicates, and NaT values. State=2
(deleted) and state=3 (failed_batch) slots are refilled.

Rebuild the snapshot after a large cleanup or reingest:

```bash
firecube chunks snapshots rebuild --product-name "$PRODUCT_URI"
```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `Chunk operations require a full product URI or a [storage] configuration` | The run has a bare product name or `--all-products`, and no storage configuration. | Pass the full product URI to `--product-name`. Or set `FIRECUBE_STORAGE_TYPE` and `FIRECUBE_STORAGE_DRIVER`. |
| `MissingScope` (exit code 2) | The run has neither `--product-name` nor `--all-products`. | Pass `--product-name <product URI>` or `--all-products`. |
| Access denied or no credentials for an `s3://` product | The URI does not supply S3 credentials. | Make credentials available to the default driver. Set the endpoint in the storage configuration if you use one. |
| Span is not aligned | It does not map cleanly to Zarr time chunks. | Use `--force` only if expanded deletion is acceptable. |
| `does not contain expected time dimension` | The cube uses a custom time dimension that older span records do not capture. | Re-run with `--time-dim <name>` matching the cube layout. |
| `explicit time dimension ... contradicts` | `--time-dim` disagrees with the span record or cube metadata. | Drop `--time-dim`, or fix the span records if they are wrong. |
| Active claim blocks deletion | Another writer owns a write domain. | Use [Recover Runs And Claims](recover.md) before deletion. |

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — find records before deletion
- **[Recover Runs And Claims](recover.md)** — clear active runs or claims that block deletion
- **[Snapshots](snapshots.md)** — rebuild the read model after cleanup
