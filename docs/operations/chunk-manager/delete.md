# Delete And Reingest

Use deletion commands when you need to remove old data, retry a failed range, or
replace a written span.

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
PRODUCT_NAME="MY_PRODUCT"
```

## Preflight

For local products, storage-deleting operations need local storage config:

```bash
export FIRECUBE_STORAGE_TYPE=local
export FIRECUBE_STORAGE_DRIVER=fsspec
export FIRECUBE_TARGET_PATH=/data/products
```

Always inspect before deleting:

```bash
firecube chunks list --product-name "$PRODUCT_NAME" --include-span
```

## Delete Tracked Records And Storage

Preview first:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_NAME" \
  --dry-run
```

Expected output:

```text
DRY RUN - Would delete:
  Chunks: 2
  Products: product.zarr
```

Delete after the dry-run matches your intent:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_NAME" \
  --yes-i-really-mean-it
```

Expected output:

```text
Deleted 2 chunks
```

Verify:

```bash
firecube chunks list --product-name "$PRODUCT_NAME"
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
  --product-name "$PRODUCT_NAME" \
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
  --product-name "$PRODUCT_NAME" \
  --range 2024-03-01,2024-03-02 \
  --dry-run
```

Delete it:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_NAME" \
  --range 2024-03-01,2024-03-02 \
  --yes-i-really-mean-it
```

`--start-date`, `--end-date`, and `--range` filter by ChunkManager record
timestamps. Use `chunks list --include-span` first when you need to confirm
semantic coverage.

## Delete Storage Chunks From Spans

Use `delete-span` when you want batch/span-level deletion from Zarr storage.

Preview:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_NAME" \
  --run-id docs-span-run \
  --dry-run
```

Expected output:

```text
DRY RUN: would delete 1 chunk keys from storage across 1 spans
```

Delete:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_NAME" \
  --run-id docs-span-run \
  --yes-i-really-mean-it
```

Expected output:

```text
Deleted 1 chunk keys from storage across 1 spans
```

Use `--force` only when you intentionally want to delete spans that are not
time-chunk aligned:

```bash
firecube chunks delete-span \
  --product-name "$PRODUCT_NAME" \
  --run-id docs-span-run \
  --force \
  --yes-i-really-mean-it
```

## Reingest A Range

Delete the range, then run ingestion again with `force_reingest`:

```bash
firecube chunks delete \
  --product-name "$PRODUCT_NAME" \
  --range 2024-03-01,2024-03-02 \
  --dry-run

firecube chunks delete \
  --product-name "$PRODUCT_NAME" \
  --range 2024-03-01,2024-03-02 \
  --yes-i-really-mean-it

firecube ingest <plugin> \
  --source /data/source/2024-03-01 \
  --target "$PRODUCT_URI" \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option force_reingest=true
```

Rebuild the snapshot after a large cleanup or reingest:

```bash
firecube chunks snapshots rebuild --product-name "$PRODUCT_NAME"
```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| Real `chunks delete` fails after dry-run worked | Storage config does not match the product. | Set `FIRECUBE_TARGET_PATH` locally or configure S3. |
| Span is not aligned | It does not map cleanly to Zarr time chunks. | Use `--force` only if expanded deletion is acceptable. |
| `does not contain expected time dimension` | The cube uses a custom time dimension that older span records do not capture. | Re-run with `--time-dim <name>` matching the cube layout. |
| `explicit time dimension ... contradicts` | `--time-dim` disagrees with the span record or cube metadata. | Drop `--time-dim`, or fix the span records if they are wrong. |
| Active claim blocks deletion | Another writer owns a write domain. | Use [Recover Runs And Claims](recover.md) before deletion. |

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — find records before deletion
- **[Recover Runs And Claims](recover.md)** — clear active runs or claims that block deletion
- **[Snapshots](snapshots.md)** — rebuild the read model after cleanup
