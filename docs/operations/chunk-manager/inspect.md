# Inspect ChunkManager State

Use inspection commands before recovery or cleanup. They do not change product
state.

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
PRODUCT_NAME="MY_PRODUCT"
```

## List Records

List the records Firecube tracks for a product:

```bash
firecube chunks list --product-name "$PRODUCT_NAME"
```

Expected output resembles:

```text
Product      Key                       Type   Size (MB)  Date
-----------------------------------------------------------------------
product.zarr span_<run>_batch_0001...  span   0.0        2026-06-03 ...
product.zarr span_<run>_batch_0000...  span   0.0        2026-06-03 ...
product.zarr <run>_schema_verification schema_verification 0.0 2026-06-03 ...

Summary: 3 chunks, 0.0 MB total
```

Include span coverage:

```bash
firecube chunks list --product-name "$PRODUCT_NAME" --include-span
```

Expected output adds a `Span` column:

```text
Product      Key                       Type   Size (MB)  Date          Span
----------------------------------------------------------------------------
product.zarr span_<run>_batch_0001...  span   0.0        2026-06-03    50
product.zarr span_<run>_batch_0000...  span   0.0        2026-06-03    50
```

Use JSON for scripts:

```bash
firecube chunks list \
  --product-name "$PRODUCT_NAME" \
  --type span \
  --format json
```

Expected output includes the product, key, type, manifest URI, and metadata:

```json
[
  {
    "product": "product.zarr",
    "key": "span_<run>_batch_0001_data",
    "type": "span",
    "size_mb": 0.0,
    "manifest": "file:///data/products/product.zarr/.firecube",
    "meta": {
      "plugin": "direct_zarr_capable_test_plugin",
      "run_id": "<run>",
      "group": "data",
      "batch_id": "batch_0001"
    }
  }
]
```

## Filter By Coverage

Use `--time-range` when span records include `time_min` and `time_max` metadata:

```bash
firecube chunks list \
  --product-name "$PRODUCT_NAME" \
  --type span \
  --time-range 2024-03-15T00:00:00:2024-03-15T23:59:59
```

`--time-range` uses overlap semantics. A span is included when its time window
intersects the query window.

## List Runs

```bash
firecube chunks runs list --product-name "$PRODUCT_NAME"
```

Expected output resembles:

```text
Run ID                               Status    State   Parts Events
------------------------------------------------------------------------
direct_zarr_capable_test_plugin-...  complete  active  2     5
```

Filter by status:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_NAME" \
  --status complete \
  --format json
```

Expected output resembles:

```json
[
  {
    "product": "product.zarr",
    "run_id": "<run>",
    "status": "complete",
    "events": 5,
    "parts": 2,
    "stale": false,
    "error": null
  }
]
```

## List Claims

```bash
firecube chunks claims list --product-name "$PRODUCT_NAME"
```

If no writer currently owns a write domain, expected output is:

```text
No claims found.
```

When a claim exists, output resembles:

```text
Product       State   Owner                 Domain
---------------------------------------------------------------------------
product.zarr  active  run-123:F024          product.zarr:zarr_region:F024
```

## Check Snapshot Status

```bash
firecube chunks snapshots status --product-name "$PRODUCT_NAME"
```

If no snapshot exists:

```text
No snapshot found for product.zarr
```

After a rebuild, expected output resembles:

```text
Product:    product.zarr
Age:        3m
Cutoff:     2026-06-03T15:54:27.100797+00:00
Generation: 1780502101660654725
Records:    <count>
```

Use JSON for automation:

```bash
firecube chunks snapshots status \
  --product-name "$PRODUCT_NAME" \
  --format json
```

## Next Steps

- **[Recover Runs And Claims](recover.md)** — recover when a run or claim blocks ingestion
- **[Delete And Reingest](delete.md)** — remove data identified by inspection output
- **[Snapshots](snapshots.md)** — fix missing or stale snapshot status
