# Inspect ChunkManager State

Use inspection commands before recovery or cleanup. They do not change product
state.

## List Records

Pass the full URI through `--product-name` for every command on this page. This
binds the command directly to the product without a storage configuration.

List the records Firecube tracks for a product:

```bash
firecube chunks list --product-name "file:///data/products/MY_PRODUCT.zarr"
```

Expected output resembles:

```text
Product      Key                       Type   Size (MB)  Date
-----------------------------------------------------------------------
MY_PRODUCT.zarr span_<run>_batch_0001...  span   0.0        2026-06-03 ...
MY_PRODUCT.zarr span_<run>_batch_0000...  span   0.0        2026-06-03 ...
MY_PRODUCT.zarr <run>_schema_verification schema_verification 0.0 2026-06-03 ...

Summary: 3 chunks, 0.0 MB total
```

Include span coverage:

```bash
firecube chunks list --product-name "file:///data/products/MY_PRODUCT.zarr" --include-span
```

Expected output adds a `Span` column:

```text
Product      Key                       Type   Size (MB)  Date          Span
----------------------------------------------------------------------------
MY_PRODUCT.zarr span_<run>_batch_0001...  span   0.0        2026-06-03    50
MY_PRODUCT.zarr span_<run>_batch_0000...  span   0.0        2026-06-03    50
```

Use JSON for scripts:

```bash
firecube chunks list \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --type span \
  --format json
```

Expected output includes the product, key, type, manifest URI, and metadata:

```json
[
  {
    "product": "MY_PRODUCT.zarr",
    "key": "span_<run>_batch_0001_data",
    "type": "span",
    "size_mb": 0.0,
    "manifest": "file:///data/products/MY_PRODUCT.zarr/.firecube",
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
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --type span \
  --time-range 2024-03-15T00:00:00:2024-03-15T23:59:59
```

`--time-range` uses overlap semantics. A span is included when its time window
intersects the query window.

## List Runs

```bash
firecube chunks runs list \
  --product-name "file:///data/products/MY_PRODUCT.zarr"
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
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --status complete \
  --format json
```

Expected output resembles:

```json
[
  {
    "product": "MY_PRODUCT.zarr",
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
firecube chunks claims list --product-name "file:///data/products/MY_PRODUCT.zarr"
```

If no writer currently owns a write domain, expected output is:

```text
No claims found.
```

When a claim exists, output resembles:

```text
Product       State   Owner                 Domain
---------------------------------------------------------------------------
MY_PRODUCT.zarr  active  run-123:F024       MY_PRODUCT:zarr_region:F024
```

## Check The Compact Cache Status

```bash
firecube chunks compact status --product-name "file:///data/products/MY_PRODUCT.zarr"
```

If no compact cache exists:

```text
No compact cache found for MY_PRODUCT.zarr
```

After a rebuild, expected output resembles:

```text
Product:    MY_PRODUCT.zarr
Age:        3m
Cutoff:     2026-06-03T15:54:27.100797+00:00
Generation: 1780502101660654725
Records:    <count>
```

Use JSON for automation:

```bash
firecube chunks compact status \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --format json
```

## Next Steps

- **[Recover Runs And Claims](recover.md)** — recover when a run or claim blocks ingestion
- **[Delete And Reingest](delete.md)** — remove data identified by inspection output
- **[Compact Cache](compact.md)** — fix a missing or stale compact cache
