# Snapshots

Snapshots are derived read models for the `.firecube/` control plane. They make
inspection and cleanup commands faster, but the WAL under `.firecube/runs/` is
the authoritative state.

## Set The Product

Set the full product URI:

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
```

Pass the full URI through `--product-name` to bind the command directly to the
product without a storage configuration.

## Check Snapshot Status

```bash
firecube chunks snapshots status --product-name "$PRODUCT_URI"
```

Expected output when no snapshot exists:

```text
No snapshot found for MY_PRODUCT.zarr
```

Use JSON when automation needs to branch on `exists`:

```bash
firecube chunks snapshots status \
  --product-name "$PRODUCT_URI" \
  --format json
```

Expected output after a rebuild resembles:

```json
{
  "exists": true,
  "completed_before": "2026-06-03T15:54:27.100797+00:00",
  "age_human": "3m",
  "generation": "1780502101660654725",
  "records": 0
}
```

## Rebuild A Snapshot

Preview:

```bash
firecube chunks snapshots rebuild \
  --product-name "$PRODUCT_URI" \
  --dry-run
```

Expected output:

```text
[dry-run] Would rebuild snapshot for product 'MY_PRODUCT.zarr'
```

Rebuild:

```bash
firecube chunks snapshots rebuild --product-name "$PRODUCT_URI"
```

Expected output:

```text
Rebuilt snapshot for MY_PRODUCT.zarr: generation=1780502101660654725 records=4
```

Use JSON if you need the snapshot path:

```bash
firecube chunks snapshots rebuild \
  --product-name "$PRODUCT_URI" \
  --format json
```

Expected output resembles:

```json
{
  "product": "MY_PRODUCT.zarr",
  "generation": "1780502101660654725",
  "records": 4,
  "snapshot_path": "file:///data/products/MY_PRODUCT.zarr/.firecube/snapshots/snapshot-1780502101660654725.jsonl",
  "locked": false,
  "remote": false
}
```

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — confirm records after rebuild
- **[Delete And Reingest](delete.md)** — clean up after inspection
