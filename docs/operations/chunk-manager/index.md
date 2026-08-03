# ChunkManager Operations

Use these pages to operate the `.firecube/` control plane for one product.
ChunkManager commands inspect and change product-local run history, span
records, write claims, snapshots, and cleanup state.

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
```

## Command Groups

| Command group | Use it for |
|---|---|
| `firecube chunks list` | Inspect ChunkManager records and span coverage. |
| `firecube chunks runs` | Inspect or abandon ingestion runs. |
| `firecube chunks claims` | Inspect or clear write-coordination claims. |
| `firecube chunks delete` | Delete tracked records and, when configured, storage data. |
| `firecube chunks delete-span` | Delete storage chunks described by span records. |
| `firecube chunks snapshots` | Check or rebuild snapshot read models. |

## Locate The Product

Pass the full product URI through `--product-name` so the command locates the
product directly:

```bash
firecube chunks list --product-name "$PRODUCT_URI"
```

Expected output resembles:

```text
Product      Key                       Type   Size (MB)  Date
-----------------------------------------------------------------------
MY_PRODUCT.zarr span_<run>_batch_0001...  span   0.0        2026-06-03 ...
MY_PRODUCT.zarr span_<run>_batch_0000...  span   0.0        2026-06-03 ...

Summary: 3 chunks, 0.0 MB total
```

`firecube chunks` may also print storage-configuration log lines when logging
is enabled. The command result is the table or JSON payload.

## Local Storage Preflight

Commands that delete storage data also need a storage type and driver. The full
URI still supplies the product location:

```bash
export FIRECUBE_STORAGE_TYPE=local
export FIRECUBE_STORAGE_DRIVER=fsspec
```

Without this storage configuration, `chunks delete` fails before deleting or
previewing matched storage records. Record-only deletion with `--manifest-only`
does not require it.

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — list records, runs, claims, or snapshots
- **[Recover Runs And Claims](recover.md)** — recover a stuck run or stale claim
- **[Delete And Reingest](delete.md)** — run cleanup or reingestion workflows
- **[Snapshots](snapshots.md)** — rebuild fast listings for chunk-manager commands
