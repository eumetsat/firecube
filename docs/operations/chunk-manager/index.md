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
| `firecube chunks delete` | Delete tracked records, and the storage objects of records that have them. |
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

## Storage Configuration For Deletion

If you pass the full product URI to `--product-name`, `chunks delete` needs no
storage type, no storage driver and no `[storage]` section to name the product.
The command deletes the storage objects through a filesystem bound to that
product. This holds for a dry run and for a real run.

A `[storage]` target path in the configuration does not change where a run with
a product URI deletes.

The URI does not supply S3 access. If the URI starts with `s3://`, the command
uses the default driver and the normal credential chain of that driver. Anonymous
access is off. Make working credentials available before you run the command. If
you use a custom endpoint, set it in the storage configuration.

Two cases still need a storage configuration:

- A bare product name given to `--product-name`.
- `--all-products`.

For these, set the storage type and driver:

```bash
export FIRECUBE_STORAGE_TYPE=local
export FIRECUBE_STORAGE_DRIVER=fsspec
```

Without it, these runs fail before they delete or preview any record.
Record-only deletion with `--manifest-only` does not need it.

`chunks delete` also needs a scope. A run with neither `--product-name` nor
`--all-products` exits with code 2 and `MissingScope`. Pass one of the two flags.

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — list records, runs, claims, or snapshots
- **[Recover Runs And Claims](recover.md)** — recover a stuck run or stale claim
- **[Delete And Reingest](delete.md)** — run cleanup or reingestion workflows
- **[Snapshots](snapshots.md)** — rebuild fast listings for chunk-manager commands
