# ChunkManager Operations

Use these pages to operate the `.firecube/` control plane for one product.
ChunkManager commands inspect and change product-local run history, span
records, write claims, the compact cache, and cleanup state.

## Command Groups

| Command group | Use it for |
|---|---|
| `firecube chunks list` | Inspect ChunkManager records and span coverage. |
| `firecube chunks runs` | Inspect or abandon ingestion runs. |
| `firecube chunks claims` | Inspect or clear write-coordination claims. |
| `firecube chunks delete` | Delete tracked records, and the storage objects of records that have them. |
| `firecube chunks delete-span` | Delete storage chunks described by span records. |
| `firecube chunks compact` | Check or rebuild the compact cache. |

## Locate The Product

Pass the product URI through `-t/--target` so the command locates the product
directly. Every ChunkManager command reads the product label back from the store.
If the store records no label, the command exits 2 and names `firecube migrate adopt`.

```bash
firecube chunks list --target "file:///data/products/MY_PRODUCT.zarr"
```

Expected output resembles:

```text
Product    Key                       Type   Size (MB)  Date
------------------------------------------------------------------
MY_PRODUCT span_<run>_batch_0001...  span   0.0        2026-06-03 ...
MY_PRODUCT span_<run>_batch_0000...  span   0.0        2026-06-03 ...

Summary: 3 chunks, 0.0 MB total
```

`firecube chunks` may also print storage-configuration log lines when logging
is enabled. The command result is the table or JSON payload.

## Storage Configuration For Deletion

If you pass the product URI to `--target`, the URI names the product.
Then `chunks delete` needs no storage type, no storage driver and no `[storage]`
section. The command deletes the storage objects through a filesystem bound to
that product. This holds for a dry run and for a real run.

A `[storage]` target path in the configuration does not change where a run with
a product URI deletes.

The URI does not supply S3 access. If the URI starts with `s3://`, the command
uses the default driver and the normal credential chain of that driver. Anonymous
access is off. Make working credentials available before you run the command. If
you use a custom endpoint, set it in the storage configuration.

`--all-products` still needs a storage configuration. Define a `[storage]` section
with a base path in the configuration file, or set `FIRECUBE_TARGET_PATH`.
Without a base, the run exits 2 before it deletes or previews any record. The
run skips a product that records no label and prints a line on stderr.

Every ChunkManager command needs a scope. A run with neither `--target` nor
`--all-products` exits 2. A run with both flags exits 2 too. Both runs print
`Provide exactly one of --target or --all-products.`

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — list records, runs, claims, or the compact cache
- **[Recover Runs And Claims](recover.md)** — recover a stuck run or stale claim
- **[Delete And Reingest](delete.md)** — run cleanup or reingestion workflows
- **[Compact Cache](compact.md)** — rebuild fast listings for chunk-manager commands
