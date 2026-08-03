# Operations

Use these pages to run coordinated write workflows and to inspect, recover,
clean up, archive, or restore Firecube products.

Operations commands work against a product URI:

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
```

For S3 products, use the full S3 URI:

```bash
PRODUCT_URI="s3://bucket/products/MY_PRODUCT.zarr"
```

## Preflight

Check the CLI surface before running destructive commands:

```bash
firecube chunks list --help
firecube archive create --help
```

For storage-deleting operations on local products, make sure the storage config
points at the product root:

```bash
export FIRECUBE_STORAGE_TYPE=local
export FIRECUBE_STORAGE_DRIVER=fsspec
export FIRECUBE_TARGET_PATH=/data/products
```

For S3 products, configure the S3 storage settings and credentials described in
[Configuration Reference](../reference/config.md).

## Common Tasks

| Task | Start here |
|---|---|
| Run several workers against one Zarr group | [Run Parallel Zarr Writes](parallel-zarr-writes.md) |
| Inspect product state | [Inspect ChunkManager State](chunk-manager/inspect.md) |
| Recover from a crashed run | [Recover Runs And Claims](chunk-manager/recover.md) |
| Delete stale data or reingest a range | [Delete And Reingest](chunk-manager/delete.md) |
| Rebuild snapshots | [Snapshots](chunk-manager/snapshots.md) |
| Create a portable `.tgm` archive | [Create Archives](archive/create.md) |
| Inspect or validate an archive | [Inspect And Validate Archives](archive/inspect.md) |
| Restore an archive to Zarr | [Restore Archives](archive/restore.md) |

## Safety Rules

- Run dry-run commands first when available.
- Use `--yes-i-really-mean-it` only after the dry-run output matches what you
  intend to change.
- Use `--force` only for the specific commands where it is documented. It is an
  operational bypass, not a confirmation flag.
- Keep `.firecube/` with the product unless you are intentionally discarding
  Firecube run history and cleanup state.

## Next Steps

- **[Run Parallel Zarr Writes](parallel-zarr-writes.md)** — preallocate, plan, and run slot workers
- **[ChunkManager Operations](chunk-manager/index.md)** — operate the product-local `.firecube/` control plane
- **[Archive Operations](archive/index.md)** — create, inspect, validate, or restore `.tgm` archives
- **[ChunkManager Records](../concepts/chunk-management.md)** — understand the model before running commands
