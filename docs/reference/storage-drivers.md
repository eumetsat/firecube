# Storage Driver Compatibility

Firecube supports two storage drivers:

- `fsspec`, the default driver for local and S3-compatible storage
- `obstore`, an optional Rust-backed S3 driver for Zarr workloads

This page lists the current user-facing compatibility surface. For the storage
model, see [Product Storage](../concepts/storage.md).

| Symbol | Meaning |
|---|---|
| ✅ | Supported |
| ⚠️ | Supported with caveats |
| ❌ | Not supported |

## Driver Choice

| Use case | fsspec | obstore | Notes |
|---|:---:|:---:|---|
| Local product storage | ✅ | ✅ | Local products use `file://` targets and `--storage-type local`. |
| S3 product storage | ✅ | ✅ | S3 products use `s3://` targets and `--storage-type s3`. |
| Zarr append writes | ✅ | ✅ | Used by `GenericZarrIngestor`. |
| Zarr region writes | ✅ | ✅ | Used by `DirectZarrIngestor` and direct-Zarr parallel writes. |
| Consolidated Zarr reads | ✅ | ✅ | Both drivers read consolidated Zarr metadata. |
| Parquet/DuckDB against local products | ✅ | ⚠️ | Local DuckDB I/O works; obstore does not add anything for local paths. |
| Parquet/DuckDB against remote S3 products | ✅ | ❌ | Use `--storage-driver fsspec` for remote Parquet/DuckDB operations. |
| Recursive delete/list operations | ✅ | ✅ | Used by cleanup and ChunkManager operations. |
| Multipart upload | ✅ | ✅ | Used for larger uploads where supported by the backend. |
| Ranged reads | ❌ | ✅ | Exposed by the obstore-backed filesystem. |
| Pre-signed URLs | ❌ | ❌ | Not currently supported by either driver. |

## One Driver Per Run

A single Firecube run uses one driver for product data, uploads, and the
`.firecube/` control plane.

| Scenario | Outcome |
|---|---|
| `--storage-driver fsspec` with `s3://` Zarr target | fsspec writes both data and the `.firecube/` control plane. |
| `--storage-driver obstore` with `s3://` Zarr target | obstore writes both data and the `.firecube/` control plane. |
| `--storage-driver obstore` with remote Parquet/DuckDB | Rejected; use `--storage-driver fsspec`. |

## Installing obstore

Install the optional extra before using the obstore driver:

```bash
uv pip install 'firecube[obstore]'
```

Then select it explicitly:

```bash
firecube ingest <plugin> \
  --source /data/input \
  --target s3://bucket/path/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type s3 \
  --storage-driver obstore \
  --output-format zarr \
  --write-mode direct
```

## Next Steps

- **[Product Storage](../concepts/storage.md)** — storage model and runtime choices
- **[Configuration Reference](config.md)** — storage environment variables and config keys
- **[ChunkManager Operations](../operations/chunk-manager/index.md)** — inspection, recovery, cleanup, snapshot, or migration commands
