# Product Storage

Product storage answers one practical question: where should Firecube write the
product, and which storage backend should the run use?

The output format is a separate choice. A Zarr or Parquet product can be written
to local disk or S3-compatible object storage by changing the ingest flags.

## What You Set On Ingest

Every ingest run sets the product target and write mode explicitly:

- `--target` is the full product URI, such as `file:///data/products/demo.zarr`
  or `s3://bucket/products/demo.zarr`.
- `--storage-type` optionally overrides the type inferred from the URI.
- `--storage-driver` optionally overrides configuration, environment, or the
  `fsspec` default.
- `--write-mode` selects the write strategy, usually `direct` or `staged`.

Firecube rejects an explicit storage type that conflicts with the target URI.

## Local Targets

Use a `file://` target when the product lives on local disk, mounted storage, or
a shared filesystem:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target file:///data/products/demo.zarr \
  --product-name demo \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

Local targets are the simplest place to develop plugins, test output shape, and
inspect products with local Python tools.

## S3 Targets

Use an `s3://` target when the product lives in S3 or S3-compatible object
storage:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target s3://bucket/products/demo.zarr \
  --product-name demo \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged
```

Set S3 credentials, endpoint, and region in the environment or config before the
run. See [Configure S3 Access](../quickstart/configuration.md) for the quickstart path
and [Configuration Reference](../reference/config.md) for the full list.

## Choose A Driver

Use `fsspec` as the default driver when you need broad local and S3 support,
including Parquet and DuckDB operations against remote storage.

Use `obstore` for supported Zarr workloads when you want the optional
Rust-backed S3 storage path. Check the compatibility page before choosing it for
a production run.

The selected driver applies to the whole run: product data and ChunkManager
records use the same storage backend.

## Choose A Write Mode

Use `direct` when workers should write directly to the final product target.
This is common for local output and for workflows where the final storage is the
right commit point.

Use `staged` when Firecube should write to a local workspace first and then copy
or upload to the final target. This is useful when temporary local writes are
faster or safer than writing each intermediate step to the final store.

Staged mode uses a local workspace. You can set workspace behavior with plugin
options such as `--option workspace=/fast-ssd/firecube-work` and
`--option cleanup_workspace=true`.

## Keep Product State Together

The readable data store and ChunkManager records belong to the same product.
ChunkManager records live in the product's `.firecube/` directory. Firecube
archive and restore commands preserve those records for Firecube-generated
products. If you move or copy a product manually, keep `.firecube/` with the data
when you want later inspection, resume, recovery, or cleanup to work.

## Next Steps

- **[Run Ingestion](../quickstart/ingestion.md)** — runnable local and S3 ingestion examples
- **[Configure S3 Access](../quickstart/configuration.md)** — set credentials and connection settings
- **[Storage Drivers](../reference/storage-drivers.md)** — inspect driver values and capabilities
- **[ChunkManager Operations](../operations/chunk-manager/index.md)** — inspect, recover, delete, rebuild snapshots, or migrate a product
