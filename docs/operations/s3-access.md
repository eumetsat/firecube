# Configure S3 Access

Configure credentials and run an installed plugin against an S3 or
S3-compatible product target.

## Prerequisites

- Firecube and a compatible plugin installed in the same environment.
- The Firecube environment activated.
- Source data accepted by the plugin.
- An S3 bucket and credentials supplied by the storage provider.

## Configure The Current Shell

Replace the values below. Omit the endpoint when the provider uses its standard
S3 endpoint. Set the region only when the provider requires one.

```bash
export FIRECUBE_ENDPOINT_URL="https://your-s3-endpoint.example.com"
export FIRECUBE_REGION="your-region"
export FIRECUBE_ACCESS_KEY="your-access-key"
export FIRECUBE_SECRET_KEY="your-secret-key"
```

Environment variables have higher precedence than `config.toml`. Explicit CLI
values still win over environment variables.

## Store Persistent Settings

Firecube reads `~/.config/firecube/config.toml` by default. Keep the file
private when it contains credentials.

```toml
[storage]
endpoint_url = "https://your-s3-endpoint.example.com"
region = "your-region"
access_key = "your-access-key"
secret_key = "your-secret-key"
```

## Run An S3 Ingestion

Replace `PLUGIN_NAME`, the source path, bucket, and product name with values
supported by the installed plugin:

```bash
firecube ingest PLUGIN_NAME \
  --input-data /absolute/path/to/source-data \
  --target s3://your-bucket/products/my_product.zarr \
  --product-name my_product \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged
```

The final summary should report
`s3://your-bucket/products/my_product.zarr` as the `stored_at` value.

## Verify The Product

Inspect the product's control-plane records:

```bash
firecube chunks list \
  --product-name s3://your-bucket/products/my_product.zarr
```

See the [Configuration Reference](../reference/config.md#storageconfig) for all
storage fields and the [Configuration Model](../concepts/configuration.md) for
precedence rules.

## Next Steps

- **[Create an Intake Catalog](intake-catalog.md)**: publish catalog entries for
  an existing Zarr or Parquet product.
- **[Storage Drivers](../reference/storage-drivers.md)**: compare supported S3
  driver behavior.
- **[Recover Runs And Claims](chunk-manager/recover.md)**: recover after a
  failed process once no writer is still active.
