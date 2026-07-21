# Run Ingestion

Ingestion consumes source files and writes them into a standardized format like **Zarr** or **Parquet**.

Before you start, install or create a plugin in
**[Plugin Setup](plugins.md)** and prepare example input data that the plugin
can read.

## Before You Run

Check the plugin name, product name, and available option sections:

```bash
uv run firecube plugins describe <your_plugin_name>
```

To see only the `--option key=value` keys accepted by `firecube ingest`:

```bash
uv run firecube ingest <your_plugin_name> --show-options
```

To inspect one option in detail, for example `pipeline_workers`:

```bash
uv run firecube plugins explain <your_plugin_name>.engine.pipeline_workers
```

The local and S3 examples below include the required product and storage flags.
For the complete command surface, see the [CLI Reference](../reference/cli.md).

## Local Example

Use local storage first. The target below writes the product to your current
directory as `${PWD}/my_product.zarr`. Set these values for your plugin and
source data:

```bash
export FIRECUBE_PLUGIN="<your_plugin_name>"
export FIRECUBE_INPUT_DATA="/absolute/path/to/your/source-data"
export FIRECUBE_PRODUCT_NAME="my_product"
export FIRECUBE_LOCAL_TARGET="file://${PWD}/${FIRECUBE_PRODUCT_NAME}.zarr"
```

Then run the ingest command:

```bash
uv run firecube ingest "$FIRECUBE_PLUGIN" \
  --input-data "$FIRECUBE_INPUT_DATA" \
  --target "$FIRECUBE_LOCAL_TARGET" \
  --product-name "$FIRECUBE_PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

## S3 Example

Set the S3 connection values in your shell first. Replace the endpoint and
region with the values for your storage provider:

```bash
export FIRECUBE_ENDPOINT_URL="https://your-s3-endpoint.example.com"
export FIRECUBE_REGION="your-region"
export FIRECUBE_ACCESS_KEY="your-access-key"
export FIRECUBE_SECRET_KEY="your-secret-key"
```

Set the plugin, source path, product name, and S3 product URI:

```bash
export FIRECUBE_PLUGIN="<your_plugin_name>"
export FIRECUBE_INPUT_DATA="/absolute/path/to/your/source-data"
export FIRECUBE_PRODUCT_NAME="my_product"
export FIRECUBE_S3_TARGET="s3://your-bucket/path/${FIRECUBE_PRODUCT_NAME}.zarr"
```

Then run the ingest command:

```bash
uv run firecube ingest "$FIRECUBE_PLUGIN" \
  --input-data "$FIRECUBE_INPUT_DATA" \
  --target "$FIRECUBE_S3_TARGET" \
  --product-name "$FIRECUBE_PRODUCT_NAME" \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged
```

For config files, custom S3 endpoints, and precedence rules, see
**[Configuration](configuration.md)**.

## Verify ChunkManager Records

Once a run finishes, ask ChunkManager what was recorded:

```bash
uv run firecube chunks list --product-name "$FIRECUBE_LOCAL_TARGET"
```

For an S3 run, use the S3 product URI:

```bash
uv run firecube chunks list --product-name "$FIRECUBE_S3_TARGET"
```

This lists spans that are active in your datacube. See
[ChunkManager Records](../concepts/chunk-management.md) for the model, then use
[ChunkManager Operations](../operations/chunk-manager/index.md) for cleanup
and recovery commands.

!!! note
    ChunkManager records are logical Firecube
    run/span records, not physical Zarr array chunks.

## Resume And Retry

To retry a run, use the same ingest command and add
`--option resume_existing=true`. For the S3 example above:

```bash
uv run firecube ingest "$FIRECUBE_PLUGIN" \
  --input-data "$FIRECUBE_INPUT_DATA" \
  --target "$FIRECUBE_S3_TARGET" \
  --product-name "$FIRECUBE_PRODUCT_NAME" \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option resume_existing=true
```

For a local retry, use `$FIRECUBE_LOCAL_TARGET` with `--storage-type local` and
the write mode you used for the local run.

Firecube checks ChunkManager before writing anything. If a previous run is still
recorded as active, Firecube stops and prints the run id:

```text
ResumeConflictError: Non-terminal run(s) [run-abc123] exist for product
'my_product'. Abandon them first:
  uv run firecube chunks runs abandon \
    --product-name my_product \
    --run-id run-abc123 \
    --reason "<reason>"
```

After you confirm no previous process is still writing, abandon the stuck run
with the full product URI:

```bash
uv run firecube chunks runs abandon \
  --target "$FIRECUBE_LOCAL_TARGET" \
  --product-name "$FIRECUBE_PRODUCT_NAME" \
  --run-id run-abc123 \
  --reason "previous run failed" \
  --yes-i-really-mean-it
```

Then run the ingest command again. See
[Recover Runs And Claims](../operations/chunk-manager/recover.md) for the full
workflow.

## Next Steps

- **[Intake Catalogs](catalogs.md)** — create catalog entries after ingestion succeeds
