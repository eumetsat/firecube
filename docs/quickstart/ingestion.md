# Run Ingestion

Run an installed plugin to write a Zarr or Parquet product and record the run in
the product's `.firecube/` control plane.

## Prerequisites

- Firecube and a plugin installed in the same environment.
- Source data supported by that plugin.
- The plugin name and output format from the plugin documentation.

## Inspect The Plugin

Replace `PLUGIN_NAME` with the name shown by `plugins list`:

```bash
uv run firecube plugins describe PLUGIN_NAME
uv run firecube ingest PLUGIN_NAME --show-options
```

Use `plugins explain` when you need the description and default for one option:

```bash
uv run firecube plugins explain PLUGIN_NAME.engine.pipeline_workers
```

## Set The Local Run

Set the plugin name, input location, product name, and output format. The output
format must be one supported by the plugin. This example uses Zarr; use
`parquet` for a Parquet plugin.

```bash
export FIRECUBE_PLUGIN_NAME="PLUGIN_NAME"
export FIRECUBE_INPUT_DATA="/absolute/path/to/source-data"
export FIRECUBE_PRODUCT_NAME="my_product"
export FIRECUBE_OUTPUT_FORMAT="zarr"
export FIRECUBE_LOCAL_TARGET="file://${PWD}/${FIRECUBE_PRODUCT_NAME}.${FIRECUBE_OUTPUT_FORMAT}"
```

Keeping the `.zarr` or `.parquet` suffix in the product URI also allows the
catalog command to identify the product format later.

## Run The Local Ingestion

```bash
uv run firecube ingest "$FIRECUBE_PLUGIN_NAME" \
  --input-data "$FIRECUBE_INPUT_DATA" \
  --target "$FIRECUBE_LOCAL_TARGET" \
  --product-name "$FIRECUBE_PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format "$FIRECUBE_OUTPUT_FORMAT" \
  --write-mode direct
```

The final JSON summary should contain the selected plugin, product name, output
format, and `stored_at` path.

## Verify The Run

Inspect the control-plane record using the full product URI:

```bash
uv run firecube chunks list --product-name "$FIRECUBE_LOCAL_TARGET"
```

The result should contain at least one `span` record for the completed run.
These are logical Firecube records, not physical Zarr or Parquet chunks.

## Run Against S3

This is an optional alternative to the local run. First complete
[Configure S3 Access](configuration.md), then set an S3 product URI with the
same format suffix:

```bash
export FIRECUBE_S3_TARGET="s3://your-bucket/path/${FIRECUBE_PRODUCT_NAME}.${FIRECUBE_OUTPUT_FORMAT}"
```

```bash
uv run firecube ingest "$FIRECUBE_PLUGIN_NAME" \
  --input-data "$FIRECUBE_INPUT_DATA" \
  --target "$FIRECUBE_S3_TARGET" \
  --product-name "$FIRECUBE_PRODUCT_NAME" \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format "$FIRECUBE_OUTPUT_FORMAT" \
  --write-mode staged
```

```bash
uv run firecube chunks list --product-name "$FIRECUBE_S3_TARGET"
```

For command failures, use [Recover Runs And Claims](../operations/chunk-manager/recover.md)
after confirming that the previous process is no longer writing.

## Next Steps

- **[Create an Intake Catalog](catalogs.md)** — make a Zarr or Parquet product discoverable
- **[CLI Reference](../reference/cli.md)** — inspect the complete command surface
