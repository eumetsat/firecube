# Run Ingestion

Run the installed plugin once to write a local Zarr product and record the run
in the product's `.firecube/` control plane.

## Prerequisites

- The quickstart virtual environment is active.
- The `weather_netcdf` plugin from
  [Create and Install the Example Plugin](plugins.md) is installed.
- The four files from [Prepare Source Data](source-data.md) exist.

## Inspect The Plugin

Confirm the plugin name and options before writing data:

```bash
firecube plugins describe weather_netcdf
firecube ingest weather_netcdf --show-options
```

## Run The Local Ingestion

```bash
mkdir -p quickstart-output

firecube ingest weather_netcdf \
  --input-data quickstart-data/weather-netcdf \
  --target "file://${PWD}/quickstart-output/weather.zarr" \
  --product-name quickstart_weather \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

The final summary should identify the selected plugin, the
`quickstart_weather` product, four processed files, and the local `stored_at`
path.

## Verify The Run

Confirm that the Zarr directory exists:

```bash
test -d quickstart-output/weather.zarr && echo "Zarr product created"
```

Expected output:

```text
Zarr product created
```

Inspect the product's control-plane records using the full product URI:

```bash
firecube chunks list \
  --product-name "file://${PWD}/quickstart-output/weather.zarr"
```

The result should contain at least one `span` record for the completed run.
These are logical Firecube records, not physical Zarr array chunks.

## Next Steps

- **[Configure S3 Access](../operations/s3-access.md)**: repeat the workflow
  against S3 or an S3-compatible service.
- **[Create an Intake Catalog](../operations/intake-catalog.md)**: make an
  existing Zarr or Parquet product discoverable through Intake.
- **[NetCDF To Zarr tutorial](../tutorials/weather-netcdf.md)**: inspect how the
  quickstart plugin transforms the source files and verify the stored values.
- **[Operations](../operations/index.md)**: inspect, recover, archive, or manage
  completed products.
