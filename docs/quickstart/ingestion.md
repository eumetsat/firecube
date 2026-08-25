# Run Ingestion

Run the installed plugin once to write a local Zarr product, then open it to
confirm the data cube looks the way you expect.

## Prerequisites

- The quickstart virtual environment is active.
- The `quickstart_plugin` plugin from
  [Install the Example Plugin](plugins.md) is installed.
- The four files from [Prepare Source Data](source-data.md) exist under
  `sample_data/`.
- Commands run from the `firecube-quickstart-plugin/` directory.

## Run The Ingestion

```bash
firecube ingest quickstart_plugin \
  --input-data sample_data \
  --target "file://${PWD}/sample_data/quickstart_plugin_out.zarr" \
  --product-name quickstart_plugin \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged
```

The final summary should identify the selected plugin, the `quickstart_plugin`
product, four processed files, and the local `stored_at` path.

## Verify The Run

Confirm that the Zarr directory exists:

```bash
test -d sample_data/quickstart_plugin_out.zarr && echo "Zarr product created"
```

Expected output:

```text
Zarr product created
```

Inspect the product's control-plane records using the full product URI:

```bash
firecube chunks list \
  --product-name "file://${PWD}/sample_data/quickstart_plugin_out.zarr"
```

The result should contain at least one `span` record for the completed run.
These are logical Firecube records, not physical Zarr array chunks.

## Open And Inspect The Cube

Firecube writes the plugin's data into a `default` group inside the Zarr
store:

```bash
python - <<'PY'
import xarray as xr

ds = xr.open_zarr(
    "sample_data/quickstart_plugin_out.zarr",
    group="default",
    consolidated=False,
)
print(ds)
print(ds["temperature_c"].isel(latitude=0, longitude=0).values.tolist())
PY
```

Expected output includes:

```text
Dimensions:  (timestamp: 4, latitude: 2, longitude: 3)
[19.4, 22.8, 27.3, 24.1]
```

The `timestamp` dimension has length 4, `latitude` has length 2, and
`longitude` has length 3. The four temperature values keep the timestamp order
defined in the source files. The result also contains `humidity_pct` and
Firecube's timestamp-state array.

This completes the quickstart: you installed Firecube, installed a plugin,
staged source data, and produced a Zarr cube you can open with `xarray`.

## Next Steps

- **[Configure S3 Access](../operations/s3-access.md)**: repeat this workflow
  against S3 or an S3-compatible service.
- **[Storage Drivers](../reference/storage-drivers.md)**: choose a different
  storage backend for the product.
- **[Create an Intake Catalog](../operations/intake-catalog.md)**: make this
  product discoverable through Intake.
- **[Plugin Development](../guides/plugins/index.md)**: build a plugin for
  your own dataset.
- **[Operations](../operations/index.md)**: inspect, recover, archive, or
  manage completed products.
