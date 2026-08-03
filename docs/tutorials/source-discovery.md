# NetCDF To Zarr: Source Discovery

## Goal

See which files Firecube discovers automatically, then add another filename
pattern without writing discovery code in the plugin.

Continue from [NetCDF To Zarr](weather-netcdf.md). You will keep the same
plugin and input directory.

## Use Built-In Discovery

For file-based templates such as `GenericZarrIngestor` and
`GenericParquetIngestor`, Firecube searches recursively under `--input-data`.
NetCDF `.nc`, HDF5 `.h5`, and ZIP `.zip` files are included by default.

Run the Weather plugin without a discovery option:

```bash
PRODUCT_URI="file://$PWD/tutorial-output/weather_netcdf_discovery.zarr"

uv run firecube ingest weather_netcdf \
  --input-data tutorial-data/weather-netcdf \
  --target "$PRODUCT_URI" \
  --product-name weather_netcdf_discovery \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

Expected output includes:

```text
"message":"Found 4 files"
...
"files_processed": 4
...
"count": 4
"product": "weather_netcdf_discovery"
```

## Add Another Filename Pattern

Create one more NetCDF file, this time with the `.nc4` suffix:

```bash
uv run python - <<'PY'
from pathlib import Path

import numpy as np
import xarray as xr

path = Path("tutorial-data/weather-netcdf/weather_05.nc4")
ds = xr.Dataset(
    data_vars={
        "temperature_c": (
            ("timestamp", "latitude", "longitude"),
            np.full((1, 2, 3), 18.7, dtype="float64"),
        ),
        "humidity_pct": (
            ("timestamp", "latitude", "longitude"),
            np.full((1, 2, 3), 72.0, dtype="float64"),
        ),
    },
    coords={
        "timestamp": [np.datetime64("2024-07-02T00:00:00", "ns")],
        "latitude": [50.0, 51.0],
        "longitude": [7.0, 8.0, 9.0],
    },
    attrs={"title": "Weather observations"},
)
ds.to_netcdf(path)
print(path)
PY
```

Expected output:

```text
tutorial-data/weather-netcdf/weather_05.nc4
```

The `.nc4` suffix is not in the built-in set. Add it with `include_patterns`:

```bash
PRODUCT_URI="file://$PWD/tutorial-output/weather_netcdf_extended.zarr"

uv run firecube ingest weather_netcdf \
  --input-data tutorial-data/weather-netcdf \
  --target "$PRODUCT_URI" \
  --product-name weather_netcdf_extended \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option 'include_patterns=["*.nc4"]'
```

Expected output includes:

```text
"message":"Found 5 files"
...
"files_processed": 5
...
"count": 5
"product": "weather_netcdf_extended"
```

`include_patterns` adds matching files to the built-in formats. It does not
exclude `.nc`, `.h5`, or `.zip` files that Firecube would otherwise discover.

## Verify

Open the product and confirm that it contains all five time steps:

```bash
uv run python - <<'PY'
import xarray as xr

ds = xr.open_zarr(
    "tutorial-output/weather_netcdf_extended.zarr",
    group="default",
    consolidated=False,
)
print(ds.sizes)
assert ds.sizes == {"timestamp": 5, "latitude": 2, "longitude": 3}
PY
```

Expected output:

```text
Frozen({'timestamp': 5, 'latitude': 2, 'longitude': 3})
```

## Next Steps

- **[NetCDF To Zarr: Observability](observability.md)** — add one custom metric to the same plugin
- **[Read Plugin Source Data](../guides/plugins/storage-access.md)** — materialize discovered items for a product reader
- **[Plugin Template API](../reference/api.md)** — look up the public plugin context and template hooks
- **[Sentinel-3 FRP To Parquet](sentinel3-frp.md)** — download and ingest a real EUMETSAT product
