# Prepare Source Data

Firecube expects you to provide source datasets. In normal use, these files may
come from a data provider, an existing local sources,
or object storage. Download or stage the source files before ingestion, then
pass their directory or storage prefix to `--input-data`.

The installed plugin decides which formats, filenames, and dataset structure it
accepts. The `weather_netcdf` plugin from the previous page expects
time-indexed NetCDF files. The example below creates four files it can read.

## Create The Example Files

Run this command from the `firecube-quickstart/` directory with the virtual
environment active:

```bash
python - <<'PY'
from pathlib import Path

import numpy as np
import xarray as xr

output = Path("quickstart-data/weather-netcdf")
output.mkdir(parents=True, exist_ok=True)

observations = [
    ("2024-07-01T00:00:00", 19.4, 68.0),
    ("2024-07-01T06:00:00", 22.8, 61.0),
    ("2024-07-01T12:00:00", 27.3, 47.0),
    ("2024-07-01T18:00:00", 24.1, 55.0),
]

for index, (timestamp, temperature, humidity) in enumerate(observations, start=1):
    dataset = xr.Dataset(
        data_vars={
            "temperature_c": (
                ("timestamp", "latitude", "longitude"),
                np.full((1, 2, 3), temperature, dtype="float64"),
            ),
            "humidity_pct": (
                ("timestamp", "latitude", "longitude"),
                np.full((1, 2, 3), humidity, dtype="float64"),
            ),
        },
        coords={
            "timestamp": [np.datetime64(timestamp, "ns")],
            "latitude": [50.0, 51.0],
            "longitude": [7.0, 8.0, 9.0],
        },
        attrs={"title": "Weather observations"},
    )
    dataset.to_netcdf(output / f"weather_{index:02d}.nc")
PY
```

Firecube's installation includes NumPy, xarray, and NetCDF support used by this
script.

## Verify The Source Data

List the generated files:

```bash
ls quickstart-data/weather-netcdf
```

Expected output:

```text
weather_01.nc  weather_02.nc  weather_03.nc  weather_04.nc
```

Each file contains one timestamp on the same 2 by 3 latitude-longitude grid.
The ingestion command passes their parent directory to the plugin.

## Next Steps

- **[Run Ingestion](ingestion.md)**: convert the four NetCDF files into one
  local Zarr product.
- **[NetCDF To Zarr tutorial](../tutorials/weather-netcdf.md)**: inspect the
  matching plugin implementation and dataset transformation.
