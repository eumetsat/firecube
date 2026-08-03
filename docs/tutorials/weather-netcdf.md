# NetCDF To Zarr

## Goal

Build a complete Firecube plugin: read a sequence of NetCDF weather files and
append them to a Zarr product that opens with `xarray`.

This tutorial uses `GenericZarrIngestor`. Each input file contains one time
step on the same latitude-longitude grid. Your plugin opens the files and
returns one `xarray.Dataset`; Firecube handles discovery, batching, and the
Zarr append.

<figure markdown="span">
  ![Four NetCDF time-step files pass through the weather plugin and are appended to one Zarr group.](../assets/images/firecube-tutorial-netcdf-zarr.svg){ width="860" }
  <figcaption markdown="span">The plugin reads each file and returns an `xarray.Dataset`; `GenericZarrIngestor` appends it to the Zarr group.</figcaption>
</figure>

## Prerequisites

- Firecube installed. Start with [Quickstart](../quickstart/index.md) or
  [Installation](../quickstart/installation.md).
- Run every command below in the same Python environment where Firecube is
  installed.

## 1. Create Input NetCDF Files

Create four small files. Each file contains temperature and humidity for one
time step on a 2 by 3 grid:

```bash
mkdir -p tutorial-data/weather-netcdf
uv run python - <<'PY'
from pathlib import Path

import numpy as np
import xarray as xr

out = Path("tutorial-data/weather-netcdf")
out.mkdir(parents=True, exist_ok=True)

observations = [
    ("2024-07-01T00:00:00", 19.4, 68.0),
    ("2024-07-01T06:00:00", 22.8, 61.0),
    ("2024-07-01T12:00:00", 27.3, 47.0),
    ("2024-07-01T18:00:00", 24.1, 55.0),
]

for index, (timestamp, temperature, humidity) in enumerate(observations, start=1):
    ds = xr.Dataset(
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
    ds.to_netcdf(out / f"weather_{index:02d}.nc")
PY
```

Check that the four input files exist:

```bash
ls tutorial-data/weather-netcdf
```

Expected output:

```text
weather_01.nc  weather_02.nc  weather_03.nc  weather_04.nc
```

## 2. Create The Plugin

```bash
uv run firecube plugins create weather-netcdf \
  --template zarr \
  --target-dir plugins_dev \
  --non-interactive
```

`--non-interactive` keeps the command from stopping for author, license, or
template prompts.

Expected output:

```text
✨ Created plugin project: plugins_dev/firecube-weather-netcdf

To install for development:
  cd plugins_dev/firecube-weather-netcdf
  uv sync
```

## 3. Implement `build_dataset`

Replace
`plugins_dev/firecube-weather-netcdf/src/firecube_weather_netcdf/ingestor.py`
with:

```python
# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations
from __future__ import annotations

from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)


@register_ingestor("weather_netcdf")
class WeatherNetcdfIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "weather_netcdf"

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = group
        if not items:
            return None

        datasets: list[xr.Dataset] = []
        for item in items:
            path = ctx.materialize(item)
            with xr.open_dataset(path) as source:
                datasets.append(source.load())

        result = xr.concat(datasets, dim="timestamp").sortby("timestamp")
        return result
```

`ctx.materialize(item)` gives file-reading libraries a local path whether the
source item is local or remote. Calling `source.load()` keeps the data available
after `xr.open_dataset(...)` closes each file.

The generated project already declares `xarray`, so this implementation needs
no additional package dependency.

## 4. Install The Plugin

```bash
uv run firecube plugins install --editable plugins_dev/firecube-weather-netcdf
uv run firecube plugins describe weather_netcdf
```

Expected output from `plugins describe`:

```text
Name:        weather_netcdf
Version:     0.1.0 (firecube-weather-netcdf)
Module:      firecube_weather_netcdf.ingestor
Product:     weather_netcdf

Options Sections:
  [ENGINE]
      pipeline_parallel [boolean] (default: False)
      ...
```

## 5. Run Ingestion

NetCDF is one of Firecube's built-in source formats, so the command does not
need an `include_patterns` option:

```bash
mkdir -p tutorial-output
PRODUCT_URI="file://$PWD/tutorial-output/weather_netcdf.zarr"

uv run firecube ingest weather_netcdf \
  --input-data tutorial-data/weather-netcdf \
  --target "$PRODUCT_URI" \
  --product-name weather_netcdf \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

Expected output includes:

```text
"message":"Found 4 files"
...
"plugin": "weather_netcdf"
...
"files_processed": 4
...
"count": 4
"product": "weather_netcdf"
```

## 6. Verify The Product

Open the Zarr product and read the temperature at the first grid point:

```bash
uv run python - <<'PY'
import numpy as np
import xarray as xr

ds = xr.open_zarr(
    "tutorial-output/weather_netcdf.zarr",
    group="default",
    consolidated=False,
)
values = ds["temperature_c"].isel(latitude=0, longitude=0).values

print(ds)
print(values.tolist())

assert ds.sizes == {"timestamp": 4, "latitude": 2, "longitude": 3}
assert np.allclose(values, [19.4, 22.8, 27.3, 24.1])
PY
```

Expected output:

```text
<xarray.Dataset> Size: ...
Dimensions:                   (timestamp: 4, latitude: 2, longitude: 3)
Coordinates:
  * timestamp                 (timestamp) datetime64[ns] ...
  * latitude                  (latitude) float64 ...
  * longitude                 (longitude) float64 ...
Data variables:
    firecube_timestamp_state  (timestamp) uint8 ...
    temperature_c             (timestamp, latitude, longitude) float64 ...
    humidity_pct              (timestamp, latitude, longitude) float64 ...
Attributes:
    title:    Weather observations
[19.4, 22.8, 27.3, 24.1]
```

Inspect what ChunkManager recorded for the run:

```bash
uv run firecube chunks list --product-name "$PRODUCT_URI"
```

Expected output:

```text
Product               Key                       Type   Size (MB)  Date
---------------------------------------------------------------------
weather_netcdf.zarr   span_weather_netcdf-...   span   0.0        ...

Summary: 1 chunks, 0.0 MB total
```

`firecube chunks` counts ChunkManager records, not physical Zarr array chunks.

## What Firecube Handled

- Recursive NetCDF discovery from `--input-data`
- Batch creation
- Serialized Zarr appends for the group
- Product-local run and span records in ChunkManager
- Basic run [metrics](../concepts/observability/metrics.md) and
  [logs](../concepts/observability/logs.md)

## Troubleshooting

- `No such plugin: weather_netcdf`: re-run
  `uv run firecube plugins install --editable plugins_dev/firecube-weather-netcdf`.
- Existing completed data on rerun: add `--option force_reingest=true` while
  developing against the same target.
- No NetCDF files are discovered: confirm that `--input-data` points to the
  directory containing the `.nc` files.

## Next Steps

- **[NetCDF To Zarr: Source Discovery](source-discovery.md)** — see how Firecube selects source files
- **[NetCDF To Zarr: Observability](observability.md)** — add one custom metric
- **[Sentinel-3 FRP To Parquet](sentinel3-frp.md)** — download and ingest a real EUMETSAT product
- **[Parallel DirectZarrIngestor](direct-zarr-parallel.md)** — learn the advanced slot-write path
