# Weather CSV Plugin

## Goal

Build the smallest useful Firecube plugin: read local CSV weather observations and
write a Zarr product that opens with `xarray`.

This tutorial uses `GenericZarrIngestor`. Firecube handles source discovery,
batching, Zarr writing, resume bookkeeping, and ChunkManager.
Because CSV is not one of Firecube's built-in EO file suffixes, the run command
passes `include_patterns=["*.csv"]`. Your plugin only turns a batch of source
files into an `xarray.Dataset`.

## Prerequisites

- Firecube installed. Start with [Quickstart](../quickstart/index.md) or
  [Installation](../quickstart/installation.md).
- Run every command below in the same Python environment where Firecube is
  installed.

## 1. Create Input CSV Files

```bash
mkdir -p tutorial-data/weather-csv
uv run python - <<'PY'
from pathlib import Path

import pandas as pd

out = Path("tutorial-data/weather-csv")
out.mkdir(parents=True, exist_ok=True)

rows = [
    ("2024-07-01T00:00:00Z", 19.4, 68.0, 2.1),
    ("2024-07-01T06:00:00Z", 22.8, 61.0, 2.8),
    ("2024-07-01T12:00:00Z", 27.3, 47.0, 4.2),
    ("2024-07-01T18:00:00Z", 24.1, 55.0, 3.4),
]

for index, row in enumerate(rows, start=1):
    timestamp, temperature_c, humidity_pct, wind_m_s = row
    pd.DataFrame(
        [
            {
                "timestamp": timestamp,
                "temperature_c": temperature_c,
                "humidity_pct": humidity_pct,
                "wind_m_s": wind_m_s,
            }
        ]
    ).to_csv(out / f"weather_{index:02d}.csv", index=False)
PY
```

Check that the four input files exist:

```bash
ls tutorial-data/weather-csv
```

Expected output:

```text
weather_01.csv  weather_02.csv  weather_03.csv  weather_04.csv
```

## 2. Scaffold The Plugin

```bash
uv run firecube plugins create weather-csv \
  --template zarr \
  --target-dir plugins_dev \
  --non-interactive
```

`--non-interactive` keeps the tutorial command from stopping for author,
license, or template prompts.

Expected output:

```text
✨ Created plugin project: plugins_dev/firecube-weather-csv

To install for development:
  cd plugins_dev/firecube-weather-csv
  uv sync
```

## 3. Implement `build_dataset`

Replace `plugins_dev/firecube-weather-csv/src/firecube_weather_csv/ingestor.py`
with:

```python
from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)


@register_ingestor("weather_csv")
class WeatherCsvIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "weather_csv"
    name = "weather_csv"

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = (group, ctx)
        if not items:
            return None

        frames = []
        for item in items:
            path = Path(item)
            frame = pd.read_csv(path)
            frame["source_file"] = path.name
            frames.append(frame)

        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            utc=True,
        ).dt.tz_localize(None)
        df = df.sort_values("timestamp")

        ds = df.set_index("timestamp")[
            ["temperature_c", "humidity_pct", "wind_m_s"]
        ].to_xarray()
        ds.attrs["title"] = "Weather CSV demo"
        return ds
```

## 4. Install The Plugin

```bash
uv run firecube plugins install --editable plugins_dev/firecube-weather-csv
uv run firecube plugins describe weather_csv
```

Expected output from `plugins describe`:

```text
Name:        weather_csv
Version:     0.1.0 (firecube-weather-csv)
Module:      firecube_weather_csv.ingestor
Product:     weather_csv

Options Sections:
  [ENGINE]
      pipeline_parallel [boolean] (default: False)
      ...
```

## 5. Run Ingestion

```bash
mkdir -p tutorial-output
PRODUCT_URI="file://$PWD/tutorial-output/weather_csv.zarr"

uv run firecube ingest weather_csv \
  --input-data tutorial-data/weather-csv \
  --target "$PRODUCT_URI" \
  --product-name weather_csv \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option 'include_patterns=["*.csv"]'
```

Expected output includes:

```text
"message":"Found 4 files"
...
"plugin": "weather_csv"
...
"files_processed": 4
...
"count": 4
"product": "weather_csv"
```

## 6. Verify The Product

Open the Zarr product:

```bash
uv run python - <<'PY'
import xarray as xr

ds = xr.open_zarr(
    "tutorial-output/weather_csv.zarr",
    group="default",
    consolidated=False,
)
print(ds)
print(ds["temperature_c"].values.tolist())

assert ds.sizes["timestamp"] == 4
assert "temperature_c" in ds
PY
```

Expected output:

```text
<xarray.Dataset> Size: ...
Dimensions:                   (timestamp: 4)
Coordinates:
  * timestamp                 (timestamp) datetime64[ns] ...
Data variables:
    firecube_timestamp_state  (timestamp) uint8 ...
    temperature_c             (timestamp) float64 ...
    humidity_pct              (timestamp) float64 ...
    wind_m_s                  (timestamp) float64 ...
Attributes:
    title:    Weather CSV demo
[19.4, 22.8, 27.3, 24.1]
```

Inspect what ChunkManager recorded for the run:

```bash
uv run firecube chunks list --product-name "$PRODUCT_URI"
```

Expected output:

```text
Product           Key                    Type   Size (MB)  Date
----------------------------------------------------------------
weather_csv.zarr  span_weather_csv-...   span   0.0        ...

Summary: 1 chunks, 0.0 MB total
```

`firecube chunks` counts ChunkManager records. It is not counting physical Zarr array chunks.

## What Firecube Handled

- Recursive input discovery from `--input-data` using `include_patterns`
- Batch creation
- Zarr append writes
- Product-local run/span records in ChunkManager
- Basic run [metrics](../concepts/observability/metrics.md) and
  [logs](../concepts/observability/logs.md)

## Troubleshooting

- `No such plugin: weather_csv`: re-run
  `uv run firecube plugins install --editable plugins_dev/firecube-weather-csv`.
- Existing completed data on rerun: add `--option force_reingest=true` while
  developing against the same target.
- No CSV files are discovered: keep `--option 'include_patterns=["*.csv"]'` on
  the ingest command; see [Weather CSV: Source Discovery](source-discovery.md).
- `ModuleNotFoundError: No module named 'pandas'`: run the command in the same
  environment where Firecube is installed.

## Next Steps

- **[Weather CSV: Source Discovery](source-discovery.md)** — see how Firecube selects source files
- **[Weather CSV: Observability](observability.md)** — add one custom metric
- **[Sentinel-3 FRP Plugin](sentinel3-frp.md)** — build a Parquet plugin from downloaded data
- **[Direct Parallel Zarr](direct-zarr-parallel.md)** — learn the advanced slot-write path
