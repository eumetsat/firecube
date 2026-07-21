# Weather CSV: Source Discovery

## Goal

Control which files Firecube sends to your plugin without writing discovery code.

Continue from [Weather CSV Plugin](weather-csv.md). You will keep the same
plugin and change the ingest command so Firecube discovers only CSV files.

The Weather plugin does not implement `discover_source_files`; the CLI pattern
tells Firecube which files to include.

## Built-In Discovery

For file-based templates such as `GenericZarrIngestor` and
`GenericParquetIngestor`, Firecube recursively discovers files under
`--input-data` and batches them before calling your hook. The built-in discovery
is conservative: it includes common EO container suffixes such as `.zip`, `.h5`,
and `.nc` by default.

For CSV input, pass `include_patterns`:

```bash
PRODUCT_URI="file://$PWD/tutorial-output/weather_csv_discovery.zarr"

uv run firecube ingest weather_csv \
  --input-data tutorial-data/weather-csv \
  --target "$PRODUCT_URI" \
  --product-name weather_csv_discovery \
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
"files_processed": 4
...
"count": 4
"product": "weather_csv_discovery"
```

## Filter Files From The CLI

Add a file the Weather plugin should ignore:

```bash
printf "notes for humans\n" > tutorial-data/weather-csv/README.txt
```

Run with the same `include_patterns` value so only CSV files are batched:

```bash
PRODUCT_URI="file://$PWD/tutorial-output/weather_csv_filtered.zarr"

uv run firecube ingest weather_csv \
  --input-data tutorial-data/weather-csv \
  --target "$PRODUCT_URI" \
  --product-name weather_csv_filtered \
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
"files_processed": 4
...
"count": 4
"product": "weather_csv_filtered"
```

Use a JSON list when a plugin accepts more than one file pattern:

```bash
--option 'include_patterns=["*.csv","*.csv.gz"]'
```

## Override Discovery Only For Special Layouts

Most plugins should use `include_patterns`. Override `discover_source_files` only
when the file layout itself carries domain rules, such as excluding temporary
files or pairing files before batching.

Inside the Weather plugin class, an override would look like this:

```python
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from firecube.ingestor.api import PluginContext


def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
    source = Path(ctx.source)
    return sorted(source.rglob("weather_*.csv"))
```

## Verify

Open the filtered product:

```bash
uv run python - <<'PY'
import xarray as xr

ds = xr.open_zarr(
    "tutorial-output/weather_csv_filtered.zarr",
    group="default",
    consolidated=False,
)
print(ds)
assert ds.sizes["timestamp"] == 4
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
```

## Next Steps

- **[Weather CSV: Observability](observability.md)** — add one custom metric to the same plugin
- **[Sentinel-3 FRP Plugin](sentinel3-frp.md)** — build a Parquet plugin from downloaded data
- **[Plugins](../concepts/plugins/index.md)** — understand plugin base classes and hooks
