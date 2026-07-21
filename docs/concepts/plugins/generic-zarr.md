# GenericZarrIngestor

`GenericZarrIngestor` is the batteries-included template for gridded Zarr output.
You return an `xarray.Dataset` and the engine handles discovery, batching,
concurrency, telemetry, config validation, metric aggregation, and the
time-axis append. Scaffold one with `firecube plugins create my-plugin --template zarr`
(see [Create a Plugin](create-a-plugin.md)).

For *how* the append path behaves (and why writes are serialized), see
[GenericZarrIngestor (Append)](../output-formats/zarr/generic-append.md).

## Required hook

| Hook | Signature | Purpose |
| :--- | :--- | :--- |
| **`build_dataset`** | `(group, items, ctx) -> xr.Dataset \| None` | Build the dataset for a group from its `items`. Return `None` to skip. |

## Optional hooks

| Hook | Signature | Purpose |
| :--- | :--- | :--- |
| `discover_source_files` | `(ctx)` | Override file discovery. Default recursive discovery is built in — see [Weather CSV: Source Discovery](../../tutorials/source-discovery.md). |
| `batch_setup` | `(ctx)` | Initialize per-batch resources (e.g. a DB connection). |
| `prepare_batch_data` | `(batch, ctx)` | Pre-load data before processing (e.g. load CSVs into a DB). |
| `cleanup_batch_data` | `(batch, ctx)` | Clean up after a batch (e.g. drop tables). |
| `batch_teardown` | `(ctx)` | Tear down per-batch resources. |

If you enable `duckdb_persist_batches=true`, your ingestor must provide the
[`DuckDbMixin`](mixins.md) hooks (`setup_duckdb`, `prepare_duckdb_schema`,
`teardown_duckdb`); the template validates this at startup.

## Minimal example

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginConfig,
    PluginContext,
    register_ingestor,
)


@dataclass
class MyPluginConfig(PluginConfig):
    variable_name: str = "T2M"
    scale_factor: float = 1.0


@register_ingestor("my_weather_plugin")
class MyWeatherIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_weather_product"
    name = "my_weather_plugin"
    plugin_config_class = MyPluginConfig

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        config: MyPluginConfig = self.plugin_config
        ds = xr.open_mfdataset(items, engine="netcdf4")
        if config.variable_name in ds:
            ds[config.variable_name] *= config.scale_factor
        return ds
```

## Next Steps

- **[Plugin Contract](contract.md)** — check `PRODUCT_NAME` and typed result requirements
- **[GenericZarrIngestor (Append)](../output-formats/zarr/generic-append.md)** — understand append-Zarr output behavior
- **[Weather CSV Plugin](../../tutorials/weather-csv.md)** — build a working Zarr plugin end to end
