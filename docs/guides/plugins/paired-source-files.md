# Read Paired Source Files

## Goal

Read a measurement and its calibration together, and skip pairs that have
not been approved for ingestion.

## Pair Measurements With Metadata

This example uses a local directory passed as `--input-data ./delivery`:

```text
delivery/
  measurement_001.nc
  measurement_001.json
  measurement_002.nc
  measurement_002.json
```

Each NetCDF file contains a `value` variable along a `time` dimension. Its
matching JSON file supplies a multiplier and an approval flag:

```json
{"gain": 1.5, "approved": true}
```

In your generated plugin, return each pair as a tuple from
`discover_source_files`. Calling `super()` keeps the built-in discovery and
`--input-filters` behavior for the measurement files:

```python
from pathlib import Path


def discover_source_files(self, ctx):
    for filename in super().discover_source_files(ctx):
        measurement = Path(filename)
        if measurement.suffix != ".nc":
            continue
        calibration = measurement.with_suffix(".json")
        if not calibration.is_file():
            raise FileNotFoundError(f"Missing calibration: {calibration}")
        yield measurement, calibration
```

Firecube keeps each tuple together as one batching item.

## Skip Unapproved Pairs

Add `filter_item` to the same plugin class. Returning `False` removes the
whole pair before batching:

```python
import json


def filter_item(self, item, ctx):
    _, calibration = item
    metadata = json.loads(calibration.read_text())
    return metadata["approved"] is True
```

## Read Both Files

Each source item is now a tuple of two files, so `read_dataset` takes both.
In your generated plugin, set `TIME_DIM` to match these inputs and replace
`read_dataset`:

```python
import json
from pathlib import Path

import xarray as xr

TIME_DIM = "time"


def read_dataset(measurement: Path, calibration: Path) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    metadata = json.loads(calibration.read_text())
    with xr.open_dataset(measurement) as source:
        dataset = source.load()
    dataset["value"] = dataset["value"] * metadata["gain"]
    return dataset
```

In `build_dataset`, change only the call to `read_dataset` so it unpacks the
tuple and downloads each file when the input is remote:

```python
datasets = [
    read_dataset(ctx.materialize(measurement), ctx.materialize(calibration))
    for measurement, calibration in items
]
```

## The Complete Plugin

The three hooks together, in the generated `ingestor.py`:

```python
import json
from pathlib import Path
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)

TIME_DIM = "time"


def read_dataset(measurement: Path, calibration: Path) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    metadata = json.loads(calibration.read_text())
    with xr.open_dataset(measurement) as source:
        dataset = source.load()
    dataset["value"] = dataset["value"] * metadata["gain"]
    return dataset


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_plugin"
    time_dim_name: ClassVar[str] = TIME_DIM

    def discover_source_files(self, ctx):
        for filename in super().discover_source_files(ctx):
            measurement = Path(filename)
            if measurement.suffix != ".nc":
                continue
            calibration = measurement.with_suffix(".json")
            if not calibration.is_file():
                raise FileNotFoundError(f"Missing calibration: {calibration}")
            yield measurement, calibration

    def filter_item(self, item, ctx):
        _, calibration = item
        metadata = json.loads(calibration.read_text())
        return metadata["approved"] is True

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        if not items:
            return None
        datasets = [
            read_dataset(ctx.materialize(measurement), ctx.materialize(calibration))
            for measurement, calibration in items
        ]
        dataset = xr.concat(
            datasets,
            dim=TIME_DIM,
            data_vars="minimal",
            coords="minimal",
            compat="equals",
            join="exact",
        )
        return dataset.sortby(TIME_DIM)
```

## Verify

Give `measurement_001.nc` the value `2.0` with `{"gain": 1.5, "approved": true}`
and `measurement_002.nc` the value `5.0` with `{"gain": 1.0, "approved": false}`,
then run the plugin:

```bash
firecube ingest my_plugin \
  --input-data ./delivery \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct
```

Open the result:

```python
import xarray as xr

print(xr.open_zarr("out.zarr", group="default", consolidated=False)["value"].values)
```

Expected output:

```text
[3.]
```

The approved measurement is stored with its gain applied; the unapproved pair
contributes nothing. Delete `measurement_002.json` and run again on a fresh
target: the run stops with `Missing calibration`.

## Next Steps

- **[Append Datasets To Zarr](generic-zarr.md)** — where `read_dataset` and `TIME_DIM` fit, and how to inspect the output
- **[Hooks & Lifecycle](../../reference/hooks.md)** — look up `discover_source_files` and `filter_item`
- **[Discover Source Data](source-discovery.md)** — discover files with `--input-filters`
