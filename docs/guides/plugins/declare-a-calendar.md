# Declare A Calendar

## Goal

Declare a CF calendar other than the Gregorian one on the time axis of a `DirectZarrIngestor` plugin. An example is the `360_day` calendar of many climate model runs. You declare the calendar once, on the `TimeAxis`. Firecube then maps each calendar date to its slot. It stores the coordinate with the CF `units` and `calendar` attributes. It refuses a value on any other calendar.

Every time axis has a calendar. If you declare none, the calendar is `proleptic_gregorian` and the coordinate is `datetime64`. Nothing on this page applies to that axis. Declare a calendar only when the source data carries one.

## Prerequisites

Read the [`DirectZarrIngestor` guide](direct-zarr.md#implement-the-plugin) first. The four hooks, the write factories and the operator workflow are the same here. The example below reads NetCDF granules whose time variable carries `calendar = "360_day"`. Create the project as in [Create a Plugin](create-a-plugin.md), and install it as in [Install Your Plugin](install-a-plugin.md). Replace the generated `ingestor.py` with the listing below. Do not change the generated `__init__.py`, because the listing uses the generated class name `MyPluginIngestor`. The page needs `xarray`, `cftime` and `h5netcdf` in that environment.

## Create The Input Files

Create 90 daily granules, one for each slot of the example axis. Each file holds one `360_day` date and four sample values:

```python
from pathlib import Path

import numpy as np
import xarray as xr

Path("input").mkdir(exist_ok=True)
days = xr.date_range("2049-01-01", periods=90, freq="1D", calendar="360_day", use_cftime=True)
for number, day in enumerate(days):
    granule = xr.Dataset(
        {"value": ("sample", np.full(4, number, dtype=np.float32))},
        coords={"time": ("t", [day])},
    )
    granule.to_netcdf(f"input/granule_{number:03d}.nc", engine="h5netcdf")
```

The commands below use `./input` as the input path.

## Supported Calendars

| Declare | Also accepted as |
|---|---|
| `proleptic_gregorian` (default) | `standard`, `gregorian` |
| `360_day` | none |
| `noleap` | `365_day` |
| `all_leap` | `366_day` |
| `julian` | none |

Names are case-insensitive. Firecube reads `standard` and `gregorian` as `proleptic_gregorian`. A date before 1582-10-15 therefore follows the proleptic Gregorian rules, as `datetime64` always does.

A Gregorian coordinate is `datetime64[ns]`. That type holds instants from about 1677-09-21 to 2262-04-11. Firecube refuses a Gregorian value outside that range. A calendar axis stores encoded numbers and has no such limit.

## Declare The Axis

Pass `calendar=` to the `TimeAxis` constructor that matches your product.

| Constructor | Calendar declaration |
|---|---|
| `TimeAxis.grid(...)` | `calendar="360_day"` and `slot_count=`. The `epoch` is a date in that calendar. |
| `TimeAxis.explicit(...)` | `calendar="360_day"` and `units="days since 2049-01-01"`. The `values` are dates in that calendar. |
| `TimeAxis.discovered(...)` | `calendar="360_day"` and `units="days since 2049-01-01"`. `inspect_item` returns dates in that calendar. |
| `TimeAxis.observed(...)` | Not available. Use `TimeAxis.grid` instead. |

Three rules differ from a Gregorian axis:

- A regular axis needs `slot_count`, and Firecube refuses `end_date`. Firecube writes the whole coordinate at preallocate time, so it must know the extent.
- An irregular axis needs `units`, because it has no epoch to derive them from. A Gregorian axis refuses `units`.
- A month on a `360_day` axis is exactly 30 days. For a monthly cube, declare a regular grid with `cadence_s=2592000`, which is 30 days of 86400 seconds. If the `epoch` is the first day of a month, slot 1 is the first day of the next month.

## Implement The Plugin

The listing is the plugin of the `DirectZarrIngestor` guide with three changes:

- `index_spec` declares the calendar.
- `read_product_item` keeps the calendar dates of the file.
- `zarr_schema` leaves the time coordinate array to Firecube.

```python
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import xarray as xr

from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexedWrite,
    IndexSpec,
    ItemInfo,
    PipelineBatch,
    PluginContext,
    TimeAxis,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


def read_product_item(path: Path) -> tuple[Any, np.ndarray]:
    """Read one granule: its calendar date and its four sample values."""
    coder = xr.coders.CFDatetimeCoder(use_cftime=True)
    with xr.open_dataset(path, decode_times=coder) as product:
        timestamp = product["time"].values[0]
        values = product["value"].values.astype(np.float32)
    return timestamp, values


@register_ingestor("my_plugin")
class MyPluginIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        return IndexSpec(
            name="my_product_v1",
            groups={
                "data": TimeAxis.grid(
                    coordinate="timestamp",
                    epoch="2049-01-01T00:00:00Z",
                    cadence_s=86400,
                    slot_count=90,
                    calendar="360_day",
                ),
            },
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        n_times = self.resolved_index(ctx).size("data")
        return [
            ZarrGroupSpec(
                group="data",
                coord_names=frozenset({"timestamp"}),
                arrays=[
                    ZarrArraySpec(
                        name="value",
                        shape=(n_times, 4),
                        dtype=np.float32,
                        chunks=(1, 4),
                        dimension_names=("timestamp", "sample"),
                    ),
                ],
            )
        ]

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        timestamp, values = read_product_item(ctx.materialize(item))
        if values.shape != (4,):
            raise ValueError(f"Expected four sample values, got {values.shape}")
        return ItemInfo(coordinate=timestamp)

    def build_write_intents(
        self,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for item in batch.items:
            timestamp, values = read_product_item(ctx.materialize(item))
            out.append(
                IndexedWrite.slot(
                    group="data",
                    array="value",
                    coordinate=timestamp,
                    data=values,
                )
            )
        return out
```

The option `decode_times=xr.coders.CFDatetimeCoder(use_cftime=True)` makes xarray return the dates of the file as calendar-aware objects. Each object carries its own `calendar`. Pass these objects unchanged to `ItemInfo` and to the write factories. Firecube compares the calendar of the value with the axis and computes the slot. If the calendar differs, the batch fails with an `IndexedWriteCompilationError` that names both calendars. A plain `datetime`, a `numpy.datetime64` or an ISO string is Gregorian. Firecube refuses it on a calendar axis for the same reason.

If your reader already produces numbers in the axis units, return them as `int` or `float`. For a regular axis the unit is seconds since the epoch. For an irregular axis it is the `units` you declared. Firecube takes a number as already encoded.

Keep the name of the coordinate in `coord_names`, and leave its `ZarrArraySpec` out. For a regular grid, as in the example, the preallocate command creates the array as `int64`. It fills the array with the encoded dates and sets `units` and `calendar` on it. For an irregular axis, the array is `int64` when every value is whole. Otherwise it is `float64` with `NaN` as the fill value. If you declare the array yourself, use these rules:

- Declare `int64`. Declare `float64` when a date falls between whole units.
- Set `fill_value=np.iinfo(np.int64).min` for `int64`, or `np.nan` for `float64`.
- Do not declare `datetime64`. Firecube refuses it because it contradicts the calendar.

The default fill value `0` is a valid encoded date. Preallocate then reads a fresh array as written and reports a diverged grid.

## Preallocate Before You Ingest

Run `firecube zarr preallocate` before the first ingest. Firecube writes a calendar coordinate once, in full, and only there. A serial run in the [`DirectZarrIngestor` guide](direct-zarr.md) needs no preallocate step, so a calendar adds a new first command. An ingest into a store that is not preallocated stops with an error that names this command.

First, inspect the axis with a dry run:

```bash
firecube zarr preallocate my_plugin \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --write-mode direct \
  --dry-run
```

The command writes nothing. It prints the resolved index to standard output. It prints one line for each group to standard error. The line shows the window and its bounds as dates on the declared calendar:

```text
group data: window [0, 90); policy=grid; items_in_window=90; first=2049-01-01T00:00:00 (360_day); last=2049-03-30T00:00:00 (360_day); would write nominal grid values and stamp firecube_preallocated
```

Then preallocate and ingest:

```bash
firecube zarr preallocate my_plugin \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --write-mode direct

firecube ingest my_plugin \
  --input-data "file://$PWD/input" \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --write-mode direct
```

If you preallocate again with the same axis, nothing changes. To see a refusal, change `epoch` to `"2050-01-01T00:00:00Z"` or `calendar` to `"noleap"` in `index_spec`. Then run the preallocate command again. The command exits with an error that names the field that differs. It writes no data. Restore the declaration of the store, or write to a new target.

The options `--slot-start` and `--slot-end` limit the data slots that a preallocate call touches. They do not limit the coordinate. Firecube always writes the whole coordinate, so a partly preallocated store never holds dates it cannot decode. A dry run with a window ends its line with `; window not applied to calendar coordinate`.

## Verify

Verify that the coordinate carries the calendar and decodes to the dates in the source files:

```bash
python -c "
import xarray as xr
ds = xr.open_zarr('/tmp/my_product.zarr', group='data', consolidated=False)
print(ds['timestamp'].encoding['units'], ds['timestamp'].encoding['calendar'])
print(ds['timestamp'].values[58:61])
"
```

The expected output for the example axis, with 30-day months and no leap rules, is:

```text
seconds since 2049-01-01 00:00:00 360_day
[cftime.Datetime360Day(2049, 2, 29, 0, 0, 0, 0, has_year_zero=True)
 cftime.Datetime360Day(2049, 2, 30, 0, 0, 0, 0, has_year_zero=True)
 cftime.Datetime360Day(2049, 3, 1, 0, 0, 0, 0, has_year_zero=True)]
```

The resolved index lists the same dates and does not open the arrays:

```bash
firecube zarr index show \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --derived
```

The list contains `2049-02-30T00:00:00`.

## Append Datasets With A Calendar

A `GenericZarrIngestor` plugin needs no declaration. Build the `xarray.Dataset` with a calendar-aware time coordinate. Open the files as in the plugin above, or use `xr.date_range(..., calendar="360_day", use_cftime=True)`. Xarray then writes the `units` and `calendar` attributes on append. Firecube decodes them when it orders and deduplicates appends in both write modes. The [Append Datasets To Zarr](generic-zarr.md) guide shows the plugin. Only the `read_dataset` body changes. Both examples register `my_plugin`. Replace the `ingestor.py` of the first example with the plugin of that guide, and reinstall the project. Do not register two plugins with the same ID.

```python
with xr.open_dataset(path, decode_times=xr.coders.CFDatetimeCoder(use_cftime=True)) as dataset:
    return dataset.load()
```

Create two batches of five daily files. The first batch starts on 2049-01-01. The second batch starts on 2049-01-06:

```python
from pathlib import Path

import numpy as np
import xarray as xr

days = xr.date_range("2049-01-01", periods=10, freq="1D", calendar="360_day", use_cftime=True)
for number, day in enumerate(days):
    folder = Path("first-batch" if number < 5 else "second-batch")
    folder.mkdir(exist_ok=True)
    daily = xr.Dataset(
        {"value": ("time", np.array([number], dtype=np.float32))},
        coords={"time": [day]},
    )
    daily.to_netcdf(folder / f"day_{number:03d}.nc", engine="h5netcdf")
```

Ingest the first batch:

```bash
firecube ingest my_plugin \
  --input-data "file://$PWD/first-batch" \
  --target file:///tmp/my_plugin_out.zarr \
  --product-name my_plugin \
  --write-mode direct
```

Firecube refuses a second run into the same store unless you opt in. Add `--option resume_existing=true` to append new timestamps:

```bash
firecube ingest my_plugin \
  --input-data "file://$PWD/second-batch" \
  --target file:///tmp/my_plugin_out.zarr \
  --product-name my_plugin \
  --write-mode direct \
  --option resume_existing=true
```

Verify that both batches landed and the calendar survived:

```bash
python -c "
import xarray as xr
ds = xr.open_zarr('/tmp/my_plugin_out.zarr', group='default', consolidated=False)
print(ds.sizes['time'], ds['time'].encoding['units'], ds['time'].encoding['calendar'])
print(ds['time'].values[[0, -1]])
"
```

The expected output for ten daily files that start on 2049-01-01 in the `360_day` calendar, five for each run, is:

```text
10 days since 2049-01-01 360_day
[cftime.Datetime360Day(2049, 1, 1, 0, 0, 0, 0, has_year_zero=True)
 cftime.Datetime360Day(2049, 1, 10, 0, 0, 0, 0, has_year_zero=True)]
```

Compare the calendar, the count and the two dates with the example. Xarray chooses the text of `units`.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Declaring `end_date` on a calendar axis | Declare `slot_count`. |
| Declaring `TimeAxis.explicit` or `TimeAxis.discovered` with a calendar and no `units` | Add `units="days since 2049-01-01"` or another CF units string. |
| Returning `numpy.datetime64` or `datetime` values on a calendar axis | Decode the source with `CFDatetimeCoder(use_cftime=True)`, or return numbers in the axis units. |
| Returning values on a different calendar than the axis | Declare the calendar that the data carries. The error names both calendars. |
| Declaring the coordinate array as `datetime64[ns]` | Leave the array out of `zarr_schema`, or declare `int64` or `float64`. |
| Declaring the coordinate array as `int64` with the default fill value | Leave the array out of `zarr_schema`, or set `fill_value=np.iinfo(np.int64).min`. |
| Running `firecube ingest` into a target that is not preallocated | Run `firecube zarr preallocate` first. |
| Declaring `TimeAxis.observed` with a calendar | Use `TimeAxis.grid` with dates on the grid. |
| Preallocating again after you change the epoch, `units` or calendar | Firecube refuses and writes no data. Keep the declaration, or preallocate a new target. |
| Running `firecube archive create --source file:///tmp/my_product.zarr --archive file:///tmp/my_product.tgm` (short flags `-s` and `-a`) on a calendar cube | Without the tensogram extra, the command exits with an error. Install it with `uv pip install 'firecube[tensogram]'`. With the extra, the default `tensogram` format cannot carry `units` and `calendar`, so Firecube refuses the cube. Add `--archive-format zip` and use a `.zip` archive path. A zip archive keeps the whole store, and `firecube archive restore --archive <zip> --target <uri>` returns the coordinate with its `units` and `calendar`. Restoring a `.tgm` archive does not restore the CF `units` and `calendar` of a time coordinate. |
| Running `firecube chunks list --product-name file:///tmp/my_product.zarr --time-range 2049-02-01:2049-03-01` (short flag `-n`) on a `360_day` store, or the same filter with `firecube chunks delete` | Firecube reads the recorded span bounds as Gregorian dates. If a bound is not a Gregorian date, such as 30 February, the command exits with an error that names the span. `delete` removes nothing. |
| Appending a Gregorian batch to a `360_day` store | The batch fails, Firecube records the run as failed and appends nothing. Build the batch on the calendar of the store. |

## Next Steps

- **[Declare The Schema And Index](direct-zarr.md)** - the full plugin contract this page builds on
- **[Index Specification Reference](../../reference/parallelism.md#firecube.ingestor.api.RegularTimeAxis)** - `calendar`, `units` and `slot_count` on the time axes
- **[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md)** - preallocate, plan and start workers
