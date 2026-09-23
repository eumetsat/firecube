# Declare A Calendar

## Goal

Write a `DirectZarrIngestor` cube whose time axis follows a CF calendar other
than the Gregorian one, such as the `360_day` calendar of many climate model
runs. You declare the calendar once, on the `TimeAxis`, and Firecube maps
every calendar date to its slot, stores the coordinate with the CF `units`
and `calendar` attributes readers expect, and refuses values on any other
calendar instead of mislabelling them.

Every time axis has a calendar. When you declare none, it is
`proleptic_gregorian`, the coordinate is stored as `datetime64`, and nothing
on this page applies. Declare a calendar only when the source data carries
one.

## Supported Calendars

| Declare | Also accepted as | Stored coordinate |
|---|---|---|
| `proleptic_gregorian` (default) | `standard`, `gregorian` | `datetime64[ns]`, no CF attributes |
| `360_day` | | `int64` seconds since the epoch, with `units` and `calendar` attributes |
| `noleap` | `365_day` | same |
| `all_leap` | `366_day` | same |
| `julian` | | same |

Names are case-insensitive. Firecube treats `standard` and `gregorian` as
`proleptic_gregorian`: dates before 1582-10-15 follow the proleptic Gregorian
calendar, as `datetime64` always has, not the mixed Julian and Gregorian
calendar CF assigns to `standard`.

## Prerequisites

This page changes the [`DirectZarrIngestor` guide's plugin](direct-zarr.md#implement-the-plugin).
Read that guide first; the four hooks, the write factories, and the operator
workflow are the same. The example below reads NetCDF granules whose time
variable carries `calendar = "360_day"`.

## Declare The Axis

Pass `calendar=` to the `TimeAxis` constructor that matches your product.

| Constructor | Calendar declaration |
|---|---|
| `TimeAxis.grid(...)` | `calendar="360_day"` and `slot_count=`; the `epoch` is a date in that calendar |
| `TimeAxis.explicit(...)` | `calendar="360_day"` and `units="days since 2049-01-01"`; `values` are dates in that calendar |
| `TimeAxis.discovered(...)` | `calendar="360_day"` and `units="days since 2049-01-01"`; `inspect_item` returns dates in that calendar |
| `TimeAxis.observed(...)` | not available; observed placement needs a Gregorian axis |

Two rules differ from a Gregorian axis:

- A regular axis needs `slot_count`; `end_date` is refused. Firecube writes
  the whole coordinate at preallocate time, so the extent must be known.
- An irregular axis needs `units`, because it has no epoch to derive them
  from. A Gregorian axis refuses `units`.

## Implement The Plugin

The listing is the `DirectZarrIngestor` guide's plugin with three changes:
`index_spec` declares the calendar, `read_product_item` keeps the calendar
dates the file carries, and `zarr_schema` leaves the time coordinate array
to Firecube.

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
    with xr.open_dataset(path, use_cftime=True) as product:
        timestamp = product["time"].values[0]
        values = product["value"].values.astype(np.float32)
    return timestamp, values


@register_ingestor("my_plugin")
class MyPlugin(DirectZarrIngestor):
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

`use_cftime=True` makes xarray return the file's dates as calendar-aware
objects that carry their own `calendar`. Hand those objects to
`ItemInfo` and to the write factories unchanged: Firecube checks that the
value's calendar matches the axis and computes the slot. A value on a
different calendar fails the run before anything is written. A plain
`datetime`, `numpy.datetime64`, or ISO string is Gregorian and is refused on
a calendar axis for the same reason.

If your reader already produces numbers in the axis units, such as seconds
since the epoch for a regular axis or days since the `units` origin for an
irregular axis, return them as `int` or `float` instead. Firecube takes a
number as already encoded.

Keep the coordinate's name in `coord_names` but leave its `ZarrArraySpec`
out: `firecube zarr preallocate` creates the array as `int64`, fills it with
the encoded dates, and stamps `units` and `calendar` on it. If you declare
the array yourself, declare `int64` (or `float64` when dates fall between
whole units) and set `fill_value=np.iinfo(np.int64).min` (or `np.nan`). The
default fill of `0` encodes the epoch date, so preallocate reads a fresh
array as already written and reports a diverged grid. A `datetime64`
coordinate array is refused because it contradicts the calendar.

## Preallocate Before You Ingest

A calendar coordinate is written once, in full, by `firecube zarr preallocate`.
An ingest against a store that has not been preallocated stops with an error
naming this command. Check the axis first:

```bash
firecube zarr preallocate my_plugin \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --write-mode direct \
  --dry-run
```

The dry run prints the window and its bounds as dates on the declared
calendar:

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
  --input-data ./path/to/input \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --write-mode direct
```

`--slot-start` and `--slot-end` still limit which data slots a preallocate
call touches, but the coordinate array is always written in full, so a
partially preallocated calendar store never holds undecodable dates.

## Verify

Confirm the coordinate carries the calendar and decodes to the dates the
source files hold:

```bash
python -c "
import xarray as xr
ds = xr.open_zarr('/tmp/my_product.zarr', group='data', consolidated=False)
print(ds['timestamp'].encoding['units'], ds['timestamp'].encoding['calendar'])
print(ds['timestamp'].values[58:61])
"
```

Expected output for the example axis, with 30-day months and no leap rules:

```text
seconds since 2049-01-01 00:00:00 360_day
[cftime.Datetime360Day(2049, 2, 29, 0, 0, 0, 0, has_year_zero=True)
 cftime.Datetime360Day(2049, 2, 30, 0, 0, 0, 0, has_year_zero=True)
 cftime.Datetime360Day(2049, 3, 1, 0, 0, 0, 0, has_year_zero=True)]
```

The resolved index shows the same dates without opening the arrays:

```bash
firecube zarr index show \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --derived
```

## Append Datasets With A Calendar

`GenericZarrIngestor` needs no declaration. Build the `xarray.Dataset` with
a calendar-aware time coordinate, for example from `xr.open_dataset(...,
use_cftime=True)` or `xr.date_range(..., calendar="360_day", use_cftime=True)`,
and xarray writes the `units` and `calendar` attributes on append. Firecube
decodes them when it orders and deduplicates appends in both write modes.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Declaring `end_date` on a calendar axis | Declare `slot_count`; the extent must be known so the coordinate can be written in full. |
| Declaring `TimeAxis.explicit` or `TimeAxis.discovered` with a calendar but no `units` | Add `units="days since 2049-01-01"` or another CF units string in the axis calendar. |
| Returning `numpy.datetime64` or `datetime` values on a calendar axis | Decode the source with `use_cftime=True` and return the calendar-aware objects, or return numbers in the axis units. |
| Returning values on a different calendar than the axis | Declare the calendar the data carries; Firecube names both calendars in the error. |
| Declaring the coordinate array as `datetime64[ns]` | Leave the array out of `zarr_schema`, or declare `int64` (or `float64`). |
| Declaring the coordinate array as `int64` with the default fill value | Leave the array out of `zarr_schema`, or set `fill_value=np.iinfo(np.int64).min`; a fill of `0` is a valid encoded date. |
| Running `firecube ingest` on a fresh target | Run `firecube zarr preallocate` first; calendar coordinates are written only there. |
| Declaring `TimeAxis.observed` with a calendar | Use `TimeAxis.grid` with dates on the grid; observed placement is Gregorian only. |
| Expecting `firecube archive create` to pack a calendar cube | Not supported yet; the command refuses rather than dropping the time coordinate. |

## Next Steps

- **[Declare The Schema And Index](direct-zarr.md)** - the full plugin contract this page builds on
- **[Discover The Time Axis](direct-zarr-auto.md)** - `TimeAxis.discovered` with a calendar and `units`
- **[Index Specification Reference](../../reference/parallelism.md#firecube.ingestor.api.RegularTimeAxis)** - `calendar`, `units`, and `slot_count` on `RegularTimeAxis` and `IrregularTimeAxis`
- **[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md)** - preallocate, plan, and launch workers
