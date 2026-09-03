# Implement DirectZarrIngestor

## Goal

Use `DirectZarrIngestor` when you want slot-based parallel ingestion: many
workers writing disjoint slices of the same Zarr product at the same time,
safely. You declare the product's time axis once, and Firecube plans
chunk-aligned slot ranges, pre-allocates the store, and lets each worker own
its slice without coordination at write time.

The plugin contract is four hooks:

- `index_spec(ctx)` declares the indexed time axis.
- `inspect_item(item, ctx)` returns `ItemInfo(coordinate=...)`.
- `zarr_schema(ctx)` declares the arrays.
- `build_write_intents(batch, ctx)` emits `WriteIntent` objects.

Firecube resolves the declared index once, sizes arrays from
`resolved_index(ctx).size(group)`, and then applies the emitted write intents.

## Choose The Time Axis

Declare the axis with a `TimeAxis` constructor. Pick the row that matches your
product:

| Your product | Constructor |
|---|---|
| Fixed cadence, real timestamps can be slightly off the nominal slot time (most sensors) | [`TimeAxis.observed(...)`](../../reference/parallelism.md#index-types) |
| Fixed cadence, every timestamp is exactly `epoch + n * cadence` | [`TimeAxis.grid(...)`](../../reference/parallelism.md#index-types) |
| Unevenly spaced, all timestamps known before ingestion | [`TimeAxis.explicit(...)`](../../reference/parallelism.md#firecube.ingestor.api.TimeAxis.explicit) |
| Unevenly spaced, timestamps only known after reading the source files | [`TimeAxis.discovered(...)`](../../reference/parallelism.md#firecube.ingestor.api.TimeAxis.discovered) |

The first row is the common case and is what the example below uses. The
last row has its own guide:
[DirectZarrIngestor (Auto)](direct-zarr-auto.md). To
understand what each choice means for the stored coordinate values and for
write verification, read the
[DirectZarrIngestor write model](../../concepts/output-formats/zarr/direct-region.md).
Products whose items map to an integer position instead of a timestamp
declare `IntegerAxis`; see the
[Index Specification Reference](../../reference/parallelism.md#firecube.ingestor.api.IntegerAxis).

## Implement The Plugin

Follow [Create a Plugin](create-a-plugin.md), choose the `zarr` template, and keep the generated registration and product name. Replace the `index_spec`, `inspect_item`, `zarr_schema`, and `build_write_intents` stubs.

The example ingests NetCDF granules that each carry one observation time and
four sample values. Only `read_product_item` knows that format: swap its body
for the product's real reader and everything below it stays the same.

```python
from pathlib import Path
from typing import ClassVar

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


def read_product_item(path: Path) -> tuple[np.datetime64, np.ndarray]:
    """Read one granule: its observation time and its four sample values."""
    with xr.open_dataset(path) as product:
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
                "data": TimeAxis.observed(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    end_date="2024-01-08T00:00:00Z",
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
                        name="timestamp",
                        shape=(n_times,),
                        dtype="datetime64[ns]",
                        chunks=(24,),
                        dimension_names=("timestamp",),
                    ),
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

`inspect_item` returns the real observation timestamp, not a rounded slot
time. Firecube maps it to a slot and stores the observed value in the
coordinate array.

Each `IndexedWrite` carries the raw `coordinate=`; Firecube resolves the slot
index, raises `IndexedWriteCompilationError` for any timestamp it cannot map,
and emits the slot's time-coordinate verify-write for you. Append
`WriteIntent.static(...)` items to the same list for arrays that never move
with the time axis, such as latitude and longitude grids. Every element must
target a declared group and array.

### Choose A Write Factory

| Path | Factory | Use |
|---|---|---|
| Common path | `IndexedWrite.slot(...)` | 1-D writes keyed by timestamp; Firecube resolves the slot. |
| Common path | `IndexedWrite.region(...)` | 2-D region writes keyed by timestamp. |
| Common path | `WriteIntent.static(...)` | Arrays that do not share the indexed axis. |
| Advanced path | `WriteIntent.slot(...)` / `WriteIntent.region(...)` | Writes with a self-resolved `index=`. |
| Advanced path | `WriteIntent.coordinate(...)` | Explicit coordinate writes (auto-emitted otherwise). |
| Escape hatch | `WriteIntent(...)` | Only when no factory fits. |

### Advanced: Resolve Indexes Yourself

When you need the slot index in hand (cross-item logic, custom slicing, or a
write whose index does not come from a coordinate), resolve it yourself and
return plain `WriteIntent` elements:

```python
index = self.resolved_index(ctx).position("data", timestamp)
out.append(WriteIntent.coordinate(group="data", index=index, value=timestamp))
out.append(WriteIntent.slot(group="data", array="value", index=index, data=values))
```

On this path you also emit the coordinate write explicitly; Firecube
auto-emits it only for slots it resolved from `IndexedWrite` elements. Both
element types mix freely in one returned list. See
[Plugin Templates](../../reference/templates.md#firecube.ingestor.api.DirectZarrIngestor.build_write_intents) for
the full contract.

## Verify

Run one ingest against a small input and confirm the store is written:

```bash
uv run firecube ingest my_plugin \
  --input-data ./path/to/input \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

The run should create the target store, write the declared arrays, and report no schema or index errors.

## Run It In Parallel

The serial run above is the smoke test. The payoff is the parallel workflow:
preallocate the store, plan chunk-aligned slot ranges, then start one ingest
worker per range. That is an operator workflow with its own page:
[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md). The
[DirectZarrIngestor (Region) tutorial](../../tutorials/direct-zarr-parallel.md)
walks it end to end with real data.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Replacing `index_spec` with the old slot-era hooks | Declare the time axis in `IndexSpec` and derive indexes from `ResolvedIndex`. |
| Returning raw source indexes from `inspect_item` | Return `ItemInfo(coordinate=...)` and let Firecube resolve the slot index. |
| Returning rounded slot times from `inspect_item` with `TimeAxis.observed` | Return the real observation timestamp; Firecube does the slot mapping. |
| Sizing arrays from the input files | Use `resolved_index(ctx).size(group)` in `zarr_schema`. |
| Importing private runtime modules | Import from `firecube.ingestor.api`. |
| Calling `.position()` for every write by hand | Return `IndexedWrite` elements with `coordinate=` and let Firecube resolve the slot. |

## Next Steps

- **[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md)** - preallocate, slot planning, and worker fan-out
- **[DirectZarrIngestor (Region) Tutorial](../../tutorials/direct-zarr-parallel.md)** - complete tutorial with real timestamps
- **[DirectZarrIngestor (Region)](../../concepts/output-formats/zarr/direct-region.md)** - the write model, time-axis regimes, and coordinate ownership
- **[Index Specification Reference](../../reference/parallelism.md)** - `TimeAxis`, `IndexSpec`, and `ResolvedIndex` types
- **[Plugin Templates](../../reference/templates.md)** - full template surface
