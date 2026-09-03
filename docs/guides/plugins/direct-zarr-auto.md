# Implement DirectZarrIngestor (Auto)

## Goal

Declare `TimeAxis.discovered` and let Firecube build the time axis for you:
it reads every item's timestamp through `inspect_item` before any write,
whatever the spacing turns out to be. Use it when you cannot, or do not want
to, declare the epoch, cadence, or timestamp list yourself.

A declaration you can make yourself buys more: `TimeAxis.explicit` skips
discovery when the timestamp list is known upfront, and a declared cadence
(`TimeAxis.grid` or `TimeAxis.observed`) lets Firecube plan slot ranges
without reading any data and extend the axis as new windows arrive. The
[routing table](direct-zarr.md#choose-the-time-axis) in the
`DirectZarrIngestor` guide compares all four.

## Minimal Example

This is the [`DirectZarrIngestor` guide's plugin](direct-zarr.md#implement-the-plugin)
with one change: `index_spec` declares `TimeAxis.discovered` instead of an
axis with a cadence. Nothing else moves. `zarr_schema` already sizes its
arrays from `resolved_index(ctx).size("data")`, which now reports the number
of discovered timestamps, and `build_write_intents` already keys each write
by `coordinate=`, which Firecube resolves against the discovered axis.

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
            groups={"data": TimeAxis.discovered(coordinate="timestamp")},
        )

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        timestamp, values = read_product_item(ctx.materialize(item))
        if values.shape != (4,):
            raise ValueError(f"Expected four sample values, got {values.shape}")
        return ItemInfo(coordinate=timestamp)

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

Before the first write, Firecube runs `inspect_item` over every source item,
sorts the collected timestamps, and builds the axis from them. Each
timestamp's position in that sorted order is its slot, so slot 0 always holds
the earliest observation regardless of file naming or discovery order.

## Write `inspect_item` For Discovery

Discovery makes `inspect_item` load-bearing. It must follow three rules:

- Return `ItemInfo(coordinate=timestamp)` for every item that belongs in the
  cube.
- Return `None` to skip an item entirely.
- Return `ItemInfo(coordinate=None)` only when the item exists but its
  timestamp cannot be read; Firecube then fails discovery loudly instead of
  writing a cube with a hole.

`inspect_item` runs once during discovery and again during the write phase,
so it must be idempotent and must not depend on the order items arrive in.
Duplicate timestamps are refused at discovery time, before any array is
created. The
[`TimeAxis.discovered` reference](../../reference/parallelism.md#firecube.ingestor.api.TimeAxis.discovered)
names each discovery error and its cause.

## Verify

A discovered axis needs no extra commands for a serial run; discovery happens
inside the normal ingest:

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

Then confirm the stored coordinate holds every source timestamp in sorted
order:

```bash
uv run python -c "
import zarr
root = zarr.open_group('/tmp/my_product.zarr', mode='r')
print(root['data/timestamp'][:])
"
```

Every timestamp read by `inspect_item` should appear exactly once, sorted
ascending, with no `NaT` entries.

## Run It In Parallel

Discovered axes support slot-range parallelism like any other declared axis.
Discovery runs once, during `firecube zarr preallocate` with `--input-data`,
and its sealed result is shared by every worker, which is why items must
resolve through references that stay valid for the whole run rather than
temporary paths. Follow
[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md) for the
workflow.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Returning `ItemInfo(coordinate=None)` to skip an item | Return `None` for a skip; `coordinate=None` means the timestamp is unreadable and fails discovery. |
| An `inspect_item` that depends on discovery order or mutates state | Keep it a pure read of one item; it runs more than once. |
| Two source items carrying the same timestamp | Deduplicate at the source or skip one of them; duplicates are refused at discovery time. |
| Declaring `TimeAxis.observed` with an invented cadence for irregular data | Use `TimeAxis.discovered`; a wrong cadence maps different observations onto the same slot. |
| Handing discovery temporary or per-process paths | Resolve items through stable references valid for the cube's lifetime. |

## Next Steps

- **[DirectZarrIngestor (Auto) Tutorial](../../tutorials/direct-zarr-auto.md)** — build and run this plugin end to end
- **[`TimeAxis.discovered` Reference](../../reference/parallelism.md#firecube.ingestor.api.TimeAxis.discovered)** — exact constructor contract and failure modes
- **[DirectZarrIngestor (Region)](../../concepts/output-formats/zarr/direct-region.md)** — how each axis regime owns the coordinate array
- **[Implement DirectZarrIngestor](direct-zarr.md)** — the full plugin contract this page builds on
- **[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md)** — preallocate, plan, and launch workers
