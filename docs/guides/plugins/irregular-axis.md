# IrregularTimeAxis And AUTO Discovery

## Goal

Use `IrregularTimeAxis` when the time coordinates of your product are not
evenly spaced and cannot be described by a fixed epoch and cadence. The axis
holds an explicit list of coordinate values, either supplied at declaration time
or discovered automatically from the source data.

This guide covers:

- when to choose `IrregularTimeAxis` over `RegularTimeAxis` or `IntegerAxis`;
- how to declare the axis with a concrete tuple of values;
- how to use `AUTO` to let the engine discover coordinates at planning time;
- how to implement `inspect_item` for the `AUTO` path;
- the `source_ref` stability contract;
- failure modes and the exceptions the engine raises.

## Choose The Right Axis

| Axis | Use when |
|---|---|
| `RegularTimeAxis` | Items arrive at a fixed cadence (e.g. every 10 minutes). |
| `IntegerAxis` | Items map to a zero-based integer position with no time meaning. |
| `IrregularTimeAxis` | Items have timestamps that are not evenly spaced, or the full set of timestamps is only known after reading the source data. |

If the cadence is fixed, prefer `RegularTimeAxis`. The engine can plan slot
ranges for parallel workers without reading any source files.

## Declare With Concrete Values

When you know all timestamps before ingestion starts, pass them as a tuple:

```python
import numpy as np
from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexSpec,
    IrregularTimeAxis,
    ItemInfo,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from typing import ClassVar


@register_ingestor("my_irregular_plugin")
class MyIrregularPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_irregular_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        timestamps = (
            np.datetime64("2026-01-01T00:00:00", "ns"),
            np.datetime64("2026-01-01T00:17:30", "ns"),
            np.datetime64("2026-01-01T00:42:00", "ns"),
        )
        return IndexSpec(
            name="my_irregular_product_v1",
            groups={
                "data": IrregularTimeAxis(
                    coordinate="timestamp",
                    values=timestamps,
                ),
            },
        )

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        stamp = read_timestamp(ctx.materialize(item))
        return ItemInfo(coordinate=stamp)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        n = self.resolved_index(ctx).size("data")
        return [
            ZarrGroupSpec(
                group="data",
                coord_names=frozenset({"timestamp"}),
                arrays=[
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(n,),
                        dtype="datetime64[ns]",
                        chunks=(n,),
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="value",
                        shape=(n, 4),
                        dtype="float32",
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
    ) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            stamp = read_timestamp(ctx.materialize(item))
            index = self.resolved_index(ctx).position("data", stamp)
            intents.append(WriteIntent.coordinate(group="data", index=index, value=stamp))
            intents.append(
                WriteIntent.slot(
                    group="data",
                    array="value",
                    index=index,
                    data=read_values(ctx.materialize(item)),
                )
            )
        return intents
```

The engine writes the coordinate array to the Zarr store during preallocate.
`resolved_index(ctx).position("data", stamp)` maps each timestamp to its
declared slot index.

## Declare With AUTO

When the full set of timestamps is only known after reading the source files,
set `values=AUTO`. The engine calls `inspect_item` on every discovered source
item before preallocate, collects the returned coordinates, and builds the axis
from them.

```python
from firecube.ingestor.api import AUTO, IrregularTimeAxis
```

```python
def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
    _ = ctx
    return IndexSpec(
        name="my_irregular_product_v1",
        groups={
            "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
        },
    )
```

With `AUTO`, `inspect_item` runs twice: once during discovery (to collect
coordinates) and once during the write phase (to produce write intents). The
implementation must be idempotent and must not depend on discovery order.

### Implement `inspect_item` For AUTO

Return `ItemInfo(coordinate=stamp)` for every item that has a valid coordinate.
Return `None` to skip an item entirely. Return `ItemInfo(coordinate=None)` only
when the item is present but its coordinate cannot be resolved; the engine
raises `MissingIrregularCoordinateError` in that case.

```python
def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
    path = ctx.materialize(item)
    stamp = read_timestamp(path)
    if stamp is None:
        # Item has no timestamp: skip it.
        return None
    return ItemInfo(coordinate=stamp)
```

The engine sorts discovered coordinates and assigns each a slot index in
ascending order. Duplicate coordinates raise `DuplicateIrregularCoordinateError`,
naming both conflicting items and the shared coordinate value.

### `source_ref` Stability Contract

During AUTO discovery the engine records a `source_ref` for each item. The
`source_ref` is a stable reference the engine uses to hand work to parallel
workers without a second discovery pass. The stability contract depends on the
kind:

| `source_ref_kind` | What the caller promises |
|---|---|
| `"path"` | Absolute filesystem path is stable for the cube's lifetime. |
| `"uri"` | URL (e.g. `s3://bucket/key`) is dereferenceable for the cube's lifetime. |
| `"identifier"` | Plugin-defined identifier is resolvable by the plugin's own logic for the cube's lifetime. |

Do not use paths that change between discovery and write (e.g. temporary
directories, session-scoped scratch paths). If the source data moves, the
engine cannot dereference the recorded reference.

## Preallocate

Run preallocate before starting workers. For `AUTO`, the engine runs discovery
during preallocate and writes the coordinate array to the Zarr store:

```bash
firecube zarr preallocate my_irregular_plugin \
  --target file:///data/products/my_irregular_product.zarr \
  --product-name my_irregular_product \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct
```

Use `--dry-run` to inspect the resolved index without writing anything:

```bash
firecube zarr preallocate my_irregular_plugin \
  --target file:///data/products/my_irregular_product.zarr \
  --product-name my_irregular_product \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct \
  --dry-run
```

The dry-run output is the same JSON format as `firecube zarr index show --json`.
No files are created or modified.

## Failure Modes

| Error | Cause | Fix |
|---|---|---|
| `MissingIrregularCoordinateError` | `inspect_item` returned `ItemInfo(coordinate=None)` for an item. | Check the source data for items with missing timestamps. |
| `DuplicateIrregularCoordinateError` | Two discovered items resolve to the same coordinate value. | Check the source data for duplicate timestamps. |
| `NoDiscoveredItemsError` | Discovery found zero items for the axis. | Verify that the source path contains data and that `filter_item` is not excluding everything. |
| `ConfigurationError` | The declared `values` tuple contains duplicates, or `values` is empty. | Pass a non-empty tuple with distinct values, or use `AUTO`. |

## See Also

- **[Index Specification Reference](../../reference/parallelism.md)** - `IrregularTimeAxis`, `AUTO`, `IndexSpec`, and `ResolvedIndex` types
- **[Exceptions Reference](../../reference/exceptions.md)** - `MissingIrregularCoordinateError`, `DuplicateIrregularCoordinateError`, and `NoDiscoveredItemsError`
- **[Implement DirectZarrIngestor](direct-zarr.md)** - the full `DirectZarrIngestor` plugin contract
- **[Run Parallel Zarr Writes](../../operations/parallel-zarr-writes.md)** - preallocate and slot-range workflow
