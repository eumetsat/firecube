# Implement DirectZarrIngestor

## Goal

Use `DirectZarrIngestor` when the plugin knows each item's time coordinate and must place data at explicit Zarr indexes. The direct path is:

- `index_spec(ctx)` declares the indexed time axis.
- `inspect_item(item, ctx)` returns `ItemInfo(coordinate=...)`.
- `zarr_schema(ctx)` declares the arrays.
- `build_write_intents(batch, ctx)` emits `WriteIntent` objects.

Firecube resolves the declared index once, sizes arrays from `resolved_index(ctx).size(group)`, and then applies the emitted write intents.

Read [Parallel Zarr writes](../../reference/parallelism.md) for the public index types and resolver helpers.

## Index Spec And Write Intents

### Choose A Write Intent Factory

| Path | Factory | Use |
|---|---|---|
| Common path | `WriteIntent.slot(...)` | 1-D writes on the time axis. |
| Common path | `WriteIntent.coordinate(...)` | Timestamp coordinate arrays. |
| Common path | `WriteIntent.static(...)` | Arrays that do not share the indexed axis. |
| Advanced path | `WriteIntent.region(...)` | 2-D region writes. |
| Escape hatch | `WriteIntent(...)` | Only when no factory fits. |

### Implement The Plugin

Follow [Create a Plugin](create-a-plugin.md), choose the `zarr` template, and keep the generated registration and product name. Replace the `index_spec`, `inspect_item`, `zarr_schema`, and `build_write_intents` stubs.

```python
from pathlib import Path
from typing import ClassVar

import numpy as np

from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexSpec,
    ItemInfo,
    PipelineBatch,
    PluginContext,
    RegularTimeAxis,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


def read_product_item(path: Path) -> tuple[np.datetime64, np.ndarray]:
    ...


@register_ingestor("my_plugin")
class MyPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        return IndexSpec(
            name="my_product_v1",
            groups={
                "data": RegularTimeAxis(
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
        stamp, values = read_product_item(ctx.materialize(item))
        if values.shape != (4,):
            raise ValueError(f"Expected four sample values, got {values.shape}")
        return ItemInfo(coordinate=stamp)

    def build_write_intents(
        self,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            stamp, values = read_product_item(ctx.materialize(item))
            index = self.resolved_index(ctx).position("data", stamp)
            intents.append(WriteIntent.coordinate(group="data", index=index, value=stamp))
            intents.append(
                WriteIntent.slot(
                    group="data",
                    array="value",
                    index=index,
                    data=values,
                )
            )
        return intents
```

Use `WriteIntent.static(...)` for arrays that never move with the time axis, such as latitude and longitude grids. Every intent must target a declared group and array.

### Let The Engine Resolve Slots

When each source item maps to one coordinate-keyed write, override
`build_indexed_write` instead of `build_write_intents` and return an
`IndexedWrite` keyed by the raw coordinate. The engine resolves the slot index
against your `IndexSpec` and builds the `WriteIntent` for you:

```python
from firecube.ingestor.api import IndexedWrite


class MyPlugin(DirectZarrIngestor):
    def build_indexed_write(self, item: object, ctx: PluginContext) -> IndexedWrite | None:
        stamp, values = read_product_item(ctx.materialize(item))
        return IndexedWrite.slot(group="data", array="value", coordinate=stamp, data=values)
```

Return `None` to drop an item. Use `IndexedWrite.region(...)` for 2-D region
writes; its `data` field also accepts a zero-argument callable resolved once at
dispatch time (`IndexedWrite.slot` requires an eager array). A coordinate the
engine cannot map raises `IndexedWriteCompilationError` before any write.

Timestamp-coordinate and static arrays carry no slot coordinate to resolve, so
they still go through `build_write_intents`: override both hooks, call
`super().build_write_intents(batch, ctx)` for the indexed compilation, and
append `WriteIntent.coordinate` / `WriteIntent.static` items. If you override
`build_write_intents` without calling `super()`, `build_indexed_write` is never
reached.

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

## Integer Axis

Use `IntegerAxis` when items map to a zero-based integer position rather than a
timestamp. The axis has a fixed size and no epoch or cadence.

```python
from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexSpec,
    IntegerAxis,
    ItemInfo,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from typing import ClassVar


@register_ingestor("my_integer_plugin")
class MyIntegerPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_integer_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        return IndexSpec(
            name="my_integer_product_v1",
            groups={
                "data": IntegerAxis(slot_count=256),
            },
        )

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        # Return the integer position as the coordinate.
        position: int = ...  # derive from item
        return ItemInfo(coordinate=position)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        n = self.resolved_index(ctx).size("data")
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="value",
                        shape=(n,),
                        dtype="float32",
                        chunks=(32,),
                        dimension_names=("index",),
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
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            idx = self.resolved_index(ctx).position("data", info.coordinate)
            intents.append(
                WriteIntent.slot(
                    group="data",
                    array="value",
                    index=idx,
                    data=...,
                )
            )
        return intents
```

### Mixed Axes

A single `IndexSpec` can mix `IntegerAxis` and `RegularTimeAxis` groups. Each
group resolves its own axis independently.

```python
from firecube.ingestor.api import IndexSpec, IntegerAxis, RegularTimeAxis


def index_spec(self, ctx):
    return IndexSpec(
        name="my_mixed_product_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                end_date="2024-01-08T00:00:00Z",
            ),
            "lookup": IntegerAxis(slot_count=64),
        },
    )
```

Use `resolved_index(ctx).size("data")` and `resolved_index(ctx).size("lookup")`
separately in `zarr_schema`. Use `resolved_index(ctx).position("data", stamp)`
for the time group and `resolved_index(ctx).position("lookup", integer_key)` for
the integer group.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Replacing `index_spec` with the old slot-era hooks | Declare the time axis in `IndexSpec` and derive indexes from `ResolvedIndex`. |
| Returning raw source indexes from `inspect_item` | Return `ItemInfo(coordinate=...)` and let Firecube resolve the slot index. |
| Sizing arrays from the input files | Use `resolved_index(ctx).size(group)` in `zarr_schema`. |
| Importing private runtime modules | Import from `firecube.ingestor.api`. |

## Next Steps

- **[Parallel Zarr Writes](../../reference/parallelism.md)** - reference for `IndexSpec`, `RegularTimeAxis`, and `ResolvedIndex`
- **[Parallel DirectZarrIngestor](../../tutorials/direct-zarr-parallel.md)** - complete tutorial with real timestamps
- **[Plugin Templates](../../reference/templates.md)** - full template surface
