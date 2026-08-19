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
                    size=1008,
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
