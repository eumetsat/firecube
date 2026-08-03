# Implement DirectZarrIngestor

## Goal

Implement a plugin that places data at known indexes in declared Zarr arrays.
This avoids the shared append cursor used by `GenericZarrIngestor`. When the
product has a fixed global extent, the optional slot contract lets separate
ingest processes write disjoint, chunk-aligned ranges of the same group.

The plugin supplies a schema and explicit write intents instead of returning an
`xarray.Dataset`; Firecube creates or validates the arrays and applies the
intents through `zarr-python`. Use this class when precise indexed placement or
same-group slot parallelism justifies that additional responsibility. It also
supports serial writes; parallelism is not enabled merely by subclassing it.

Read
[DirectZarrIngestor (Region)](../../concepts/output-formats/zarr/direct-region.md)
before implementing the schema.

## Edit The Plugin Class

Follow [Create a Plugin](create-a-plugin.md), select `zarr` and the
`zarr-python` write strategy, then [install the plugin](install-a-plugin.md).

Edit `src/firecube_my_plugin/ingestor.py`. Keep the generated registration and
product name, and replace the `zarr_schema` and `build_write_intents` stubs.
The creation-time write strategy selects this class. The later ingest flag
`--write-mode direct` selects direct target writing rather than staged upload.

## Declare The Schema And Writes

The source parser and array layout are product-specific. This small example
shows the contract for one indexed array without prescribing a source format:

```python
from pathlib import Path
from typing import ClassVar

import numpy as np

from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


def read_product_item(path: Path) -> tuple[int, np.datetime64, np.ndarray]:
    ...


@register_ingestor("my_plugin")
class MyPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                coord_names=frozenset({"timestamp"}),
                arrays=[
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(0,),
                        dtype="datetime64[ns]",
                        chunks=(24,),
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="value",
                        shape=(0, 4),
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
    ) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            index, timestamp, values = read_product_item(ctx.materialize(item))
            if values.shape != (4,):
                raise ValueError(f"Expected four sample values, got {values.shape}")

            intents.extend(
                [
                    WriteIntent(
                        group="data",
                        array="timestamp",
                        ts_index=index,
                        data=None,
                        kind="timestamp",
                        timestamp_val=timestamp,
                    ),
                    WriteIntent(
                        group="data",
                        array="value",
                        ts_index=index,
                        data=values,
                        kind="1d",
                    ),
                ]
            )
        return intents
```

Replace the illustrative group, arrays, dimensions, chunks, and parser with the
product's actual schema. Every intent must name a declared group and array.
Use `time_indexed=False` with a `static` intent for arrays that do not share the
indexed axis.

In serial ingestion, an indexed array may start at length zero. Firecube grows
it when a later intent requires more capacity. A fixed global extent is required
only for slot-range parallelism.

See the [Plugin Template API](../../reference/api.md#directzarringestor) for all
schema and intent fields.

## Parallel Writes

Serial ingestion needs only `zarr_schema` and `build_write_intents`. To let
several processes write disjoint ranges of one group, the class must also
provide the complete parallel contract:

| API | Requirement |
|---|---|
| `SUPPORTS_SLOT_RANGE_PARALLELISM = True` | Opt in to slot-range validation. |
| `timestamp_to_ts_index(group, timestamp_val)` | Map each indexed coordinate value to one deterministic integer index. |
| `global_expected_time_count(ctx)` | Return the complete indexed extent for every group with time-indexed arrays. |
| `slot_index_model(ctx)` | Return the `SlotIndexModel` that defines the product's indexed axes. |
| `filter_items_to_slot_range(...)` | Recommended: avoid processing source items outside the assigned range. |

Firecube checks the three required method overrides when the class is defined;
omitting any of them while parallel support is enabled raises `TypeError`.
`filter_items_to_slot_range` defaults to a passthrough, but an intent outside
the assigned range still fails with `WriteIntentRangeError`.

Use the [Parallel DirectZarrIngestor tutorial](../../tutorials/direct-zarr-parallel.md)
for a complete implementation and worker run. The
[Plugin Template API](../../reference/api.md#optional-slot-range-parallelism)
contains the exact signatures and slot-index types.

## Verify

First check registration and configuration:

```bash
cd firecube-my-plugin
uv run firecube plugins describe my_plugin
uv run firecube ingest my_plugin --show-options
```

Then ingest a small, representative input supported by the product parser:

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

Open the written groups with the product's normal reader. Confirm the declared
shape, chunks, dimensions, coordinate values, and at least one known written
region. Repeat the same input and confirm that static data and indexed writes
follow the product's replay policy.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Treating the schema as implicit | Declare every group and array that an intent can target. |
| Omitting named dimensions | Set `dimension_names` when downstream xarray readers need them. |
| Leaving the indexed coordinate unwritten | Emit a `timestamp` intent for the configured index coordinate. |
| Assuming serial direct writes need preallocation | Start indexed arrays at zero length unless parallel workers require a fixed extent. |

## Next Steps

- **[DirectZarrIngestor (Region)](../../concepts/output-formats/zarr/direct-region.md)** — understand schema-driven array placement
- **[Parallel Zarr Writes](../../concepts/output-formats/zarr/parallel-writes.md)** — understand the optional slot safety model
- **[Parallel DirectZarrIngestor](../../tutorials/direct-zarr-parallel.md)** — build a complete slot-capable plugin
- **[Plugin Template API](../../reference/api.md)** — look up schema and write-intent fields
