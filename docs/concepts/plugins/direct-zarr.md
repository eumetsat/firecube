# DirectZarrIngestor

`DirectZarrIngestor` writes gridded data straight into pre-allocated Zarr regions
instead of appending. Use it for out-of-order data, sparse slices, maximum write
throughput, or parallel writes to one store. Scaffold one with
`firecube plugins create my-plugin --template zarr --write-strategy zarr-python`
(see [Create a Plugin](create-a-plugin.md)).

For *how* region writes behave, see
[DirectZarrIngestor (Region)](../output-formats/zarr/direct-region.md). For running
many workers against one store, see
[Parallel Zarr Writes](../output-formats/zarr/parallel-writes.md).

## Required hooks

| Hook | Signature | Purpose |
| :--- | :--- | :--- |
| **`zarr_schema`** | `(ctx) -> list[ZarrGroupSpec]` | Declare the groups and arrays in the store. |
| **`build_write_intents`** | `(batch, ctx) -> list[WriteIntent]` | Convert a batch into specific write operations. |

> See [Plugin Contract](./contract.md) for the full `PRODUCT_NAME`, `OutputPaths`, and `ResultMetrics` requirements.

```python
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


@register_ingestor("my_gridded_plugin")
class MyGriddedIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_gridded_product"
    name = "my_gridded_plugin"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="temperature",
                        shape=(1000, 900, 900),  # (time, y, x)
                        dtype=np.float32,
                        chunks=(1, 450, 450),
                    )
                ],
                coord_names=frozenset({"y", "x"}),
            )
        ]

    def build_write_intents(
        self,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> list[WriteIntent]:
        intents = []
        for item in batch.items:
            ts_index = item.metadata["ts_index"]
            data = self._load_data(item)
            intents.append(
                WriteIntent(
                    group="data",
                    array="temperature",
                    ts_index=ts_index,
                    data=data,
                    kind="region",
                    y_slice=slice(0, 900),
                )
            )
        return intents
```

### WriteIntent

| Field | Purpose |
| :--- | :--- |
| `group` | Zarr group name. |
| `array` | Array name within the group. |
| `ts_index` | Integer index along the time dimension. |
| `data` | NumPy array to write. |
| `kind` | `"region"`, `"1d"`, or `"timestamp"`. |
| `y_slice` | Optional Y-dimension slice (with `"region"`). |
| `channel_index` | Optional channel-dimension index. |
| `timestamp_val` | Actual timestamp value (with `"timestamp"`). |

### Schema specs

- `ZarrGroupSpec(group, arrays, coord_names=frozenset({"y", "x", "channel"}))`
- `ZarrArraySpec(name, shape, dtype, chunks=None, fill_value=None, expected_time_count=None)`

## Parallel hooks (optional)

To let multiple workers write disjoint time-index ranges of one store, opt in and
implement the deterministic indexing hooks:

| Member | Purpose |
| :--- | :--- |
| `SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True` | Opt in (default `False`). |
| `timestamp_to_ts_index(group, timestamp_val) -> int` | Deterministic, stateless time→index mapping (enforced). |
| `global_expected_time_count(ctx) -> dict[str, int]` | Total time steps per group, for pre-allocation (enforced). |
| `filter_items_to_slot_range(items, slot_start, slot_end, ctx)` | Drop out-of-range items (strongly recommended; not enforced). |

The operator-facing workflow (slot flags, scheduling, resume) is in
[Parallel Zarr Writes](../output-formats/zarr/parallel-writes.md).

## Next Steps

- **[DirectZarrIngestor (Region)](../output-formats/zarr/direct-region.md)** — understand direct region write behavior
- **[Parallel Zarr Writes](../output-formats/zarr/parallel-writes.md)** — operate slot workers
- **[Plugin Contract](contract.md)** — check typed result and product-name rules
