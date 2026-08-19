# Parallel DirectZarrIngestor

## Goal

Build a `DirectZarrIngestor` that maps real timestamps to a regular time axis, then run two workers against one Zarr store.

## Prerequisites

- Firecube installed. Start with [Installation](../quickstart/installation.md).
- Run every command below in the same Python environment where Firecube is installed.

## 1. Create Tiny Timestamped Files

Each `.npy` file represents one real timestamp. The file name carries the timestamp that `inspect_item()` reads.

```bash
mkdir -p tutorial-data/grid-parallel
uv run python - <<'PY'
from pathlib import Path

import numpy as np

out = Path("tutorial-data/grid-parallel")
out.mkdir(parents=True, exist_ok=True)

for i, stamp in enumerate([
    "2024-01-01T00:00:00Z",
    "2024-01-01T00:10:00Z",
    "2024-01-01T00:20:00Z",
    "2024-01-01T00:30:00Z",
    "2024-01-01T00:40:00Z",
    "2024-01-01T00:50:00Z",
    "2024-01-01T01:00:00Z",
    "2024-01-01T01:10:00Z",
]):
    np.save(out / f"{stamp}.npy", np.full((4,), float(i), dtype="float32"))
PY
```

## 2. Create A Plugin Project

```bash
uv run firecube plugins create grid-parallel \
  --template zarr \
  --write-strategy zarr-python \
  --target-dir plugins_dev \
  --non-interactive
```

## 3. Implement The Plugin

Replace `plugins_dev/firecube-grid-parallel/src/firecube_grid_parallel/ingestor.py` with:

```python
from __future__ import annotations

from datetime import datetime, timezone
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


def read_product_item(path: Path) -> tuple[datetime, np.ndarray]:
    stamp = datetime.fromisoformat(path.stem.replace("Z", "+00:00")).astimezone(timezone.utc)
    data = np.load(path).astype("float32")
    return stamp, data


@register_ingestor("grid_parallel")
class GridParallelIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "grid_parallel"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        return IndexSpec(
            name="grid_parallel_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    size=8,
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
                        chunks=(4,),
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="temperature_cells",
                        shape=(n_times, 4),
                        dtype=np.float32,
                        chunks=(4, 4),
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
                    array="temperature_cells",
                    index=index,
                    data=values,
                )
            )
        return intents
```

## 4. Install The Plugin

```bash
uv run firecube plugins install --editable plugins_dev/firecube-grid-parallel
uv run firecube plugins describe grid_parallel
```

## 5. Preallocate And Plan Ranges

```bash
mkdir -p tutorial-output
PRODUCT_URI="file://$PWD/tutorial-output/grid_parallel.zarr"

uv run firecube zarr preallocate grid_parallel \
  --target "$PRODUCT_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct

uv run firecube zarr slots grid_parallel \
  --target "$PRODUCT_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct \
  --slot-size 4 \
  --format table
```

The plan should contain two half-open ranges: `[0, 4)` and `[4, 8)`.

## 6. Run Two Workers

Worker 0:

```bash
uv run firecube ingest grid_parallel \
  --input-data tutorial-data/grid-parallel \
  --target "$PRODUCT_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --slot-start 0 \
  --slot-end 4
```

Worker 1:

```bash
uv run firecube ingest grid_parallel \
  --input-data tutorial-data/grid-parallel \
  --target "$PRODUCT_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --slot-start 4 \
  --slot-end 8
```

## 7. Verify The Store

```bash
uv run python - <<'PY'
import zarr

root = zarr.open_group("tutorial-output/grid_parallel.zarr", mode="r")
arr = root["data/temperature_cells"]

print("shape:", arr.shape)
print("first cell by timestamp:", arr[:, 0].tolist())

assert arr.shape == (8, 4)
PY
```

## What Firecube Handled

- Resolving the regular time axis from `index_spec()`
- Mapping real timestamps to slot indexes through `inspect_item()`
- Writing timestamp coordinates and data rows with `WriteIntent.coordinate()` and `WriteIntent.slot()`
- Validating the slot ranges before each worker writes

## Next Steps

- **[Parallel Zarr Writes](../concepts/output-formats/zarr/parallel-writes.md)** - model overview
- **[Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)** - operator workflow
- **[DirectZarrIngestor](../guides/plugins/direct-zarr.md)** - plugin contract
