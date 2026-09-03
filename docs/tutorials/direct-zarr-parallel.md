# DirectZarrIngestor (Region)

## Goal

Build a `DirectZarrIngestor` that reads NetCDF files, maps their timestamps to a regular time axis, and runs two workers against one Zarr store.

## Prerequisites

- Firecube installed. Start with [Installation](../quickstart/installation.md).
- Run every command below in the same Python environment where Firecube is installed.

## 1. Create Tiny NetCDF Files

Each NetCDF file holds one observation: four `temperature_cells` values and a global `timestamp` attribute that says when the observation was taken. The plugin reads the timestamp from inside the file, not from the file name.

```bash
mkdir -p tutorial-data/grid-parallel
uv run python - <<'PY'
from pathlib import Path

import numpy
import xarray

output_directory = Path("tutorial-data/grid-parallel")
output_directory.mkdir(parents=True, exist_ok=True)

timestamps = [
    "2024-01-01T00:00:00Z",
    "2024-01-01T00:10:00Z",
    "2024-01-01T00:20:00Z",
    "2024-01-01T00:30:00Z",
    "2024-01-01T00:40:00Z",
    "2024-01-01T00:50:00Z",
    "2024-01-01T01:00:00Z",
    "2024-01-01T01:10:00Z",
]

for file_number, timestamp in enumerate(timestamps):
    values = numpy.full((4,), float(file_number), dtype="float32")
    dataset = xarray.Dataset(
        data_vars={"temperature_cells": ("sample", values)},
        attrs={"timestamp": timestamp},
    )
    dataset.to_netcdf(output_directory / f"temperature-{file_number:02d}.nc")

print("wrote", len(timestamps), "files")
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

from datetime import datetime
from pathlib import Path
from typing import ClassVar

import numpy as np
import xarray

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


def read_observation(path: Path) -> tuple[datetime, np.ndarray]:
    """Read one granule: its observation time and its four sample values."""
    with xarray.open_dataset(path) as dataset:
        timestamp = datetime.fromisoformat(str(dataset.attrs["timestamp"]))
        values = dataset["temperature_cells"].values.astype(np.float32)
    return timestamp, values


@register_ingestor("grid_parallel")
class GridParallelIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "grid_parallel"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        return IndexSpec(
            name="grid_parallel_v1",
            groups={
                "data": TimeAxis.observed(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    end_date="2024-01-01T01:20:00Z",  # 8 slots of 600 s from the epoch
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
        timestamp, values = read_observation(ctx.materialize(item))
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
            timestamp, values = read_observation(ctx.materialize(item))
            out.append(
                IndexedWrite.slot(
                    group="data",
                    array="temperature_cells",
                    coordinate=timestamp,
                    data=values,
                )
            )
        return out
```

Both hooks read the file through the same `read_observation` helper:

- `inspect_item()` hands each observation's timestamp to Firecube as the item's coordinate, so the engine can place the item on the declared axis.
- `build_write_intents()` returns one `IndexedWrite.slot` per observation, keyed by the same timestamp. Firecube resolves the slot index and writes the slot's time-coordinate value for you; the plugin never computes an index.

## 4. Install The Plugin

```bash
uv run firecube plugins install --editable plugins_dev/firecube-grid-parallel
uv run firecube plugins describe grid_parallel
```

## 5. Preallocate And Plan Ranges

The declared axis stores observed timestamps, so preallocation reads the
source data once to discover the real observation time for each slot in the
window. That is why this `preallocate` call passes `--input-data` and the
slot window; a `TimeAxis.grid` or `TimeAxis.explicit` axis is filled from the
declaration alone and needs neither.

```bash
mkdir -p tutorial-output
PRODUCT_URI="file://$PWD/tutorial-output/grid_parallel.zarr"

uv run firecube zarr preallocate grid_parallel \
  --target "$PRODUCT_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct \
  --input-data tutorial-data/grid-parallel \
  --slot-start 0 \
  --slot-end 8

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
temperature = root["data/temperature_cells"]

print("shape:", temperature.shape)
print("first cell by timestamp:", temperature[:, 0].tolist())

assert temperature.shape == (8, 4)
assert temperature[:, 0].tolist() == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
PY
```

## What Firecube Handled

- Resolving the regular time axis from `index_spec()`
- Mapping real timestamps to slot indexes through `inspect_item()`
- Resolving each `IndexedWrite.slot` coordinate to its slot and writing the slot's timestamp value automatically
- Validating the slot ranges before each worker writes

## Next Steps

- **[Parallel Zarr Writes](../concepts/output-formats/zarr/parallel-writes.md)** - model overview
- **[Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)** - operator workflow
- **[DirectZarrIngestor](../guides/plugins/direct-zarr.md)** - plugin contract
