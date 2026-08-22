# Parallel DirectZarrIngestor

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
from typing import ClassVar

import numpy as np
import xarray

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
        file_path = ctx.materialize(item)
        dataset = xarray.open_dataset(file_path)
        timestamp_text = str(dataset.attrs["timestamp"])
        values = dataset["temperature_cells"].values
        dataset.close()

        if values.shape != (4,):
            raise ValueError(f"Expected four sample values, got {values.shape}")

        timestamp = datetime.fromisoformat(timestamp_text)
        return ItemInfo(coordinate=timestamp)

    def build_write_intents(
        self,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            file_path = ctx.materialize(item)
            dataset = xarray.open_dataset(file_path)
            timestamp_text = str(dataset.attrs["timestamp"])
            values = dataset["temperature_cells"].values
            dataset.close()

            timestamp = datetime.fromisoformat(timestamp_text)
            slot_index = self.resolved_index(ctx).position("data", timestamp)

            intents.append(
                WriteIntent.coordinate(group="data", index=slot_index, value=timestamp)
            )
            intents.append(
                WriteIntent.slot(
                    group="data",
                    array="temperature_cells",
                    index=slot_index,
                    data=values,
                )
            )
        return intents
```

The two hooks read the file the same explicit way:

- `inspect_item()` opens the NetCDF file, reads the `timestamp` attribute, parses it with `datetime.fromisoformat()`, and hands the timestamp to Firecube as the item's coordinate.
- `build_write_intents()` opens the file again, maps the timestamp to a slot index with `position()`, and emits one coordinate write and one data write for that slot.

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
- Writing timestamp coordinates and data rows with `WriteIntent.coordinate()` and `WriteIntent.slot()`
- Validating the slot ranges before each worker writes

## Next Steps

- **[Parallel Zarr Writes](../concepts/output-formats/zarr/parallel-writes.md)** - model overview
- **[Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)** - operator workflow
- **[DirectZarrIngestor](../guides/plugins/direct-zarr.md)** - plugin contract
