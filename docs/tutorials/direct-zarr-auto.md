# DirectZarrIngestor (Auto)

## Goal

Build a `DirectZarrIngestor` that declares `TimeAxis.discovered`: no epoch,
no cadence, no timestamp list. Firecube reads each observation's timestamp
from the source files, sorts them, and materializes the time axis before any
write.

## Prerequisites

- Firecube installed. Start with [Installation](../quickstart/installation.md).
- Run every command below in the same Python environment where Firecube is installed.

## 1. Create Tiny NetCDF Files

Each NetCDF file holds one observation: four `value` samples and a `time`
variable that says when the observation was taken. The gaps between
observations are deliberately uneven, 7, 12, and 43 minutes; discovery does
not care, but no cadence declaration could fit them.

```bash
mkdir -p tutorial-data/irregular
uv run python - <<'PY'
from pathlib import Path

import numpy
import xarray

output_directory = Path("tutorial-data/irregular")
output_directory.mkdir(parents=True, exist_ok=True)

timestamps = [
    "2024-01-01T00:00:00",
    "2024-01-01T00:07:00",
    "2024-01-01T00:19:00",
    "2024-01-01T01:02:00",
]

for file_number, timestamp in enumerate(timestamps):
    dataset = xarray.Dataset(
        data_vars={
            "value": ("sample", numpy.full((4,), float(file_number), dtype="float32")),
            "time": ("t", numpy.array([timestamp], dtype="datetime64[ns]")),
        },
    )
    dataset.to_netcdf(output_directory / f"observation-{file_number:02d}.nc")

print("wrote", len(timestamps), "files")
PY
```

## 2. Create A Plugin Project

```bash
uv run firecube plugins create irregular-times \
  --template zarr \
  --write-strategy zarr-python \
  --target-dir plugins_dev \
  --non-interactive
```

## 3. Implement The Plugin

Replace `plugins_dev/firecube-irregular-times/src/firecube_irregular_times/ingestor.py` with:

```python
from __future__ import annotations

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


def read_observation(path: Path) -> tuple[np.datetime64, np.ndarray]:
    """Read one observation file: its time and its four sample values."""
    with xarray.open_dataset(path) as dataset:
        timestamp = dataset["time"].values[0]
        values = dataset["value"].values.astype(np.float32)
    return timestamp, values


@register_ingestor("irregular_times")
class IrregularTimesIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "irregular_times"

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        _ = ctx
        return IndexSpec(
            name="irregular_times_v1",
            groups={"data": TimeAxis.discovered(coordinate="timestamp")},
        )

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        timestamp, values = read_observation(ctx.materialize(item))
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
                        chunks=(4,),
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="value",
                        shape=(n_times, 4),
                        dtype=np.float32,
                        chunks=(4, 4),
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
            timestamp, values = read_observation(ctx.materialize(item))
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

Three things carry the whole tutorial:

- `index_spec()` declares `TimeAxis.discovered`; no epoch, no cadence, no
  timestamp list.
- `inspect_item()` reads each file's real observation time. Firecube runs it
  over every item before any write, sorts the collected timestamps, and
  builds the axis from them, so `resolved_index(ctx).size("data")` in
  `zarr_schema()` already knows there are four slots.
- `build_write_intents()` returns one `IndexedWrite.slot` per observation,
  keyed by the same timestamp. Firecube resolves the slot and writes the
  slot's time-coordinate value for you.

## 4. Install The Plugin

```bash
uv run firecube plugins install --editable plugins_dev/firecube-irregular-times
uv run firecube plugins describe irregular_times
```

## 5. Ingest

Discovery happens inside the normal ingest; a serial run needs no other
command:

```bash
uv run firecube ingest irregular_times \
  --input-data tutorial-data/irregular \
  --target "file://$PWD/tutorial-output/irregular_times.zarr" \
  --product-name irregular_times \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

## 6. Verify The Store

```bash
uv run python - <<'PY'
import zarr

root = zarr.open_group("tutorial-output/irregular_times.zarr", mode="r")
timestamps = root["data/timestamp"][:]
value = root["data/value"]

print("timestamps:", timestamps)
print("first cell by slot:", value[:, 0].tolist())

assert value.shape == (4, 4)
assert value[:, 0].tolist() == [0.0, 1.0, 2.0, 3.0]
PY
```

The timestamp array holds the four real observation times, sorted ascending,
with the uneven gaps preserved exactly. Each observation's values sit in the
slot of its own timestamp: slot 0 is the earliest observation regardless of
file naming or discovery order.

## What Firecube Handled

- Running `inspect_item()` over every item before any write
- Sorting the discovered timestamps into the time axis and refusing
  duplicates or unreadable timestamps loudly
- Sizing the declared arrays from the discovered slot count
- Resolving each `IndexedWrite.slot` coordinate to its slot and writing the
  slot's timestamp value automatically

## Next Steps

- **[Implement DirectZarrIngestor (Auto)](../guides/plugins/direct-zarr-auto.md)** - the plugin-author guide behind this tutorial
- **[`TimeAxis.discovered` Reference](../reference/parallelism.md#firecube.ingestor.api.TimeAxis.discovered)** - exact constructor contract and failure modes
- **[DirectZarrIngestor (Region)](direct-zarr-parallel.md)** - run two workers against one store
