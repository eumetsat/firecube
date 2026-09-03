# Reproducibility

What Firecube guarantees when the same ingestion runs twice, and how to
answer the two questions that follow from it: are these two stores the same
product, and which time slots of a store hold real data.

## Tier 1: Identical Values

This tier always holds. Given the same input catalog, the same plugin at a
pinned version, and the same schema at preallocation time, two ingestions
produce value-identical and schema-identical stores, including NaN and NaT
positions, across machines, worker counts, and slot-range splits: each item's
slot position is resolved from the declared index, not from arrival or
completion order.

Verify it between any two stores of the same product:

```bash
uv run firecube zarr compare "$PRODUCT_URI" "$RERUN_URI" \
  --storage-type local \
  --storage-driver fsspec
echo "compare exit: $?"
```

Expected output when the stores are equivalent:

```text
compare exit: 0
```

A store that differs prints one line per mismatched array and exits `3`, so
the command can gate a promotion or migration step directly. See
[Compare Zarr Stores](../operations/zarr-compare.md) for the full contract.

## Which Slots Hold Real Data

Three signals exist. Two are trustworthy, and they answer different
questions. To follow along, create a partially ingested store with the
plugin and input files from the
[DirectZarrIngestor (Region) tutorial](../tutorials/direct-zarr-parallel.md):
an eight-slot horizon where only the first four slots are materialized and
ingested.

```bash
PARTIAL_URI="file://$PWD/tutorial-output/grid_partial.zarr"

uv run firecube zarr preallocate grid_parallel \
  --target "$PARTIAL_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct \
  --input-data tutorial-data/grid-parallel \
  --slot-start 0 \
  --slot-end 4

uv run firecube ingest grid_parallel \
  --input-data tutorial-data/grid-parallel \
  --target "$PARTIAL_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --slot-start 0 \
  --slot-end 4
```

**The time coordinate answers "which slots have an observation".** In the
observed and discovered regimes the coordinate stores each slot's real
timestamp, and a slot with no recorded observation is NaT; workers refuse to
write data into a NaT slot.

```bash
uv run python - <<'PY'
import numpy as np
import zarr

timestamps = zarr.open_array(
    "tutorial-output/grid_partial.zarr/data/timestamp", mode="r"
)[:]
written = ~np.isnat(timestamps)
print(f"slots: {len(timestamps)}  observed={written.sum()}  NaT={(~written).sum()}")
for slot, timestamp in enumerate(timestamps):
    marker = "DATA" if written[slot] else "NaT "
    print(f"  slot {slot}  {marker}  {timestamp}")
PY
```

Expected output:

```text
slots: 8  observed=4  NaT=4
  slot 0  DATA  2024-01-01T00:00:00.000000000
  slot 1  DATA  2024-01-01T00:10:00.000000000
  slot 2  DATA  2024-01-01T00:20:00.000000000
  slot 3  DATA  2024-01-01T00:30:00.000000000
  slot 4  NaT   NaT
  slot 5  NaT   NaT
  slot 6  NaT   NaT
  slot 7  NaT   NaT
```

A NaT slot definitely holds no data. A non-NaT slot has its observation
timestamp recorded; the timestamp is written when the axis window is
materialized, which can run ahead of the data workers, so inside a window
still being ingested a timestamp can briefly precede its data. In the grid
regime the coordinate is computed from the declaration at preallocation, so
it carries no NaT signal at all.

**For fan-out ingestion, the control plane answers "which ranges are
committed".**

```bash
uv run firecube zarr slots grid_parallel \
  --target "$PARTIAL_URI" \
  --product-name grid_parallel \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct \
  --slot-size 4 \
  --format table
```

Expected output:

```text
group	slot_start	slot_end
data	4	8
```

Ranges `[0, 4)` are committed; `[4, 8)` remain. This view comes from the run
records under `.firecube/runs/`, which record a slot range only for
slot-range runs. It answers "which worker wrote which range" in the parallel
workflow; a serial ingest records no slot range, so it is not a general data
census. The record format is in the
[Control-Plane Spec](../reference/control-plane-spec.md).

**The data arrays answer neither question.** Fill values are representable
values: the tutorial's temperature array fills with `0.0` while slot 0
legitimately measures `0.0`; a `uint16` counts array fills with `65535`,
which is also a valid count; a quality-flag array fills with `0`. A written
slot can be byte-identical to an unwritten one. Never infer coverage from
array values.

## Tier 2: Identical Bytes

Byte-level identity needs more than Tier 1: a pinned environment with the
same zarr-python version, the same codec pipeline, and the same
`zarr_write_empty_chunks` setting. Outside those preconditions, chunk files
can differ in count and content while every value is identical; toggling
`zarr_write_empty_chunks` alone changed a measured product's chunk-file count
by roughly 21 percent with no value change (measured 2026-08).

Compare values, not bytes, unless you control all three preconditions.

## Next Steps

- **[Compare Zarr Stores](../operations/zarr-compare.md)**: the verification
  command's full contract.
- **[Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)**: plan
  and resume slot-range ingestion with `zarr slots`.
- **[Control-Plane Spec](../reference/control-plane-spec.md)**: the record
  formats behind these answers.
