# DirectZarrIngestor (Region)

Use `DirectZarrIngestor` when the Zarr store has a known layout and each source
item can be mapped to exact array positions. It is the Zarr path for
out-of-order data, sparse slices, fixed time indexes, and parallel writes to one
group.

## User Story

You know the final grid and time axis before writing. A source file may contain a
scanline, tile, timestamp, channel, or sparse region. Your plugin turns each
piece into a write intent that says where the data belongs. Firecube writes that
region, records the claim and run state, and refuses conflicting slot ranges.

<figure markdown="span">
  ![DirectZarrIngestor maps source items to exact regions in a preallocated Zarr store.](../../../assets/images/firecube-direct-zarr-region.svg){ width="820" }
  <figcaption markdown="span">Direct region writes trade a smaller plugin surface for precise placement into a known Zarr layout.</figcaption>
</figure>

## What The Plugin Provides

A direct-region plugin declares the store schema up front, then returns write
intents for each batch. The plugin owns the mapping from source data to indexes.
Firecube owns storage access, schema setup, write coordination, ChunkManager
records, and observability.

For the plugin hook surface, see
[DirectZarrIngestor](../../plugins/direct-zarr.md).

## When To Use It

- Source items arrive out of order.
- The product has a fixed time axis or known final shape.
- Each source item maps to a deterministic time index or region.
- You need to write sparse slices instead of complete time steps.
- You need multiple workers writing the same Zarr group.

## Tradeoff

Direct region writes give more control, but the plugin must be more explicit.
You need a schema, deterministic indexes, and safe region boundaries. For
parallel slot workers, slot ranges must be disjoint and aligned to the Zarr time
chunk size.

If your data is simply chronological `xarray.Dataset` batches, use
[GenericZarrIngestor (Append)](generic-append.md) instead.

## Parallelism Model

A single direct-region worker can write exact regions without appending. Multiple
workers can write the same group only when the plugin opts into slot-range
parallelism and each worker owns a disjoint, chunk-aligned range.

<figure markdown="span">
  ![Parallel Zarr workers write disjoint chunk-aligned slot ranges into one Zarr group.](../../../assets/images/firecube-parallel-zarr-slots.svg){ width="820" }
  <figcaption markdown="span">Direct region writes can scale inside one group when each worker owns a disjoint, chunk-aligned slot range.</figcaption>
</figure>

The operator workflow is in [Parallel Zarr Writes](parallel-writes.md).

## Next Steps

- **[GenericZarrIngestor (Append)](generic-append.md)** — simpler default write path
- **[Parallel Zarr Writes](parallel-writes.md)** — many workers writing the same group
- **[DirectZarrIngestor](../../plugins/direct-zarr.md)** — implement schema and write-intent hooks
- **[Direct Parallel Zarr Tutorial](../../../tutorials/direct-zarr-parallel.md)** — build and run a slot-capable plugin
