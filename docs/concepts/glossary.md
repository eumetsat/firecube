# Glossary

Use this page when you need a short definition before reading a deeper concept
page.

## Terms

**Plugin** — Product-specific code that reads source items and shapes output
data. See [Plugins](plugins/index.md).

**Run** — One `firecube ingest ...` execution against one product target.

**Batch** — A group of source items processed as one unit of work.

**Product URI** — The full `file://` or `s3://` location of the output product,
such as `file:///data/products/MY_PRODUCT.zarr`.

**Product root** — The storage location for one Firecube product. It contains
the readable data store and ChunkManager state.

**Data store** — The product data that users and downstream tools read, such as
Zarr arrays, Parquet files, or a Tensogram `.tgm` file.

**ChunkManager** — Firecube's product lifecycle surface. It records runs, spans,
write claims, snapshots, and cleanup state so products can be inspected,
resumed, recovered, and cleaned up.

**ChunkManager record** — A logical Firecube record about run, span, claim, or
snapshot state. It is not a physical Zarr array chunk.

**Zarr chunk** — A physical storage chunk inside a Zarr array. Zarr chunks are
controlled by output-format tuning options and affect read/write performance.

**Claim** — A write-coordination record used to prevent conflicting work against
the same product area.

**Span** — A record of what one run wrote, usually with group or time coverage.

**Snapshot** — A derived ChunkManager view used by inspection and cleanup
commands.

**Storage driver** — The I/O implementation selected for a run, such as
`fsspec` or `obstore`.

**Output format** — The shape Firecube writes: Zarr for gridded datacubes,
Parquet for tabular records, or Tensogram for packaging finished Zarr products.

**Write mode** — The write strategy selected for ingestion. `staged` writes to a
temporary local product before committing; `direct` writes to the target product
path directly.

**Pipeline worker** — A worker inside one Firecube run that can prepare or write
batches concurrently when the plugin class and output format allow it.

**Slot range** — A fixed index range assigned to a direct-Zarr worker. Slot
ranges must be disjoint and chunk-aligned for safe parallel writes.

## Next Steps

- **[Concepts](index.md)** — return to the chapter map
- **[Storage & ChunkManager](chunkmanager.md)** — understand product state,
  recovery, claims, and cleanup
- **[Output Formats](output-formats/index.md)** — choose Zarr, Parquet, or
  Tensogram
- **[Parallelism](parallelism.md)** — choose a safe parallel model
