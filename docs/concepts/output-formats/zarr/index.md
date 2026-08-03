# Zarr

Zarr stores multidimensional data as chunked arrays. A product can contain one
or more groups, and each group can contain arrays with their own dimensions,
data types, chunk shapes, and attributes.

Chunking lets readers retrieve part of a large product without reading every
value. The chunk layout also affects write safety and performance because a
physical chunk is the smallest shared storage unit that writers can touch.

<figure markdown="span">
  ![Firecube Zarr product layout showing groups, arrays, physical chunks, and ChunkManager.](../../../assets/images/firecube-zarr-store-layout.svg){ width="820" }
  <figcaption markdown="span">A Firecube Zarr product keeps array data in Zarr groups and physical chunks. ChunkManager keeps run, span, and claim records beside the store.</figcaption>
</figure>

## Zarr Chunks And ChunkManager Records

A Zarr chunk is a physical block of array data. ChunkManager records are
Firecube control-plane metadata stored beside the product under `.firecube/`.
They describe runs, written spans, and active claims; they are not Zarr chunks.

This distinction matters when inspecting or recovering a product. Zarr tools
read the arrays. Firecube's `chunks` commands inspect and manage the associated
run and coordination state.

## Choose A Zarr Write Model

Firecube supports two Zarr authoring classes. Parallel writes are an optional
extension of one of them, not a third plugin class.

| Data your plugin can supply | Write model | Write consequence |
|---|---|---|
| Complete `xarray.Dataset` batches, already ordered along the append dimension | [GenericZarrIngestor (Append)](generic-append.md) | Firecube finds the end of the group and serializes dataset construction and append mutations. |
| A declared array schema and exact indexed write locations | [DirectZarrIngestor (Region)](direct-region.md) | The plugin controls placement; serial stores may grow as later indexes are written. |
| The direct-write contract plus a fixed extent and deterministic indexes | [Parallel Zarr Writes](parallel-writes.md) | Separate processes can own disjoint, chunk-aligned ranges of one group. |

Choose sequential appends when complete dataset batches represent the product
naturally. Choose direct writes when the plugin must control individual array
writes or must remove the serialized same-group append path. Add slot-range
parallelism only when several processes must write one Zarr group and the
product can satisfy its fixed-layout safety requirements. A
`DirectZarrIngestor` without the slot contract remains a serial writer.

## Next Steps

- **[GenericZarrIngestor (Append)](generic-append.md)** — understand the
  `GenericZarrIngestor` write model
- **[DirectZarrIngestor (Region)](direct-region.md)** — understand the
  `DirectZarrIngestor` write model
- **[Plugin Development](../../../guides/plugins/index.md)** — choose and
  implement a plugin authoring class
- **[Performance Tuning](../../performance.md)** — tune chunking, sharding,
  compression, and batch size
