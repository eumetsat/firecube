# DirectZarrIngestor (Region)

`DirectZarrIngestor` places data at absolute indexes in declared Zarr arrays.
It does not need the shared end-of-group cursor used by dataset appends. This
makes precise indexed writes possible and provides the foundation for several
processes to write disjoint ranges of one group.

Choose this model when the plugin needs explicit control over array placement
or when constructing a complete dataset for each batch is not a good fit. The
plugin emits write intents rather than returning an `xarray.Dataset`; Firecube
applies them through `zarr-python`.

## How Direct Writes Work

1. The plugin declares the product's Zarr groups and arrays.
2. Firecube creates missing arrays or validates the existing schema.
3. For each batch, the plugin returns write intents for specific arrays and
   indexes.
4. Firecube validates and applies those intents, then records the run.

<figure markdown="span">
  ![DirectZarrIngestor converts source values into region, one-dimensional, timestamp, or static write intents that Firecube applies to a declared Zarr schema.](../../../assets/images/firecube-direct-zarr-region.svg){ width="820" }
  <figcaption markdown="span">Direct writes replace the append cursor with explicit indexed placement.</figcaption>
</figure>

## Schema And Write Intents

The schema defines each array's shape, chunks, dimensions, data type, and
attributes. A write intent then describes one write:

| Intent kind | Purpose |
|---|---|
| `region` | Write a spatial slice at one indexed position in a rank-3 or rank-4 array. |
| `1d` | Write one complete indexed slot of an array. |
| `timestamp` | Write one indexed coordinate value. |
| `static` | Write a non-indexed array once and verify it on replay. |

The plugin owns the mapping from source data to those writes. Firecube owns
schema setup and validation, storage access, write coordination, and run
tracking.

## Serial Store Growth

A serial direct-write store does not need a known final indexed extent. An
indexed array may begin at length zero, and Firecube grows it when an intent
targets a later index. Static arrays are created at their declared shape.

A fixed, preallocated global extent is required only when the plugin opts into
slot-range parallelism.

## When To Use It

Use `DirectZarrIngestor` when explicit array placement is part of the product
contract, including sparse regions, indexed coordinate writes, or static arrays
that accompany indexed data. If the plugin can return complete, compatible
`xarray.Dataset` batches, [GenericZarrIngestor (Append)](generic-append.md)
provides a smaller authoring surface.

## Optional Parallel Writes

A `DirectZarrIngestor` plugin can opt into multiple processes writing the same
group. Firecube first creates or validates the complete indexed extent. The
slot planner then produces half-open ranges whose boundaries align with every
time-indexed array's physical chunks. An external scheduler passes one range to
each process. Because each write uses an absolute index and no two ranges share
a chunk, the processes do not contend for an append cursor or the same physical
chunk.

That extension requires a fixed global layout, deterministic index mapping,
and disjoint chunk-aligned ranges. It partitions the first indexed dimension;
it does not allow several processes to write different spatial regions of the
same index. Serial direct writes do not require those hooks or constraints, and
selecting `DirectZarrIngestor` alone does not enable parallelism.

See [Parallel Zarr Writes](parallel-writes.md) for the safety model.

## Next Steps

- **[`DirectZarrIngestor` Guide](../../../guides/plugins/direct-zarr.md)** —
  implement the schema and write-intent hooks
- **[Parallel Zarr Writes](parallel-writes.md)** — understand the optional
  slot-range model
- **[GenericZarrIngestor (Append)](generic-append.md)** — compare the complete
  dataset contract
- **[Parallel DirectZarrIngestor Plugin](../../../tutorials/direct-zarr-parallel.md)** —
  build and run a slot-capable plugin
