# GenericZarrIngestor (Append)

`GenericZarrIngestor` uses a dataset-append write model. For each group and
batch, the plugin returns a complete `xarray.Dataset`. Firecube appends that
dataset along the plugin's declared append dimension and records the write.

Before each append, Firecube reads the current group length to find where the
next dataset begins. The group shape, metadata, and trailing chunk are shared
mutable state, so dataset construction and append mutations pass through one
serialized Zarr write section.

Choose this model when complete dataset batches are a natural representation
of the product. The source format is not part of the contract.

## How Sequential Appends Work

1. Firecube discovers source items and forms a batch.
2. The plugin reads the batch and returns an `xarray.Dataset` for a Zarr group.
3. Firecube validates the append dimension and appends the dataset to that
   group.
4. The next dataset continues after the data already written.

<figure markdown="span">
  ![GenericZarrIngestor converts ordered source items into xarray datasets and appends each batch along the declared dimension of a Zarr group.](../../../assets/images/firecube-generic-zarr-append.svg){ width="820" }
  <figcaption markdown="span">Each returned dataset is appended along the declared dimension. The plugin shapes the dataset; Firecube performs the append.</figcaption>
</figure>

## Data Contract

Each returned dataset must:

- contain the declared append dimension;
- be ordered on that dimension;
- use values that are unique and do not overlap another batch; and
- keep variables, dimensions, coordinates, and data types compatible with
  earlier batches.

Firecube writes the dataset it receives. It does not sort or align the plugin's
product data.

## Parallelism Model

Pipeline workers can perform work before the Zarr write section concurrently,
but `build_dataset` and the append to one group pass through one writer. This
prevents two appends from reading the same cursor or changing shared group state
at the same time.

<figure markdown="span">
  ![GenericZarrIngestor parallelism model showing concurrent batch preparation, serialized appends to one group, and separate write domains for distinct groups.](../../../assets/images/firecube-generic-zarr-parallel-groups.svg){ width="820" }
  <figcaption markdown="span">Preparation can run concurrently. Appends to one group are serialized; distinct products or groups are separate write domains.</figcaption>
</figure>

Pipeline workers help only when meaningful preparation happens before the
serialized section. They do not make `build_dataset` or same-group appends
concurrent. Do not run multiple append writers against the same group.

## When To Use Direct Writes

Use [`DirectZarrIngestor`](direct-region.md) when the plugin needs to declare
the array layout and place data through explicit write intents instead of
returning complete datasets. Use its optional parallel model when several
processes must write disjoint, chunk-aligned ranges of one group. Direct writes
replace the append cursor with absolute indexes supplied by the plugin.

## Next Steps

- **[`GenericZarrIngestor` Guide](../../../guides/plugins/generic-zarr.md)** —
  implement and verify the dataset hook
- **[DirectZarrIngestor (Region)](direct-region.md)** — compare explicit array placement
  with dataset appends
- **[Weather CSV Plugin](../../../tutorials/weather-csv.md)** — follow a complete
  `GenericZarrIngestor` example
- **[Parallelism](../../parallelism.md)** — compare write domains across output
  formats
