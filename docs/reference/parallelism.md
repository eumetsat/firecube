# Index Specification

This reference covers the public types used to declare and resolve direct-Zarr index specs. `TimeAxis` provides the recommended intent-named constructors for time axes. `IndexSpec`, `RegularTimeAxis`, `IntegerAxis`, and `IrregularTimeAxis` describe the layout. `AUTO` is the sentinel that tells the engine to discover coordinates at planning time. `inspect_item()` returns `ItemInfo`, and `resolve_index_spec()` turns the declarative spec into a cached `ResolvedIndex` for the run.

`coerce_to_epoch_s()` normalizes supported timestamp values to Unix epoch seconds.

`ResolvedIndexRecord` is the on-disk control-plane record written after the engine resolves an `IndexSpec`.

{{ render_api_summary("firecube.core.api", [
    "TimeAxis",
    "AxisSpec",
    "IndexSpec",
    "IntegerAxis",
    "IrregularTimeAxis",
    "AUTO",
    "RegularTimeAxis",
    "ItemInfo",
    "ResolvedIndex",
    "ResolvedIndexRecord",
    "coerce_to_epoch_s",
    "resolve_index_spec",
]) }}

## Index Types

Import these from `firecube.ingestor.api`.

::: firecube.ingestor.api.TimeAxis

::: firecube.ingestor.api.AxisSpec

::: firecube.ingestor.api.IndexSpec

::: firecube.ingestor.api.IntegerAxis

::: firecube.ingestor.api.IrregularTimeAxis

::: firecube.ingestor.api.AUTO

::: firecube.ingestor.api.RegularTimeAxis

::: firecube.ingestor.api.ItemInfo

::: firecube.ingestor.api.ResolvedIndex

::: firecube.ingestor.api.ResolvedIndexRecord

::: firecube.ingestor.api.resolve_index_spec

## Core API

`coerce_to_epoch_s()` is exported from `firecube.core.api` only; the ingestor
facade does not re-export it.

::: firecube.core.api.coerce_to_epoch_s

## See Also

- [Implement `DirectZarrIngestor`](../guides/plugins/direct-zarr.md)
- [DirectZarrIngestor (Region) tutorial](../tutorials/direct-zarr-parallel.md)
- [Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)
