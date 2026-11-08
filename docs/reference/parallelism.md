# Index Specification

This reference covers the public types used to declare and resolve direct-Zarr index specs. `TimeAxis` provides the recommended intent-named constructors for time axes. `IndexSpec`, `RegularTimeAxis`, `IntegerAxis`, and `IrregularTimeAxis` describe the layout. `AUTO` is the sentinel that tells the engine to discover coordinates at planning time. `inspect_item()` returns `ItemInfo`, and `resolve_index_spec()` turns the declarative spec into a cached `ResolvedIndex` for the run.

`coerce_to_epoch_s()` normalizes supported timestamp values to Unix epoch seconds.

`ResolvedIndexRecord` is the on-disk control-plane record written after the engine resolves an `IndexSpec`.

{{ render_api_summary("firecube.sdk", [
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

Import these from `firecube.sdk`.

::: firecube.sdk.TimeAxis

::: firecube.sdk.AxisSpec

::: firecube.sdk.IndexSpec

::: firecube.sdk.IntegerAxis

::: firecube.sdk.IrregularTimeAxis

::: firecube.sdk.AUTO

::: firecube.sdk.RegularTimeAxis

::: firecube.sdk.ItemInfo

::: firecube.sdk.ResolvedIndex

::: firecube.sdk.ResolvedIndexRecord

::: firecube.sdk.resolve_index_spec

## Time Conversion

Import `coerce_to_epoch_s()` from `firecube.sdk`.

::: firecube.sdk.coerce_to_epoch_s

## See Also

- [Declare The Schema And Index](../guides/plugins/direct-zarr.md)
- [MTG FCI L1C benchmark notebook](../showcase/mtg-fci-l1c-benchmarks.ipynb)
- [Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)
