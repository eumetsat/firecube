# Index Specification

This reference covers the public types used to declare and resolve direct-Zarr index specs. `IndexSpec` and `RegularTimeAxis` describe the layout. `inspect_item()` returns `ItemInfo`, and `resolve_index_spec()` turns the declarative spec into a cached `ResolvedIndex` for the run.

`coerce_to_epoch_s()` normalizes supported timestamp values to Unix epoch seconds.

{{ render_api_summary("firecube.core.api", [
    "AxisSpec",
    "IndexSpec",
    "RegularTimeAxis",
    "ItemInfo",
    "ResolvedIndex",
    "coerce_to_epoch_s",
    "resolve_index_spec",
]) }}

## Core API

::: firecube.core.api.AxisSpec

::: firecube.core.api.IndexSpec

::: firecube.core.api.RegularTimeAxis

::: firecube.core.api.ItemInfo

::: firecube.core.api.ResolvedIndex

::: firecube.core.api.coerce_to_epoch_s

::: firecube.core.api.resolve_index_spec

## Ingestor Re-Exports

The public ingestor facade re-exports the declarative types used by plugin code.

::: firecube.ingestor.api.AxisSpec

::: firecube.ingestor.api.IndexSpec

::: firecube.ingestor.api.RegularTimeAxis

::: firecube.ingestor.api.ItemInfo

::: firecube.ingestor.api.ResolvedIndex

## Resolved Index Behavior

`ResolvedIndex` is the object plugin code uses after resolution. The key methods are `size(group)`, `position(group, coordinate)`, and `coordinate(group, index)`.

Use `resolved_index(ctx).size(group)` when a schema needs the declared time extent before writes begin. Use `resolved_index(ctx).position(group, coordinate)` when a write needs the slot index for a timestamp-like value.

## See Also

- [Implement `DirectZarrIngestor`](../guides/plugins/direct-zarr.md)
- [Parallel DirectZarrIngestor tutorial](../tutorials/direct-zarr-parallel.md)
- [Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)
