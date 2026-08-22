# Index Specification

This reference covers the public types used to declare and resolve direct-Zarr index specs. `IndexSpec`, `RegularTimeAxis`, and `IntegerAxis` describe the layout. `inspect_item()` returns `ItemInfo`, and `resolve_index_spec()` turns the declarative spec into a cached `ResolvedIndex` for the run.

`coerce_to_epoch_s()` normalizes supported timestamp values to Unix epoch seconds.

`ResolvedIndexRecord` is the on-disk control-plane record written after the engine resolves an `IndexSpec`.

{{ render_api_summary("firecube.core.api", [
    "AxisSpec",
    "IndexSpec",
    "IntegerAxis",
    "RegularTimeAxis",
    "ItemInfo",
    "ResolvedIndex",
    "ResolvedIndexRecord",
    "coerce_to_epoch_s",
    "resolve_index_spec",
]) }}

## Core API

::: firecube.core.api.AxisSpec

::: firecube.core.api.IndexSpec

::: firecube.core.api.IntegerAxis

::: firecube.core.api.RegularTimeAxis

::: firecube.core.api.ItemInfo

::: firecube.core.api.ResolvedIndex

::: firecube.core.api.ResolvedIndexRecord

::: firecube.core.api.coerce_to_epoch_s

::: firecube.core.api.resolve_index_spec

## Ingestor Re-Exports

The public ingestor facade re-exports the declarative types used by plugin code.

::: firecube.ingestor.api.AxisSpec

::: firecube.ingestor.api.IndexSpec

::: firecube.ingestor.api.IntegerAxis

::: firecube.ingestor.api.RegularTimeAxis

::: firecube.ingestor.api.ItemInfo

::: firecube.ingestor.api.ResolvedIndex

::: firecube.ingestor.api.ResolvedIndexRecord

## Resolved Index Behavior

`ResolvedIndex` is the object plugin code uses after resolution. The key methods are `size(group)`, `position(group, coordinate)`, and `coordinate(group, index)`.

Use `resolved_index(ctx).size(group)` when a schema needs the declared axis extent before writes begin. Use `resolved_index(ctx).position(group, coordinate)` when a write needs the slot index for a timestamp or integer coordinate.

`ResolvedIndexRecord` is the persisted form of the resolved index. The engine writes it to `.firecube/index/current.json` after the first successful resolution. Subsequent runs read it back and verify that the declared `IndexSpec` produces the same `identity_hash` before writing. Use `firecube zarr index show` to inspect the record and `firecube zarr index rebuild` to regenerate it from a plugin declaration.

## Multi-group index specs and axis sharing

When a plugin's `index_spec(ctx)` returns an `IndexSpec` with multiple
groups, and the ingestion caller does not pass `slot_group` to name one
specific group, Firecube requires all groups to reference the **same axis
object** (Python `is`-identity, not value-equality). Two structurally
equal `RegularTimeAxis(...)` instances constructed separately will be
rejected with a `ConfigurationError` at bind time.

### Why identity, not equality?

Slot-range filtering must operate on ONE canonical axis. If two groups
declare distinct axis objects, even with identical `epoch`, `cadence_s`,
and `coordinate`, Firecube cannot pick which is authoritative without
operator input.

### Two ways to satisfy the requirement

**1. Share one axis instance across groups** (recommended for uniform
temporal geometry, e.g., a satellite-imagery plugin whose 1 km and 2 km
data share the same time axis):

```python
from firecube.ingestor.api import IndexSpec, RegularTimeAxis

axis = RegularTimeAxis(
    coordinate="timestamp",
    epoch="2024-01-01T00:00:00Z",
    cadence_s=600,
    end_date="2024-01-08T00:00:00Z",  # or equivalently slot_count=1008
)
return IndexSpec(name="my_product_v1", groups={
    "data_1km": axis,
    "data_2km": axis,   # SAME OBJECT, not a fresh construction
})
```

**2. Pass `--slot-group` when calling into the ingestor** to name which
group's axis is authoritative:

```bash
firecube ingest --slot-group data_1km ...
```

### Common pitfall

```python
# WRONG - two structurally equal axes, distinct Python objects:
return IndexSpec(name="my_product_v1", groups={
    "data_1km": RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        slot_count=1008,
    ),
    "data_2km": RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        slot_count=1008,
    ),
})
# Raises ConfigurationError at ingestion time.
```

## See Also

- [Implement `DirectZarrIngestor`](../guides/plugins/direct-zarr.md)
- [Parallel DirectZarrIngestor tutorial](../tutorials/direct-zarr-parallel.md)
- [Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)
