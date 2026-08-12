# Slot-Range Parallelism

This reference covers the types and methods for parallel `DirectZarrIngestor`
writes, where independent workers write disjoint half-open slot ranges of the
same store. Methods live on `DirectZarrIngestor`; range and model types are
imported from `firecube.ingestor.api` and `firecube.core.api` as indicated.

Set `SUPPORTS_SLOT_RANGE_PARALLELISM = True` only when the plugin implements
all three required methods below. Firecube checks the overrides when the
class is defined. An intent whose index falls outside the worker's assigned
range fails the run with
[`WriteIntentRangeError`](exceptions.md#firecube.ingestor.api.WriteIntentRangeError).

{{ render_api_summary("firecube.ingestor.api", [
    "DirectZarrIngestor.timestamp_to_ts_index",
    "DirectZarrIngestor.global_expected_time_count",
    "DirectZarrIngestor.slot_index_model",
    "DirectZarrIngestor.filter_items_to_slot_range",
    "SlotRange",
    "PlannedRange",
    "validate_slot_range",
    "chunk_align_ranges",
    "compute_covered_ranges",
    "validate_chunk_alignment",
    "warn_if_misaligned",
]) }}

## Required Methods

::: firecube.ingestor.api.DirectZarrIngestor.timestamp_to_ts_index
    options:
        inherited_members: true

::: firecube.ingestor.api.DirectZarrIngestor.global_expected_time_count
    options:
        inherited_members: true

::: firecube.ingestor.api.DirectZarrIngestor.slot_index_model
    options:
        inherited_members: true

## Optional Filtering

`filter_items_to_slot_range` is optional but recommended. Its default returns
all items unchanged. Firecube still rejects any resulting intent whose index
is outside the worker's assigned half-open range.

::: firecube.ingestor.api.DirectZarrIngestor.filter_items_to_slot_range
    options:
        inherited_members: true

## Range Types

::: firecube.ingestor.api.SlotRange

::: firecube.ingestor.api.PlannedRange

## Range Validation

::: firecube.ingestor.api.validate_slot_range

::: firecube.ingestor.api.chunk_align_ranges

::: firecube.ingestor.api.compute_covered_ranges

::: firecube.ingestor.api.validate_chunk_alignment

::: firecube.ingestor.api.warn_if_misaligned

## Slot-Index Model

The slot-index model types are imported from `firecube.core.api`. The related
epoch/ISO time helpers are documented in
[Core Utilities](core-utilities.md#time-conversion).

::: firecube.core.api.SlotIndexModel

::: firecube.core.api.SlotAxis

## See Also

- [Implement `DirectZarrIngestor`](../guides/plugins/direct-zarr.md)
- [Parallel DirectZarrIngestor tutorial](../tutorials/direct-zarr-parallel.md)
- [Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)
- [Exceptions](exceptions.md)
