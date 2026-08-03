# Plugin Template API Reference

This reference covers the public authoring members of
`GenericZarrIngestor`, `GenericParquetIngestor`, and `DirectZarrIngestor`.
Import the symbols from `firecube.ingestor.api` unless a section names
`firecube.core.api` explicitly.

Plugins that own the complete batch pipeline use a separate, advanced
contract. See the
[Advanced Custom Pipeline API](advanced-plugin-api.md) for that surface.

## Shared Template Surface

### Registration

::: firecube.ingestor.api.register_ingestor
    options:
        show_root_heading: true
        show_source: false

Every concrete plugin class must also declare a non-empty
`PRODUCT_NAME: ClassVar[str]`. Firecube checks this when the class is defined.

### Plugin Context

::: firecube.ingestor.api.PluginContext
    options:
        show_root_heading: true
        show_source: false
        show_bases: false
        members:
          - source
          - target
          - temp_root
          - run_id
          - options
          - option
          - materialize
          - telemetry

### Plugin Configuration

::: firecube.ingestor.api.PluginConfig
    options:
        show_root_heading: true
        show_source: false

### Batch Input

`GenericParquetIngestor` and `DirectZarrIngestor` receive a `PipelineBatch`.
`GenericZarrIngestor` receives the batch's source items directly.

::: firecube.ingestor.api.PipelineBatch
    options:
        show_root_heading: true
        show_source: false

### Source Discovery

All three templates inherit
`discover_source_files(self, ctx: PluginContext) -> Iterable[Any]`. By default,
it recursively discovers `.zip`, `.h5`, and `.nc` files below `ctx.source`.
When `include_patterns` is set, those patterns select the source names instead.
Override the method only when source layout rules cannot be expressed as
patterns.

## GenericZarrIngestor

Subclass `GenericZarrIngestor` and implement `build_dataset`.

::: firecube.ingestor.api.GenericZarrIngestor
    options:
        show_root_heading: true
        show_source: false
        show_bases: false
        members:
          - build_dataset

The inherited `time_dim_name: ClassVar[str]` selects the append dimension and
defaults to `"timestamp"`. It is a class declaration, not a `--option` field.

::: firecube.ingestor.api.ZarrTemplateConfig
    options:
        show_root_heading: true
        show_source: false

## GenericParquetIngestor

Subclass `GenericParquetIngestor` and implement `build_dataset`. The remaining
methods are optional customizations.

::: firecube.ingestor.api.GenericParquetIngestor
    options:
        show_root_heading: true
        show_source: false
        show_bases: false
        members:
          - build_dataset
          - get_batch_groups
          - output_relpath
          - write_parquet

### Parquet Template Configuration

`ParquetTemplateConfig` currently declares `parquet_partition_by` and
`parquet_row_group_size`, but the default writer does not apply either field.
Do not configure them until writer support is implemented.

::: firecube.ingestor.api.ParquetTemplateConfig
    options:
        show_root_heading: true
        show_source: false

## DirectZarrIngestor

Subclass `DirectZarrIngestor` and implement `zarr_schema` and
`build_write_intents`.

::: firecube.ingestor.api.DirectZarrIngestor
    options:
        show_root_heading: true
        show_source: false
        show_bases: false
        members:
          - zarr_schema
          - build_write_intents

### Schema And Write Types

::: firecube.ingestor.api.ZarrGroupSpec
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ZarrArraySpec
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.WriteIntent
    options:
        show_root_heading: true
        show_source: false

### Optional Slot-Range Parallelism

Set `SUPPORTS_SLOT_RANGE_PARALLELISM = True` only when the plugin implements
all three required methods below. Firecube checks the overrides when the class
is defined.

::: firecube.ingestor.api.DirectZarrIngestor.timestamp_to_ts_index
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.DirectZarrIngestor.global_expected_time_count
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.DirectZarrIngestor.slot_index_model
    options:
        show_root_heading: true
        show_source: false

`filter_items_to_slot_range` is optional but recommended. Its default returns
all items unchanged. Firecube still rejects any resulting intent whose index is
outside the worker's assigned half-open range.

::: firecube.ingestor.api.DirectZarrIngestor.filter_items_to_slot_range
    options:
        show_root_heading: true
        show_source: false

The slot-index model types are imported from `firecube.core.api`:

::: firecube.core.api.SlotIndexModel
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.SlotAxis
    options:
        show_root_heading: true
        show_source: false

## See Also

- [Plugin Development Overview](../guides/plugins/index.md)
- [Implement `GenericZarrIngestor`](../guides/plugins/generic-zarr.md)
- [Implement `GenericParquetIngestor`](../guides/plugins/generic-parquet.md)
- [Implement `DirectZarrIngestor`](../guides/plugins/direct-zarr.md)
- [Advanced Custom Pipeline API](advanced-plugin-api.md)
