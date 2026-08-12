# Plugin Templates

This reference covers the template classes plugin authors subclass and the
members each template expects. Import all symbols from
`firecube.ingestor.api`.

Plugins that own the complete batch pipeline subclass `BaseIngestor` directly;
see [Hooks & Lifecycle](hooks.md) for that surface.

{{ render_api_summary("firecube.ingestor.api", [
    "register_ingestor",
    "GenericZarrIngestor",
    "GenericParquetIngestor",
    "DirectZarrIngestor",
    "GenericTensogramIngestor",
]) }}

## Registration

::: firecube.ingestor.api.register_ingestor

Every concrete plugin class must also declare a non-empty
`PRODUCT_NAME: ClassVar[str]`. Firecube checks this when the class is defined.
Typed options are declared through [`PluginConfig`](config.md#pluginconfig)
and the template config classes listed in the
[Configuration Reference](config.md).

## GenericZarrIngestor

Subclass `GenericZarrIngestor` and implement `build_dataset`. The template
appends the returned dataset to the target Zarr store along the time
dimension, once per write group per batch.

::: firecube.ingestor.api.GenericZarrIngestor
    options:
        show_bases: false
        inherited_members: true
        members:
          - build_dataset
          - get_batch_groups
          - get_zarr_config

Options are declared through
[`ZarrTemplateConfig`](config.md#zarrtemplateconfig).

### Write Groups

`get_batch_groups` returns the logical write groups for a batch and defaults
to `["default"]`. Each group name is used directly as the Zarr group path in
the store, so nested paths such as `"sst/quality"` are valid. `build_dataset`
is called once per group per batch and receives the complete batch item list
for every group; the plugin selects per group which variables or items to
return. Returning `None` skips the group for that batch. The returned list
must be deterministic across runs.

`DirectZarrIngestor` derives its groups from the declared schema instead; see
[DirectZarrIngestor](#directzarringestor).

### Time Dimension

The inherited `time_dim_name: ClassVar[str]` selects the append dimension and
defaults to `"timestamp"`. It is a class declaration, not a `--option` field.
When the target store already exists, Firecube verifies the declared dimension
against the store before writing:

::: firecube.ingestor.api.verify_dim_compatibility

## GenericParquetIngestor

Subclass `GenericParquetIngestor` and implement `build_dataset`. The remaining
methods are optional customizations.

::: firecube.ingestor.api.GenericParquetIngestor
    options:
        show_bases: false
        inherited_members: true
        members:
          - build_dataset
          - get_batch_groups
          - output_relpath
          - write_parquet

`ParquetTemplateConfig` currently declares `parquet_partition_by` and
`parquet_row_group_size`, but the default writer does not apply either field;
see the [Configuration Reference](config.md#parquettemplateconfig).

## GenericTensogramIngestor

Subclass `GenericTensogramIngestor` and implement `build_dataset`. The
template writes each batch through the Tensogram strategy.

::: firecube.ingestor.api.GenericTensogramIngestor
    options:
        show_bases: false
        inherited_members: true
        members:
          - build_dataset
          - get_batch_groups

Options are declared through
[`TensogramTemplateConfig`](config.md#tensogramtemplateconfig).

## DirectZarrIngestor

Subclass `DirectZarrIngestor` and implement `zarr_schema` and
`build_write_intents`.

::: firecube.ingestor.api.DirectZarrIngestor
    options:
        show_bases: false
        inherited_members: true
        members:
          - zarr_schema
          - build_write_intents
          - get_batch_groups

On this template `get_batch_groups` is derived from the groups declared in
`zarr_schema` and is not an override point; overriding it breaks the
agreement between groups and schema.

For parallel writes across disjoint slot ranges, see
[Slot-Range Parallelism](parallelism.md).

### Schema And Write Types

::: firecube.ingestor.api.ZarrGroupSpec

::: firecube.ingestor.api.ZarrArraySpec

::: firecube.ingestor.api.WriteIntent

## See Also

- [Plugin Development Overview](../guides/plugins/index.md)
- [Customize Source Discovery](../guides/plugins/source-discovery.md)
- [Implement `GenericZarrIngestor`](../guides/plugins/generic-zarr.md)
- [Implement `GenericParquetIngestor`](../guides/plugins/generic-parquet.md)
- [Implement `DirectZarrIngestor`](../guides/plugins/direct-zarr.md)
- [Hooks & Lifecycle](hooks.md)
- [Context & Results](context.md)
