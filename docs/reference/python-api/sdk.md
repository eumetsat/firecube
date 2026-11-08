# SDK

The module `firecube.sdk` is the public Python door for plugin authors. A plugin imports every Firecube name it uses from this module. The module `firecube.operations` is the door for tooling. It opens a product and runs the archive, migrate, release and snapshot operations. See [Operations](operations.md).

The modules `firecube.ingestor.api` and `firecube.core.api` are deprecated until v0.2.0. Import the same names from `firecube.sdk`. A name that `firecube.sdk` does not carry stays importable from the old module until v0.2.0.

Each table below lists the names of one topic with the first line of their docstrings. A name links to its full entry on the page that owns it.

## Templates And Registration

{{ render_api_summary("firecube.sdk", [
    "BaseIngestor",
    "GenericZarrIngestor",
    "GenericParquetIngestor",
    "GenericTensogramIngestor",
    "DirectZarrIngestor",
    "register_ingestor",
    "WriteIntent",
    "ZarrArraySpec",
    "ZarrGroupSpec",
    "IndexedWrite",
    "verify_dim_compatibility",
    "BatchResourceRegistry",
]) }}

## Context And Results

{{ render_api_summary("firecube.sdk", [
    "PluginContext",
    "StorageContext",
    "ExecutionParams",
    "RuntimeIngestContext",
    "PipelineBatch",
    "PipelineRunState",
    "IngestResult",
    "PipelineResult",
    "OutputPaths",
    "ResultMetrics",
    "PipelineMetrics",
    "StorageMetrics",
]) }}

## Configuration

{{ render_api_summary("firecube.sdk", [
    "EngineConfig",
    "PluginConfig",
    "TemplateConfig",
    "ZarrTemplateConfig",
    "ParquetTemplateConfig",
    "TensogramTemplateConfig",
]) }}

## Index Specification

{{ render_api_summary("firecube.sdk", [
    "TimeAxis",
    "AxisSpec",
    "IndexSpec",
    "IntegerAxis",
    "IrregularTimeAxis",
    "RegularTimeAxis",
    "AUTO",
    "ItemInfo",
    "ResolvedIndex",
    "ResolvedIndexRecord",
    "resolve_index_spec",
    "coerce_to_epoch_s",
]) }}

## Exceptions

{{ render_api_summary("firecube.sdk", [
    "IngestorError",
    "ConfigurationError",
    "ExtentUnknownError",
    "UnboundedAxisError",
    "MissingIrregularCoordinateError",
    "DuplicateIrregularCoordinateError",
    "NoDiscoveredItemsError",
    "IndexedWriteCompilationError",
    "SchemaDriftError",
    "SchemaSizeMismatchError",
    "ManifestError",
    "StorageError",
    "ResumeConflictError",
    "RangeOverlapError",
    "WriteIntentRangeError",
]) }}

## Metadata Migration

{{ render_api_summary("firecube.sdk", [
    "MigrationStep",
    "StoreView",
    "AttributePatch",
    "MetadataPlan",
    "ReleaseRecord",
    "NodeView",
    "ABSENT",
]) }}

## Utilities

{{ render_api_summary("firecube.sdk", [
    "parse_uri",
    "is_remote_target",
    "infer_target_protocol",
    "local_path_from_target",
    "create_filesystem_for_uri",
    "path_stats",
    "ensure_directory",
    "delete_path",
    "discover",
    "DiscoveryRequest",
    "SourceItem",
    "discover_input_files",
    "prepare_netcdf_for_zarr",
    "clean_netcdf_encoding",
    "normalize_string_vars",
    "rename_time_dim",
    "read_hdf5_array",
    "materialize_hdf5_path",
    "extract_all_from_zips",
    "epoch_s_to_iso",
    "iso_to_epoch_s",
    "normalize_epoch_iso",
    "RESERVED_ARRAY_ATTRS",
    "assert_attrs_safe",
    "FIRECUBE_STATIC_WRITTEN_ATTR",
    "physical_chunk_keys_for_region",
    "chunk_axis_range",
    "axis_selection_is_chunk_aligned",
    "ZarrCompareReport",
    "compare_zarr_stores",
]) }}

## Names Without A Reference Entry

`firecube.sdk` exports these names. Their docstrings are the only reference text.

- `CatalogGroupInfo`
- `IngestContext`
- `Ingestor`
- `ItemManifestEntry`
- `SourceFile`
- `decode_time_array`
- `discover_ingestors`
- `read_chunk_grid_with_shards`

## See Also

- [Operations](operations.md)
- [Templates](../templates.md)
- [Hooks & Lifecycle](../hooks.md)
- [Context & Results](../context.md)
- [Configuration](../config.md)
- [Index Specification](../parallelism.md)
- [Exceptions](../exceptions.md)
- [Metadata Migration](../migration.md)
- [Core Utilities](../core-utilities.md)
