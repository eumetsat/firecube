# Public API Reference

This page contains the automatically generated public SDK reference for Firecube.
Plugin code should import these symbols from `firecube.ingestor.api` or
`firecube.core.api` — never from deeper internal modules.

The reference is organized in three tiers so plugin authors can find the right
surface for the task at hand:

1. **Primary Plugin Authoring Surface** — everything required to build a working
   plugin from scratch. Start here.
2. **Advanced Plugin Authoring** — exceptions, advanced context types,
   write-strategy customization, and pipeline internals for non-trivial plugins.
3. **Utilities & Type System** — range/slot helpers, source-file types,
   protocols, and core infrastructure (`firecube.core.api`).

## Primary Plugin Authoring Surface

Start here. Everything needed to write a working plugin. Import from
`firecube.ingestor.api` unless noted.

### Registration & Discovery

::: firecube.ingestor.api.register_ingestor
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.discover_ingestors
    options:
        show_root_heading: true
        show_source: false

### Base Class

::: firecube.ingestor.api.BaseIngestor
    options:
        show_root_heading: true
        show_source: false

### Context & Configuration

::: firecube.ingestor.api.PluginContext
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.EngineConfig
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.PluginConfig
    options:
        show_root_heading: true
        show_source: false

### Results & Metrics

::: firecube.ingestor.api.PipelineResult
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.OutputPaths
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ResultMetrics
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.merge_batch_metrics
    options:
        show_root_heading: true
        show_source: false

### Generic Template Classes

::: firecube.ingestor.api.GenericZarrIngestor
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.GenericParquetIngestor
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.GenericTensogramIngestor
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.DirectZarrIngestor
    options:
        show_root_heading: true
        show_source: false

### Template Configurations

::: firecube.ingestor.api.ZarrTemplateConfig
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ParquetTemplateConfig
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.TensogramTemplateConfig
    options:
        show_root_heading: true
        show_source: false

### DirectZarrIngestor Types

::: firecube.ingestor.api.WriteIntent
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ZarrArraySpec
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ZarrGroupSpec
    options:
        show_root_heading: true
        show_source: false

## Advanced Plugin Authoring

Errors to catch, advanced context types, write-strategy customization, and
pipeline internals for complex plugins.

### Exceptions

::: firecube.ingestor.api.IngestorError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ConfigurationError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.StorageError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ManifestError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.RangeOverlapError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.ResumeConflictError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.SchemaDriftError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.SchemaSizeMismatchError
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.WriteIntentRangeError
    options:
        show_root_heading: true
        show_source: false

### Advanced Context Types

::: firecube.ingestor.api.RuntimeIngestContext
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.StorageContext
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.IngestContext
    options:
        show_root_heading: true
        show_source: false

### Pipeline Internals

::: firecube.ingestor.api.IngestManifest
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.PipelineRunState
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.PipelineBatch
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.IngestResult
    options:
        show_root_heading: true
        show_source: false

### Metrics Types

::: firecube.ingestor.api.PipelineMetrics
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.StorageMetrics
    options:
        show_root_heading: true
        show_source: false

### Write Strategies

::: firecube.ingestor.api.AppendWriteStrategy
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.RegionWriteStrategy
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.AppendStrategy
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.IndexedRegionStrategy
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.TensogramWriteStrategy
    options:
        show_root_heading: true
        show_source: false

### Advanced Configuration

::: firecube.ingestor.api.TemplateConfig
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.CatalogGroupInfo
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.config_keys
    options:
        show_root_heading: true
        show_source: false

## Utilities & Type System

Range/slot helpers, source-file types, protocols, and core infrastructure.
Most symbols here come from `firecube.ingestor.api` or `firecube.core.api`.

### Range & Slot Utilities

::: firecube.ingestor.api.chunk_align_ranges
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.compute_covered_ranges
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.validate_chunk_alignment
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.validate_slot_range
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.warn_if_misaligned
    options:
        show_root_heading: true
        show_source: false

### Source File Types

::: firecube.ingestor.api.SourceFile
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.LocalSourceFile
    options:
        show_root_heading: true
        show_source: false

### Run Tracking

::: firecube.ingestor.api.SpanCoverage
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.PlannedRange
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.SlotRange
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.WriteDomain
    options:
        show_root_heading: true
        show_source: false

### Protocols

::: firecube.ingestor.api.Ingestor
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.PipelineHost
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.DatasetProducer
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.is_dataset_producer
    options:
        show_root_heading: true
        show_source: false

::: firecube.ingestor.api.SlotRangeCapable
    options:
        show_root_heading: true
        show_source: false

### Core Infrastructure (firecube.core.api)

::: firecube.core.api.parse_uri
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.create_filesystem_for_uri
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.discover_input_files
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.StorageConfig
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.RunInfo
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.describe_control_plane
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.RegionZarrWriterProtocol
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.delete_path
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.ensure_directory
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.ensure_product_uri
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.resolve_dataset_target
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.infer_target_protocol
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.is_remote_target
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.local_path_from_target
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.path_stats
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.clean_netcdf_encoding
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.prepare_netcdf_for_zarr
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.read_hdf5_array
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.materialize_hdf5_path
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.rename_time_dim
    options:
        show_root_heading: true
        show_source: false

::: firecube.core.api.require_tensogram
    options:
        show_root_heading: true
        show_source: false

## Next Steps

- **[Plugin Contract](../concepts/plugins/contract.md)** — check required plugin rules
- **[Plugin Storage Access](../concepts/plugins/storage-access.md)** — use `PluginContext` storage safely
- **[Plugins](../concepts/plugins/index.md)** — choose a base class and plugin path
