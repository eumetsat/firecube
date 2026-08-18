# Context & Results

This reference covers the context object passed to plugin hooks, the storage
sessions it exposes, batch inputs, and the result and metrics types. Import
all symbols from `firecube.ingestor.api`.

{{ render_api_summary("firecube.ingestor.api", [
    "PluginContext",
    "StorageContext",
    "PipelineBatch",
    "PipelineRunState",
    "IngestResult",
    "PipelineResult",
    "OutputPaths",
    "ResultMetrics",
    "PipelineMetrics",
    "StorageMetrics",
]) }}

## PluginContext

::: firecube.ingestor.api.PluginContext
    options:
        show_bases: false
        members:
          - source
          - target
          - in_memory
          - output_format
          - storage
          - temp_root
          - force_reingest
          - incremental
          - dry_run
          - telemetry
          - options
          - run_id
          - option
          - materialize

## StorageContext

::: firecube.ingestor.api.StorageContext
    options:
        show_bases: false

## Batch Input

`GenericParquetIngestor` and `DirectZarrIngestor` receive a `PipelineBatch`.
`GenericZarrIngestor` receives the batch's source items directly.

::: firecube.ingestor.api.PipelineBatch

## Run State

::: firecube.ingestor.api.PipelineRunState

## Results

::: firecube.ingestor.api.IngestResult

::: firecube.ingestor.api.PipelineResult

::: firecube.ingestor.api.OutputPaths

## Metrics

::: firecube.ingestor.api.ResultMetrics

::: firecube.ingestor.api.PipelineMetrics

::: firecube.ingestor.api.StorageMetrics

## See Also

- [Templates](templates.md)
- [Hooks & Lifecycle](hooks.md)
