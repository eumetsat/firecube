# Context & Results

This reference covers the context object passed to plugin hooks, the storage
sessions it exposes, batch inputs, and the result and metrics types. Import
all symbols from `firecube.sdk`.

{{ render_api_summary("firecube.sdk", [
    "PluginContext",
    "StorageContext",
    "ExecutionParams",
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

::: firecube.sdk.PluginContext
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

::: firecube.sdk.StorageContext
    options:
        show_bases: false

## ExecutionParams

::: firecube.sdk.ExecutionParams
    options:
        show_bases: false

## Batch Input

`GenericParquetIngestor` and `DirectZarrIngestor` receive a `PipelineBatch`.
`GenericZarrIngestor` receives the batch's source items directly.

::: firecube.sdk.PipelineBatch

## Run State

::: firecube.sdk.PipelineRunState

## Results

::: firecube.sdk.IngestResult

::: firecube.sdk.PipelineResult

::: firecube.sdk.OutputPaths

## Metrics

::: firecube.sdk.ResultMetrics

::: firecube.sdk.PipelineMetrics

::: firecube.sdk.StorageMetrics

## See Also

- [Templates](templates.md)
- [Hooks & Lifecycle](hooks.md)
