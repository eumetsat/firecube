# Advanced Custom Pipeline API Reference

This reference is only for plugins that subclass `BaseIngestor` directly. Use a
Zarr or Parquet template unless the plugin must own batch output writes and
coordination.

Firecube does not yet expose a public storage-writer protocol for custom output
code. A new external custom pipeline cannot be implemented using only stable,
typed public storage APIs. Do not import the concrete storage session from an
internal module as a workaround.

## Base Class

::: firecube.ingestor.api.BaseIngestor
    options:
        show_root_heading: true
        show_source: false
        members:
            - _process_batch
            - discover_source_files
            - resolve_output_uri

Custom plugin hooks receive `PluginContext`, the read-only plugin-facing
context documented in the [template API reference](api.md#plugin-context).

## Runtime Context

`RuntimeIngestContext` is the engine-owned context used to run the pipeline. It
is not the context type passed to plugin hooks and must not be reused across
runs.

::: firecube.ingestor.api.RuntimeIngestContext
    options:
        show_root_heading: true
        show_source: false

## Batch Result

::: firecube.ingestor.api.PipelineBatch
    options:
        show_root_heading: true
        show_source: false

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

## See Also

- [Plugin extensions](../guides/plugins/extensions.md)
- [Custom pipeline plugins](../guides/plugins/base-ingestor.md)
- [Package and Register a Plugin](../guides/plugins/contract.md)
