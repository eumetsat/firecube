# Plugin Authors

This page explains what Firecube does around your plugin hooks. It is for
writing or maintaining a plugin when you need the runtime model: runs, batches,
resume checks, storage writes, ChunkManager records, and telemetry.

For hook-by-hook implementation details, use the [Plugins](../concepts/plugins/index.md)
chapter.

## What The Engine Does Around Your Plugin

A `firecube ingest` command starts one run for one product target. Before your
plugin processes data, Firecube resolves the runtime options, checks
ChunkManager for resume conflicts, discovers source items, and splits work into
batches.

During the run, the engine owns:

- source discovery and batching;
- pipeline workers;
- output writes through the selected storage driver;
- ChunkManager run, span, claim, and cleanup records;
- final run metrics, logs, and traces.

Your plugin owns the product-specific work: read the source items, build arrays
or records, describe coordinates and variables, and emit product-specific
telemetry when useful.

## What Your Hooks Receive

Plugin hooks receive a `PluginContext`. Treat it as read-only runtime state for
the current command. It carries options, run metadata, storage context, and
telemetry services.

Use `ctx.materialize(source_item)` when a source item may need to be available
as a local file before parsing. Use `ctx.storage.output` only when plugin code
needs output-storage metadata, and use `ctx.telemetry` for product-specific
counters, gauges, and spans.

## What Your Hooks Return

Return public SDK types from `firecube.ingestor.api`. For custom batch hooks,
return `PipelineResult` with `OutputPaths`:

```python
from firecube.ingestor.api import OutputPaths, PipelineBatch, PipelineResult


def build_result(batch: PipelineBatch, output_path: str) -> PipelineResult:
    return PipelineResult(
        batch=batch,
        outputs=OutputPaths(primary=output_path),
    )
```

Do not construct `PipelineResult(output_path=...)`.

Template classes such as `GenericZarrIngestor`, `GenericParquetIngestor`, and
`DirectZarrIngestor` usually hide this detail. Implement the template hooks
instead of rebuilding the pipeline.

## Resume Checks

ChunkManager is the authority for normal resume decisions. Firecube checks it
before calling plugin batch hooks.

The practical behavior is:

- an active prior run blocks the command until it is abandoned or recovered;
- overlapping spans block a normal run;
- `resume_existing=true` allows a retryable workflow to continue;
- `force_reingest=true` deliberately reprocesses existing spans.

The data store is not inspected during normal resume. Zarr validation happens
only when validation is explicitly enabled.

## What Plugin Code Should Not Do

Plugin code should not:

- write ChunkManager records directly;
- create its own worker pool for Firecube batches;
- configure global logging handlers;
- push Prometheus metrics directly;
- import OpenTelemetry or Prometheus clients directly;
- deep-import private runtime modules;
- infer required CLI settings from target paths.

The engine handles these boundaries so plugins stay small and product-focused.

## Next Steps

- **[Plugins](../concepts/plugins/index.md)** — plugin system overview and base-class choice
- **[Create a Plugin](../concepts/plugins/create-a-plugin.md)** — scaffold a plugin and choose a template
- **[Plugin Contract](../concepts/plugins/contract.md)** — required public SDK types and result rules
- **[GenericZarrIngestor](../concepts/plugins/generic-zarr.md)** — implement append-Zarr output hooks
- **[GenericParquetIngestor](../concepts/plugins/generic-parquet.md)** — implement Parquet output hooks
- **[DirectZarrIngestor](../concepts/plugins/direct-zarr.md)** — implement direct-region Zarr output hooks
- **[Plugin Observability](../concepts/plugins/observability.md)** — emit product metrics and spans
- **[Plugin Storage Access](../concepts/plugins/storage-access.md)** — materialize source items and use storage metadata
- **[Plugin CLI And Config](../concepts/plugins/cli-and-config.md)** — add plugin options
- **[Output Formats](../concepts/output-formats/index.md)** — choose Zarr, Parquet, or Tensogram
- **[Parallelism](../concepts/parallelism.md)** — choose a safe parallel model
- **[Firecube Contributors](firecube-contributors.md)** — engine internals and contribution boundaries
