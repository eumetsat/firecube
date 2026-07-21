# Plugin System

A plugin is the product-specific part of an ingestion. It reads source items and
describes the output data.

Firecube handles the operational work around it: source discovery, batching,
parallel execution, storage writes, resume checks, ChunkManager records,
cleanup, and observability.

Most plugins subclass one template class and implement one or two hooks.

## Choose A Base Class

Start by choosing the base class that matches the shape of your output:

| If your output is... | Start with |
|---|---|
| Gridded arrays appended in order | [`GenericZarrIngestor`](generic-zarr.md) |
| Gridded arrays written into fixed slots, sparse ranges, or parallel direct writes | [`DirectZarrIngestor`](direct-zarr.md) |
| Rows, detections, or feature tables | [`GenericParquetIngestor`](generic-parquet.md) |
| A custom pipeline that does not fit the templates | [`BaseIngestor`](base-ingestor.md) |

The two Zarr paths differ by *how your data arrives*. If you are unsure, see
[Zarr](../output-formats/zarr/index.md) to choose. To see how the classes
relate, open [BaseIngestor](base-ingestor.md#how-the-pieces-fit).

## What Plugins Own

| Plugin code owns | Firecube owns |
|---|---|
| Product-specific parsing, coordinates, variables, records, and options. | Discovery, batching, workers, storage drivers, write safety, cleanup, and telemetry plumbing. |
| Hook implementations for the selected base class. | The run lifecycle around those hooks. |
| Optional annotations such as catalog labels or product metrics. | Catalog generation, run metrics, logs, traces, and ChunkManager records. |

## Next Steps

- **[Create a Plugin](create-a-plugin.md)** — scaffold a new plugin
- **[GenericZarrIngestor](generic-zarr.md)** — implement append-Zarr output hooks
- **[GenericParquetIngestor](generic-parquet.md)** — implement Parquet output hooks
- **[DirectZarrIngestor](direct-zarr.md)** — implement direct-region Zarr output hooks
- **[BaseIngestor](base-ingestor.md)** — implement a fully custom batch pipeline
- **[Plugin Contract](contract.md)** — check required rules
- **[Plugin CLI And Config](cli-and-config.md)** — add plugin options
- **[Plugin Storage Access](storage-access.md)** — use storage safely
- **[Plugin Observability](observability.md)** — emit product metrics or spans
- **[Mixins](mixins.md)** — add optional helpers
