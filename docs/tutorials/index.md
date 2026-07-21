# Tutorials

Tutorials are step-by-step examples that end in a working plugin and verified
output. They are grouped as tracks so it is clear which pages build on the same
plugin.

## Before You Start

Install Firecube first: start with the [Quickstart](../quickstart/index.md) or
[Installation](../quickstart/installation.md) page. Run tutorial commands in the
same Python environment where Firecube is installed.

## First Plugin Track

- **[Weather CSV Plugin](weather-csv.md)** — build the smallest useful Zarr
  plugin from local CSV files.
- **[Weather CSV: Source Discovery](source-discovery.md)** — keep the same
  plugin and control which source files Firecube batches.
- **[Weather CSV: Observability](observability.md)** — keep the same plugin and
  add one product-specific metric.

## Product Examples

- **[Sentinel-3 FRP Plugin](sentinel3-frp.md)** — build a small Parquet plugin
  for downloaded Sentinel-3 FRP products.

## Advanced Zarr

- **[Direct Parallel Zarr](direct-zarr-parallel.md)** — write fixed Zarr slot
  ranges with `DirectZarrIngestor`.

## Next Steps

- **[Plugins](../concepts/plugins/index.md)** — understand the plugin system
- **[Output Formats](../concepts/output-formats/index.md)** — choose Zarr, Parquet, or Tensogram
- **[Parallelism](../concepts/parallelism.md)** — choose a safe parallel model
