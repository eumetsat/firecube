# Tutorials

Tutorials are step-by-step examples that end in a working plugin and verified
output. The first three pages build one plugin in order. The Sentinel-3 and
parallel Zarr tutorials are independent examples. To choose an authoring
surface for a new product, start with
[Plugin Development](../guides/plugins/index.md); the tutorials use concrete
products only to demonstrate those generic contracts.

## Before You Start

Install Firecube first: start with the [Quickstart](../quickstart/index.md) or
[Installation](../quickstart/installation.md) page. Run tutorial commands in the
same Python environment where Firecube is installed.

## First Plugin: NetCDF To Zarr

Complete these pages in order. They use the same generated plugin and input
data throughout:

1. **[Build The Plugin](weather-netcdf.md)** — read a sequence of NetCDF files
   and append them to Zarr.
2. **[NetCDF To Zarr: Source Discovery](source-discovery.md)** — extend the same plugin run to
   another NetCDF filename pattern.
3. **[NetCDF To Zarr: Observability](observability.md)** — add one product-specific
   metric and inspect it through a Prometheus Pushgateway.

## Sentinel-3 FRP To Parquet

This is the first tutorial that uses real EUMETSAT data. **[Download a
Sentinel-3 FRP product with EUMDAC](sentinel3-frp.md)**, build a plugin for its
standard MWIR detections, and write them to Parquet.

## Parallel DirectZarrIngestor

**[Build a slot-capable `DirectZarrIngestor`](direct-zarr-parallel.md)** and
write two disjoint slot ranges into one preallocated Zarr array.

## Next Steps

- **[Plugin Development](../guides/plugins/index.md)** — choose a plugin authoring class
- **[Output Formats](../concepts/output-formats/index.md)** — choose Zarr, Parquet, or Tensogram
- **[Parallelism](../concepts/parallelism.md)** — choose a safe parallel model
