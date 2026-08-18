# Tutorials

Tutorials are step-by-step examples that extend a working plugin and verify its
output. The first three pages use the plugin created in the Quickstart. The
Sentinel-3 and parallel Zarr tutorials are independent examples. To choose an
authoring surface for a new product, start with
[Plugin Development](../guides/plugins/index.md); the tutorials use concrete
products only to demonstrate those generic contracts.

## Before You Start

Complete the [Quickstart](../quickstart/index.md) first. It creates the local
`weather_netcdf` plugin, source files, and initial Zarr product used by the
first three tutorials. Keep its virtual environment active.

## First Plugin: NetCDF To Zarr

Complete these pages in order. They use the same generated plugin and input
data throughout:

1. **[Understand The Plugin](weather-netcdf.md)** — inspect how the quickstart
   plugin reads a sequence of NetCDF files and appends them to Zarr.
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
