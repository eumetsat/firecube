# Best Practices & Performance

You already know how to run Firecube. This section is for making production
runs reliable, observable, and fast enough for the workload.

Firecube performance decisions usually come after one operational question:
where is the run spending time? Measure first, then choose the smallest change
that matches the bottleneck.

## Common Paths

- **Going to production?** Start with [Production Guide](production-guide.md).
  It covers the default way to run one product job, preserve ChunkManager
  records, pass secrets, clean workspaces, and enable observability.
- **A run is slow?** Use [Performance Tuning](performance.md). It starts from
  the pipeline phases and maps bottlenecks to the right tuning knobs.
- **Need more workers?** Use [Parallelism](parallelism.md). It explains which
  parallel model fits `GenericZarrIngestor`, `DirectZarrIngestor`,
  `GenericParquetIngestor`, and staged uploads.
- **Need measured examples?** Use [Benchmarks](benchmarks.md). Treat the
  numbers as workload examples, then benchmark your own product with the same
  measurements.

## Recommended Defaults

For most production runs:

- run one `firecube ingest` command per product target;
- pass `--product-name`, `--target`, `--storage-type`, `--storage-driver`,
  `--output-format`, and `--write-mode` explicitly;
- keep ChunkManager records with the product when moving, copying, packaging, or
  restoring data;
- start with one worker, then add parallelism from the output format and plugin
  class;
- configure logs, metrics, and traces before the first production run.

## Next Steps

- **[Production Guide](production-guide.md)** — run Firecube reliably in production
- **[Performance Tuning](performance.md)** — tune by bottleneck
- **[Parallelism](parallelism.md)** — choose safe workers and write domains
- **[Benchmarks](benchmarks.md)** — compare measured workload examples
- **[Operations](../operations/index.md)** — inspect, recover, and clean up products
- **[CLI Reference](../reference/cli.md)** — complete command surface
