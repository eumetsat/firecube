# Traces

Firecube emits OpenTelemetry spans while a command runs. Traces are useful when
you need to inspect slow phases, understand batch timing, or connect a Firecube
run to an external job.

## Enable Tracing

For an ingest command, the trace service name is:

```text
firecube-ingestor-<plugin>
```

Set an OTLP HTTP endpoint to export spans:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318/v1/traces"
```

If your collector needs headers, pass them through OpenTelemetry's standard
header variable:

```bash
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer <token>"
```

For local debugging without a collector:

```bash
export OTEL_DEBUG=true
```

## What Traces Show

An ingest trace starts at the CLI command, then follows the run through
discovery, batching, batch processing, writes, finalization, and staged upload
when S3 staged writes are used.

Template spans add detail where the engine owns the work. For example,
`GenericZarrIngestor` emits preparation and Zarr write spans around each batch.
Plugins can add product-specific spans for work the engine cannot describe.

When `pipeline_parallel=true`, Firecube preserves trace parentage across worker
threads. Batch spans still appear under the run trace instead of becoming
disconnected root spans.

## Plugin Spans

Use `ctx.telemetry.span(...)` for product-specific phases. See
[Plugin Observability](../../guides/plugins/observability.md) for the full span
contract and usage guidance.

## Run Correlation

Use `--option run_id=...` when an external job already has a stable identifier
you want to carry into ChunkManager records and manifests.

Firecube also reads `KFP_RUN_ID` and adds it to trace spans as `kfp_run_id`. If
your environment provides one, set it before running the command:

```bash
export KFP_RUN_ID="<external-job-id>"
```

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target s3://bucket/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option run_id="<external-job-id>"
```

Use Pushgateway grouping keys for metric grouping; do not add `run_id` as a
metric label unless you intentionally accept the higher cardinality. See
[Observability Reference](../../reference/observability.md#pushgateway-grouping).

## Next Steps

- **[Metrics](metrics.md)** — end-of-run metrics and Pushgateway setup
- **[Logs](logs.md)** — structured logs and stdout/stderr behavior
- **[Observability Reference](../../reference/observability.md)** — complete observability surface
