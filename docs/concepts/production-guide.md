# Production Guide

Production Firecube runs should be predictable: one command writes one product,
the product remains inspectable through ChunkManager, and failed work can be
retried or cleaned up deliberately.

This page is for CI, cron, containers, or an external orchestrator. The external
system decides when commands run. Firecube owns the ingest lifecycle around the
plugin.

## Run One Product Per Job

Run one `firecube ingest` invocation for one product target. Fan out across
products, dates, regions, or other independent work at the external job level.

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target file:///data/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged
```

This keeps failures understandable: one job has one product URI, one run record,
and one set of ChunkManager records to inspect.

Use full product URIs such as `file:///data/products/MY_PRODUCT.zarr` or
`s3://bucket/products/MY_PRODUCT.zarr`. Firecube infers storage type from the
URI and defaults the driver to configured settings or `fsspec`. Product name,
output format, and write mode remain separate choices.

## Keep ChunkManager Records With The Product

ChunkManager records live in the product's `.firecube/` directory. They are part
of the Firecube product state, not disposable logs.

If you move, copy, package, or restore a product manually, keep `.firecube/`
with the readable data when you want later inspection, resume, recovery, or
cleanup to work. Firecube archive operations preserve this state for supported
package and restore workflows.

See [Storage & ChunkManager](chunkmanager.md) for the mental model and
[Operations](../operations/index.md) for inspection and recovery commands.

## Make Retries Explicit

Use `resume_existing=true` when a workflow is meant to resume rather than start
from scratch:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target file:///data/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option resume_existing=true
```

On retry, Firecube checks ChunkManager before writing. If an active run or
conflicting span exists, Firecube blocks the command instead of guessing. Use
[Operations](../operations/index.md) to inspect the product, abandon a stuck
run, recover stale claims, or clean up records.

## Pass Secrets Through The Environment

Do not put object-storage credentials in commands. Export them in the runtime
environment or inject them through the external job system, then keep the
Firecube command explicit:

```bash
export FIRECUBE_ENDPOINT_URL="https://your-s3-endpoint.example.com"
export FIRECUBE_REGION="<region>"
export FIRECUBE_ACCESS_KEY="<access-key>"
export FIRECUBE_SECRET_KEY="<secret-key>"
```

The production command should change target URI and credentials, not plugin
code.

## Choose Parallelism From The Output Format

Parallelism is safe when every worker owns a distinct write domain:

- `GenericParquetIngestor` can use pipeline workers because batches write
  independent files.
- `GenericZarrIngestor` can use pipeline workers for preparation, but appends
  to one Zarr group are serialized.
- `DirectZarrIngestor` can use slot workers for one Zarr group only when the
  plugin supports deterministic, chunk-aligned slot ranges.
- staged uploads use `upload_workers` after the pipeline has completed.

Use [Parallelism](parallelism.md) before raising worker counts or starting
multiple jobs against one product.

## Control Workspace Cleanup

Staged writes need local scratch space. In containers, set an explicit workspace
and clean it up on successful runs:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target s3://bucket/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type s3 \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option workspace=/tmp/firecube \
  --option cleanup_workspace=true
```

Use [Performance Tuning](performance.md) when you need to choose between direct
and staged writes or size the workspace.

## Enable Observability Before You Need It

Use JSON logs for production capture:

```bash
export FIRECUBE_LOG_FORMAT=json
```

Configure metrics and traces where available:

```bash
export FIRECUBE_PUSHGATEWAY_URL="http://pushgateway:9091"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318/v1/traces"
```

Firecube creates an in-memory Prometheus registry for the ingest process when a
Pushgateway URL is configured. Engine metrics and plugin `ctx.telemetry.emit(...)`
calls update that registry during the run. During teardown, Firecube emits the
final run-summary metrics and pushes the registry to Pushgateway once.

See [Observability](observability/index.md) for the model and
[Observability Reference](../reference/observability.md) for the complete
environment-variable surface.

## Production Checklist

- One product URI per job.
- Required CLI flags passed explicitly.
- ChunkManager records kept with the product.
- Credentials injected through environment variables or external job secrets.
- Retry behavior chosen deliberately with `resume_existing=true` when needed.
- Parallelism chosen from the plugin class and output format.
- Workspace path and cleanup configured for staged writes.
- Logs, metrics, and traces configured before the first production run.

## Next Steps

- **[Operations](../operations/index.md)** — inspect, recover, and clean up products
- **[Parallelism](parallelism.md)** — choose safe workers and write domains
- **[Performance Tuning](performance.md)** — tune batches, writes, uploads, and validation
- **[Observability](observability/index.md)** — configure metrics, logs, and traces
