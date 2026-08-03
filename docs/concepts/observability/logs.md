# Logs

Firecube logs to stderr. Command output, including JSON manifests and table
output, goes to stdout.

## Format

JSON logs are the default:

```bash
export FIRECUBE_LOG_FORMAT=json
```

Use plain logs for local debugging:

```bash
export FIRECUBE_LOG_FORMAT=plain
```

Default JSON log fields:

```text
asctime,hostname,process,thread,name,filename,lineno,level,trace_id,span_id,message
```

When a log line is emitted inside an active trace span, `trace_id` and `span_id`
are populated. Use those fields to join logs with traces in your observability
backend.

## Logging Environment Variables

Common settings:

- `FIRECUBE_LOG_FORMAT=json|plain` controls structured or plain output.
- `FIRECUBE_LOG_LEVEL=INFO` sets the root logging level.
- `FIRECUBE_LOG_STRUCTURED_FIELDS=...` changes the JSON fields.
- `FIRECUBE_DEBUG=true` enables DEBUG logs for Firecube namespaces without
  enabling every third-party library.

Firecube ignores generic `LOG_LEVEL`, `LOG_FORMAT`, and
`LOG_STRUCTURED_FIELDS`; use the `FIRECUBE_` names.

## Progress Logs

`--option no_progress=true` suppresses coarse pipeline progress messages:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target file:///data/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option no_progress=true
```

It does not suppress all INFO logs. Direct parallel Zarr evidence logs are still
emitted because they are operational evidence, not progress noise.

## Plugin Logging Rules

Plugins must not configure logging handlers (`basicConfig`, `dictConfig`,
`addHandler`, `setLevel`). Firecube CLI configures logging before plugin code
runs. See [Plugin Observability](../../guides/plugins/observability.md) for
the full logging boundary.

## Next Steps

- **[Metrics](metrics.md)** — end-of-run metrics and Pushgateway setup
- **[Traces](traces.md)** — OTLP setup and span names
- **[Observability Reference](../../reference/observability.md)** — complete observability surface
