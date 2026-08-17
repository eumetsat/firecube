# Metrics

Use metrics when you need counts, durations, throughput, storage failures, or
small product-specific measurements from a Firecube run.

Firecube is a batch CLI, so it does not expose a `/metrics` endpoint. It buffers
metrics while the command runs and pushes the final snapshot to Prometheus
Pushgateway at the end when a Pushgateway is configured.

## Enable Metrics

Set the Pushgateway URL before running Firecube:

```bash
export FIRECUBE_PUSHGATEWAY_URL="http://localhost:9091"
```

Or set it in `~/.config/firecube/config.toml`:

```toml
[metrics]
pushgateway_url = "http://localhost:9091"
```

If no Pushgateway is configured, the command still runs. Metrics are just not
pushed.

Disable metric pushing for an environment:

```bash
export FIRECUBE_METRICS_DISABLED=true
```

## What Firecube Emits

The engine emits standard run metrics automatically: batch counts, file counts,
pipeline timing, storage timing, validation timing, error counts, and related
run-summary signals.

Plugin code should not emit those engine-owned metrics manually. Plugin metrics
should be product-specific values that the engine cannot infer, such as records
extracted, detections found, scenes parsed, or domain-specific quality counters.

## Metric Kinds

Use `counter` for values that increase during a run, such as files parsed or
records extracted. Prometheus names get `_total` when needed.

Use `gauge` for current values, sizes, durations, latest measurements, or values
that can go up and down.

Use explicit units in metric names, for example `_seconds`, `_bytes`, or
`_rows`.

## Plugin Metrics

Plugin metrics are emitted through the injected `ctx.telemetry` object:

```python
if ctx.telemetry is not None:
    ctx.telemetry.emit(
        "weather_csv_files",
        len(items),
        kind="counter",
        meta={"group": group, "status": "success"},
    )
```

See [Plugin Observability](../../guides/plugins/observability.md) for the full plugin
contract, including `ctx.telemetry.emit(...)`, span usage, and what plugins must
not import.

## Labels

Metric metadata becomes Prometheus labels only when the key is allowlisted.
Good labels are bounded and low-cardinality:

```python
meta={"group": "default", "status": "success"}
```

Avoid labels with unbounded values:

- `run_id`
- `batch_id`
- timestamps
- file paths or URIs
- exception messages
- stack traces

If operators need additional bounded labels, extend the allowlist in config or
environment. See [Observability Reference](../../reference/observability.md#labels).

## Standard Run Metrics

The complete standard metric table is generated from the canonical Firecube
metric schema. The same table is also available in the
[Observability Reference](../../reference/observability.md#standard-run-metrics).

<details markdown="1">
<summary>Standard run metrics</summary>

{{ render_metrics_table() }}

</details>

## Next Steps

- **[Observability Reference](../../reference/observability.md)** — complete metric, label, grouping, and environment-variable reference
- **[NetCDF To Zarr: Observability](../../tutorials/observability.md)** — add one custom metric to a plugin
- **[Logs](logs.md)** — structured logs and stdout/stderr behavior
- **[Traces](traces.md)** — OTLP setup and span correlation
