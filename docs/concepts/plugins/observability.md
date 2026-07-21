# Plugin Observability

## Goal

Emit product-specific metrics and spans from plugin hooks using the injected
`ctx.telemetry` object. This page covers the two public APIs plugins may use,
what plugins must not do, and what the engine already handles on their behalf.

## Minimal Example

```python
from typing import ClassVar

from firecube.ingestor.api import GenericZarrIngestor, PluginContext, register_ingestor


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def build_dataset(self, group: str, items: list[object], ctx: PluginContext):
        if ctx.telemetry is not None:
            with ctx.telemetry.span("my_plugin.parse", {"group": group}):
                results = self._parse(items)

            ctx.telemetry.emit(
                "my_plugin_files_parsed",
                len(items),
                kind="counter",
                meta={"group": group, "status": "success"},
            )
        else:
            results = self._parse(items)

        return results
```

Guard every `ctx.telemetry` call with `if ctx.telemetry is not None:`. Telemetry
is optional and may be absent in test environments or when observability is
disabled.

For a complete walkthrough with verification steps, see the
[Weather CSV: Observability](../../tutorials/observability.md).

## Required API

| API | Required | Purpose |
|-----|----------|---------|
| `ctx.telemetry.emit(name, value, kind=..., meta=...)` | Yes, for metrics | Emit a product-specific counter or gauge |
| `ctx.telemetry.span(name, attributes)` | Yes, for traces | Context manager that wraps a timed phase |
| `if ctx.telemetry is not None:` | Yes | Guard before any telemetry call |

### Emit Metrics

```python
ctx.telemetry.emit(
    "my_plugin_records_extracted",
    record_count,
    kind="gauge",
    meta={"group": group, "status": "success"},
)
```

`kind` is either `"counter"` (value increments a running total, gets `_total`
suffix in Prometheus) or `"gauge"` (value sets the current measurement). Use
explicit units in metric names: `_seconds`, `_bytes`, `_rows`.

Only emit values the engine cannot infer. Good candidates: files parsed by a
product-specific parser, records extracted from a source file, detections found
in a scene.

### Span Usage

```python
with ctx.telemetry.span("my_plugin.read_source", {"group": group}):
    data = self._read(items)
```

Use spans for phases where timing matters. Keep span names stable across runs
and attributes low-cardinality. One span per batch phase is appropriate; one
span per file in a large batch is not.

Do not import OpenTelemetry directly. Use `ctx.telemetry.span(...)`.

## What Plugins Should Not Emit

The engine owns these metrics and emits them automatically. Plugin code must not
emit them manually:

- pipeline counters and timings
- storage/write metrics
- ChunkManager health metrics
- run IDs as metric labels

Do not construct `metrics["storage"]` yourself. Firecube fills storage metrics
from the configured storage session at finalization.

Avoid high-cardinality label values in `meta`:

- `run_id`, `batch_id`
- timestamps
- file paths or URIs
- exception messages or stack traces

## Logging Rules

Plugin code may use `self._log` (the logger Firecube injects into the ingestor)
or a standard `logging.getLogger(__name__)` logger. Either is fine.

Plugins must not:

- call `print(...)` for operational logs
- call `logging.basicConfig(...)`, `logging.config.dictConfig(...)`, or
  `logger.addHandler(...)` to configure logging handlers
- call `logger.setLevel(...)` to change the root or handler level
- log raw credentials, tokens, or secret values

Firecube CLI configures logging before plugin code runs. Plugins that reconfigure
logging can corrupt JSON output or suppress structured fields that operators
depend on.

## Configuration

No plugin-side configuration is needed to use `ctx.telemetry`. The CLI
configures the telemetry backend from environment variables and config file
before invoking plugin hooks.

To enable metric pushing, set a Pushgateway URL:

```bash
export FIRECUBE_PUSHGATEWAY_URL="http://localhost:9091"
```

To enable trace export, set an OTLP endpoint:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318/v1/traces"
```

Full environment variable reference: [Observability Reference](../../reference/observability.md).

## Test It

Run the plugin and check that the metric name appears in Pushgateway output:

```bash
export FIRECUBE_PUSHGATEWAY_URL="http://localhost:9091"

uv run firecube ingest my_plugin \
  --source /data/source \
  --target file:///data/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct

curl -s "$FIRECUBE_PUSHGATEWAY_URL/metrics" | grep "firecube_my_plugin"
```

To test without a Pushgateway, check that the plugin runs without errors and
that the ingest manifest on stdout includes the expected output fields. Telemetry
calls are no-ops when `ctx.telemetry is None`, so the guard also serves as a
test-environment fallback.

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Importing `opentelemetry` directly | Do not import OpenTelemetry directly. Use `ctx.telemetry.span(...)` |
| Importing `prometheus_client` directly | Do not import Prometheus directly. Use `ctx.telemetry.emit(...)` |
| Calling `logging.basicConfig` in plugin | Firecube CLI configures logging; plugins use `logging.getLogger(__name__)` |
| Calling `logging.config.dictConfig` in plugin | Firecube CLI configures logging; plugins use `logging.getLogger(__name__)` |
| Calling `logger.addHandler(...)` in plugin | Firecube CLI configures logging; plugins use `logging.getLogger(__name__)` |
| Emitting `run_id` as a metric label | Use Pushgateway grouping keys instead; see [Observability Reference](../../reference/observability.md#pushgateway-grouping) |
| Emitting pipeline counters or storage metrics | Engine fills these automatically; plugin code should not emit them |
| Missing `if ctx.telemetry is not None:` guard | Telemetry is optional; always guard before calling any telemetry method |

## Next Steps

- **[Metrics](../observability/metrics.md)** — metric kinds, labels, and plugin metric emission
- **[Traces](../observability/traces.md)** — span setup, span names, and plugin spans
- **[Logs](../observability/logs.md)** — JSON logs and plugin logging rules
- **[Weather CSV: Observability](../../tutorials/observability.md)** — add one custom metric to a plugin
- **[Observability Reference](../../reference/observability.md)** — complete observability surface
