# Add Plugin Telemetry

## Goal

Emit product-specific metrics and spans through the telemetry facade supplied
to plugin hooks.

## Minimal Example

```python
def build_dataset(self, group, items, ctx):
    if ctx.telemetry is None:
        return parse_product(items)

    with ctx.telemetry.span("my_plugin.parse", {"group": group}):
        dataset = parse_product(items)

    ctx.telemetry.emit(
        "my_plugin_scenes_parsed",
        len(items),
        kind="counter",
        meta={"group": group},
    )
    return dataset
```

Metric `kind` is `"counter"` or `"gauge"`. Keep metric names stable and label
values low-cardinality. File paths, timestamps, run IDs, and error text should
not be labels.

Check `ctx.telemetry is not None` before calling the facade. Telemetry can be
absent when a hook is invoked outside a configured Firecube run. See the
[Observability Reference](../../reference/observability.md) for the complete
metric and span surface.

## Keep Runtime Configuration Separate

Telemetry backends are configured by the Firecube process. Plugin code does not
configure exporters, handlers, or global log levels.

Use a module logger created with `logging.getLogger(__name__)` for operational
logging.

## Verify

Run the plugin with the deployment's configured telemetry backend. Confirm the
product output first, then confirm that the custom metric or span has the
expected name, value, and low-cardinality attributes. Also invoke the hook in
the plugin's normal isolated verification with telemetry absent so the guard is
exercised.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Importing Prometheus or OpenTelemetry clients | Use `ctx.telemetry`. |
| Emitting pipeline or storage metrics | Let Firecube emit runtime-owned metrics. |
| Calling `logging.basicConfig()` or adding handlers | Use the logger configured by Firecube. |
| Using `print()` for operational logs | Use a module logger. |

## Next Steps

- **[Metrics](../../concepts/observability/metrics.md)** — understand metric ownership and interpretation
- **[Traces](../../concepts/observability/traces.md)** — understand trace context and spans
- **[Logs](../../concepts/observability/logs.md)** — understand structured runtime logs
- **[Observability Reference](../../reference/observability.md)** — look up telemetry fields
