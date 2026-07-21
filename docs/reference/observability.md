# Observability Reference

This reference lists Firecube's metrics, logs, traces, and Pushgateway
configuration surface.

For the operational model, see [Observability](../concepts/observability/index.md).
For plugin telemetry rules, see
[Plugin Observability](../concepts/plugins/observability.md).

## Metric Kinds

| Kind | Meaning | Name normalization |
|---|---|---|
| `counter` | Value increments a running counter | Adds `_total` when missing |
| `gauge` | Value sets the current measurement | No suffix added |

Metric names are sanitized for Prometheus and get a `firecube_` prefix when
missing.

## Labels

Every pushed metric includes these base labels:

| Label | Meaning |
|---|---|
| `plugin` | Plugin name |
| `product` | Product name |
| `output_format` | Output format such as `zarr` or `parquet` |
| `write_mode` | `direct` or `staged` |

Default optional metadata labels:

```text
group, forecast_horizon_hours, msg_region, satellite, status, error_type, resolution
```

Extend the label allowlist with either config or environment:

```toml
[metrics]
label_allowlist = ["frp_variant", "custom_label"]
```

```bash
export FIRECUBE_METRICS_LABEL_ALLOWLIST="frp_variant,custom_label"
```

Avoid labels with unbounded values such as run IDs, batch IDs, file paths,
timestamps, exception messages, or stack traces.

## Environment Variables

{{ render_env_vars() }}

## Trace Spans

Common ingest spans:

| Span | Key attributes | When emitted |
|---|---|---|
| `firecube.cli.ingest` | `firecube.plugin`, `firecube.write_mode` | Ingest CLI command |
| `firecube.ingest` | `firecube.run_id`, `firecube.plugin`, `firecube.product` | Full plugin run |
| `firecube.batch` | `firecube.batch_id` | Each pipeline batch |
| `firecube.finalize` | none | Run finalization |
| `firecube.upload_s3` | `firecube.target` | Staged upload to S3 |

Template-specific spans:

| Template path | Additional spans |
|---|---|
| `GenericZarrIngestor` | `firecube.batch.prepare`, `firecube.batch.zarr_write` |
| Tensogram output | `firecube.batch.prepare`, `firecube.batch.tensogram_write` |
| `GenericParquetIngestor` | Uses the common batch span unless the plugin emits its own spans |
| `DirectZarrIngestor` | Uses the common batch span unless the plugin emits its own spans |

## Config File

Firecube also reads metric settings from `~/.config/firecube/config.toml`:

```toml
[metrics]
pushgateway_url = "http://localhost:9091"
label_allowlist = ["frp_variant", "custom_label"]
```

Environment variables override the config file for `pushgateway_url`.
Label allowlists are additive: defaults, config values, and environment values
are merged.

## Pushgateway Grouping

By default, Firecube groups pushed metrics by:

```text
instance,plugin,product
```

Use grouping keys for per-run separation instead of adding `run_id` as a metric
label. For example:

```bash
export FIRECUBE_PUSHGATEWAY_GROUPING_KEYS="instance,plugin,product,workflow"
export FIRECUBE_PUSHGATEWAY_GROUP_WORKFLOW="nightly-fire"
```

## Standard Run Metrics

These metrics are emitted from the run summary.

{{ render_metrics_table() }}

The telemetry sink may also emit best-effort process memory gauges such as
`firecube_process_memory_peak_rss_bytes`.

## Storage Metrics In The Manifest

The ingest JSON manifest includes an engine-owned `metrics.storage` object when
storage information is available.

| Field | Meaning |
|---|---|
| `control_root` | Product-local `.firecube/` directory URI used by ChunkManager |
| `latest_pointer` | Product-local `LATEST.json` pointer URI |
| `path` | Final output path |
| `bytes` | Bytes written |
| `files` | Files written |
| `duration_s` | Storage write or upload duration |
| `storage_type` | Storage class such as `local` or `s3` |

Plugins should not construct this object manually.

## Next Steps

- **[Observability](../concepts/observability/index.md)** — metrics, logs, and traces model
- **[Metrics](../concepts/observability/metrics.md)** — metric usage and plugin emission
- **[Plugin Observability](../concepts/plugins/observability.md)** — plugin telemetry rules
