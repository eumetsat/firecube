# Configuration Model

Firecube configuration controls three things:

- which run you are starting;
- how Firecube connects to storage and runs the engine;
- which product-specific options a plugin receives.

Use this page to decide whether a value belongs on the command line, in an
environment variable, in `config.toml`, or in a plugin default.

## What Belongs Where

| Input | Use it for |
|---|---|
| CLI flags | Run identity and storage choices for one command, such as `--target`, `--product-name`, `--storage-type`, `--storage-driver`, and `--write-mode`. |
| Environment variables | S3 credentials, endpoint, region, and deployment-specific storage defaults. |
| `config.toml` | Reusable settings you want to keep between runs, such as storage defaults, metrics settings, and plugin defaults. |
| `--option key=value` | Engine, output-format, or plugin options for one run. |
| Plugin defaults | Product defaults declared by the installed plugin. |

## Precedence

Storage settings loaded through the storage configuration use this precedence:

1. CLI overrides
2. Environment variables
3. `config.toml`
4. Built-in defaults

Product name is resolved in this order:

1. `--product-name`
2. `default_product_name` in the plugin configuration
3. the plugin's `PRODUCT_NAME`
4. fail if no product name is available

!!! warning "Current limitation"
    `default_product_name` is currently rejected when plugin options are
    validated. Until that is corrected, pass `--product-name` or declare
    `PRODUCT_NAME` on the plugin class.

## Runtime Flags Stay Explicit

The documentation examples keep each ingest decision visible by passing:

- `--product-name`
- `--target`
- `--storage-type`
- `--storage-driver`
- `--output-format`
- `--write-mode`

Use `--target` for the output product location. Use `--product-name` for the
logical product identity when the plugin does not declare `PRODUCT_NAME` or the
run needs an override. Passing the storage type and driver explicitly keeps the
selected backend unambiguous. For `firecube chunks ...`, pass the product URI
through `--product-name` when the command needs to locate the control-plane
records.

For runnable examples, see [Run Ingestion](../quickstart/ingestion.md). For the
complete command surface, see the [CLI Reference](../reference/cli.md).

## Environment Variables

Use environment variables for S3 credentials and connection settings:

```bash
export FIRECUBE_ENDPOINT_URL="https://your-s3-endpoint.example.com"
export FIRECUBE_REGION="your-region"
export FIRECUBE_ACCESS_KEY="your-access-key"
export FIRECUBE_SECRET_KEY="your-secret-key"
export FIRECUBE_STORAGE_DRIVER="fsspec"
```

Environment variables are a good fit for values that should change by shell,
container, or deployment. They override `config.toml`, but explicit CLI
overrides still win.

## Config File

By default, Firecube looks for:

```text
~/.config/firecube/config.toml
```

Use the root `--config-file` option to point at another file:

```bash
firecube --config-file /path/to/config.toml ingest <plugin_name> --show-options
```

Use `config.toml` for settings you want to keep between runs:

```toml
[storage]
endpoint_url = "https://your-s3-endpoint.example.com"
region = "your-region"
access_key = "your-access-key"
secret_key = "your-secret-key"
driver = "fsspec"

[metrics]
pushgateway_url = "http://localhost:9091"

[plugins.my_plugin]
pipeline_parallel = true
pipeline_workers = 4
pipeline_batch_size = 40
custom_option = "value"
```

If you store credentials in this file, keep it private.

`[plugins.<name>]` applies only when that plugin is installed in the same Python
environment where the Firecube CLI runs. Use `firecube plugins describe` to
confirm the plugin name:

```bash
firecube plugins describe <plugin_name>
```

## Option Tiers

Values passed through `--option key=value` and `[plugins.<name>]` are validated
against active option tiers. Unknown keys fail immediately, so typos are not
silently ignored.

| Tier | Examples | Applies to |
|---|---|---|
| Engine options | `pipeline_parallel`, `pipeline_workers`, `pipeline_batch_size`, `include_patterns`, `resume_existing` | Runtime behavior shared by plugins. |
| Output options | `zarr_chunk_shape`, `zarr_compression`, `zarr_consolidate` | Zarr template behavior. |
| Plugin options | `region`, thresholds, filters, product-specific switches | Options declared by the installed plugin. |

Options with the `x_` prefix (e.g. `--option x_foo=1`) are in the experimental
namespace. They bypass tier validation and are passed through to plugins untyped.
Use them only for in-development plugin features that are not yet promoted to a
declared option tier.

The active tiers depend on the plugin and output format. Inspect them before
writing a config file:

```bash
firecube plugins describe <plugin_name>
firecube ingest <plugin_name> --show-options
firecube plugins explain <plugin_name>.engine.pipeline_workers
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Unknown option` | The key is not accepted by any active option tier. | Run `firecube ingest <plugin_name> --show-options` and use one of the listed keys. |
| Missing required flag | The target or write mode was omitted, or no product name fallback exists. | Add `--target` and `--write-mode`; add `--product-name` when the plugin has no `PRODUCT_NAME`. |
| Storage mismatch error | The target URI and `--storage-type` do not agree. | Use `file://` with `--storage-type local` or `s3://` with `--storage-type s3`. |
| Plugin defaults do not apply | The `[plugins.<name>]` section name does not match the installed plugin. | Run `firecube plugins describe <plugin_name>` and use that plugin name in `config.toml`. |
| S3 credentials are ignored | The variables or config keys are not visible to the process running Firecube, or a CLI override is winning. | Print the environment in the same shell before running Firecube and check for explicit CLI overrides. |

## Next Steps

- **[Configure S3 Access](../operations/s3-access.md)**: set credentials and connection settings.
- **[Configuration Reference](../reference/config.md)**: inspect the complete generated schema.
- **[Run Ingestion](../quickstart/ingestion.md)**: run one local ingestion.
