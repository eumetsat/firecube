# Create a Plugin

The fastest way to start is to **scaffold a plugin**, then fill in one hook.

## 1. Scaffold

```bash
firecube plugins create my-plugin
```

Run it interactively, or pass options directly:

```bash
firecube plugins create my-plugin \
  --template zarr \
  --author "Jane Doe" \
  --non-interactive
```

The `--template` (and, for Zarr, `--write-strategy`) you pick determines which
**base class** your generated ingestor extends:

| `--template` | `--write-strategy` | Base class | For |
|---|---|---|---|
| `zarr` | `xarray` (default) | [`GenericZarrIngestor`](generic-zarr.md) | Gridded data as an `xarray.Dataset`, appended in time order |
| `zarr` | `zarr-python` | [`DirectZarrIngestor`](direct-zarr.md) | Gridded data written into pre-allocated regions (out-of-order, sparse, parallel) |
| `parquet` | — | [`GenericParquetIngestor`](generic-parquet.md) | Tabular records |
| `base` | — | [`BaseIngestor`](base-ingestor.md) | Fully custom pipelines |

## 2. What you get

```text
firecube-my-plugin/
├── pyproject.toml          # metadata + firecube.plugins entry point
├── README.md
├── src/firecube_my_plugin/
│   ├── __init__.py         # imports the ingestor so registration runs
│   └── ingestor.py         # your ingestor class with a hook stub to fill in
└── tests/
    └── test_ingestor.py
```

Install it into the same environment as the Firecube CLI:

```bash
uv pip install -e ./firecube-my-plugin
firecube plugins list        # your plugin should appear
```

## 3. Fill in the hook

The generated `ingestor.py` has one stub to implement — `build_dataset` for the
Generic templates, or `zarr_schema` + `build_write_intents` for Direct. Open the
reference for your base class to see every hook you can override and what each
one does:

- **[GenericZarrIngestor](generic-zarr.md)**
- **[GenericParquetIngestor](generic-parquet.md)**
- **[DirectZarrIngestor](direct-zarr.md)**
- **[BaseIngestor](base-ingestor.md)**

## Author's checklist

Whichever base class you use:

- **Declare `PRODUCT_NAME`** (`ClassVar[str]`) on every concrete ingestor — enforced at class-definition time. See [Plugin Contract](./contract.md#product_name).
- **Import only from `firecube.ingestor.api`** (and `firecube.core.api`). Deep imports into `runtime/`, `core/`, or `templates/` internals are not part of the contract. See [Plugin Contract](./contract.md#public-imports-only).
- **Return a typed `PipelineResult`** — `OutputPaths` for outputs, `ResultMetrics` for metrics; never `output_path=` or plain dicts. See [Plugin Contract](./contract.md#typed-results).
- **Materialize source items and use storage metadata intentionally** — use
  `ctx.materialize(...)` for source files and `ctx.storage.output` for
  output-storage metadata. See [Plugin Storage Access](./storage-access.md).
- **Emit plugin metrics via `ctx.telemetry`** — never import Prometheus/OTel
  directly. See
  [Metrics](../observability/metrics.md). See [Plugin Observability](./observability.md).
- **Declare plugin config as a `PluginConfig` subclass** — don't use plain dicts or ad-hoc attributes for plugin-specific settings. See [Plugin CLI And Config](./cli-and-config.md).

## Next Steps

- **[Plugin Contract](contract.md)** — check required rules for concrete plugins
- **[GenericZarrIngestor](generic-zarr.md)** — build append-Zarr plugins
- **[GenericParquetIngestor](generic-parquet.md)** — build Parquet plugins
- **[DirectZarrIngestor](direct-zarr.md)** — build direct-region Zarr plugins
- **[Plugin CLI And Config](cli-and-config.md)** — add plugin options
