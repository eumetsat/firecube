# Install a Plugin

## When To Use This

Install an existing product plugin before running ingestion. Plugin authors
should use [Plugin Development](../guides/plugins/index.md) instead.

## Prerequisites

- Firecube installed in a Python environment.
- The distribution name, local path, or Git URL of a compatible plugin package.

## Steps

1. Install the plugin into the environment that provides the Firecube CLI:

   ```bash
   uv run firecube plugins install "PLUGIN_PACKAGE"
   ```

2. List installed plugins and inspect the plugin's options:

   ```bash
   uv run firecube plugins list
   ```

   The output contains the plugin name used by other Firecube commands. Replace
   `PLUGIN_NAME` below with that value:

   ```bash
   uv run firecube plugins describe PLUGIN_NAME
   uv run firecube ingest PLUGIN_NAME --show-options
   ```

Read the plugin package documentation for its input data, output format, and
product-specific options.

## Verify

`plugins list` should contain the installed plugin name. `plugins describe`
should then print its package, product, description, and option sections without
an import error.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Plugin is missing from `plugins list` | The package is installed in a different environment. | Install it with the same `uv` environment used to run Firecube. |
| Plugin import fails | The plugin and Firecube versions are incompatible or a plugin dependency is missing. | Check the plugin's declared Firecube version and reinstall its dependencies. |
| Plugin has no expected options | The wrong plugin name was inspected. | Use the exact name printed by `plugins list`. |

## Next Steps

- **[Run Ingestion](ingestion.md)** — ingest source data with the installed plugin
- **[Plugin Development](../guides/plugins/index.md)** — create a plugin for a new product
