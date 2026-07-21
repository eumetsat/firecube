# Plugin Setup

Firecube provides the ingestion engine. Dataset-specific logic lives in plugins, so either
install an existing plugin or create one before running ingestion.

## Option 1: Install An Existing Plugin

Install an external plugin package using the package name from that plugin's
documentation:

```bash
uv run firecube plugins install firecube-my-plugin
```

Or install a local plugin checkout in editable mode:

```bash
uv run firecube plugins install --editable /path/to/firecube-my-plugin
```

## Option 2: Create A Plugin

If no plugin exists for your data, start with the interactive scaffold:

```bash
uv run firecube plugins create my-plugin
```

For a first datacube plugin, choose `zarr` when the wizard asks for the
template. Accept the default `xarray` write strategy unless you already need
direct Zarr region writes. Choose `parquet` instead for row-based outputs.

Expected interaction:

```text
Creating a new Firecube plugin.
Plugin Name [my-plugin]:
Author Name [Firecube Developer]:
Author Email [dev@example.com]:
License [MIT]:
Template (base, zarr, parquet) [base]: zarr
Zarr write strategy (xarray, zarr-python) [xarray]:

Created plugin project: /path/to/firecube-my-plugin

To install for development:
  cd /path/to/firecube-my-plugin
  uv sync
```

The scaffold creates a small plugin project:

```text
firecube-my-plugin/
  README.md
  pyproject.toml
  src/firecube_my_plugin/ingestor.py
  tests/test_ingestor.py
```

Use non-interactive mode only for scripts or CI:

```bash
export FIRECUBE_PLUGIN_DIR="${PWD}/plugins-dev"
uv run firecube plugins create my-plugin \
  --target-dir "$FIRECUBE_PLUGIN_DIR" \
  --template zarr \
  --non-interactive
```

Then check out **[Create a Plugin](../concepts/plugins/create-a-plugin.md)** for the
plugin options, or follow the **[Weather CSV walkthrough](../tutorials/weather-csv.md)**
for a complete working example.

## List Active Plugins

```bash
uv run firecube plugins list
```

## Inspect A Plugin

To see metadata, the logical product name, and available options for a plugin:

```bash
uv run firecube plugins describe <plugin>
```

Output highlights:

- **Module**: Python path to the ingestor.
- **Product**: Logical output name supported by the plugin.
- **Options Sections**: Available `--option` keys grouped by section (e.g. `ENGINE`), each with its type and built-in default.

## Detailed Options

To see all CLI flags and options supported during ingestion:

```bash
uv run firecube ingest <plugin> --show-options
```

## Next Steps

- **[Run Ingestion](ingestion.md)** — run the installed or generated plugin
