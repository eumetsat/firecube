# Install Your Plugin

## Goal

Install the created package into its development environment so the Firecube
CLI can discover it. An editable installation makes later Python code changes
available without reinstalling the package.

## Prerequisites

- A project created with [Create a Plugin](create-a-plugin.md).
- The Python environment used to run Firecube.

## Install For Development

From the created project directory:

```bash
cd firecube-my-plugin
uv sync
uv run firecube plugins install --editable .
```

The install command reports the Python environment it uses, installs the local
package, and checks plugin discovery in a fresh Python process. The detected
plugin list should contain `my_plugin`.

## Verify The Installation

Check the registered plugin ID, product name, and available options:

```bash
uv run firecube plugins list
uv run firecube plugins describe my_plugin
uv run firecube ingest my_plugin --show-options
```

Use the ID printed by `plugins list` in later `firecube ingest` commands. The
generated data-conversion methods can still be incomplete at this point;
inspection does not require running ingestion.

The plugin should appear in `plugins list`. `plugins describe` should show its
registered ID and product name, and `--show-options` should print the available
configuration without an import error.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `my_plugin` is missing from `plugins list` | Run the install command from the directory containing `pyproject.toml`. |
| Firecube imports another copy of the plugin | Check the environment path printed by `plugins install` and run all commands through the same `uv run` environment. |
| The package imports but registration is missing | Confirm the `firecube.plugins` entry point in `pyproject.toml` targets a module that imports the registered ingestor class. |

## Next Steps

Before implementing the template hooks, know what the plugin will actually
receive:

- **[Discover Source Data](source-discovery.md)** — know what discovery finds
  and how items reach the plugin
- **[Customize Source Discovery](customize-source-discovery.md)** — control
  which items discovery finds and how they're grouped

Then implement the template selected during creation:

- **[`GenericZarrIngestor` (Append)](generic-zarr.md)** — implement the ordered
  `xarray.Dataset` contract
- **[`GenericParquetIngestor` (Tabular)](generic-parquet.md)** — implement the
  table or data-frame contract
- **[`DirectZarrIngestor` (Region)](direct-zarr.md)** — implement the schema and
  explicit write-intent contract
- **[Custom Pipeline Plugins](base-ingestor.md)** — implement a fully custom
  pipeline when no template fits
