# Create a Plugin

## Goal

Create a Python package containing the Firecube registration metadata and the
methods required by the selected plugin template.

## Prerequisites

- Firecube installed in an activated Python environment (`source .venv/bin/activate`).
- A directory in which to create the plugin project.
- A product contract selected from the
  [Plugin Development Overview](index.md).

## Run The Interactive Command

Run the command in the directory where the new plugin project should be
created:

```bash
firecube plugins create my-plugin
```

The wizard asks for the plugin name, author, email, license, and template. At
the template prompts, enter the values that match the product:

| Product contract | Authoring class | Wizard selections |
|---|---|---|
| Complete, ordered multidimensional datasets with serialized same-group appends | `GenericZarrIngestor` (Append) | `Template`: `zarr`; `Zarr write strategy`: `xarray` |
| Tables or data frames | `GenericParquetIngestor` (Tabular) | `Template`: `parquet` |
| Known Zarr indexes or disjoint slot workers writing one group | `DirectZarrIngestor` (Region) | `Template`: `zarr`; `Zarr write strategy`: `zarr-python` |
| A product no template represents | Custom pipeline, advanced | `Template`: `base` |

The `Template` prompt defaults to `zarr`, and the write strategy defaults to
`xarray`, so accepting both defaults creates a `GenericZarrIngestor` plugin:

```text
Template (zarr, parquet, base) [zarr]:
Zarr write strategy (xarray, zarr-python) [xarray]:
```

The plugin name must start with a letter and use only letters, digits, `-`,
and `_`. The command derives every other name from it: `my-plugin` becomes the
plugin ID `my_plugin`, the class `MyPluginIngestor`, and the project
`firecube-my-plugin/`.

The command creates `firecube-my-plugin/`. Its generated `ingestor.py` is a
complete plugin for the selected template with one reader function left
unimplemented; every hook is already wired to it, and it raises
`NotImplementedError` until product behavior is added. The generated README
owns the implementation, test, installation, inspection, and first-run steps
for that scaffold.

## Create A Plugin Non-Interactively

Use explicit options in automation:

```bash
# GenericZarrIngestor
firecube plugins create my-plugin \
  --template zarr \
  --non-interactive

# GenericParquetIngestor
firecube plugins create my-plugin \
  --template parquet \
  --non-interactive

# DirectZarrIngestor
firecube plugins create my-plugin \
  --template zarr \
  --write-strategy zarr-python \
  --non-interactive
```

Add `--author`, `--email`, `--license`, and `--target-dir` when the generated
metadata or location must be supplied by the calling environment. `--license`
takes an SPDX license identifier or expression such as `MIT`, `Apache-2.0`, or
`GPL-3.0-or-later`; any other value is recorded as `LicenseRef-<value>`, with
characters outside the SPDX alphabet replaced by `-`. The same value goes into
`pyproject.toml` and the generated source headers. `--write-strategy` applies
only to `--template zarr`.

## Verify The Created Project

The project should contain `pyproject.toml`, `README.md`, `tests/`, and the
`src/firecube_my_plugin/` package.

Open `src/firecube_my_plugin/ingestor.py` and confirm that the class declares
`PRODUCT_NAME` and uses the selected template. Zarr templates also define a
`ZarrStorageConfig` class whose commented settings show where to set chunking,
compression, and sharding. Then follow the generated
README from implementation through the first local ingestion.

## Troubleshooting

| Symptom | Fix |
|---|---|
| The command creates the project in an unexpected location | Run it from the intended parent directory or pass `--target-dir`. |
| The generated class uses `BaseIngestor` | Run the command again and explicitly select the template that matches the product contract. |
| The project directory already exists | Choose another plugin name or remove the unused directory before retrying. |
| `Invalid plugin name` | Start the name with a letter and use only letters, digits, `-`, and `_`. |

## Next Steps

- **[Install Your Plugin](install-a-plugin.md)**: install the created package
  so Firecube can discover it
- **[Plugin Development Overview](index.md)**: reconsider the authoring class
  before implementing product behavior
