# Quickstart Overview

This quickstart walks you through installing Firecube, installing the example
`firecube-quickstart-plugin`, generating NetCDF source files, and writing one
Zarr product to local storage.

Firecube provides the ingestion engine. A plugin provides the dataset-specific
code that discovers, reads, and converts source files.

## Before You Start

You need:

- `Python 3.12+`, `uv`, and `git` installed

The installation step creates a working directory with a `.venv` and installs
Firecube from PyPI. Activate `.venv` and keep that environment active for
the remaining commands.

## Quickstart Path

1. **[Installation](installation.md)**: Create `.venv`, install Firecube from
   PyPI, and verify the CLI.
2. **[Install the Example Plugin](plugins.md)**: Clone and install the
   `firecube-quickstart-plugin` example.
3. **[Prepare Source Data](source-data.md)**: Generate four deterministic
   NetCDF files for the example ingestion.
4. **[Run Ingestion](ingestion.md)**: Write one local Zarr product and open it
   with `xarray`.

## Next Steps

- **[Installation](installation.md)**: create and activate the environment, install Firecube, and
  verify the Firecube CLI.
