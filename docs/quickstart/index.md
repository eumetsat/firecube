# Quickstart Overview

This quickstart walks you through installing Firecube, creating a small dataset
plugin, generating NetCDF source files, and writing one Zarr product to local
storage.

Firecube provides the ingestion engine. A plugin provides the dataset-specific
code that discovers, reads, and converts source files.

## Before You Start

You need:

- `Python 3.12+` and `uv` installed

The installation step creates a working directory with a `.venv` and installs
Firecube from PyPI. Keep the virtual environment active and run the remaining
commands from that directory.

## Quickstart Path

1. **[Installation](installation.md)**: Create `.venv`, install Firecube from
   PyPI, and verify the CLI.
2. **[Create and Install the Example Plugin](plugins.md)**: Create the local
   `weather_netcdf` plugin, implement its dataset conversion, and install it.
3. **[Prepare Source Data](source-data.md)**: Create four deterministic NetCDF
   files for the example ingestion.
4. **[Run Ingestion](ingestion.md)**: Write one local Zarr product and inspect
   its ChunkManager record.

## Next Steps

- **[Installation](installation.md)**: create the environment, activate it, and
  verify the Firecube CLI.
