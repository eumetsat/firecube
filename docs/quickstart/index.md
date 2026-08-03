# Quickstart Overview

This quickstart walks you through installing Firecube, adding an existing
product plugin, and writing your first product to local storage. An optional S3
path uses the same plugin and source data. To build a plugin for a new product,
start with [Plugin Development](../guides/plugins/index.md).

For a Zarr or Parquet product, you can finish by creating an Intake catalog and
opening one of its discovered sources.

## Before You Start

You need:

- `Python 3.12+` and `uv` installed
- The package name of an existing Firecube plugin
- Source data supported by that plugin
- S3 credentials if you want to run the optional S3 path

The installation step creates a working directory with a `.venv` and installs
Firecube from PyPI. Run the remaining `uv run ...` commands from that directory.

## Quickstart Path

1. **[Installation](installation.md)**: Create `.venv`, install Firecube from
   PyPI, and verify the CLI.
2. **[Install a Plugin](plugins.md)**: Install an existing product plugin and
   confirm that Firecube discovers it.
3. **[Run Ingestion](ingestion.md)**: Write a local product, inspect its
   ChunkManager record, and optionally repeat the run against S3.
4. **[Create an Intake Catalog](catalogs.md)**: Generate and open a catalog for
   an ingested Zarr or Parquet product.

Use **[Configure S3 Access](configuration.md)** before running the optional S3
path.

## Next Steps

- **[Installation](installation.md)** — install Firecube and verify the CLI
