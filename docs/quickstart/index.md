# Quickstart Overview

This quickstart walks you through setting up Firecube, creating a Python environment, installing or creating an ingestion plugin, and running your first local and S3 ingestion.

By the end, you will have an Intake catalog entry you can use to discover and work with your data.

## Before You Start

You need:

- `Python 3.12+` and `uv` installed.
- Access to an existing Firecube plugin, or a plan to create one
- Example input data for that plugin
- S3 credentials if you want to run the S3 example (optional)

The installation step creates a project `.venv`. The commands in this quickstart use `uv run ...` from the repository root.

## Quickstart Path

1. **[Installation](installation.md)**: Clone the repository, create `.venv`,
   install dependencies, and verify the CLI.
2. **[Plugin Setup](plugins.md)**: Install an existing plugin, or scaffold a
   new plugin and continue with plugin-author docs.
3. **[Run Ingestion](ingestion.md)**: Run the same plugin against local storage,
   then against S3 when credentials are configured.
4. **[Intake Catalogs](catalogs.md)**: Generate an analysis-ready catalog item
   for the ingested product.

Use **[Configuration](configuration.md)** when you need credentials, storage
endpoints, or plugin defaults.

## Next Steps

- **[Installation](installation.md)** — install Firecube and verify the CLI
