# {start_case_name} Ingestor Plugin

> ⚠️ **This scaffold is incomplete.** You must implement `{hook_summary}` before this plugin will produce output. Until then, ingestion runs will raise `NotImplementedError`.

## What this plugin does

<!-- TODO: write a one-line summary of what {start_case_name} ingests and emits. -->

## Development

```bash
uv sync
uv run pytest
```

## Install into firecube

```bash
uv run firecube plugins install --editable .
uv run firecube plugins describe {plugin_name}
```

`firecube plugins describe` should list `[ENGINE]` options. If you add plugin-specific options via a `PluginConfig` subclass, they will appear under `[PLUGIN]`.

## After you implement the hook

Once `{hook_summary}` is implemented, run:

```bash
uv run firecube ingest {plugin_name} \
  --input-data /path/to/your/input \
  --target file:///tmp/{plugin_name}_out.{output_extension} \
  --product-name {plugin_name} \
  --storage-type local \
  --storage-driver fsspec \
  --output-format {output_format} \
  --write-mode {write_mode_default}
```

For S3 targets, swap `--storage-type local` for `--storage-type s3` and use an `s3://bucket/key.{output_extension}` URI.
