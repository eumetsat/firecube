# Customize Source Discovery

## Goal

Control which source items a plugin ingests: widen the built-in file
selection with patterns, exclude files the defaults would pick up, or replace
file discovery entirely for sources that are not file trees.

## Add Filename Patterns

`include_patterns` is an engine option, so it needs no plugin code. Patterns
**add** to the built-in selection — they do not replace it:

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --option 'include_patterns=["*.nc4"]'
```

Patterns match the file's base name, its path relative to `--input-data`, and
its full path or URI, so both `"*.nc4"` and `"level2/*.nc4"` work.

## Exclude Files

No CLI option excludes files. Override
[`discover_source_files`](../../reference/hooks.md#firecube.ingestor.api.BaseIngestor.discover_source_files)
and pass `exclude` to the `discover_input_files` helper it calls. Excluded
patterns apply before selection, so they win over the default suffixes and
over `include_patterns`:

```python
from firecube.core.api import discover_input_files
from firecube.ingestor.api import GenericZarrIngestor, register_ingestor


@register_ingestor("my_plugin")
class MyPluginIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "my_product"

    def discover_source_files(self, ctx):
        return discover_input_files(
            ctx.source,
            exclude=["*_quicklook.nc", "incoming/*"],
        )
```

The same call accepts `include_suffixes` to change the accepted extensions,
`recursive=False` to read only the top directory, and `preferred_globs` for
pattern additions in code.

For an `s3://` source that needs explicit endpoint or credential settings,
pass a [`StorageConfig`](../../reference/config.md#storageconfig) as
`storage_config`; local sources ignore the argument.


## Verify

Run the plugin and compare the discovered count against the files you expect:

```bash
firecube ingest my_plugin --input-data /data/raw --target "file://$PWD/out.zarr" 2>&1 | grep "Found"
```

Expected output:

```text
"message":"Found 4 files"
```

A count of zero logs a warning and processes nothing; that is a discovery
problem, not an ingestion failure.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Expecting `include_patterns` to restrict discovery to those patterns | Patterns only add files. Override the hook and pass `exclude` or `include_suffixes` to narrow the set. |
| Omitting `--input-data` with default discovery | Pass the flag, or override `discover_source_files` if the plugin has no input path. |
| Returning item objects a reader cannot resolve | Return path or URI strings, or handle the item type in every hook that consumes it. |
| Building an `fsspec` or S3 client to list remote sources | Pass the run's storage configuration to `discover_input_files`. |

## Next Steps

Discovery and grouping are set; implement the template that converts what
they find:

- **[`GenericZarrIngestor` (Append)](generic-zarr.md)** — implement the ordered `xarray.Dataset` contract
- **[`GenericParquetIngestor` (Tabular)](generic-parquet.md)** — implement the table or data-frame contract
- **[`DirectZarrIngestor` (Region)](direct-zarr.md)** — implement the schema and explicit write-intent contract
- **[NetCDF To Zarr: Source Discovery](../../tutorials/source-discovery.md)** — a runnable end-to-end example
- **[Hooks & Lifecycle](../../reference/hooks.md#firecube.ingestor.api.BaseIngestor.discover_source_files)** — the hook contract
- **[Core Utilities](../../reference/core-utilities.md#firecube.core.api.discover_input_files)** — all `discover_input_files` parameters
