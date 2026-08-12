# Customize Source Discovery

## Goal

Control which source items a plugin ingests: widen the built-in file
selection with patterns, exclude files the defaults would pick up, or replace
file discovery entirely for sources that are not file trees.

Every ingestor inherits discovery from `BaseIngestor`, so the behavior and
options here apply to all templates and to custom pipeline plugins.

## Use Built-In Discovery

By default a plugin discovers files below `--input-data` (`ctx.source`),
which may be a local path or a remote URI reached through the run's storage
configuration. The default selection is:

- files ending in `.zip`, `.h5`, or `.nc`
- extensionless files whose content looks like HDF5 (local sources only)

Discovery is recursive, and results are sorted so batching is deterministic.
The run logs the count it found:

```text
"message":"Found 4 files"
```

## Add Filename Patterns

`include_patterns` is an engine option, so it needs no plugin code. Patterns
**add** to the built-in selection — they do not replace it:

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --option 'include_patterns=["*.nc4"]'
```

Set it per plugin in `~/.config/firecube/config.toml` instead:

```toml
[plugins.my_plugin]
include_patterns = ["*.nc4", "level2/*.grib"]
```

Patterns match the file's base name, its path relative to `--input-data`, and
its full path or URI, so both `"*.nc4"` and `"level2/*.nc4"` work.

## Exclude Files

There is no exclusion option; excluding requires overriding the hook and
calling the discovery helper with `exclude`. Patterns passed to `exclude` are
applied before selection runs, so an excluded path is never picked up by the
default suffixes or by `include_patterns`:

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

## Discover Without An Input Path

Overriding the hook removes the `--input-data` requirement, which is how a
plugin ingests from a catalog, an API, or a fixed set of URIs:

```python
def discover_source_files(self, ctx):
    return [
        f"s3://archive/{product}/{day}.nc"
        for day in self.plugin_config.days
    ]
```

Return path or URI strings unless the plugin's own hooks handle richer item
objects end to end. Discovered items pass through `filter_item` and batching,
then reach `build_dataset` (or `build_write_intents`), where
`ctx.materialize(item)` resolves each one to a local path — see
[Read Plugin Source Data](storage-access.md).

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

- **[Read Plugin Source Data](storage-access.md)** — materialize discovered items in a hook
- **[NetCDF To Zarr: Source Discovery](../../tutorials/source-discovery.md)** — a runnable end-to-end example
- **[Hooks & Lifecycle](../../reference/hooks.md#firecube.ingestor.api.BaseIngestor.discover_source_files)** — the hook contract
- **[Core Utilities](../../reference/core-utilities.md#firecube.core.api.discover_input_files)** — all `discover_input_files` parameters
