# Discover Source Data

## Goal

Know which files Firecube finds under `--input-data` before writing any
plugin code, how each one reaches the plugin, and how to widen or narrow that
selection with filters.

## Use Built-In Discovery

By default a plugin discovers files below `--input-data` (`ctx.source`), which
may be a local path, a `file://` URI, or an `s3://` prefix reached through the
run's storage configuration. The default selection is:

- files ending in `.zip`, `.h5`, `.nc`, `.nc4`, `.hdf`, or `.he5`, in any letter case
- extensionless files whose content looks like HDF5 (local sources only)

Discovery is recursive, and results are sorted by file name so batching is
deterministic. The run logs the count it found:

```text
"message":"Found 4 files"
```

Discovery only finds the files. Reading them is your plugin's `read_dataset`,
which receives each file as a local path:

```python
def read_dataset(path: Path) -> xr.Dataset:
    with xr.open_dataset(path) as dataset:
        return dataset.load()
```

See [Append Datasets To Zarr](generic-zarr.md) for where this goes and
[Discover Zipped Data](discover-zipped-data.md) for archives.

## Include And Exclude Files

Use `--input-filters` to add file types and exclude unwanted inputs. Replace
`my_plugin`, the product name, and paths with your own; the plugin must have a
reader for each included format. This command also writes the output product:

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct \
  --input-filters '["*.csv","!*_quicklook.nc","!incoming/*"]'
```

This adds CSV files alongside the built-in formats, excludes `_quicklook.nc`
files, and excludes files under `incoming/`, including its subdirectories.

- Positive patterns add to built-in discovery; they do not restrict it to that
  format. To add CSV files without exclusions, use `--input-filters '["*.csv"]'`.
- Exclusions win regardless of order. `["!*", "*.csv"]` excludes everything.
- Patterns match the file name, relative path, or full path/URI. Matching is
  case-sensitive: `!*.nc` does not exclude `file.NC`.

Keep the JSON list inside single quotes so the shell leaves it intact.

## Save Filters For Later Runs

Add filters to your [configuration file](../../concepts/configuration.md):

```toml
[plugins.my_plugin]
input_filters = ["*.csv", "!*_quicklook.nc", "!incoming/*"]
```

A CLI list replaces the saved list for that run. Use `--input-filters '[]'` to
clear saved filters and return to built-in discovery. The same flag works with
`firecube zarr slots` and `firecube zarr preallocate`.

## Verify

Run the plugin and check the log line discovery emits, before anything else
in the pipeline runs:

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct 2>&1 | grep "Found"
```

Expected output:

```text
"message":"Found 4 files"
```

If nothing under `--input-data` matched, there is no `Found` line; the run
stops with `No source files found after applying input filters`. Check the input path and filters.

 When an empty delivery is expected, pass `--option allow_empty_source=true` for a successful no-op.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Expecting `--input-filters '["*.csv"]'` to restrict discovery to CSV files | Positive patterns add to built-in discovery. Add exclusions such as `"!*.nc"` to narrow the set. |
| Passing the JSON list without quotes | The shell splits it into separate arguments. Wrap the list in single quotes. |
| Expecting `!*.nc` to exclude `file.NC` | Matching is case-sensitive. Add a pattern for each spelling. |
| Overriding `discover_source_files` and expecting filters to still apply | A custom hook replaces built-in discovery. Pass the filters to `discover_input_files` yourself. |

## Next Steps

- **[Read Paired Source Files](paired-source-files.md)** — use a custom discovery
  hook to keep measurements and metadata together.
- **[Discovery Hook](../../reference/hooks.md#firecube.ingestor.api.BaseIngestor.discover_source_files)**
  — override discovery for catalogs or custom ordering. If file names do not
  sort chronologically, order items by timestamp before batching for appends.
- **[Discovery Helper](../../reference/core-utilities.md#firecube.core.api.discover_input_files)**
  — apply filters in a custom hook; replacing built-in discovery does not
  apply them automatically.
