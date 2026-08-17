# Discover Source Data

## Goal

Know which files Firecube finds under `--input-data` before writing any
plugin code, and how each one reaches the plugin.

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

Every template and custom pipeline plugin gets this from `BaseIngestor`. To
widen, narrow, or replace it, see
[Customize Source Discovery](customize-source-discovery.md).

## Set The Source

`--input-data` accepts a local path, `file://`, or an `s3://` prefix. The
plugin interprets its contents. The output target remains a strict product
URI.

## Verify

Run the plugin and check the log line discovery emits, before anything else
in the pipeline runs:

```bash
firecube ingest my_plugin --input-data /data/raw --target "file://$PWD/out.zarr" 2>&1 | grep "Found"
```

Expected output:

```text
"message":"Found 4 files"
```

A count of zero means nothing under `--input-data` matched the default
selection — see [Customize Source Discovery](customize-source-discovery.md).

## Common Mistakes

| Mistake | Fix |
|---|---|
| Omitting `--input-data` with default discovery | Pass the flag, or [customize discovery](customize-source-discovery.md) if the plugin has no input path. |
| Expecting an unsupported file type to be found | Add a pattern, don't rename files — see [Customize Source Discovery](customize-source-discovery.md). |

## Next Steps

- **[Customize Source Discovery](customize-source-discovery.md)** — control which items reach the plugin and how they're grouped
- **[Product Storage](../../concepts/storage.md)** — understand source and target storage roles
- **[Storage Drivers](../../reference/storage-drivers.md)** — inspect driver values and capabilities
- **[Context & Results](../../reference/context.md)** — look up `PluginContext`
