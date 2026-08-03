# Read Plugin Source Data

## Goal

Resolve discovered source items for product readers without constructing a
second storage client inside the plugin.

## Use Source Materialization in a Hook

```python
def build_dataset(self, group, items, ctx):
    local_paths = [ctx.materialize(item) for item in items]
    return parse_product_files(local_paths)
```

For a local item, `ctx.materialize()` returns a resolved local path. For a
remote item, Firecube downloads it into the run workspace and returns the
cached path.

`parse_product_files` represents the product reader already used by the plugin.

Use `ctx.temp_root` only for temporary work. Cleanup may remove the directory
after the run. `ctx.source` is the original value passed through
`--input-data`; prefer discovered items in the hook unless the reader needs the
source root itself.

Template hooks return datasets, tables, or write intents. They do not open the
output store directly; the selected template performs output I/O.

See the [Plugin Template API](../../reference/api.md#plugin-context) for the
complete public context surface.

## Set The Source

`--input-data` accepts a local path or an `s3://` prefix. The plugin interprets
its contents. The output target remains a strict product URI.

## Verify

Run the plugin once with a small local input file and once with the same data
through the configured remote backend. Compare parsed product values,
not only whether materialization completed.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Passing a remote URI to a local-only parser | Call `ctx.materialize(item)` first. |
| Creating an `fsspec`, boto, or S3 client in the hook | Use the runtime-provided source materialization path. |
| Writing output from a template hook | Return the value required by the template. |
| Keeping a scratch path after the run | Treat `ctx.temp_root` as run-scoped. |

## Next Steps

- **[Product Storage](../../concepts/storage.md)** — understand source and target storage roles
- **[Storage Drivers](../../reference/storage-drivers.md)** — inspect driver values and capabilities
- **[Plugin Template API](../../reference/api.md)** — look up `PluginContext`
