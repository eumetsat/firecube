# Plugin Storage Access

## Goal

Plugins write output through `ctx.storage.output`, access an engine-managed
scratch directory via `ctx.temp_root`, and pull remote sources into a local path
via `ctx.materialize(source)`. The engine owns all storage decisions: which
backend, which credentials, and whether writes are staged or direct. Plugins
never construct filesystems or parse URIs.

## Minimal Example

```python
from typing import ClassVar

from firecube.ingestor.api import GenericZarrIngestor, PluginContext, StorageContext, register_ingestor


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def build_dataset(self, group: str, items: list[object], ctx: PluginContext):
        # ctx.storage is a StorageContext; ctx.storage.output is the active session.
        output_uri = ctx.storage.output.uri

        # Pull a remote source file to a local path before reading it.
        local_path = ctx.materialize(items[0])

        # Use ctx.temp_root for any scratch files your plugin needs.
        scratch = ctx.temp_root / "work" if ctx.temp_root else None

        # Write your data using the output session, not a raw filesystem handle.
        store = ctx.storage.output.open_zarr_store(group)
        ...
```

## Required API

All members are on `PluginContext`, importable from `firecube.ingestor.api`.
`StorageContext` is also exported from `firecube.ingestor.api` for type
annotations. See [Public API Reference](../../reference/api.md) for full
generated docs.

| API | Type | Purpose |
|-----|------|---------|
| `ctx.storage` | `StorageContext \| None` | Container for storage role bindings |
| `ctx.storage.output` | `StorageSession \| None` | Active output session; use `.uri` and `.open_zarr_store()` |
| `ctx.temp_root` | `Path \| None` | Engine-managed scratch directory for this run |
| `ctx.materialize(source)` | `-> Path` | Downloads or resolves a source item to a local path |

`ctx.storage` is a `StorageContext` with one public field: `output`. There is no
`.filesystem` field and no `.staging_paths`. If `ctx.storage` or
`ctx.storage.output` is `None`, the engine did not bind storage for this run
(e.g. a dry run).

## Configuration

Storage type, driver, and write mode are all operator-supplied CLI flags. Plugins
don't set these. The engine reads them and wires up `ctx.storage.output`
accordingly before calling your hook.

```bash
uv run firecube ingest my_plugin \
  --source /data/raw \
  --target s3://my-bucket/my_product.zarr \
  --product-name my_product \
  --storage-type s3 \
  --storage-driver fsspec \
  --write-mode staged
```

For local development, swap `--storage-type local` and `--write-mode direct`.
The plugin code stays the same either way.

## Test It

Run a dry-run pass to confirm the engine resolves storage without writing:

```bash
uv run firecube ingest my_plugin \
  --source /data/raw \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct \
  --option dry_run=true
```

Check that `ctx.storage.output` is not `None` in your hook by adding a guard:

```python
if ctx.storage is None or ctx.storage.output is None:
    raise RuntimeError("No output storage bound. Check CLI flags.")
```

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Calling `fsspec.filesystem(...)` directly | Use `ctx.storage.output` — the engine binds the right backend |
| Using `ctx.workspace` | Use `ctx.temp_root` (the correct public name) |
| Choosing write mode inside the plugin | Operator sets `--write-mode`; plugin code is the same for staged and direct |
| Parsing URI scheme (`s3://` vs `file://`) | Don't. The engine resolves backend from explicit `--storage-type` and `--storage-driver` flags |
| Building product URIs by string concatenation | Use `ctx.storage.output.uri` — the engine constructs the correct target path |
| Importing `StorageSession` from `firecube.ingestor.api` | `StorageSession` is not exported; access it through `ctx.storage.output` by reference |

## Next Steps

- **[Product Storage](../storage.md)** — storage types, drivers, and write modes
- **[Storage Driver Compatibility](../../reference/storage-drivers.md)** — what each driver supports
- **[Run Ingestion](../../quickstart/ingestion.md)** — worked examples for local and S3
- **[Public API Reference](../../reference/api.md)** — `PluginContext`, `StorageContext`
- **[CLI And Config](cli-and-config.md)** — all required CLI flags
