# Restore Archives

Use `firecube archive restore` to decode a `.tgm` archive back into a Zarr
store.

```bash
ARCHIVE_URI="file:///data/archives/MY_PRODUCT.tgm"
RESTORED_URI="file:///data/restored/MY_PRODUCT.zarr"
```

## Restore To Zarr

```bash
firecube archive restore \
  --archive "$ARCHIVE_URI" \
  --target "$RESTORED_URI"
```

Expected output resembles:

```text
restored group: data
Zarr restored: file:///data/restored/MY_PRODUCT.zarr
  Groups: data
  Variables: data
  Coordinates:
  Control-plane: restored
```

## Verify The Restored Product

Inspect the restored product's run history:

```bash
firecube chunks runs list --product-name MY_PRODUCT.zarr
```

Expected output resembles:

```text
Run ID                               Status    State   Parts Events
------------------------------------------------------------------------
archive-restore-3e67c307-...         complete  active  4     4
direct_zarr_capable_test_plugin-...  complete  active  1     7
```

Inspect restored ChunkManager records:

```bash
firecube chunks list --product-name MY_PRODUCT.zarr
```

## Overwrite An Existing Restore Target

Preview:

```bash
firecube archive restore \
  --archive "$ARCHIVE_URI" \
  --target "$RESTORED_URI" \
  --overwrite \
  --dry-run
```

Overwrite in a non-interactive shell:

```bash
firecube archive restore \
  --archive "$ARCHIVE_URI" \
  --target "$RESTORED_URI" \
  --overwrite \
  --yes-i-really-mean-it
```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| Restore target exists | Firecube will not replace it by default. | Use `--overwrite` with `--yes-i-really-mean-it`. |
| `requires the tensogram extras` | Archive support is not installed. | Install `firecube[tensogram]` in the active environment. |
| Restored product has no expected variables | The archive is missing expected content. | Run `archive info` and `archive list`. |

## Next Steps

- **[Inspect And Validate Archives](inspect.md)** — confirm archive contents before restoring
- **[ChunkManager Operations](../chunk-manager/index.md)** — inspect or operate the restored product state
