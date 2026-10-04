# Archive Operations

Use archive operations to turn a finished Zarr product into a portable
Tensogram `.tgm` or `.zip` file. A `.tgm` archive can also be inspected,
validated, and restored back to Zarr.

Archive operations are separate from normal product ingestion. A `.tgm` archive
is a portable artifact; the restored Zarr product gets its own ChunkManager
state.

## Prerequisites

Install Firecube with the tensogram extra:

```bash
uv pip install 'firecube[tensogram]'
```

Use the storage flags required by the archive command:

```bash
--storage-type local
--storage-driver fsspec
```

For S3-backed products, use `--storage-type s3` and configure credentials as
described in [Configuration Reference](../../reference/config.md).

## Command Groups

| Command | Use it for |
|---|---|
| `firecube archive create` | Convert Zarr to a `.tgm` or `.zip` file. |
| `firecube archive info` | Show archive metadata. |
| `firecube archive list` | List archived groups and variables. |
| `firecube archive validate` | Check archive integrity. |
| `firecube archive restore` | Restore `.tgm` back to Zarr. |

`.zip` archives are currently supported by `firecube archive create` only.

## Next Steps

- **[Create Archives](create.md)** — create a `.tgm` or `.zip` archive
- **[Inspect And Validate Archives](inspect.md)** — inspect metadata or verify archive integrity
- **[Restore Archives](restore.md)** — restore a `.tgm` file to Zarr
- **[Tensogram Output Format](../../concepts/output-formats/archive.md)** — format overview
