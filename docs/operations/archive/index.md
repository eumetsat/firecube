# Archive Operations

Use archive operations to turn a finished Zarr product into a portable
Tensogram `.tgm` or `.zip` file. Both formats can be inspected, validated,
and restored back to Zarr.

Archive operations are separate from normal product ingestion. An archive
is a portable artifact; the restored Zarr product gets its own ChunkManager
state.

## Prerequisites

For `.tgm` archives, install Firecube with the tensogram extra. `.zip`
archives need no extra:

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
| `firecube archive restore` | Restore `.tgm` or `.zip` back to Zarr. |

`.zip` archives must be local files and restore to local targets only.

## Next Steps

- **[Create Archives](create.md)** — create a `.tgm` or `.zip` archive
- **[Inspect And Validate Archives](inspect.md)** — inspect metadata or verify archive integrity
- **[Restore Archives](restore.md)** — restore a `.tgm` or `.zip` file to Zarr
- **[Back Up A Product With Incremental Backups](incremental-backups.md)** — keep the chunk bytes of each release in a chain of `.zip` archives
- **[Read A Release With Xarray](read-a-release-with-xarray.md)** — open the data of a release from its `.zip` archive
- **[Tensogram Output Format](../../concepts/output-formats/archive.md)** — format overview
