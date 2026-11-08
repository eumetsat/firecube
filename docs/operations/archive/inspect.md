# Inspect And Validate Archives

Use archive inspection commands before transferring, publishing, or restoring a
`.tgm` or `.zip` file.

```bash
ARCHIVE_URI="file:///data/archives/MY_PRODUCT.tgm"
```

## Show Metadata

```bash
firecube archive info --archive "$ARCHIVE_URI"
```

Expected output resembles:

```text
Path:           /data/archives/MY_PRODUCT.tgm
Format:         v1 (multi-group)
Size:           0.00 MB
Groups:         data
Control-plane:  present

Group: data
  Source:    file:///data/products/MY_PRODUCT.zarr
  Codec:     zstd
  Variables:
    data: shape=[1000, 10] dtype=float32 chunks=[100, 10]
```

Use JSON for scripts:

```bash
firecube archive info --archive "$ARCHIVE_URI" --format json
```

`archive info` offers the formats `table` and `json`. Expected output resembles:

```json
{
  "path": "/data/archives/MY_PRODUCT.tgm",
  "format": "v1",
  "size_bytes": 34048,
  "groups": [
    "data"
  ],
  "has_controlplane": true,
  "group_details": [
    {
      "group": "data",
      "variables": {
        "data": {
          "shape": [1000, 10],
          "dtype": "float32",
          "dims": ["timestamp", "x"],
          "zarr_chunks": [100, 10]
        }
      },
      "compression": "zstd",
      "source_uri": "file:///data/products/MY_PRODUCT.zarr",
      "archived_at": "2026-11-07T12:10:53.536335+00:00"
    }
  ]
}
```

## List Contents

```bash
firecube archive list --archive "$ARCHIVE_URI"
```

Expected output resembles:

```text
Messages: 2

Group: data
  Source:  file:///data/products/MY_PRODUCT.zarr
  Codec:   zstd
  Objects: 1
    [0] data: shape=[1000, 10] dtype=float32 compression=zstd

[control-plane]
  Product: product.zarr
```

Add `-f json` to print the contents as JSON. The output starts with the keys
`format`, `message_count` and `messages`.

## Root Group Label

If the archive holds the root group of the source product, the commands label
that group `(root)`. `archive info` and `archive list` print `Group: (root)`.
`archive info` prints `(root)` in the `Groups:` line. `archive create` prints it
in its own `Groups:` line.

```text
Groups:         (root)

Group: (root)
  Source:    file:///data/products/MY_PRODUCT.zarr
```

## Validate Integrity

```bash
firecube archive validate --archive "$ARCHIVE_URI"
```

Expected output:

```text
VALID: /data/archives/MY_PRODUCT.tgm (2 message(s), no issues)
```

Use `--quick` for a structure-only check:

```bash
firecube archive validate --archive "$ARCHIVE_URI" --quick
```

Expected output:

```text
VALID: /data/archives/MY_PRODUCT.tgm (2 message(s), no issues)
```

Add `-f json` to print the result as JSON:

```json
{
  "path": "/data/archives/MY_PRODUCT.tgm",
  "valid": true,
  "issues": [],
  "message_count": 2
}
```

A `.tgm` archive with no messages is invalid. The command exits with code 1:

```text
INVALID: /data/archives/EMPTY.tgm (1 issue(s))
  [file] offset=0: Archive contains no messages.
```

## Zip Archives

`info`, `list`, and `validate` accept `.zip` archives with the same flags:

```bash
firecube archive info --archive "file:///data/archives/MY_PRODUCT.zarr.zip"
```

Expected output resembles:

```text
Path:           /data/archives/MY_PRODUCT.zarr.zip
Format:         zip
Size:           0.01 MB
Entries:        18
Groups:         (root), data
Control-plane:  present

Group: data
  Variables:
    temperature: shape=[4, 2, 3] dtype=float64 chunks=[4, 2, 3] codec=zstd
```

`archive list` prints each group as an xarray dataset. `archive validate`
checks the CRC of every entry and that the archive opens as Zarr; `--quick`
skips the CRC check.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `Path ... does not exist` | The archive path is wrong or create failed. | Check `ARCHIVE_PATH` and rerun `archive create`. |
| `requires the tensogram extras` | `.tgm` support is not installed. | Install `firecube[tensogram]` in the active environment. |
| `INVALID: ...` with `Archive contains no messages.` | The `.tgm` archive holds no messages. `archive validate` exits with code 1. | Recreate the archive from the source product. |
| `Error: Archive contains no messages.` | `archive info` reads a `.tgm` archive with no messages and exits with code 1. | Recreate the archive from the source product. |
| Validation fails | The archive is incomplete or corrupt. | Recreate the archive from the source product. |

## Next Steps

- **[Create Archives](create.md)** — rebuild or replace an archive
- **[Restore Archives](restore.md)** — restore a validated archive to a Zarr product
