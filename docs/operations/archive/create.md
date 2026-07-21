# Create Archives

Use `firecube archive create` to convert an existing Zarr product into a
portable `.tgm` archive.

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
ARCHIVE_URI="file:///data/archives/MY_PRODUCT.tgm"
```

## Create A Full Archive

```bash
firecube archive create \
  --source "$PRODUCT_URI" \
  --archive "$ARCHIVE_URI" \
  --compression zstd
```

Expected output resembles:

```text
Archive created: /data/archives/MY_PRODUCT.tgm
  Groups: data
  Variables: data
  Size: 0.00 MB | Codec: zstd
```

## Create A Group Archive

Use `--group` when the Zarr product has multiple groups and you want one of
them:

```bash
firecube archive create \
  --source "$PRODUCT_URI" \
  --archive "$ARCHIVE_URI" \
  --group data \
  --compression zstd
```

## Archive A Time Range Or Variable Set

```bash
firecube archive create \
  --source "$PRODUCT_URI" \
  --archive "$ARCHIVE_URI" \
  --group F024 \
  --start-date 2024-01-01 \
  --end-date 2024-02-01 \
  --variables FWI,lat,lon
```

## Overwrite An Existing Archive

Preview the command when replacing an existing file:

```bash
firecube archive create \
  --source "$PRODUCT_URI" \
  --archive "$ARCHIVE_URI" \
  --overwrite \
  --dry-run
```

Overwrite in a non-interactive shell:

```bash
firecube archive create \
  --source "$PRODUCT_URI" \
  --archive "$ARCHIVE_URI" \
  --overwrite \
  --yes-i-really-mean-it
```

## Verify

```bash
firecube archive validate --archive "$ARCHIVE_URI"
```

Expected output:

```text
VALID: /data/archives/MY_PRODUCT.tgm (2 message(s), no issues)
```

## Next Steps

- **[Inspect And Validate Archives](inspect.md)** — inspect metadata or contents
- **[Restore Archives](restore.md)** — restore the archive to Zarr
- **[Tensogram Output Format](../../concepts/output-formats/archive.md)** — format overview
