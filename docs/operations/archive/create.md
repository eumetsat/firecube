# Create Archives

Use `firecube archive create` to convert an existing Zarr product into a
portable `.tgm` or `.zip` archive.

```bash
ARCHIVE_URI="file:///data/archives/MY_PRODUCT.tgm"
```

## Create A Full Archive

```bash
firecube archive create \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --archive "$ARCHIVE_URI" \
  --archive-format tensogram \
  --compression zstd
```

Expected output resembles:

```text
Archive created: /data/archives/MY_PRODUCT.tgm
  Groups: data
  Variables: data
  Size: 0.00 MB
```

## Create A Zip Archive

Use `--archive-format zip` to pack the Zarr product into a single `.zip` file:

```bash
firecube archive create \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --archive "file:///data/archives/MY_PRODUCT.zarr.zip" \
  --archive-format zip
```

Expected output resembles:

```text
Archive created: /data/archives/MY_PRODUCT.zarr.zip
  Size: 0.00 MB
```

Zip archives contain the whole product. `--group`, `--start-date`,
`--end-date`, and `--variables` are rejected with `--archive-format zip`.

## Create A Group Archive

Use `--group` when the Zarr product has multiple groups and you want one of
them:

```bash
firecube archive create \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --archive "$ARCHIVE_URI" \
  --group data \
  --compression zstd
```

## Archive A Time Range Or Variable Set

```bash
firecube archive create \
  --target "file:///data/products/MY_PRODUCT.zarr" \
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
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --archive "$ARCHIVE_URI" \
  --overwrite \
  --dry-run \
  --yes-i-really-mean-it
```

In a non-interactive shell, the preview needs `--yes-i-really-mean-it` too. The
preview writes nothing. Expected output resembles:

```text
[dry-run] Would archive: file:///data/products/MY_PRODUCT.zarr -> /data/archives/MY_PRODUCT.tgm
  Compression: zstd
```

If the file exists and you omit `--overwrite`, the command exits 1 with the
message `Target file already exists`.

Overwrite in a non-interactive shell:

```bash
firecube archive create \
  --target "file:///data/products/MY_PRODUCT.zarr" \
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

Each create is recorded as an `archive-create-…` run in the source product,
with status `complete` or `failed`:

```bash
firecube chunks runs list --target "file:///data/products/MY_PRODUCT.zarr"
```

If Firecube cannot write the record, the run can stay `started`. See
[Access And Claims](#access-and-claims).

The record holds the start time, group, date window, variables and size of the
archive.

Products without a `.firecube/schema.json` file are archived without a record
and are not modified.

## Access And Claims

If the source has a `.firecube/schema.json` file, `archive create` writes to
the control plane of the source. Use credentials with write access to the
source.

- The command takes the maintenance claim of the product for the whole create.
  If another claim is active on the product, the command stops and names it.
- The command refuses anonymous storage access (`--storage-anonymous`) and
  exits with code 2. Use credentials that can write to the source.
- If the command cannot write the record, it exits with code 1. The archive
  file stays in place. The message names the archive path and the record error.
  The run of the failed create can stay `started`. Recover with these steps:
  1. Fix the cause of the record error.
  2. List the runs that have the status `started`:

     ```bash
     firecube chunks runs list --target "file:///data/products/MY_PRODUCT.zarr" --status started
     ```

  3. Abandon the earlier `archive-create-…` run:

     ```bash
     firecube chunks runs abandon \
       --target "file:///data/products/MY_PRODUCT.zarr" \
       --run-id <run-id> \
       --reason "archive create record failed" \
       --yes-i-really-mean-it
     ```

  4. Run the create again with `--overwrite --yes-i-really-mean-it`.

A source without a `.firecube/schema.json` file gets no claim and no record.

## Next Steps

- **[Inspect And Validate Archives](inspect.md)** — inspect metadata or contents
- **[Restore Archives](restore.md)** — restore the archive to Zarr
- **[Tensogram Output Format](../../concepts/output-formats/archive.md)** — format overview
