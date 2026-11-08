# Back Up A Product With Incremental Backups

Use this guide to keep the chunk bytes of each release of a product in ZIP archives. The first archive is a full backup. Each later archive is an incremental backup that holds only what changed.

You run the backup by hand, once for each release. It captures the product at the moment of the release. It does not protect the live product between releases.

## Prerequisites

- Stop every writer of the product. See [Maintenance Operations](../maintenance/index.md#prerequisites).
- Use a Zarr v3 product that has a control plane.
- Pick a local disk with room for the archives. Each archive needs its own path.

```bash
TARGET="file:///data/products/MY_PRODUCT.zarr"
```

## Write The First Backup

Create a release with `--archive`. A product with no earlier archive gets a full backup:

```bash
firecube release create 1.0.0 \
  --target "$TARGET" \
  --archive "file:///data/archives/MY_PRODUCT-1.0.0.zip" \
  --yes-i-really-mean-it
```

Omit `--yes-i-really-mean-it` when you run on a terminal. The command then asks
for confirmation. Add `--dry-run` to preview the backup without a write.

Expected output, shortened:

```text
wrote zarr.json
snapshot 88d475ffde8048dd…
inventory 7fe3256a4b988d09… lists 23 objects
full backup /data/archives/MY_PRODUCT-1.0.0.zip holds 23 objects, 41323 bytes
recorded release '1.0.0' at snapshot 88d475ffde8048dd…
release '1.0.0' is current
…
Retention:
  Kind:              zip
  Archive:           file:///data/archives/MY_PRODUCT-1.0.0.zip
  Mode:              full
  Chain length:      1
  Deleted keys:      0
  Size (bytes):      41323
  Digest:            sha256:7973775e2ebe726d…
  Inventory:         7fe3256a4b988d09…
  Run:               archive-create-d8edb87c-…
```

## Write The Next Backup

Ingest more data. Then create the next release with a new version and a new archive path:

```bash
firecube release create 1.1.0 \
  --target "$TARGET" \
  --archive "file:///data/archives/MY_PRODUCT-1.1.0.zip" \
  --yes-i-really-mean-it
```

The release retains a ZIP archive already, so Firecube writes an incremental backup:

```text
wrote zarr.json
snapshot c78b897f8b349aa5…
inventory 8a7b0a92835b6de0… lists 35 objects
incremental backup /data/archives/MY_PRODUCT-1.1.0.zip holds 19 objects, 23548 bytes
recorded release '1.1.0' at snapshot c78b897f8b349aa5…
release '1.1.0' is current
…
Retention:
  Kind:              zip
  Archive:           file:///data/archives/MY_PRODUCT-1.1.0.zip
  Mode:              incremental
  Previous release:  1.0.0
  Chain length:      2
  Deleted keys:      0
  Size (bytes):      23548
  Digest:            sha256:f7cacbd16b4ad6ac…
  Inventory:         8a7b0a92835b6de0…
  Run:               archive-create-d5ae46ca-…
```

The backup holds the objects that are new or changed. It also holds every Zarr metadata document. `Deleted keys:` counts the objects that the product lost since the previous release.

Repeat this step for each release. After the product deleted one object, release `1.2.0` shows:

```text
  Mode:              incremental
  Previous release:  1.1.0
  Chain length:      3
  Deleted keys:      1
```

## Inspect And Validate A Backup

Read the marker of the newest backup:

```bash
firecube archive info --archive "file:///data/archives/MY_PRODUCT-1.2.0.zip"
```

Expected output:

```text
Path:           /data/archives/MY_PRODUCT-1.2.0.zip
Format:         zip
Size:           0.02 MB
Entries:        19
Mode:           incremental
Version:        1.2.0
Previous release: 1.1.0
Previous backup URI: file:///data/archives/MY_PRODUCT-1.1.0.zip
Deleted keys:   1
Groups:         (root), data
Control-plane:  present

Group: data
  Variables:
    data: shape=[200, 10] dtype=float32 chunks=[100, 10] codec=zstd
    lat: shape=[10] dtype=float64 chunks=[10] codec=zstd
```

Validate the file:

```bash
firecube archive validate --archive "file:///data/archives/MY_PRODUCT-1.2.0.zip"
```

Expected output:

```text
VALID: /data/archives/MY_PRODUCT-1.2.0.zip (no issues)
```

`archive validate` tests one file. To test the whole chain, run `release verify`:

```bash
firecube release verify 1.2.0 --target "$TARGET"
```

Expected output:

```text
Release 1.2.0: metadata matches snapshot 8d627a77a46e9f7c…
archive: present
backup chain: intact
```

The other results are in [the release guide](../maintenance/verify-a-release.md).

## Restore From The Newest Backup

Name the newest incremental backup. Do not name the full backup:

```bash
firecube archive restore \
  --archive "file:///data/archives/MY_PRODUCT-1.2.0.zip" \
  --target "file:///data/restored/MY_PRODUCT.zarr"
```

Expected output:

```text
Zarr restored: /data/restored/MY_PRODUCT.zarr
  Groups: data
  Variables: data/data
  Coordinates: data/lat
  Control-plane: restored
```

Firecube follows the markers down to the full backup and tests every link before it writes. If a link is missing or changed, the command exits 1 and leaves the target unchanged. It then restores the full backup and each incremental backup in order.

The restored product holds the state of the inventory of release `1.2.0`. The restored product has no record of that release. To name the restored state, see [Name The Restored State](restore.md#name-the-restored-state).

## Start A New Chain

A long chain needs many files and a long restore. To start a new chain, add `--archive-full`:

```bash
firecube release create 2.0.0 \
  --target "$TARGET" \
  --archive "file:///data/archives/MY_PRODUCT-2.0.0.zip" \
  --archive-full \
  --yes-i-really-mean-it
```

The `Retention:` block shows `Mode: full` and `Chain length: 1`. A restore of release `2.0.0` needs this file only.

## Keep The Chain Safe

!!! warning "Copy every archive of a chain"
    Copy each archive to storage that you back up. If one archive is lost or changed, Firecube cannot restore the newer ones.

Do not move an archive. Each marker names the previous archive by its absolute path.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `Something exists at …, the path given as --archive` | A file exists at the archive path. Firecube never overwrites an archive. | Pick another path, or delete the old file. Run the command again. |
| `Cannot restore the backup chain of …: it breaks at …: the archive file does not exist` | A previous archive is not at the path that the marker names. | Put the archive back at that path. The target stays unchanged. |
| `Cannot restore the backup chain of …: it breaks at …: the file has digest …` | A previous archive changed after Firecube wrote it. | Put back the good copy. The target stays unchanged. |
| `backup chain: broken at 1.1.0` from `release verify` | The archive of that version is missing or changed. | Put back the good copy. If none exists, write a new full backup with `--archive-full`. |
| `Target Zarr already exists` | The restore target exists. | Restore to a new path, or see [Overwrite An Existing Restore Target](restore.md#overwrite-an-existing-restore-target). |
| `--archive-full requires --archive.` | `--archive-full` has no archive to select. | Add `--archive`. |

## Next Steps

- **[Create A Release](../maintenance/create-a-release.md#retain-the-chunk-bytes)** — all rules and refusals of the archive option
- **[Restore Archives](restore.md#restore-an-incremental-backup-chain)** — details of a chain restore
- **[Inspect And Validate Archives](inspect.md)** — the other archive commands
- **[Verify A Release](../maintenance/verify-a-release.md)** — test the archives of a release
