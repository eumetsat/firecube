# Read A Previous Release With Xarray

Use this page to read the data of a release that you backed up as a ZIP archive.
It assumes that you made the release with `firecube release create --archive` and
that the archive files are on the machine where you read them.

## Prerequisites

- Activate an environment that has Firecube, `xarray` and `zarr`:

  ```bash
  source .venv/bin/activate
  ```

  The steps were run with xarray 2026.7.0 and zarr 3.2.1. Use `firecube` and
  `python` directly after you activate it.
- The archive files are local files. A remote archive is not covered.

## 1. Find The Archive And Its Mode

1. Show the release. The `Retention:` block names the archive and its mode:

   ```bash
   firecube release show 1.1.0 \
     --target "file:///data/products/MY_PRODUCT.zarr"
   ```

   Expected output ends with lines like these:

   ```text
   Retention:
     Kind:              zip
     Archive:           file:///data/archives/MY_PRODUCT-1.1.0.zip
     Mode:              incremental
     Previous release:  1.0.0
   ```

2. Read the mode from the archive file itself:

   ```bash
   firecube archive info --archive "file:///data/archives/MY_PRODUCT-1.1.0.zip"
   ```

   Expected output resembles:

   ```text
   Path:           /data/archives/MY_PRODUCT-1.1.0.zip
   Format:         zip
   Size:           0.02 MB
   Entries:        20
   Mode:           incremental
   Version:        1.1.0
   Previous release: 1.0.0
   Previous backup URI: file:///data/archives/MY_PRODUCT-1.0.0.zip
   Deleted keys:   0
   Groups:         (root)
   Control-plane:  present

   Group: (root)
     Variables:
       t: shape=[2] dtype=int64 chunks=[2] codec=zstd
       v: shape=[2, 4] dtype=float64 chunks=[1, 4] codec=zstd
       x: shape=[4] dtype=int64 chunks=[4] codec=zstd
   ```

3. If `Mode` is `full`, go to step 2. If `Mode` is `incremental`, go to step 3.

## 2. Read A Full Backup

A full backup holds every object of the release. This example assumes that the
arrays sit in the root group. The `Groups:` line of `archive info` lists the
groups. If it names a group such as `data`, the open below returns an empty
dataset, `Frozen({})`, without an error. In that case, restore the archive as in
step 3, then open the restored directory with `group="data"` and
`consolidated=False`. A full backup restores the same way as an incremental backup.

For a root group, open the archive in place:

```python
import warnings

import xarray as xr

warnings.filterwarnings("ignore", message="Object at")

ds = xr.open_dataset(
    "zip::/data/archives/MY_PRODUCT-1.0.0.zip",
    engine="zarr",
    group="/",
    consolidated=False,
    zarr_format=3,
)
print(ds.sizes)
```

Use an absolute path after `zip::`. Expected output resembles:

```text
Frozen({'t': 2, 'x': 4})
```

The `warnings.filterwarnings` line is optional. Without it, the open prints two
`ZarrUserWarning` lines. They say that `.firecube` and `firecube-archive.json` are
not Zarr nodes. The warnings are harmless. The two entries hold the control plane
and the archive marker.

## 3. Read An Incremental Backup

An incremental backup holds only the changed objects and the metadata. Every release
is an incremental backup when the current release already retains a ZIP archive. A
release made with `--archive-full`, or after a release without a ZIP archive, is a
full backup. Do not open an incremental backup in place. Restore the backup chain
first, then open the restored copy.

1. Restore the release. Name the incremental backup of the release. The command finds
   the earlier backups of the backup chain itself:

   ```bash
   firecube archive restore \
     --archive "file:///data/archives/MY_PRODUCT-1.1.0.zip" \
     --target "file:///data/restored/MY_PRODUCT.zarr"
   ```

   Expected output resembles:

   ```text
   Zarr restored: /data/restored/MY_PRODUCT.zarr
     Variables: v
     Coordinates: t, x
     Control-plane: restored
   ```

2. Open the restored directory:

   ```python
   import warnings

   import xarray as xr

   warnings.filterwarnings("ignore", message="Object at")

   ds = xr.open_dataset(
       "/data/restored/MY_PRODUCT.zarr",
       engine="zarr",
       consolidated=False,
   )
   print(ds.sizes)
   ```

   Expected output resembles:

   ```text
   Frozen({'t': 2, 'x': 4})
   ```

The restore rules for a backup chain, a target and a broken backup are in
[Restore An Incremental Backup Chain](restore.md#restore-an-incremental-backup-chain).

## 4. Compare The Result

1. Compare the sizes that `ds.sizes` prints with the `shape=` values that
   `archive info` printed in step 1. For the example above, `v` has the shape
   `[2, 4]`, and `ds.sizes` shows `t: 2` and `x: 4`.
2. Read one value that you know from the time of the release:

   ```python
   print(ds["v"].values)
   ```

You read the state of the product at the moment Firecube cut the release. You do
not read the later live state of the product. A write after the release does not
appear in the archive.

## Why The Common Spelling Fails

Many xarray guides open a zipped Zarr store with `consolidated=True` and no
`zarr_format`. That spelling does not work for a Firecube archive:

```python
xr.open_dataset(
    "zip::/data/archives/MY_PRODUCT-1.0.0.zip", engine="zarr", group="/", consolidated=True
)
```

This call raises `zarr.errors.GroupNotFoundError` for every archive. The call has
no `zarr_format=3`, and the open finds no group in the archive. Add `zarr_format=3`
to every call.

A Firecube archive holds the metadata that the product held when you made the
release. If the product had consolidated metadata, `consolidated=True` with
`zarr_format=3` works:

```python
ds = xr.open_dataset(
    "zip::/data/archives/MY_PRODUCT-1.0.0.zip",
    engine="zarr",
    group="/",
    consolidated=True,
    zarr_format=3,
)
```

For example, `to_zarr(..., zarr_format=3, consolidated=True)` writes such a
product. If the product had no consolidated metadata, the same call raises
`ValueError`. The message starts with
`Consolidated metadata requested with 'use_consolidated=True' but not found`.
Pass `consolidated=False` and `zarr_format=3`. That spelling works for every
full archive of a root-group product, so this page uses it.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `zarr.errors.GroupNotFoundError: No group found in store` | The call has no `zarr_format=3`. | Add `zarr_format=3` and `consolidated=False`. |
| `ValueError: Consolidated metadata requested with 'use_consolidated=True' but not found` | The call has `consolidated=True`, and the product had no consolidated metadata when you made the release. | Pass `consolidated=False`. |
| `KeyError: "There is no item named 't/c/0' in the archive"` | The archive is an incremental backup. It does not hold that chunk. | Restore the archive, then open the restored directory. See step 3. |
| `RuntimeWarning: Failed to open Zarr store with consolidated metadata` | The call has no `consolidated=` argument, and the archive holds no consolidated metadata. xarray tried the consolidated read first. | Pass `consolidated=False`. |
| `ZarrUserWarning: Object at … is not recognized as a component of a Zarr hierarchy` | The ZIP archive holds `.firecube` and `firecube-archive.json`. A restored product holds `.firecube`. | Ignore the warning, or add the `warnings.filterwarnings` line. |
| `Error: Target Zarr already exists` | The restore target exists. | Choose a new target, or follow the overwrite rules in [Restore Archives](restore.md#overwrite-an-existing-restore-target). |
| `Cannot restore the backup chain of …` | An earlier backup of the chain is missing or changed. | See the recovery rows in [Restore Archives](restore.md#failure-recovery). |

## Limits

The archive must be a local file. The examples read on the machine that holds the
archives. This page does not cover archives on a remote object store.

Firecube has no historical-read API. You cannot open release 1.0.0 of the live
product. You can open only an archive file, or a copy that you restored from it.
A release made without `--archive` has no ZIP archive for this procedure. See [Create A Release](../maintenance/create-a-release.md#retain-the-chunk-bytes).

## Next Steps

- **[Inspect And Validate Archives](inspect.md)** — read the metadata of an archive before you open it
- **[Restore Archives](restore.md)** — restore options, target rules and recovery
- **[Verify A Release](../maintenance/verify-a-release.md)** — find a lost or changed archive of a release
