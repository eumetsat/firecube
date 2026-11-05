# Restore Archives

Use `firecube archive restore` to restore a `.tgm` or `.zip` archive back into
a Zarr store.

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

A `.tgm` archive carries the spans, the runs and the claims of the product. A
restore replays the spans and the runs that own them. It writes a record of the
restore itself and a `schema.json`. It does not restore the claims, the metadata
snapshots or the release records. A product restored from a `.tgm` has no
release. To keep a release, retain it in a zip archive. See
[Create A Release](../maintenance/create-a-release.md#retain-the-chunk-bytes).

## Restore A Zip Archive

```bash
firecube archive restore \
  --archive "file:///data/archives/MY_PRODUCT.zarr.zip" \
  --target "$RESTORED_URI"
```

The target must be local. The output is the same as for `.tgm`, without the
`restored group:` lines.

Firecube refuses a zip entry that leaves the target. Such an entry has an
absolute name, a backslash, or an empty, `.` or `..` segment. The check runs
before Firecube deletes the target or restores any entry.

Firecube also refuses a corrupt entry, an archive without a root `zarr.json`,
and a `zarr.json` entry that is not valid Zarr v3 metadata. If a check fails,
Firecube deletes and replaces no existing data. It can already have written the
control-plane records of the restore.

A zip archive keeps the control plane of the product, except the claims. The
metadata snapshots and the release records come back with the data.

## Restore An Incremental Backup Chain

A release made with `--archive` writes a full backup first. Each later release
writes an incremental backup. The full backup and the incremental backups above
it form a backup chain. To restore a release, restore its newest backup. Name
the newest incremental backup, not the full backup:

```bash
firecube archive restore \
  --archive "file:///data/archives/MY_PRODUCT-1.2.0.zip" \
  --target "file:///data/restored/MY_PRODUCT.zarr"
```

Expected output resembles:

```text
Zarr restored: /data/restored/MY_PRODUCT.zarr
  Variables: pressure, temperature
  Coordinates:
  Control-plane: restored
```

The command reads the backup chain from the markers of the backups. The marker
of an incremental backup names the previous backup by absolute path and by
digest. The command follows the markers down to a full backup. It restores the
full backup first, then each incremental backup in order. An incremental backup
puts its objects and deletes the keys that its marker lists as `deleted_keys`.
The command deletes exactly the listed keys and expands no wildcard.

Firecube checks the whole backup chain before it writes anything. If a previous
backup is missing, has another digest, or cannot be read, the command exits 1. It
leaves the target unchanged and writes no run record:

```text
Error: Cannot restore the backup chain of /data/archives/MY_PRODUCT-1.2.0.zip: it breaks at /data/archives/MY_PRODUCT-1.1.0.zip: the archive file does not exist
```

Keep every backup of a chain at the path that the marker names. A moved backup
breaks the chain. To inspect a marker, run
`firecube archive info --archive "<uri>"`.

The restored product equals the inventory of the release, and not the live
product at the end of the release. Firecube writes the record of the release, the
pointer to the current release and the completed run after the inventory. They
are not in the backup. They arrive with the next incremental backup. After the
restore, the current release of the product is the previous release, and the
record of the restored release is absent. `release verify` of the previous
release can report changed metadata, because the restored `zarr.json` is the one
of the newer release. To name the restored state, create a new release after the
steps in [Name The Restored State](#name-the-restored-state).

The restore uses the target rules of a zip restore. A target that exists is
refused without `--overwrite`. Firecube refuses a local target that holds a
symlink on the path of a key that the chain puts or deletes. The symlink can
point inside or outside the product. The refusal comes before the command writes
anything. A symlink on a path that the chain does not touch is not refused. If
the restore fails after it starts to write, the target stays partly restored and
Firecube does not roll it back. Delete the target and run the command again.

Each incremental backup applies its deletions before its objects. A Zarr array
that changed its rank between two releases therefore restores.

### Name The Restored State

A restore copies the runs of the archived product. The inventory of a release
holds the run of that release as `started`, because Firecube completes the run
after it takes the inventory. The copy is not a live process. While it stays
`started`, `release create` on the restored product exits 1.

1. Check that the original writer has stopped. If the release process of the
   original product still runs, do not abandon a run.
2. List the runs of the restored product that are still `started`:

   ```bash
   firecube chunks runs list --product-name "$RESTORED_URI" --status started
   ```

   The run to abandon has an ID that starts with `release-`. It is the run of
   the newest archived release. Expected output resembles:

   ```text
   Run ID                               Status     State    Parts Events
   --------------------------------------------------------------------------------
   release-af838c6e-a5bb-4a7a-adcc-69d250497292 started    active   1     1
   ```

3. Abandon the copied run:

   ```bash
   firecube chunks runs abandon \
     --product-name "$RESTORED_URI" \
     --run-id "<run-id>" \
     --reason "copied from the archive; original writer stopped" \
     --yes-i-really-mean-it
   ```

   The command prints `Abandoned run <run-id> for` and the name of the target.
4. Create the release. Use a version that no record of the product uses:

   ```bash
   firecube release create "restored-1.2.0" \
     --target "$RESTORED_URI" \
     --product-name MY_PRODUCT
   ```

To check the result, run `firecube release show --target "$RESTORED_URI"
--product-name MY_PRODUCT`. The current release is the new version.

If you skip steps 1 to 3, the create command exits 1 and names the run:

```text
Error: Cannot run exclusive maintenance for product MY_PRODUCT: non-terminal or unreadable run(s) exist: release-af838c6e-a5bb-4a7a-adcc-69d250497292 (status=started).
```

## Verify The Restored Product

Inspect the restored product's run history:

```bash
firecube chunks runs list --product-name "$RESTORED_URI"
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
firecube chunks list --product-name "$RESTORED_URI"
```

## Overwrite An Existing Restore Target

`--overwrite` replaces the whole target. Data and run history written to the
target after the archive was created are lost.

If the archive fails its checks, Firecube refuses it. It deletes and replaces
no existing data. It can already have written the control-plane records of
the restore. These are its claim, its run under
`.firecube/runs/archive-restore-*/`, and `.firecube/schema.json` if that file
was absent.

On a local target, the overwrite keeps these paths under `.firecube/`:

- `claims/`, which holds the claim of the restore
- `schema.json`
- `runs/<run-id>/`, which holds the records of the restore itself

Firecube deletes the rest of the old control plane and all old data. The
restore releases its claim when it ends. If the target or a kept path is a
symlink, Firecube refuses the overwrite and deletes nothing. Firecube treats a
remote `.tgm` target as before.

Preview:

```bash
firecube archive restore \
  --archive "$ARCHIVE_URI" \
  --target "$RESTORED_URI" \
  --overwrite \
  --dry-run \
  --yes-i-really-mean-it
```

In a non-interactive shell, the preview needs `--yes-i-really-mean-it` too. The
preview changes nothing.

Overwrite in a non-interactive shell:

```bash
firecube archive restore \
  --archive "$ARCHIVE_URI" \
  --target "$RESTORED_URI" \
  --overwrite \
  --yes-i-really-mean-it
```

## Recover From An Interrupted Overwrite

On a local target, a restore that stops during the replacement leaves a
restore run in the `started` state. If Firecube cannot record a failure, the
run can be `started` or `failed`. In that case the restore prints a line that
starts with `Warning:`. A remote `.tgm` root restore replaces the whole store
as before, so it need not leave a run to find. Take these steps:

1. List the runs and read the status of the restore run:

   ```bash
   firecube chunks runs list --product-name "$RESTORED_URI"
   ```

2. If the restore run is still `started`, abandon it. Its ID starts with
   `archive-restore-`. Do not abandon a run in another status.

   ```bash
   firecube chunks runs abandon \
     --product-name "$RESTORED_URI" \
     --run-id "<run-id>" \
     --reason "interrupted restore" \
     --yes-i-really-mean-it
   ```

3. Run the restore again:

   ```bash
   firecube archive restore \
     --archive "$ARCHIVE_URI" \
     --target "$RESTORED_URI" \
     --overwrite \
     --yes-i-really-mean-it
   ```

The target can hold a partial restore until step 3 ends.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `Invalid Tensogram archive <path>` | The archive failed its structural check. Firecube deleted and replaced no existing data. | Do not restore this archive. Create a new archive from the source product. |
| `holds a symlink at` | The target or a kept path under `.firecube/` is a symlink. Firecube deleted nothing. | Replace the symlink with a real directory, or restore to another target. |
| `Warning: archive restore failure not recorded` | The restore failed and Firecube could not record the failure. | Follow the steps in "Recover From An Interrupted Overwrite". |
| `Warning: claim <domain> at <path> not released` | The restore ended and Firecube could not release its claim. | Clear the claim: `firecube chunks claims clear --product-name "$RESTORED_URI" --domain "<domain>" --yes-i-really-mean-it`. |
| Restore target exists | Firecube will not replace it by default. | Use `--overwrite` with `--yes-i-really-mean-it`. |
| `requires the tensogram extras` | Archive support is not installed. | Install `firecube[tensogram]` in the active environment. |
| `Restoring a .zip archive to a remote target is not yet supported` | Zip restores write locally only. | Restore to a local target, then copy it to S3. |
| `Unsafe entry '<name>' in archive <path>` | The zip holds an entry that leaves the target. Firecube refused it before it restored any entry. | Do not restore this archive. Create a new archive from the source product. |
| `Cannot restore the backup chain of …: it breaks at …: the archive file does not exist` | A previous backup is not at the path that the marker names. | Put the backup back at that path. If no copy exists, restore an older release or a full backup. |
| `Cannot restore the backup chain of …: … the file has digest …, not the … its incremental backup records` | A previous backup changed after Firecube wrote it. | Replace it with the good copy. Run `firecube release verify` to find the broken link. |
| `Cannot restore the backup chain of …: it breaks at …: the target holds a symlink at …` | A local target has a symlink on the path of a key that the chain puts or deletes. | Delete the symlink or restore to another target. |
| `release create` on a restored product: `non-terminal or unreadable run(s) exist` | The restore copied the `started` run of an archived release. | Follow [Name The Restored State](#name-the-restored-state). |
| Restore of an incremental backup stops with an error after it started | The target is partly restored. | Delete the target. Run the command again. |
| Restored product has no expected variables | The archive is missing expected content. | Run `archive info` and `archive list`. |

## Next Steps

- **[Inspect And Validate Archives](inspect.md)** — confirm archive contents before restoring
- **[Read A Release With Xarray](read-a-release-with-xarray.md)** — open the data of a release from its archive or from a restored copy
- **[Verify A Release](../maintenance/verify-a-release.md)** — check the archives of a release before you need them
- **[ChunkManager Operations](../chunk-manager/index.md)** — inspect or operate the restored product state
