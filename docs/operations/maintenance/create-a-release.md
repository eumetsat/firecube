# Create A Release

Use `firecube release create` to name one state of the metadata of a product.

A release is a named record. It binds a version string to a coherent snapshot of
the metadata. It also lists the runs since the previous release and the producer
of the latest run that recorded one. `release create` stamps the root attribute
`firecube_release` with the version.

By default, a release names metadata only. It does not cover chunk bytes. To
keep the chunk bytes of a release, see
[Retain The Chunk Bytes](#retain-the-chunk-bytes). To compare the metadata with
a release later, see [Verify A Release](verify-a-release.md).

## Prerequisites

- Stop every writer of the product. See
  [Maintenance Operations](index.md#prerequisites).
- Make sure that the product records a product label and has a control plane.
  If it has none, run `firecube migrate adopt` first. See
  [Adopt A Foreign Store](adopt-a-foreign-store.md).

## Preview A Release

Add `--dry-run` to see what the command would record. A dry run makes its
refusals from reads only and writes nothing. It asks no confirmation.

```bash
firecube release create 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --note "first release" \
  --dry-run
```

Expected output resembles:

```text
would record release '1.0.0' and make it current; nothing is written
Dry run: nothing is written.
Release:      1.0.0
Would:        record the release and make it current
Previous:     -
Note:         first release
Documents:    zarr.json
Retention:    none
```

A dry run with `--archive` or `--retain-versions` also prints the retention
block. The object counts in that block are current-cut counts: the run also
retains the documents that it writes. See
[Retain The Chunk Bytes](#retain-the-chunk-bytes).

## Create A Release

`release create` changes the store. On a terminal, the command asks for
confirmation. Off a terminal, the command exits 2 and writes nothing until you
add `--yes-i-really-mean-it`.

```bash
firecube release create 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --note "first release" \
  --yes-i-really-mean-it
```

Expected output for a Zarr v2 store with a consolidated `.zmetadata`:

```text
wrote .zattrs, .zmetadata
snapshot dadfa2e8c110e0fd…
recorded release '1.0.0' at snapshot dadfa2e8c110e0fd…
release '1.0.0' is current
release '1.0.0' created in run release-bb4fa223-…
Release:      1.0.0
Created:      2026-11-04T00:31:51+00:00
Previous:     -
Snapshot:     dadfa2e8c110e0fd…
Run:          release-bb4fa223-…
Producer:     unknown
Note:         first release
Runs since:   3
  adopt-345bfd72-…
  migrate-2e97c56b-…
  release-bb4fa223-…
Retention:    none
```

On a Zarr v3 store, the first line is `wrote zarr.json`. The other lines keep
their shape. The `Runs since` list holds the runs of your store.

`Producer: unknown` means that no run of the product recorded a producer.

Firecube keeps the version as you give it, and each version is used once. If
you repeat `release create` for an existing version and the metadata still
equals its snapshot, the command prints `release '1.0.0' is current already`.
It exits 0 and writes nothing.

## List And Show Releases

```bash
firecube release list \
  --target "file:///data/products/MY_PRODUCT.zarr"
```

The command prints one line per release, oldest first. A `*` marks the current
release:

```text
* 1.0.0  2026-11-04T00:31:51+00:00  snapshot=dadfa2e8c110e0fd…  producer=unknown
```

A product with no release prints `No releases` and exits 0.

Show one release with its note and the runs since the previous release:

```bash
firecube release show 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr"
```

Without a version, the command shows the current release. It exits 1 when the
release does not exist. If a migration step created the release, the list of
runs includes the migrate run.

## Retain The Chunk Bytes

You run Firecube's incremental backup manually for each release. It captures
only the state at the cut of that release. It provides no scheduled or ongoing
protection of the live product. `release verify` verifies the backup files, not
the live data.

A release can also keep the objects that it names. Choose one of two options.
They exclude each other. Both write an inventory, which lists every object of
the product with its size and its digest or version identifier. The inventory
leaves out `.firecube/claims/` and the temporary files of publication.

The inventory is not atomic. No storage backend lists a product in one step.
The inventory is coherent only while every writer of the product is stopped.
Firecube holds its own claim and does not stop a process outside Firecube.

### Retain In A Zip Archive

Choose the retention when you create a release. A record is never rewritten, so
`release create` refuses to add an archive to a version that exists. Use a
version that no record uses. Add `--archive` with a `file://` URI:

```bash
firecube release create 1.1.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --archive "file:///data/archives/MY_PRODUCT-1.1.0.zip" \
  --yes-i-really-mean-it
```

A dry run of the same command prints the planned retention. The output
resembles:

```text
Retention:
  Kind:              zip
  Archive:           file:///data/archives/MY_PRODUCT-1.1.0.zip
  Mode:              full
  Chain length:      1
  Deleted keys:      0
  Objects:           52
  (current-cut counts; the run also retains the documents it writes)
```

The archive is a local ZIP file. The product can be remote. The product must be
a Zarr v3 store, because the command refuses a product without a root
`zarr.json`.

The command prints two more progress lines than a release without retention.
The first backup of a product is a full backup:

```text
inventory 3415078ca68e6b0e… lists 31 objects
full backup /data/archives/MY_PRODUCT-1.1.0.zip holds 31 objects, 18187 bytes
```

A later release writes an incremental backup if the current release also
retains a ZIP archive and `--archive-full` is not set. The progress line then
starts with `incremental backup` and counts only the objects that it holds.

The output of `release create` and `release show` ends with a `Retention:`
block:

```text
Retention:
  Kind:              zip
  Archive:           file:///data/archives/MY_PRODUCT-1.1.0.zip
  Mode:              full
  Chain length:      1
  Deleted keys:      0
  Size (bytes):      18187
  Digest:            sha256:4c403f8cfea97d55…
  Inventory:         3415078ca68e6b0e…
  Run:               archive-create-157c693e-…
```

The first release with an archive writes a full backup. It holds every object of
the inventory. If the current release retains a ZIP archive, the next release
writes an incremental backup. An incremental backup holds three kinds of entry:

- every object that is new or has another digest than in the previous inventory
- every Zarr metadata document, whatever its digest
- one entry, `firecube-archive.json`, the marker

The marker names the previous release, the file of the previous backup and the
digest of that file. It also lists the keys that the product deleted since the
previous release. The `Retention:` block of an incremental backup shows
`Mode: incremental`, the `Previous release:` version, the `Chain length:` and the
`Deleted keys:` count. To read the marker of an archive, run
`firecube archive info --archive <uri>`.

The backups of a product form a backup chain. The chain starts at a full backup
and ends at the newest incremental backup. Each incremental backup depends on the
previous backup. The chain length of a full backup is 1.

To write a full backup in place of an incremental backup, add `--archive-full`.
Use it to start a new backup chain.

Each archive needs its own path. A path that holds anything is refused, and
Firecube never overwrites it:

```text
Error: Something exists at /data/archives/MY_PRODUCT-1.1.0.zip, the path given as --archive. A release never reuses or overwrites an archive: remove it or pick another path, then run again.
```

The refusal comes before the command takes the inventory.

An archive file is the only copy of the bytes. The durability of a backup chain
rests on its full backup and on every incremental backup between the full backup
and the newest one. If one archive is lost or changed, Firecube cannot restore
the newer ones. Copy every archive of a backup chain to storage that you back
up. Write a new full backup from time to time with `--archive-full`, to keep
backup chains short.

An archive holds the objects of the inventory. It does not hold the record of its
own release, the pointer to the current release or the completed run of the
release. Firecube writes them after the inventory. They arrive in the next
incremental backup.

Firecube publishes an archive with a hard link, so that two releases never
overwrite one file. A filesystem without hard links fails to publish the archive.
Such filesystems are FUSE mounts, FAT volumes and some network shares. Write the
archive to a local disk and copy it afterwards.

Firecube refuses two kinds of key before it writes the archive:

- A local file whose name ends in `#<digits>`, for example `payload#12`. Rename
  the file.
- A key that the product deleted since the previous release and whose name holds
  a control character, for example a newline. A restore of the backup chain
  cannot restore such a key. Add `--archive-full` to write a full backup instead.

Both refusals leave no inventory, no archive and no release record. The refusal
of a deleted key reads:

```text
Error: The key 'line\nbreak' was deleted since release '1' and holds a control character, so it cannot be restored from a backup chain and no incremental backup lists it. Deleted keys refused: 1. Write a full backup with --archive-full instead.
```

### Retain As Object Versions

On a versioned S3 bucket, the release can record the version identifier of each
object. This copies no bytes:

```bash
firecube release create 2.0.0 \
  --target "s3://my-bucket/products/MY_PRODUCT.zarr" \
  --storage-type s3 \
  --storage-driver fsspec \
  --retain-versions \
  --yes-i-really-mean-it
```

Expected output ends with these lines:

```text
inventory 5393dc285018cee1… lists 26 object versions and 0 delete markers
…
Retention:
  Kind:              s3-versions
  Bucket versioning: enabled
  Objects:           26
  Inventory:         5393dc285018cee1…
  Run:               release-e8cc87f3-…
```

The option has these requirements. Assume that the settings of the bucket stay
the same while the command runs. Then Firecube refuses the command before it
writes anything when a requirement is not met. If the versioning of the bucket changes during the command,
the refusal can come late. By then the command has stamped the root attribute
and taken the snapshot.

- The product is on S3 and you use `--storage-driver fsspec`. The `obstore`
  driver cannot list object versions.
- The bucket has versioning enabled. A bucket with versioning `suspended` or
  `absent` is refused.
- Every current object has a version identifier. An object that was written before
  you enabled versioning has none. The command names the count and the first key.
  Copy each such object over itself, then run the command again.

Versioning must stay enabled for as long as you rely on the release. The
retained bytes also depend on the lifecycle rules of the bucket. A lifecycle
rule that expires old versions destroys the bytes that the release names. Firecube
records the lifecycle configuration when the bucket allows it to read the
configuration. Otherwise it records `unavailable`. `release verify` reports a
change of either setting. See [Verify A Release](verify-a-release.md).

To verify a large release fast, run `firecube release verify --sample N`. The
option applies to a release made with `--retain-versions` only.

Firecube tested `--retain-versions` against a mock S3 server only. It has not
run against a real object store. Run `release verify` on your store after the
first release.

## Recover From An Interrupted Create

Run the same `release create` command again. The rerun resumes from the last
completed step. If the first run stamped the root attribute but did not record
the release, the rerun continues from the snapshot step:

```text
run release-cec9a4c8-… stamped release '1.0' before; resuming from the snapshot
```

If a record exists but the release is not current, the rerun completes the
release. It does so only while the live metadata still equals the snapshot of
the record.

If the first run wrote an archive and stopped before it recorded the release, the
archive file stays. Firecube never reuses it, because the file can differ from
the inventory of a new run. The rerun is refused and writes nothing:

1. Delete the archive file that the message names, or choose another path.
2. Run the same `release create` command again.

The rerun takes a new inventory and writes a new archive. The inventory file of
the stopped run stays in `.firecube/releases/inventories/`. Nothing refers to it
and it does no harm.

If a record of the version exists already, give the same `--archive` value or
none. A record is never rewritten. A different archive for an existing version is
refused. Choose another version.

A release process that is killed outright leaves its run in the status
`started` and the exclusive claim in place. The next `release create` is
refused until you retire that run. This differs from an interrupted
[migration](migrate.md#recover-from-an-interrupted-run), which you resume. A
new `release create` always starts a new run, so abandon the old run.

1. Make sure that no process of the run is still running.
2. Abandon the run. This also releases the claim that the run owns:

   ```bash
   firecube chunks runs abandon \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --run-id <run-id> \
     --reason "release process killed" \
     --yes-i-really-mean-it
   ```

3. If the claim names no recorded run, clear it:

   ```bash
   firecube chunks claims clear \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --domain MY_PRODUCT:maintenance:exclusive \
     --force --yes-i-really-mean-it
   ```

4. Run the same `release create` command again, with `--yes-i-really-mean-it`
   off a terminal.

See [Recover Runs And Claims](../chunk-manager/recover.md) for more detail.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `Confirmation required: release create changes the store` (exit 2) | The run is off a terminal and has no `--yes-i-really-mean-it`. | Preview with `--dry-run`. Then add `--yes-i-really-mean-it`. |
| `records no product label. Record one with firecube migrate adopt` (exit 2) | The store records no label. The command wrote nothing. | Run `firecube migrate adopt`. |
| `release '…' exists and names snapshot …, and the store's metadata no longer matches it` | The version exists, and the metadata changed since. | Choose another version. |
| `release version '…' cannot be stored` | The version has an ASCII control character, or the record of the release does not fit the storage limits. | Use a shorter version without control characters. See the limits below this table. |
| `Write-domain claim conflict for …:maintenance:exclusive` | Another run or a crash holds the claim. | Follow [Recover From An Interrupted Create](#recover-from-an-interrupted-create). |
| `stamped release '…' and did not complete; run that step again` | A migration step that creates a release stopped before it recorded the release. | Run the same `migrate run` command again. |
| `product … has no release '…'` | The version does not exist. | Run `firecube release list`. |
| `Something exists at …, the path given as --archive` | A file already exists at the archive path. | Delete the file or choose another path. Run the command again. |
| `Invalid value for '--archive': a release archive is a local file:// URI.` | The archive URI is not a local `file://` URI. | Give a `file://` URI. The product can stay remote. |
| `--archive-full requires --archive.` | `--archive-full` has no archive to select. | Add `--archive`. |
| `--archive and --retain-versions are mutually exclusive.` | A release retains by one means. | Use one of the two options. |
| `--retain-versions needs an s3 product on the fsspec storage driver` | The product is not on S3, or the driver is `obstore`. | Use an S3 product with `--storage-driver fsspec`. |
| `The bucket of … has versioning absent` (or `suspended`) | The bucket does not keep versions. | Enable versioning on the bucket. Run the command again. |
| `… current objects of … have no usable version id` | Objects were written before versioning was enabled. | Copy each object over itself. Run the command again. |
| `The product … has no Zarr v3 root document zarr.json` | The product is not a Zarr v3 store. | Create the release without `--archive`. |
| `Release '…' exists and retains …` | The version has a record with other retention. | Choose another version. |
| `Files of … on disk that its storage driver does not list: …` or `The file '…' of … ends in '#<digits>'` | A local file has a name that ends in `#<digits>`. | Rename the file. Run the command again. |
| `The key '…' was deleted since release '…' and holds a control character` | An incremental backup cannot list the deleted key, and a restore of the backup chain cannot restore it. | Add `--archive-full`. Run the command again. |
| An operating-system error such as `Operation not permitted` while the archive is published | The filesystem of the archive has no hard links. | Write the archive to a local disk. |

The storage limits on a version string are:

- On a local file system, the record name must fit the name limit of the file
  system minus 21 bytes. With the usual limit of 255 bytes, the version takes
  at most 229 bytes after Firecube escapes `%`, `/` and a leading `.`.
- On a local file system, the full path of the record plus 21 bytes must stay
  below the path limit. A long path to the product lowers the version limit.
- On S3, the whole object key must fit 1024 bytes. The key must not contain
  `?versionId=`.

## Next Steps

- **[Verify A Release](verify-a-release.md)** — detect a change of the metadata, or a loss of retained bytes, since the release
- **[Restore An Incremental Backup Chain](../archive/restore.md#restore-an-incremental-backup-chain)** — rebuild a product from the backups of a release
- **[Read A Release With Xarray](../archive/read-a-release-with-xarray.md)** — open the data of a release from its archive
- **[Migrate Metadata](migrate.md)** — change attributes before a new release
- **[Maintenance Operations](index.md)** — overview of the command groups
