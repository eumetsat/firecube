# Verify A Release

Use `firecube release verify` to find out whether the metadata of a product
still equals a release.

The command compares the live metadata documents with the snapshot of the
release. It does not see an edit of chunk bytes. Content protection is the policy
of the storage bucket. Firecube detects drift and does not prevent it.

If the release retains its chunk bytes, the command also tests that retention.
For a zip archive, it tests the archive file and the
[backup chain](create-a-release.md#retain-the-chunk-bytes) below it. For object
versions, it tests the bucket settings and the listed versions. The command exits
1 when the metadata differs or the retention does not hold.

## Prerequisites

- Make sure that the release exists. List the releases with
  `firecube release list`. See
  [List And Show Releases](create-a-release.md#list-and-show-releases).

## Verify The Metadata

```bash
firecube release verify 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr"
```

The command re-reads the live metadata documents. It compares them byte for
byte with the snapshot of the release. Without a version, it uses the current
release. The command takes no claim and writes nothing.

If the metadata matches and the retention of the release holds, the command exits
0. A release without retention has nothing more to hold:

```text
Release 1.0.0: metadata matches snapshot dadfa2e8c110e0fd…
```

If any document differs, the command exits 1 and names the paths:

```text
Release 1.0.0: metadata differs from snapshot dadfa2e8c110e0fd…
  changed: temperature/.zattrs
  removed: .zmetadata
```

Add `-f json` for a machine-readable result with the keys `added`, `changed`,
`removed`, `matches`, `retention`, `snapshot` and `version`.

## Verify A Zip Archive Release

For a release made with `--archive`, the command prints two more lines after the
metadata line:

```text
Release 1.2.0: metadata matches snapshot f4ca8645…
archive: present
backup chain: intact
```

The `archive:` line describes the archive file of this release:

- `present`: the file exists and has the recorded digest.
- `missing`: no file exists at the recorded path.
- `changed`: the file exists and has another digest.

The `backup chain:` line describes the previous backups below an incremental
backup. The command follows the previous backup of each link through the release
records. It tests that each previous backup exists and has its recorded digest.
If a link fails, the line names the version of the first failing link, counted
from the newest:

```text
archive: present
backup chain: broken at 1.1.0
```

A full backup has no previous backup, so its backup chain is `intact`. The command
hashes every archive file of the backup chain, so a long chain of large files
takes time.

The JSON result holds the same facts in a `retention` object:

```json
{
  "retention": {
    "archive": "present",
    "backup_chain": "broken",
    "broken_at": "1.1.0",
    "holds": false,
    "kind": "zip"
  }
}
```

The `holds` key is `true` when the retained bytes are intact. A release without
retention has `"kind": null` and `"holds": true`, and the text output has no
extra lines.

## Verify A Release With Object Versions

For a release made with `--retain-versions`, the command prints four lines after
the metadata line:

```text
Release 2.0.0: metadata matches snapshot 803de3fb…
retention: unchanged
lifecycle: unchanged
inventory: intact
checked: all
```

- `retention:` is `changed` when the versioning state of the bucket differs from
  the state that the release recorded.
- `lifecycle:` is `changed` when the lifecycle configuration of the bucket
  differs from the recorded one. A recorded value `unavailable` compares equal
  only to an unreadable configuration.
- `inventory:` is `lost N` when N tested versions are gone, are delete markers,
  have another size or cannot be reached. The command lists each lost key below the line as
  `  lost: <key>`.
- `checked:` is `all` or `sample N`.

By default, the command sends one HEAD request for each listed version. On a large
product this takes time and costs requests. To test a random subset, add
`--sample`:

```bash
firecube release verify 2.0.0 \
  --target "s3://my-bucket/products/MY_PRODUCT.zarr" \
  --storage-type s3 \
  --storage-driver fsspec \
  --sample 100
```

The line `checked: sample 100` shows that the command tested a subset. A sample
that is larger than the number of versions tests all of them and prints
`checked: all`. A sample does not prove that every version exists. `--sample`
applies to a release made with `--retain-versions` only. For a zip release the
command ignores it.

The JSON result holds `kind`, `holds`, `retention`, `lifecycle`, `inventory`,
`lost`, `checked` and `checked_count`, and a `lost_keys` list when a version is
lost.

## See What Changed

To see what changed inside a document, compare the snapshot of the release
with a new snapshot:

```bash
firecube snapshot take \
  --target "file:///data/products/MY_PRODUCT.zarr"
firecube snapshot diff <release-snapshot-id> latest \
  --target "file:///data/products/MY_PRODUCT.zarr"
```

The identifier of the release snapshot is in the output of
`firecube release show`. See [Snapshot Metadata](snapshot.md).

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `release verify` exits 1 and lists paths | The metadata differs from the release. | Compare with `snapshot diff`. Restore the documents or create a new release. |
| `release verify` exits 0 but the data is wrong | Verify does not read the chunk bytes of the live product. | Protect the chunks with the bucket policy. Compare them with your own checksums. |
| `product … has no release '…'` | The version does not exist. | Run `firecube release list`. |
| `archive: missing` and exit 1 | No file exists at the archive path of the release. | Put the file back at the path in `Archive:` of `release show`. If you lost the file, create a new release with `--archive-full`. |
| `archive: changed` and exit 1 | The file has another digest than the record. | Replace the file with a good copy. Do not restore from the changed file. |
| `backup chain: broken at <version>` and exit 1 | A previous backup of an incremental backup is missing or changed, or its record is missing. | Put back the good copy of the archive of that version. If no copy exists, the newer incremental backups cannot be restored. Create a new release with `--archive-full`. |
| `retention: changed` and exit 1 | The versioning state of the bucket is not the recorded one. | Enable versioning again. If the change is intended, create a new release. |
| `lifecycle: changed` and exit 1 | A lifecycle rule of the bucket changed. | Make sure that no rule expires the retained versions. Delete such a rule, or create a new release. |
| `inventory: lost N` and exit 1 | N listed object versions are gone or unreachable. | Read the `lost:` keys. Restore the versions from a copy. If you cannot, the release no longer holds those bytes. |
| `… needs an s3 product on the fsspec storage driver` | The release lists object versions, and the command used the `obstore` driver. | Run the command with `--storage-driver fsspec`. |

## Next Steps

- **[Create A Release](create-a-release.md)** — name a new state of the metadata
- **[Snapshot Metadata](snapshot.md)** — compare the snapshot of a release with the live store
- **[Maintenance Operations](index.md)** — overview of the command groups
