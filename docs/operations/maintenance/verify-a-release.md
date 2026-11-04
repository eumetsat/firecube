# Verify A Release

Use `firecube release verify` to find out whether the metadata of a product
still equals a release.

The command compares the live metadata documents with the snapshot of the
release. It detects a change of the metadata documents only. It does not see an
edit of chunk bytes. Content protection is the policy of the storage bucket.
Firecube detects drift and does not prevent it.

## Prerequisites

- Make sure that the release exists. List the releases with
  `firecube release list`. See
  [List And Show Releases](create-a-release.md#list-and-show-releases).

## Verify The Metadata

```bash
firecube release verify 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

The command re-reads the live metadata documents. It compares them byte for
byte with the snapshot of the release. Without a version, it uses the current
release. The command takes no claim and writes nothing.

If the metadata matches, the command exits 0:

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
`removed`, `matches`, `snapshot` and `version`.

## See What Changed

To see what changed inside a document, compare the snapshot of the release
with a new snapshot:

```bash
firecube snapshot take \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
firecube snapshot diff <release-snapshot-id> latest \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

The identifier of the release snapshot is in the output of
`firecube release show`. See [Snapshot Metadata](snapshot.md).

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `release verify` exits 1 and lists paths | The metadata differs from the release. | Compare with `snapshot diff`. Restore the documents or create a new release. |
| `release verify` exits 0 but the data is wrong | Verify does not read chunk bytes. | Protect the chunks with the bucket policy. Compare them with your own checksums. |
| `product … has no release '…'` | The version does not exist. | Run `firecube release list`. |

## Next Steps

- **[Create A Release](create-a-release.md)** — name a new state of the metadata
- **[Snapshot Metadata](snapshot.md)** — compare the snapshot of a release with the live store
- **[Maintenance Operations](index.md)** — overview of the command groups
