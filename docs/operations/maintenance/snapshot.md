# Snapshot Metadata

Use `firecube snapshot` to capture the metadata documents of a product and to
compare two captures.

A snapshot is a byte-exact copy of the Zarr metadata documents and of the
control-plane documents of the product. It holds no chunk bytes. It does not
prove that the chunks decode under an older metadata image. No command
restores a snapshot, and no command removes one.

## Prerequisites

- Stop every writer of the product. A writer that runs during `snapshot take`
  can make the snapshot mix states. See
  [Maintenance Operations](index.md#prerequisites).
- Make sure that the target is a Zarr store. `snapshot take` needs no control
  plane.

## Take A Snapshot

```bash
firecube snapshot take \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

Expected output resembles:

```text
Snapshot:     a899640037411f64…
Parent:       -
Manifest:     905e4db085a7c417…
Taken:        2026-11-04T00:31:43+00:00
Consistency:  observation
Trigger:      cli
Run:          -
```

The sample shortens each identifier. Firecube prints 64 hexadecimal digits.

The command reads no chunk. It creates the directory `.firecube/metadata/` in
the store and writes nothing else. The
[Control-Plane Spec](../../reference/control-plane-spec.md#metadata-snapshots-metadata)
describes the layout of that directory.

Add `-f json` to print the record as JSON with sorted keys.

## Read The Consistency

The `Consistency` line shows how far you can trust a snapshot:

- `observation`: the command took no claim. A writer that runs at the same time
  can make the snapshot mix states.
- `coherent`: a maintenance run took the snapshot while it held the exclusive
  claim. The claim refuses the Firecube writers that check it. It does not stop
  the writers listed in [Maintenance Operations](index.md#prerequisites).

`snapshot take` always makes an `observation`. `migrate run` and
`release create` make `coherent` snapshots. See [Migrate Metadata](migrate.md)
and [Create A Release](create-a-release.md).

## List Snapshots

```bash
firecube snapshot list \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

The command prints one line per snapshot, oldest first:

```text
a899640037411f64…  2026-11-04T00:31:43+00:00  observation  cli  run=-
a827266280f251f9…  2026-11-04T00:31:44+00:00  coherent  migrate-before  run=migrate-2e97c56b-…
efd16ef6fe8ce2438…  2026-11-04T00:31:44+00:00  coherent  migrate-after  run=migrate-2e97c56b-…
```

A product with no snapshot prints `No metadata snapshots` and exits 0.

## Show One Snapshot

Pass the full 64-digit identifier from `snapshot list`, or `latest`.

```bash
firecube snapshot show latest \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

Expected output resembles:

```text
Snapshot:     efd16ef6fe8ce243…
Parent:       a827266280f251f9…
Manifest:     902887b7713a2db4…
Taken:        2026-11-04T00:31:44+00:00
Consistency:  coherent
Trigger:      migrate-after
Run:          migrate-2e97c56b-…
Documents:    12
  .firecube/schema.json  firecube_schema  87 B  sha256:881cb8f63416
  .zattrs  v2_attrs  324 B  sha256:260503fd362c
  .zgroup  v2_group  22 B  sha256:7c578b275a7c
  .zmetadata  v2_consolidated  1716 B  sha256:a394d15b3fd6
  temperature/.zarray  v2_array  321 B  sha256:e47f736c1caf
  ...
```

## Compare Two Snapshots

```bash
firecube snapshot diff <before-id> latest \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

Each of the two arguments is a full identifier or `latest`. Expected output
resembles:

```text
Before:  a827266280f251f9…
After:   efd16ef6fe8ce243…
~ .zattrs
~ .zmetadata
Nodes:
  Node: (root)
    attribute added: firecube_last_modifier = {"at": "2026-11-04T00:31:44+00:00", "distribution": "firecube", "operation": "migrate", "run_id": "migrate-2e9...
    attribute added: title = "Daily temperature"
```

The first block lists the changed documents. `+` marks an added document, `-`
a removed document and `~` a changed document.

The `Nodes:` block lists the changes inside each node document. Firecube
shortens a long value in this listing.

If the snapshots are equal, the command prints `No differences`. The command
exits 0 whether or not the snapshots differ.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `is not a snapshot id: expected 64 lowercase hex digits` | The argument is a prefix or contains capital letters. | Copy the full identifier from `snapshot list`, or pass `latest`. |
| `No metadata snapshots` | The product has no snapshot. | Run `snapshot take`. |
| Exit 1 from `show` or `diff` | The identifier names no snapshot of this product. | Run `snapshot list` and copy an identifier. |
| A snapshot that mixes old and new values | A writer ran while `snapshot take` read the documents. | Stop the writers. Take the snapshot again. |

## Next Steps

- **[Migrate Metadata](migrate.md)** — change attributes and get a coherent before and after pair
- **[Create A Release](create-a-release.md)** — bind a coherent snapshot to a version name
- **[Verify A Release](verify-a-release.md)** — compare the live metadata with the snapshot of a release
- **[Maintenance Operations](index.md)** — overview of the command groups
