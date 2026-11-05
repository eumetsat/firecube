# Maintenance Operations

Use maintenance operations to change the metadata of a finished Zarr product
and to keep a record of each state you hand over. They do not write a chunk.
Two options of `release create` read chunk objects: `--archive` reads them to
archive them, and `--retain-versions` lists their versions.

The `firecube snapshot` commands work on any Zarr v2 or v3 store. A real
`migrate run` and `release create` need a control plane. A store that Firecube
did not write has none, so run `migrate adopt` on it first.

## Prerequisites

- Install Firecube, then activate its virtual environment:

  ```bash
  source .venv/bin/activate
  ```

- Make sure that you can write to the product.
- Stop every writer of the product before you run `migrate adopt`,
  `migrate run` or `release create`.

`migrate run` and `release create` take the exclusive maintenance claim of the
product. The claim refuses a Firecube writer that checks it and starts afterwards.
`firecube zarr multires` and `firecube zarr consolidate-time-coord` check it
once, when they start. See
[Scheduling And Write Safety](../../concepts/orchestration/write-safety.md#exclusive-maintenance)
for the other writers that check it.

The claim does not stop these writers:

- a process that already writes, including the process of an abandoned run
- a command that a plugin adds under `firecube plugins`
- any process outside Firecube

`migrate adopt` takes no claim. Only you can keep the writers away from it.

If a crash leaves the claim in place, follow
[Recover Runs And Claims](../chunk-manager/recover.md).

## Command Groups

| Command group | Use it for |
|---|---|
| `firecube snapshot` | Capture the metadata documents of a product and compare two captures. |
| `firecube migrate` | Give a foreign store a control plane, and change attributes in the metadata documents with a recorded, repeatable step. |
| `firecube release` | Name one state of the metadata, and detect later whether it changed. Optionally keep the chunk bytes of the state in a zip archive, or as object versions on S3. |

Every command except `firecube migrate list` needs `--target` and
`--product-name`.

## Next Steps

- **[Snapshot Metadata](snapshot.md)** — capture, list, show and compare snapshots
- **[Adopt A Foreign Store](adopt-a-foreign-store.md)** — give a store that Firecube did not write a control plane
- **[Migrate Metadata](migrate.md)** — change an attribute with a metadata-only step
- **[Create A Release](create-a-release.md)** — name a state of the metadata
- **[Verify A Release](verify-a-release.md)** — detect drift since a release
- **[Migrate An Adopted Store](migrate-an-adopted-store.md)** — tutorial: take an adopted xarray store through a change and a release
- **[Write A Migration Step](../../guides/plugins/write-a-migration-step.md)** — plugin authors: ship your own step
