# Maintenance Operations

Use maintenance operations to change the metadata of a finished Zarr product
and to keep a record of each state you hand over. They do not write a chunk.
Two options of `release create` read chunk objects: `--archive` reads them to
archive them, and `--retain-versions` lists their versions.

Every maintenance command reads the product label back from the store. A store
that Firecube did not write records no label, and the command exits 2. Run
`migrate adopt` on such a store first. A real `migrate run` and `release create`
also need the control plane that adoption writes.

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

Every command except `firecube migrate list` needs `-t/--target`. Only
`firecube migrate adopt` also needs `--product-name`, because it records the
label.

## Preview And Ask For Confirmation

Five maintenance commands take `--dry-run`: `snapshot take`, `migrate run`,
`migrate adopt`, `release create` and `zarr multires`. `release create` also
takes it together with `--archive`. A dry run writes nothing. It makes the
refusals that the page of that command lists. `zarr index rebuild` takes
`--dry-run` too. See
[Inspect And Manage The Resolved Index](../firecube-index.md).

The dry runs of `migrate run`, `migrate adopt` and `zarr preallocate` do not
check for a held exclusive maintenance claim. They exit 0 while the real run
exits 1. A clean preview of those three commands is no proof that the product is free.

Three commands ask for confirmation: `migrate run`, `migrate adopt` and
`release create`. On a terminal, the command asks a question. Off a terminal,
the command exits 2 until you add `--yes-i-really-mean-it`. `--dry-run` skips
the question.

## Next Steps

- **[Snapshot Metadata](snapshot.md)** — capture, list, show and compare snapshots
- **[Adopt A Foreign Store](adopt-a-foreign-store.md)** — give a store that Firecube did not write a control plane
- **[Migrate Metadata](migrate.md)** — change an attribute with a metadata-only step
- **[Create A Release](create-a-release.md)** — name a state of the metadata
- **[Verify A Release](verify-a-release.md)** — detect drift since a release
- **[Migrate An Adopted Store](migrate-an-adopted-store.md)** — tutorial: take an adopted xarray store through a change and a release
- **[Write A Migration Step](../../guides/plugins/write-a-migration-step.md)** — plugin authors: ship your own step
