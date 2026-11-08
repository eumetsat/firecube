# Migrate Metadata

Use `firecube migrate` to change attributes in the metadata documents of a
product. A migration step changes metadata only. It cannot move or rewrite a
chunk.

A real run has these properties:

- It runs under the exclusive maintenance claim of the product.
- It is one maintenance run, journaled before the first write.
- It takes a coherent snapshot before the change and one after it.
- It sets the root attribute `firecube_last_modifier`.

## Prerequisites

- Stop every writer of the product. See
  [Maintenance Operations](index.md#prerequisites).
- Make sure that the product records a product label and has a control plane.
  If it has none, run `firecube migrate adopt` first. See
  [Adopt A Foreign Store](adopt-a-foreign-store.md).
- Find the identifier of the step. List the installed steps:

  ```bash
  firecube migrate list
  ```

  Expected output resembles:

  ```text
  firecube:set-attrs  firecube  0.1.8
  ```

  Each line shows the step identifier, the distribution that provides it and
  its version. The command needs no product.

The built-in step is `firecube:set-attrs`. A plugin can install more steps.

## Preview A Run

Add `--dry-run` to print the plan. A dry run takes no claim and writes
nothing, not even `.firecube/`.

```bash
firecube migrate run firecube:set-attrs \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --set title="Daily temperature" \
  --dry-run
```

Expected output for a Zarr v2 store with a consolidated `.zmetadata`:

```text
set /:title
class /:title: descriptive, allowed
write .zattrs
write .zmetadata (consolidated image)
planned outcome: executed
```

A Zarr v3 store with consolidated metadata in the root `zarr.json` prints one
`write` line instead of two:

```text
write zarr.json (consolidated image)
```

Each `set` or `remove` line names the node and the attribute. Each `class`
line shows how Firecube classes the attribute and whether it allows the
change. Each `write` line names a document that the run rewrites. The last
line starts with `planned outcome:`.

A migration rewrites the consolidated image that the store already has. It
never creates one.

## Run The Step

Run the same command without `--dry-run`. On a terminal, the command asks for
confirmation. Off a terminal, the command exits 2 and writes nothing until you
add `--yes-i-really-mean-it`:

```bash
firecube migrate run firecube:set-attrs \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --set title="Daily temperature" \
  --yes-i-really-mean-it
```

The refusal without the flag reads:

```text
Error: Confirmation required: migrate run changes the store. Re-run with --yes-i-really-mean-it, preview it with --dry-run, or run interactively.
```

Expected output for a Zarr v2 store with a consolidated `.zmetadata`:

```text
set /:title
class /:title: descriptive, allowed
write .zattrs
write .zmetadata (consolidated image)
outcome: executed
before snapshot fb7ffe6294783fd5b5ea227168566a4e01d0f83c17b00a628eddbc1c458c3567
wrote 2 of 2 planned documents
after snapshot ad48a4d79070aeec27462b37e7979254159e137f964200d6bdac82f30d9e9675
step firecube:set-attrs executed in run migrate-4b00e9dc-87f8-43be-b76a-d337e8552786
```

Expected output for a Zarr v3 store with consolidated metadata:

```text
set /:title
class /:title: descriptive, allowed
write zarr.json (consolidated image)
outcome: executed
before snapshot f9bdead6fb4b769a…
wrote 1 of 1 planned documents
after snapshot 30d9a1a1041c400e…
step firecube:set-attrs executed in run migrate-138ff41c-…
```

The built-in step takes these options:

| Option | Meaning |
|---|---|
| `--set KEY=VALUE` | Set an attribute to a string. Repeat the option for more attributes. |
| `--remove KEY` | Remove an attribute. Repeat the option for more attributes. |
| `--node PATH` | Change this node. The default is the root group. |

A step from a plugin takes `--option KEY=VALUE`. A key that you give once is a
string. A key that you give more than once is a list in the order given.

If the store records no product label, both a real run and a dry run exit 2 and
write nothing:

```text
Error: The product at file:///data/products/MY_PRODUCT.zarr records no product label. Record one with firecube migrate adopt --target file:///data/products/MY_PRODUCT.zarr --product-name <label>.
```

Run `firecube migrate adopt`, then run the step again.

## Verify

1. Read the recorded runs:

   ```bash
   firecube migrate status \
     --target "file:///data/products/MY_PRODUCT.zarr"
   ```

   Expected output resembles:

   ```text
   2026-11-04T00:31:44+00:00  migrate-2e97c56b-…  firecube:set-attrs  firecube 0.1.8  executed  complete
   ```

   The columns are the time, the run, the step, the distribution and version,
   the outcome (`executed` or `adopted`) and the status.

2. Compare the before and after snapshots. The identifiers come from the run
   output.

   ```bash
   firecube snapshot diff <before-id> <after-id> \
     --target "file:///data/products/MY_PRODUCT.zarr"
   ```

   The diff lists your attribute and `firecube_last_modifier`. It lists no
   other change. See [Snapshot Metadata](snapshot.md).

## Allow A Refused Change

Firecube sorts each attribute that a step sets or removes into a class. The
class decides whether the run accepts the change.

| Class | Examples | Rule |
|---|---|---|
| physical | `_ARRAY_DIMENSIONS`, `_FillValue`, every name that starts with `firecube_` | Firecube never changes it. No flag overrides this. |
| interpretation | `units`, `calendar`, `scale_factor`, `add_offset` | Firecube refuses it unless you pass `--allow interpretation`. |
| descriptive | `title`, `summary`, `history`, `long_name` | Firecube allows it. |
| unknown | Any other name | Firecube refuses it unless you pass `--allow unknown`. |

The [classifier table](../../guides/plugins/write-a-migration-step.md#choose-the-attributes-a-step-changes)
lists every name of each class.

An interpretation attribute changes how a reader decodes the values. Pass the
override only when you know that the stored values match the new attribute.

Example of a refusal on a Zarr v3 store:

```text
class /:units: interpretation, refused
Error: Step firecube:set-attrs changes attributes it may not change: /:units (interpretation). Pass --allow interpretation to change them.
```

Firecube records each allowed override in the metadata of the run.

## Run A Step Once Per Store

Firecube applies a step id once per store. If the step already ran on the
store, the command writes nothing and prints:

```text
step firecube:set-attrs already applied by run migrate-2e97c56b-…
```

This holds when you give other options. For `firecube:set-attrs`, put all
attributes of one change into one command. To change more attributes later, use
a step with another id.

The same step id at another version of its distribution is refused. An upgrade
of the plugin does not allow the id again. The message names both versions:

```text
Error: step 'my-plugin:0.1.0->0.2.0' ran as my-plugin 0.2.0 in run migrate-873ba7cd-9938-4dfa-9e58-77918a2b303e (complete), and the installed step is my-plugin 0.3.0
```

A step whose entry point has no distribution is refused. If another step or a
writer changed the documents of the first run, the command refuses. It names
the documents whose bytes match neither the planned before state nor the planned
after state.

## Recover From An Interrupted Run

A crash can leave the exclusive claim and an unfinished run in place.

1. Make sure that no process of the run is still running.
2. List the claim and the run:

   ```bash
   firecube chunks claims list --target "file:///data/products/MY_PRODUCT.zarr"
   firecube chunks runs list --target "file:///data/products/MY_PRODUCT.zarr" --status started
   ```

3. Clear the claim of the stopped migration. Do not abandon the run. A claim
   that is not marked stale needs `--force`:

   ```bash
   firecube chunks claims clear \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --domain MY_PRODUCT:maintenance:exclusive \
     --force --yes-i-really-mean-it
   ```

4. Run the same `migrate run` command again, with `--yes-i-really-mean-it` off a
   terminal. The rerun resumes the journal of
   the interrupted run under the same run id and applies the journaled bytes.
   Example for a run that stopped after the first of two document writes:

   ```text
   resuming the plan of run migrate-f0e1587e-…
   wrote 1 of 2 planned documents
   after snapshot 0d0f6dfc26c1e630…
   step firecube:set-attrs executed in run migrate-f0e1587e-…
   ```

The migration never clears a claim on its own.

### Give Up An Interrupted Run

Abandon the run only if you do not want its journal applied. An abandoned run
is excluded from resumption. The next `migrate run` plans afresh from the
current state of the documents. It uses a new run id and takes a new before
snapshot. It does not apply the bytes of the abandoned run.

```bash
firecube chunks runs abandon \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --run-id <run-id> \
  --reason "migrate process killed" \
  --yes-i-really-mean-it
```

This command also releases the claim that the run owns. After it, run the
same `migrate run` command again. If the interrupted run already wrote a
document, the new run can report `outcome: adopted`. The record of the old run
stays with the status `abandoned`.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `records no product label. Record one with firecube migrate adopt` (exit 2) | The store was not written by Firecube and is not adopted. The run wrote nothing. | Run `firecube migrate adopt`. See [Adopt A Foreign Store](adopt-a-foreign-store.md). |
| `Confirmation required: migrate run changes the store` (exit 2) | The run is off a terminal and has no `--yes-i-really-mean-it`. | Preview with `--dry-run`. Then add `--yes-i-really-mean-it`. |
| `Write-domain claim conflict for …:maintenance:exclusive` | Another run or a crash holds the claim. | Follow [Recover From An Interrupted Run](#recover-from-an-interrupted-run). |
| `changes attributes it may not change: … (interpretation)` | The step touches an interpretation attribute. | Pass `--allow interpretation`, or change the step. |
| `changes attributes it may not change: … (unknown)` | The attribute is in no list. | Pass `--allow unknown`, or use a descriptive name. |
| `changes attributes it may not change: … (physical)` | The step touches a physical attribute. | Remove the attribute from the step. No flag overrides this. |
| `the store has no node at '…'` | `--node` names a node that does not exist. | Read the paths in `snapshot show latest`. |
| `no installed migration step has the id '…'` | The step is not installed. | Run `firecube migrate list`. Install the plugin that provides the step. |
| `ran as … in run …, and the installed step is …` | The step id already ran at another version. | Do not run it again. Ask the plugin author for a step with a new id. |
| `hold bytes that are neither the planned before nor after state` | A later writer changed the documents of an earlier run of this step. | Restore the documents, or stop the writer that changed them. |
| `holds its own consolidated metadata` | A sub-group has a consolidated image. | Remove that image. Keep the root image only. |

## Next Steps

- **[Snapshot Metadata](snapshot.md)** — compare the before and after snapshots
- **[Create A Release](create-a-release.md)** — name the state after the change
- **[Migrate An Adopted Store](migrate-an-adopted-store.md)** — tutorial on a store that xarray wrote
- **[Recover Runs And Claims](../chunk-manager/recover.md)** — claim and run recovery in detail
