# Adopt A Foreign Store

Use `firecube migrate adopt` to give a Zarr store that Firecube did not write a
control plane. A foreign store is, for example, a store that xarray wrote.

Adoption records the store as an imported product with unverified chunks. It
writes `.firecube/schema.json`, the product label in `.firecube/product.json`
and one `adopt` run. It writes no Zarr document and no chunk, and it invents no
history of ingested spans.

A command that needs the recorded product label refuses a store that records
none. The refusal exits 2 and names `firecube migrate adopt`. This holds for
`snapshot`, `release`, `migrate run` and `chunks`. Pure-storage readers and first
writes, such as `zarr validate` and `archive create`, do not need the label.
A real `migrate run` and `release create` also need the control plane that
adoption writes.

## Prerequisites

- Install Firecube and activate its virtual environment.
- Make sure that you can write to the store.
- Stop every writer of the store. See
  [Maintenance Operations](index.md#prerequisites).
- Make sure that the store has a root group document: `zarr.json` for Zarr
  format 3, or `.zgroup` for Zarr format 2.

The commands on this page use a sample store. Create it with xarray, or use your
own store and change the path:

```python
import numpy as np
import xarray as xr

ds = xr.Dataset(
    {"temperature": ("time", np.arange(24.0))},
    coords={"time": np.arange(24)},
)
ds.to_zarr("/data/products/MY_PRODUCT.zarr", zarr_format=2, consolidated=True)
```

## Read The Store Format

Open the store with plain zarr. Read its format before you adopt it.

```python
import zarr

group = zarr.open_group("/data/products/MY_PRODUCT.zarr", mode="r", use_consolidated=True)
print("zarr format:", group.metadata.zarr_format)
```

Expected output:

```text
zarr format: 2
```

## Adopt The Store

1. Preview the adoption. A dry run writes nothing:

   ```bash
   firecube migrate adopt \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --product-name MY_PRODUCT \
     --dry-run
   ```

   Expected output resembles:

   ```text
   would adopt the store as product MY_PRODUCT in one adopt run (Zarr format 2); nothing is written
   product label MY_PRODUCT would be recorded
   ```

2. Adopt the store. On a terminal, the command asks for confirmation. Off a
   terminal, the command exits 2 until you add `--yes-i-really-mean-it`:

   ```bash
   firecube migrate adopt \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --product-name MY_PRODUCT \
     --yes-i-really-mean-it
   ```

   Expected output for a Zarr format 2 store resembles:

   ```text
   adopted the store as product MY_PRODUCT (run adopt-63a5a61d-…, Zarr format 2)
   Warning: the store of product MY_PRODUCT is Zarr format 2. Firecube cannot append to it, so ingest and preallocate do not support this store. Convert it to Zarr format 3 outside Firecube before you ingest into it.
   ```

   The command prints the warning on stderr, after the line that names the run.
   The adoption succeeds and exits 0. Add `-f json` to print the result as JSON
   on stdout. The progress lines then go to stderr.

   A Zarr format 3 store prints the first line only:

   ```text
   adopted the store as product MY_PRODUCT (run adopt-27c54b51-…, Zarr format 3)
   ```

If the store is already a product, the command writes nothing, prints no
warning and exits 0:

```text
product MY_PRODUCT already has a control plane; nothing to adopt
```

If the store records another label, the command exits 2 and writes nothing:

```text
Error: Product label 'OTHER' is refused: the product at file:///data/products/MY_PRODUCT.zarr records the label 'MY_PRODUCT'.
```

## Verify

1. Take a snapshot. The snapshot lists the control-plane document and the Zarr
   documents that the store held before the adoption:

   ```bash
   firecube snapshot take \
     --target "file:///data/products/MY_PRODUCT.zarr"
   firecube snapshot show latest \
     --target "file:///data/products/MY_PRODUCT.zarr"
   ```

   Expected output resembles:

   ```text
   Snapshot:     9116405736e4a6e5c75489614fea736b4877f9737c763377fa792fb7f26dc4a8
   Parent:       -
   Manifest:     b567ed8dc5c0789cbc2e363d139351da3b579b443f3c993418c1ead112187b59
   Taken:        2026-11-07T12:08:54+00:00
   Consistency:  observation
   Trigger:      cli
   Run:          -
   Documents:    8
     .firecube/schema.json  firecube_schema  87 B  sha256:58d5516a75dc
     .zattrs  v2_attrs  2 B  sha256:44136fa355b3
     ...
   ```

   The adoption changed no Zarr document. The page
   [Snapshot Metadata](snapshot.md) describes the other snapshot commands.

2. List the runs of the product:

   ```bash
   firecube chunks runs list --target "file:///data/products/MY_PRODUCT.zarr"
   ```

   Expected output resembles:

   ```text
   Run ID                               Status     State    Parts Events
   --------------------------------------------------------------------------------
   adopt-4d47c395-186c-494b-9a7f-f443999d852a complete   active   2     2
   ```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `the store has no root group document (zarr.json or .zgroup)` | The target is not a Zarr store, or the path is wrong. The command wrote nothing. | Correct the `--target` path. Create the root group, then adopt again. |
| `Warning: the store of product … is Zarr format 2` | The store is Zarr format 2. Firecube cannot append to it. | Convert the store to Zarr format 3 outside Firecube before you ingest into it. Maintenance commands still work on the store. |
| `records no product label. Record one with firecube migrate adopt` (exit 2) | `migrate run` or `migrate run --dry-run` found a store that records no label. It writes nothing. | Run `firecube migrate adopt`. Run the migration again. |

## Next Steps

- **[Migrate An Adopted Store](migrate-an-adopted-store.md)** — tutorial: add a title, compare snapshots and create a release
- **[Migrate Metadata](migrate.md)** — change attributes of an adopted store
- **[Snapshot Metadata](snapshot.md)** — more on the snapshot commands
