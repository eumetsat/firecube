# Adopt A Foreign Store

Use `firecube migrate adopt` to give a Zarr store that Firecube did not write a
control plane. A foreign store is, for example, a store that xarray wrote.

Adoption records the store as an imported product with unverified chunks. It
writes `.firecube/schema.json` and one `adopt` run. It writes no Zarr document
and no chunk, and it invents no history of ingested spans.

Two commands need a control plane: a real `firecube migrate run` and
`firecube release create`. Both refuse a store that has none and write nothing.
`firecube migrate run --dry-run` still prints the plan. The `firecube snapshot`
commands work on a store that has no control plane.

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

1. Take a snapshot of the metadata before the adoption:

   ```bash
   firecube snapshot take \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --product-name MY_PRODUCT
   ```

   Copy the identifier from the `Snapshot:` line.

2. Adopt the store:

   ```bash
   firecube migrate adopt \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --product-name MY_PRODUCT
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

## Verify

1. Take a second snapshot and compare the two:

   ```bash
   firecube snapshot take \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --product-name MY_PRODUCT
   firecube snapshot diff <before-id> latest \
     --target "file:///data/products/MY_PRODUCT.zarr" \
     --product-name MY_PRODUCT
   ```

   Expected output resembles:

   ```text
   Before:  c8aef7318ceb252a…
   After:   6d2973fa3e0188da…
   + .firecube/schema.json
   ```

   The only change is the new control-plane document. No Zarr document
   changed.

2. List the runs of the product:

   ```bash
   firecube chunks runs list --product-name "file:///data/products/MY_PRODUCT.zarr"
   ```

   Expected output resembles:

   ```text
   Run ID                               Status     State    Parts Events
   --------------------------------------------------------------------------------
   adopt-63a5a61d-e80f-416a-9e10-d70b64229859 complete   active   2     2
   ```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `the store has no root group document (zarr.json or .zgroup)` | The target is not a Zarr store, or the path is wrong. The command wrote nothing. | Correct the `--target` path. Create the root group, then adopt again. |
| `Warning: the store of product … is Zarr format 2` | The store is Zarr format 2. Firecube cannot append to it. | Convert the store to Zarr format 3 outside Firecube before you ingest into it. Maintenance commands still work on the store. |
| `the store of product … has no control plane; adopt it with firecube migrate adopt before running a migration` | `migrate run` found no control plane. It exits 1 and writes nothing. | Run `firecube migrate adopt`. Run the migration again. |
| `the store of product … has no control plane; a real run needs firecube migrate adopt first` | `migrate run --dry-run` found no control plane. It prints the plan and writes nothing. | Run `firecube migrate adopt` before the real run. |

## Next Steps

- **[Migrate An Adopted Store](migrate-an-adopted-store.md)** — tutorial: add a title, compare snapshots and create a release
- **[Migrate Metadata](migrate.md)** — change attributes of an adopted store
- **[Snapshot Metadata](snapshot.md)** — more on the snapshot commands
