# Migrate An Adopted Store

In this tutorial you add a title to a Zarr store that xarray wrote and that you
adopted. You compare a snapshot from before the change with one from after it.
You name the result as a release. At the end you read the store with plain zarr
and xarray.

## Prerequisites

- Complete [Adopt A Foreign Store](adopt-a-foreign-store.md) with the sample
  store. The tutorial starts from that store, adopted as the product
  `MY_PRODUCT`.
- Install Firecube and activate its virtual environment.
- Stop every writer of the store.

## 1. Read The Store Before The Change

Open the store with plain zarr and xarray. Keep the output for step 6.

```python
import zarr
import xarray as xr

path = "/data/products/MY_PRODUCT.zarr"
group = zarr.open_group(path, mode="r", use_consolidated=True)
print("zarr format:", group.metadata.zarr_format)
print("zarr title:", dict(group.attrs).get("title"))
ds = xr.open_zarr(path, consolidated=True)
print("xarray title:", ds.attrs.get("title"))
print("temperature sum:", float(ds["temperature"].sum()))
```

Expected output:

```text
zarr format: 2
zarr title: None
xarray title: None
temperature sum: 276.0
```

## 2. Preview The Change

```bash
firecube migrate run firecube:set-attrs \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT \
  --set title="Daily temperature" \
  --dry-run
```

Expected output:

```text
set /:title
class /:title: descriptive, allowed
write .zattrs
write .zmetadata (consolidated image)
planned outcome: executed
```

The plan writes two documents: the root `.zattrs` and the consolidated image.
The command wrote nothing.

## 3. Run The Migration

```bash
firecube migrate run firecube:set-attrs \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT \
  --set title="Daily temperature"
```

Expected output resembles:

```text
set /:title
class /:title: descriptive, allowed
write .zattrs
write .zmetadata (consolidated image)
outcome: executed
before snapshot 4d6472579a4e39ff…
wrote 2 of 2 planned documents
after snapshot 836fa76e07510d1c…
step firecube:set-attrs executed in run migrate-bd606a74-…
```

Copy the two snapshot identifiers. They are the coherent snapshots from before
and after the change. The sample shortens each identifier. Firecube prints 64
hexadecimal digits.

## 4. Compare The Snapshots

```bash
firecube snapshot diff <before-id> <after-id> \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

Expected output resembles:

```text
Before:  4d6472579a4e39ff…
After:   836fa76e07510d1c…
~ .zattrs
~ .zmetadata
Nodes:
  Node: (root)
    attribute added: firecube_last_modifier = {"at": "2026-11-04T03:30:52.499239+00:00", "distribution": "firecube", "operation": "migrate", "run_id": "migrate-bd6...
    attribute added: title = "Daily temperature"
```

Two documents changed. Two root attributes appeared: your `title` and
`firecube_last_modifier`. No array document changed.

## 5. Create And Verify A Release

```bash
firecube release create 1.0.0 \
  --note "title added" \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
firecube release verify 1.0.0 \
  --target "file:///data/products/MY_PRODUCT.zarr" \
  --product-name MY_PRODUCT
```

Expected output of `release verify`:

```text
Release 1.0.0: metadata matches snapshot 45505a8e596f2a9d…
```

The command exits 0. The release stamped the root attribute `firecube_release`
with the version `1.0.0`.

## 6. Read The Store With Plain zarr And xarray

Run the code of step 1 again. Expected output:

```text
zarr format: 2
zarr title: Daily temperature
xarray title: Daily temperature
temperature sum: 276.0
```

Both libraries read the new title. The data sum is the same as before the
change.

## Next Steps

- **[Migrate Metadata](migrate.md)** — options, attribute classes and recovery
- **[Create A Release](create-a-release.md)** — list, show and recover a release
- **[Verify A Release](verify-a-release.md)** — detect drift of the metadata
- **[Snapshot Metadata](snapshot.md)** — list and show snapshots
