# Route Writes To Multiple Groups

## Goal

Write each batch to more than one group in the same Zarr store, for example
the measurements under `data` and their flags under `flags`.

## Split One Dataset Into Groups

Add `get_batch_groups` to your generated plugin class and replace the
generated `build_dataset` with the one below. Firecube calls
`get_batch_groups` once per batch, then calls `build_dataset` once for each
name it returned, passing the same batch every time. The last three lines keep
only the variables that belong to the group being built:

```python
def get_batch_groups(self, items, ctx):
    return ["data", "flags"]  # sorted, always the same: these become the group paths


def build_dataset(self, group, items, ctx):
    datasets = [read_dataset(ctx.materialize(item)) for item in items]
    dataset = xr.concat(datasets, dim=TIME_DIM, data_vars="minimal", coords="minimal")
    dataset = dataset.sortby(TIME_DIM)
    if group == "flags":
        return dataset[["flag"]]
    return dataset[["value"]]
```

`read_dataset` and `TIME_DIM` are the ones you already set in
[Append Datasets To Zarr](generic-zarr.md).

## Groups From Separate Files

When each group has its own files, for example `data_20240101.nc` and
`flags_20240101.nc` in the same directory, use this `build_dataset` instead.
The first line keeps the files whose name contains the group name, so for
`group="flags"` only the `flags_*.nc` files of the batch are read. A batch is
cut by count, not by product, so it can hold no file for a group at all; then
the list is empty and returning `None` skips that group for this batch:

```python
def build_dataset(self, group, items, ctx):
    group_items = [item for item in items if group in str(item)]
    if not group_items:
        return None  # this batch has no file for this group
    datasets = [read_dataset(ctx.materialize(item)) for item in group_items]
    dataset = xr.concat(datasets, dim=TIME_DIM, data_vars="minimal", coords="minimal")
    return dataset.sortby(TIME_DIM)
```

`DirectZarrIngestor` works differently: it declares its groups in
`zarr_schema` and names the group on each write. See
[Declare The Schema And Index](direct-zarr.md).

## Verify

Run your plugin on a small input, then open one group and confirm it holds
only its own variables. Swap the group name to check the next one:

```python
import xarray as xr

print(xr.open_zarr("/tmp/my_plugin_out.zarr", group="flags", consolidated=False))
```

## Common Mistakes

| Mistake | Fix |
|---|---|
| Every group ends up with every variable | `build_dataset` returned the whole dataset. Return only the group's variables, as in the last three lines above. |
| Groups differ between runs | `get_batch_groups` must return the same sorted list every time. |

## Next Steps

- **[Append Datasets To Zarr](generic-zarr.md)** — the generated plugin these methods go into
- **[Declare The Schema And Index](direct-zarr.md)** — groups on `DirectZarrIngestor`
