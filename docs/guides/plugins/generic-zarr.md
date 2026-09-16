# Append Datasets To Zarr

## Goal

Implement a plugin that converts each batch into a complete
`xarray.Dataset`. Firecube appends each returned dataset to the selected Zarr
group along the plugin's append dimension.

Use this class when complete, ordered dataset batches are the product's natural
write unit.

The source file format does not determine the class. Read
[GenericZarrIngestor (Append)](../../concepts/output-formats/zarr/generic-append.md)
for the write and concurrency model.

## Edit The Plugin Class

Follow [Create a Plugin](create-a-plugin.md), select `zarr` and the `xarray`
write strategy, then [install the plugin](install-a-plugin.md).

Edit `src/firecube_my_plugin/ingestor.py`. Keep the generated registration and
product name, then work through the three steps below in order. Ingestion
stops with `NotImplementedError` until `TIME_DIM` and `read_dataset` are done.

## Set `TIME_DIM`

`TIME_DIM` is the name of the time dimension in the datasets `read_dataset`
returns. Firecube appends each batch to the store along it, and the store
keeps this name. Set it to the name your files use:

```python
TIME_DIM = "time"
```

## Define `read_dataset`

`read_dataset` turns one source item into an `xarray.Dataset`. `path` is
always a local file: `build_dataset` downloads remote source items first.
`.load()` reads the data into memory before the file closes. Select or rename
variables here when the store should not keep everything in the file:

```python
def read_dataset(path: Path) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    with xr.open_dataset(path) as dataset:
        return dataset.load()
```

When the file has no time dimension, `read_dataset` adds it. This version
takes the time from the file name with `parse_pattern`, which needs
`uv add 'firecube[patterns]'`, and opens the group of the file that holds the
variables:

```python
import numpy as np

from firecube.ingestor.extensions import parse_pattern

PATTERN = "measurement_{recorded:%Y%m%d}.nc"


def read_dataset(path: Path, filename: str) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    fields = parse_pattern(PATTERN, filename)
    with xr.open_dataset(path, group="PRODUCT") as source:
        dataset = source.load()
    return dataset.expand_dims({TIME_DIM: [np.datetime64(fields["recorded"], "ns")]})
```

## Implement `build_dataset`

`build_dataset` reads every source item of a batch with `read_dataset`,
concatenates the batch along `TIME_DIM`, and sorts it. Keep the four
`xr.concat` keywords; see [Common Mistakes](#common-mistakes):

```python
    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        if not items:
            return None
        datasets = [read_dataset(ctx.materialize(item)) for item in items]
        dataset = xr.concat(
            datasets,
            dim=TIME_DIM,
            data_vars="minimal",
            coords="minimal",
            compat="equals",
            join="exact",
        )
        return dataset.sortby(TIME_DIM)
```

With the file-name version of `read_dataset`, pass the name along. Take it
from the source item, because a remote file is downloaded under a hashed name:

```python
    datasets = [read_dataset(ctx.materialize(item), Path(item).name) for item in items]
```

## The Complete Plugin

This plugin reads a batch of NetCDF files with `xarray` and appends them along
`TIME_DIM`; replace the file format and variable selection with what the
product's data actually needs:

```python
from pathlib import Path
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)

TIME_DIM = "time"


def read_dataset(path: Path) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    with xr.open_dataset(path) as dataset:
        return dataset.load()


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_plugin"
    time_dim_name: ClassVar[str] = TIME_DIM

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        if not items:
            return None
        datasets = [read_dataset(ctx.materialize(item)) for item in items]
        dataset = xr.concat(
            datasets,
            dim=TIME_DIM,
            data_vars="minimal",
            coords="minimal",
            compat="equals",
            join="exact",
        )
        return dataset.sortby(TIME_DIM)
```

See the [Plugin Templates](../../reference/templates.md#genericzarringestor)
for the exact hook signature and optional group, path, and writer
customizations, or [Firecube 101: NetCDF To Zarr](../../showcase/netcdf-to-zarr.ipynb)
for a notebook that creates a plugin and checks the written store.

When one local file path is not enough, these guides show `read_dataset` for
the layout and, where needed, the `build_dataset` change that goes with it:

| Your source data | Guide |
|---|---|
| Files Firecube does not find by default, or files to exclude | [Discover Source Data](source-discovery.md) |
| Data inside `.zip` archives | [Discover Zipped Data](discover-zipped-data.md) |
| Times or other values that exist only in the file name | [Parse Filename Fields](parse-filename-fields.md) |
| Two files per source item, such as data plus metadata | [Read Paired Source Files](paired-source-files.md) |

## Verify

First check registration and configuration:

```bash
cd firecube-my-plugin
firecube plugins describe my_plugin
firecube ingest my_plugin --show-options
```

Then ingest a small, representative input supported by the product reader:

```bash
firecube ingest my_plugin \
  --input-data ./path/to/input \
  --target file:///tmp/my_plugin_out.zarr \
  --product-name my_plugin \
  --output-format zarr \
  --write-mode direct
```

Open the `default` group and confirm the expected variables and coordinates,
the `TIME_DIM` values, and at least one known data value:

```python
import xarray as xr

print(xr.open_zarr("/tmp/my_plugin_out.zarr", group="default", consolidated=False))
```

A second run into the same store is refused. Add
`--option resume_existing=true` to skip timestamps already written and append
new ones.

If built-in discovery does not include the product's source names, pass
`--input-filters` or customize discovery before verifying ingestion.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Leaving `TIME_DIM` empty | Ingestion stops with `TIME_DIM is not set`. Set it to the time dimension `read_dataset` returns. |
| `TIME_DIM` names a dimension the dataset does not have | `build_dataset` raises `ValueError` listing the dimensions it found. Set `TIME_DIM` to that name, or rename the dimension in `read_dataset`. |
| Returning data read lazily after the file closed | Call `.load()` inside the `with` block. |
| Returning incompatible schemas from different files | Normalize dimensions, coordinates, variables, and data types in `read_dataset`. |
| `xarray.MergeError: conflicting values for variable` | A variable without `TIME_DIM` differs between files of one batch. If it varies per file, add `TIME_DIM` to its dimensions in `read_dataset`; otherwise make the files agree. |
| `xarray.AlignmentError: cannot align objects with join='exact'` | The grid coordinates differ between files of one batch. Make them match in `read_dataset`. |
| Removing an `xr.concat` keyword from `build_dataset` | The four keywords keep static variables and bounds from being copied per time step and stop silent mismatches. Keep them, also in your own `build_dataset`. |
| `SchemaDriftError` on append | A variable without `TIME_DIM` changed, appeared, or was force-reingested with a different value. Firecube writes such variables once and refuses to change them. Add `TIME_DIM` to it in `read_dataset` if it varies per file, or create a new store. |
| `Group attributes differ from stored; keeping first-write values` on every append | Group attributes are written once. Per-file attributes such as `history` or `date_created` differ every time; drop them in `build_dataset` with `dataset.attrs.pop("history", None)` before returning. Keep attributes that describe the whole dataset. |
| Starting another append writer for the same group | Keep appends to one group serialized. |
| Opening the target store from a hook while workers are writing | Wrap the access in `with self.write_lock:` so it waits for the current batch write. |

## Next Steps

- **[GenericZarrIngestor (Append)](../../concepts/output-formats/zarr/generic-append.md)** — understand ordering and serialized group writes
- **[Quickstart](../../quickstart/index.md)** — run an installed NetCDF-to-Zarr plugin
- **[Firecube 101: NetCDF To Zarr](../../showcase/netcdf-to-zarr.ipynb)** — inspect the example plugin and verify its stored values
- **[Route Writes To Multiple Groups](multi-group-writes.md)** — write more than the single default group
- **[Configure Zarr Chunking](configure-zarr-chunking.md)** — set chunk shape, and from there compression and sharding, before the first real run
- **[Add Plugin Configuration Options](add-config-options.md)** — declare typed options the plugin validates before ingestion
- **[NetCDF Preparation](../../reference/core-utilities.md#firecube.core.api.prepare_netcdf_for_zarr)** and **[String Normalization](../../reference/core-utilities.md#firecube.core.api.normalize_string_vars)** — prepare arrays for writing
- **[Add Plugin Telemetry](observability.md)** — emit metrics and use standard module logging
- **[Plugin Templates](../../reference/templates.md)** — look up the public template types
