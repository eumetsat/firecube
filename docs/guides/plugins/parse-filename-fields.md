# Parse Filename Fields

## Goal

Read a timestamp, sequence number, or other field that exists only in the
source filename, and use it in your plugin's dataset. Firecube's `parse_pattern`
helper uses [Trollsift](https://github.com/pytroll/trollsift) to extract the
fields.

Add the parser dependency to your plugin with `uv add 'firecube[patterns]'`.

## Example Inputs

Suppose your input directory contains these two NetCDF files:

```text
measurement_probe_0042_20260912T103000.nc
measurement_probe_0043_20260912T104500.nc
```

Each file contains one scalar variable called `value`: `2.5` in the first file
and `3.5` in the second. Their timestamps and sequence numbers appear only in
the filenames.

## Describe The Filename Format

The pattern describes the provider's filename convention. Text outside braces,
such as `measurement_` and `.nc`, must match literally. Each name inside braces
becomes a dictionary key; the part after `:` controls its conversion:

| Field or format part | Example input | Meaning | Parsed value |
|---|---|---|---|
| `{device}` | `probe` | Named text field | String `"probe"` |
| `{sequence:04d}` | `0042` | Zero-padded integer field of width four | Integer `42` |
| `{recorded:%Y%m%dT%H%M%S}` | `20260912T103000` | Named datetime field, built from the parts below | `datetime(2026, 9, 12, 10, 30)` |
| `%Y` in `recorded` | `2026` | Four-digit year | Year: `2026` |
| `%m` in `recorded` | `09` | Two-digit month | Month: `9` (September) |
| `%d` in `recorded` | `12` | Two-digit day | Day: `12` |
| `T` in `recorded` | `T` | Literal separator between date and time | No value; must match the filename |
| `%H` in `recorded` | `10` | Hour on a 24-hour clock | Hour: `10` |
| `%M` in `recorded` | `30` | Two-digit minute | Minute: `30` |
| `%S` in `recorded` | `00` | Two-digit second | Second: `0` |
| Timezone of `recorded` | Absent from this filename | Trollsift does not infer a timezone | `tzinfo=None` |
| Plugin timezone convention | Source times defined as UTC in this example | The plugin records the provider's convention | `timezone="UTC"` on the output time coordinate |

## Parse The Name In `read_dataset`

Your generated plugin already has everything else. Set `TIME_DIM`, add
`PATTERN` next to it, and replace `read_dataset` with the version below. Add
the imports your plugin does not have yet:

```python
from pathlib import Path

import numpy as np
import xarray as xr

from firecube.ingestor.extensions import parse_pattern

TIME_DIM = "timestamp"
PATTERN = "measurement_{device}_{sequence:04d}_{recorded:%Y%m%dT%H%M%S}.nc"


def read_dataset(path: Path, filename: str) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    try:
        fields = parse_pattern(PATTERN, filename)
    except ValueError as exc:
        raise ValueError(f"Unexpected measurement filename: {filename}") from exc

    with xr.open_dataset(path) as source:
        value = float(source["value"].item())

    dataset = xr.Dataset(
        {
            "value": (TIME_DIM, [value]),
            "sequence": (TIME_DIM, [fields["sequence"]]),
        },
        coords={TIME_DIM: [np.datetime64(fields["recorded"], "ns")]},
    )
    dataset[TIME_DIM].attrs["timezone"] = "UTC"
    return dataset
```

Then pass the filename to `read_dataset` from `build_dataset`. Take it from
the source item, not from `path`: a remote file is downloaded under a hashed
name.

```python
def build_dataset(self, group, items, ctx):
    datasets = [read_dataset(ctx.materialize(item), Path(item).name) for item in items]
    dataset = xr.concat(datasets, dim=TIME_DIM, data_vars="minimal", coords="minimal")
    return dataset.sortby(TIME_DIM)
```

## The Complete Plugin

The generated `ingestor.py` with both changes in place:

```python
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)
from firecube.ingestor.extensions import parse_pattern

TIME_DIM = "timestamp"
PATTERN = "measurement_{device}_{sequence:04d}_{recorded:%Y%m%dT%H%M%S}.nc"


def read_dataset(path: Path, filename: str) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    try:
        fields = parse_pattern(PATTERN, filename)
    except ValueError as exc:
        raise ValueError(f"Unexpected measurement filename: {filename}") from exc

    with xr.open_dataset(path) as source:
        value = float(source["value"].item())

    dataset = xr.Dataset(
        {
            "value": (TIME_DIM, [value]),
            "sequence": (TIME_DIM, [fields["sequence"]]),
        },
        coords={TIME_DIM: [np.datetime64(fields["recorded"], "ns")]},
    )
    dataset[TIME_DIM].attrs["timezone"] = "UTC"
    return dataset


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_plugin"
    time_dim_name: ClassVar[str] = TIME_DIM

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        if not items:
            return None
        datasets = [read_dataset(ctx.materialize(item), Path(item).name) for item in items]
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

## How It Works

For the first filename, `parse_pattern` returns `device="probe"`, `sequence=42`,
and `recorded=datetime(2026, 9, 12, 10, 30)`.

`read_dataset` reads `value` from the local file and returns a dataset with one
step along `TIME_DIM`, using `recorded` as its time and storing `sequence`
alongside the measurement. The generated `build_dataset` concatenates the
datasets of a batch along `TIME_DIM` and sorts them by time.

With the two example inputs, the batch dataset contains:

| timestamp | sequence | value |
|---|---|---|
| 2026-09-12 10:30:00 | 42 | 2.5 |
| 2026-09-12 10:45:00 | 43 | 3.5 |

The `device` field is available if your plugin needs it; this example uses
only the timestamp and sequence number. Change the literal text and named fields in
`PATTERN` to match your provider's convention, then use the corresponding
keys in `fields`.

A malformed filename or invalid date raises an error naming the input.
Use [`--input-filters`](source-discovery.md) to exclude unwanted
files before they reach `read_dataset`.

## Verify

Run your plugin on the two example files:

```bash
firecube ingest my_plugin \
  --input-data /data/raw \
  --target "file://$PWD/out.zarr" \
  --product-name my_product \
  --write-mode direct
```

Then open the result:

```python
import xarray as xr

cube = xr.open_zarr("out.zarr", group="default", consolidated=False)
print(cube[["sequence", "value"]].to_dataframe())
```

Expected output:

```text
                     sequence  value
timestamp                           
2026-09-12 10:30:00        42    2.5
2026-09-12 10:45:00        43    3.5
```

Both timestamps and sequence numbers come from the file names; the values
come from the files.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Passing the full path to `parse_pattern` | The pattern describes the file name. Parse the name, not the path. |
| Parsing `path.name` inside `read_dataset` | A remote source item is downloaded under another name. Pass `Path(item).name` from `build_dataset`, as above. |
| Every file fails with `Unexpected measurement filename` | Check `PATTERN` itself: a malformed pattern raises the same `ValueError`. |
| Writing `{sequence}` for `0042` | Without `:04d` the field stays the string `"0042"`. Declare the width and type. |
| Treating `recorded` as timezone-aware | Trollsift returns naive datetimes. Record the provider's convention on the time coordinate, as the example does with `timezone="UTC"`. |
| Catching the parse error and skipping the file | A skipped file leaves no trace in the product. Raise, and exclude unwanted files with `--input-filters` instead. |
| Literal text that differs from the filename | Every character outside braces must match exactly, including separators such as `_` and `.nc`. |

## Next Steps

- **[Parser Reference](../../reference/extensions.md#firecube.ingestor.extensions.parse_pattern)** — matching, errors, and datetime behavior
- **[Append Datasets To Zarr](generic-zarr.md)** — where `read_dataset` and `TIME_DIM` fit, and how to verify the store
