# Discover Zipped Data

## Goal

Your input files are `.zip` archives. Discovery finds them below
`--input-data` and passes each one to your plugin's `read_dataset` as a local
path, exactly like any other file. Opening the archive is the plugin's responsibility.

## Read The File Inside The Archive

Your generated plugin already has everything else: `build_dataset` passes each
local archive path to `read_dataset`, and `TIME_DIM` names the time dimension
of the file inside the archive.

The only change is `read_dataset`. Replace it with the version below and add
the imports it needs. It unpacks the archive into a temporary folder with the
[`extract_all_from_zips`](../../reference/core-utilities.md#firecube.core.api.extract_all_from_zips)
helper, reads the file inside, and lets the folder disappear at the end of the
`with` block:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

import xarray as xr

from firecube.core.api import extract_all_from_zips


# CHANGE THIS: the pattern of the data file inside your archives, e.g. "*.nc", "*.h5", "*.csv"
DATA_FILE_PATTERN = "*.CHANGE_ME"


def read_dataset(path: Path) -> xr.Dataset:
    """Read one source item as an ``xarray.Dataset`` with a ``TIME_DIM`` dimension."""
    with TemporaryDirectory() as folder:
        extracted, failures = extract_all_from_zips([path], lambda _: Path(folder))
        if failures:
            raise RuntimeError(f"Cannot read {path}: {failures[path]}")
        files = sorted(extracted[path].rglob(DATA_FILE_PATTERN))
        if len(files) != 1:
            raise RuntimeError(f"Expected one {DATA_FILE_PATTERN} file in {path}, found {files}")
        with xr.open_dataset(files[0]) as dataset:
            return dataset.load()
```

Set `DATA_FILE_PATTERN` to match the data file inside your archives.

## Verify

Run your plugin on two zips:

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

xr.open_zarr("out.zarr", group="default", consolidated=False)
```

Check that the output contains the timestamps and values from both input files.

## Common Mistakes

| Mistake | Fix |
|---|---|
| `xr.open_dataset("file.zip")` | A zip is not a dataset. Extract it first, as above. |
| Reading after the `with` block | The folder is gone. Call `.load()` inside the block. |
| Ignoring `failures` | A broken zip is reported there, not raised. Check it and raise. |
| One shared folder for many zips | Give every zip its own folder; a failure never deletes a shared one. |

## Next Steps

- [`extract_all_from_zips`](../../reference/core-utilities.md#firecube.core.api.extract_all_from_zips) — unpack archives, in parallel with `workers=`
- **[Append Datasets To Zarr](generic-zarr.md)** — where `read_dataset` and `TIME_DIM` fit
