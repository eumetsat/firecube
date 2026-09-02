# Implement GenericZarrIngestor

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
product name, and replace the `build_dataset` stub.

## Implement `build_dataset`

This example reads a batch of NetCDF files with `xarray` and appends them
along `time_dim_name`; replace the file format and variable selection with
what the product's data actually needs:

```python
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext, register_ingestor


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"
    time_dim_name: ClassVar[str] = "time"

    def build_dataset(
        self,
        group: str,  # Called once per output group; most plugins ignore this and use "default".
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        if not items:
            return None

        paths = [ctx.materialize(item) for item in items]
        dataset = xr.open_mfdataset(paths, combine="by_coords")
        return dataset.sortby(self.time_dim_name)
```

See the [Plugin Templates](../../reference/templates.md#genericzarringestor)
for the exact hook signature and optional group, path, and writer
customizations, or the quickstart plugin's
[`build_dataset` implementation](https://github.com/eumetsat/firecube-quickstart-plugin/blob/main/src/firecube_quickstart_plugin/ingestor.py)
for a complete, runnable version of this example.

## Verify

First check registration and configuration:

```bash
cd firecube-my-plugin
uv run firecube plugins describe my_plugin
uv run firecube ingest my_plugin --show-options
```

Then ingest a small, representative input supported by the product reader:

```bash
uv run firecube ingest my_plugin \
  --input-data ./path/to/input \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

Open the written group with the product's normal reader. Confirm the expected
variables and coordinates, the append-dimension values, and at least one known
data value. Run the same input again and confirm that the product remains
consistent with the plugin's resume policy.

If built-in discovery does not include the product's source names, pass
`include_patterns` or customize discovery before verifying ingestion.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Returning an unordered dataset | Sort on `time_dim_name` before returning. |
| Returning incompatible batch schemas | Normalize dimensions, coordinates, variables, and data types in the product reader. |
| Passing a remote URI to a local-only reader | Resolve each item with `ctx.materialize(item)`. |
| Starting another append writer for the same group | Keep appends to one group serialized. |
| Setting `time_dim_name` to a name absent from the returned dataset | Match the dataset's dimension name exactly, or the write raises a `ValueError`. |

## Next Steps

- **[GenericZarrIngestor (Append)](../../concepts/output-formats/zarr/generic-append.md)** — understand ordering and serialized group writes
- **[Quickstart](../../quickstart/index.md)** — create and run a complete local plugin with this template
- **[NetCDF To Zarr](../../tutorials/weather-netcdf.md)** — inspect the example plugin and verify its stored values
- **[Route Writes To Multiple Groups](multi-group-writes.md)** — write more than the single default group
- **[Add Plugin Configuration Options](add-config-options.md)** — declare typed options the plugin validates before ingestion
- **[Plugin Templates](../../reference/templates.md)** — look up the public template types
