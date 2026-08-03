# Implement GenericZarrIngestor

## Goal

Implement a plugin that converts each batch into a complete
`xarray.Dataset`. Firecube appends each returned dataset to the selected Zarr
group along the plugin's append dimension.

Use this class when complete, ordered dataset batches are the product's natural
write unit. Firecube determines the next position from the current group length;
the `build_dataset` call and append run inside one serialized Zarr write section.
Pipeline workers do not make appends to the same group concurrent.

The source file format does not determine the class. Read
[GenericZarrIngestor (Append)](../../concepts/output-formats/zarr/generic-append.md)
for the write and concurrency model.

## Edit The Plugin Class

Follow [Create a Plugin](create-a-plugin.md), select `zarr` and the `xarray`
write strategy, then [install the plugin](install-a-plugin.md).

Edit `src/firecube_my_plugin/ingestor.py`. Keep the generated registration and
product name, and replace the `build_dataset` stub.

## Implement `build_dataset`

The source reader is product-specific. The example below shows the template
boundary without prescribing a source format:

```python
from pathlib import Path
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext, register_ingestor


def read_product_items(paths: list[Path]) -> xr.Dataset:
    ...


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"
    time_dim_name: ClassVar[str] = "time"

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        if not items:
            return None

        paths = [ctx.materialize(item) for item in items]
        dataset = read_product_items(paths)
        return dataset.sortby(self.time_dim_name)
```

`read_product_items` represents the reader and normalization code for the
product. Return a loaded dataset whose file handles do not depend on an already
closed source context.

The returned dataset must contain `time_dim_name`, be ordered on that
dimension, and use values that do not overlap another batch. Its variables,
dimensions, coordinates, and data types must remain compatible between
batches. Return `None` when the batch has no data to write.

Firecube calls the hook once for each output group. Most plugins use the
`"default"` group and do not branch on `group`.

See the [Plugin Template API](../../reference/api.md#genericzarringestor) for
the exact hook signature and template configuration.

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

## Next Steps

- **[GenericZarrIngestor (Append)](../../concepts/output-formats/zarr/generic-append.md)** — understand ordering and serialized group writes
- **[NetCDF To Zarr](../../tutorials/weather-netcdf.md)** — follow a complete plugin tutorial
- **[Read Plugin Source Data](storage-access.md)** — use source items with local-path readers
- **[Plugin Template API](../../reference/api.md)** — look up the public template types
