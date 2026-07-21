# GenericParquetIngestor

`GenericParquetIngestor` is the batteries-included template for tabular output.
You return a `pyarrow.Table` or `pandas.DataFrame` and the engine writes one
Parquet file per group/batch. Scaffold one with
`firecube plugins create my-plugin --template parquet` (see
[Create a Plugin](create-a-plugin.md)).

For how Parquet output behaves (independent files, fully parallel), see
[Parquet](../output-formats/parquet.md).

## Required hook

| Hook | Signature | Purpose |
| :--- | :--- | :--- |
| **`build_dataset`** | `(group, batch, ctx) -> pyarrow.Table \| pandas.DataFrame \| None` | Build the table for a group from the batch. Return `None` to skip. |

!!! note
    The Parquet `build_dataset` takes the whole `batch`, unlike the Zarr
    template's `(group, items, ctx)` form.

## Optional hooks

| Hook | Signature | Purpose |
| :--- | :--- | :--- |
| `write_parquet` | — | Override the default writer (e.g. GeoParquet metadata, custom compression). |
| `discover_source_files` | `(ctx)` | Override file discovery. Default recursive discovery is built in — see [Weather CSV: Source Discovery](../../tutorials/source-discovery.md). |
| `batch_setup` | `(ctx)` | Initialize per-batch resources. |
| `prepare_batch_data` | `(batch, ctx)` | Parse/prepare once per batch; store intermediate data in `batch.metadata`. |
| `cleanup_batch_data` | `(batch, ctx)` | Clean up batch metadata/resources. |
| `batch_teardown` | `(ctx)` | Tear down per-batch resources. |

## Minimal example

```python
from __future__ import annotations

from typing import ClassVar

import pyarrow as pa

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
    PluginContext,
    register_ingestor,
)


@register_ingestor("my_table_plugin")
class MyTableIngestor(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_table_product"
    name = "my_table_plugin"

    def build_dataset(
        self,
        group: str,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> pa.Table | None:
        # Parse batch.items into columnar form and return a pyarrow.Table.
        ...
```

## Next Steps

- **[Plugin Contract](contract.md)** — check `PRODUCT_NAME` and typed result requirements
- **[Parquet](../output-formats/parquet.md)** — understand Parquet output behavior
- **[Sentinel-3 FRP Plugin](../../tutorials/sentinel3-frp.md)** — build a small Parquet plugin end to end
