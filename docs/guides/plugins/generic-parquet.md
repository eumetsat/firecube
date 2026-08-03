# Implement GenericParquetIngestor

## Goal

Implement a plugin that converts each batch into a `pyarrow.Table` or
`pandas.DataFrame`. Firecube writes each returned table as a Parquet part below
the product URI.

Use this class when rows are the product's natural write unit. The source file
format does not determine the class.

## Edit The Plugin Class

Follow [Create a Plugin](create-a-plugin.md), select `parquet`, then
[install the plugin](install-a-plugin.md).

Edit `src/firecube_my_plugin/ingestor.py`. Keep the generated registration and
product name, and replace the `build_dataset` stub.

## Implement `build_dataset`

The source reader is product-specific. The example below shows where it meets
the template contract:

```python
from pathlib import Path
from typing import ClassVar

import pyarrow as pa

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
    PluginContext,
    register_ingestor,
)


def read_product_rows(paths: list[Path]) -> pa.Table:
    ...


@register_ingestor("my_plugin")
class MyPlugin(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def build_dataset(
        self,
        group: str,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> pa.Table | None:
        if not batch.items:
            return None

        paths = [ctx.materialize(item) for item in batch.items]
        return read_product_rows(paths)
```

`read_product_rows` represents the reader and normalization code for the
product. It may return a `pyarrow.Table`; the hook may instead return a
`pandas.DataFrame` when pandas is installed. Return `None` when the batch has no
rows to write.

The Parquet hook receives a `PipelineBatch`, so its source items are in
`batch.items`. Firecube calls the hook once for each output group. Most plugins
use the `"default"` group.

See the [Plugin Template API](../../reference/api.md#genericparquetingestor) for
the exact hook signature and optional group, path, and writer customizations.

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
  --target file:///tmp/my_product.parquet \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --output-format parquet \
  --write-mode direct
```

Open the target as a Parquet dataset. Confirm its schema, row count, and at
least one known value. Treat the target as a dataset root containing part files,
not as one output file.

If built-in discovery does not include the product's source names, pass
`include_patterns` or customize discovery before verifying ingestion.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Treating `batch` as a list | Read source items from `batch.items`. |
| Returning an unsupported object | Return a `pyarrow.Table`, a `pandas.DataFrame`, or `None`. |
| Expecting one output file | Treat the target as a Parquet dataset root containing parts. |
| Passing a remote URI to a local-only reader | Resolve each item with `ctx.materialize(item)`. |

## Next Steps

- **[Parquet](../../concepts/output-formats/parquet.md)** — understand the persisted dataset layout
- **[Sentinel-3 FRP To Parquet](../../tutorials/sentinel3-frp.md)** — download and ingest a real EUMETSAT product
- **[Read Plugin Source Data](storage-access.md)** — use source items with local-path readers
- **[Plugin Template API](../../reference/api.md)** — look up the public template types
