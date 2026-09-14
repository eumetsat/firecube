# Write Tables To Parquet

## Goal

Implement a plugin that converts each batch into a `pyarrow.Table` or
`pandas.DataFrame`. Firecube writes each returned table as a Parquet part below
the product URI.

Use this class when rows are the product's natural write unit. The source file
format does not determine the class. Read
[Parquet](../../concepts/output-formats/parquet.md) for the persisted dataset
layout and write model.

Use a fresh, empty target for each run. This template refuses targets containing
data or previous runs, including with `resume_existing` or `force_reingest`.
Those options cannot safely identify which Parquet rows to retain or replace.
After a failed run, choose a new target and keep the old one for inspection.

## Edit The Plugin Class

Follow [Create a Plugin](create-a-plugin.md), select `parquet`, then
[install the plugin](install-a-plugin.md).

Edit `src/firecube_my_plugin/ingestor.py`. Keep the generated registration and
product name, and implement the `read_table` reader that the generated
`build_dataset` calls, or replace `build_dataset` as in the listing below.

## Implement `build_dataset`

This example reads a batch of CSV files with `pyarrow` and concatenates
them into one table; replace the file format and parsing with what the
product's data actually needs:

```python
from typing import ClassVar

import pyarrow as pa
import pyarrow.csv

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
    PluginContext,
    register_ingestor,
)


@register_ingestor("my_plugin")
class MyPlugin(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def build_dataset(
        self,
        group: str,  # Called once per output group; most plugins ignore this and use "default".
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> pa.Table | None:  # May also return a pandas.DataFrame if installed; None to skip.
        if not batch.items:
            return None

        paths = [ctx.materialize(item) for item in batch.items]
        tables = [pyarrow.csv.read_csv(path) for path in paths]
        return pa.concat_tables(tables)
```

See the [Plugin Templates](../../reference/templates.md#genericparquetingestor)
for the exact hook signature and optional group, path, and writer
customizations, or [Sentinel-3 Fire Detections](../../showcase/sentinel3-fire-detections.ipynb)
for a notebook that creates a plugin and writes detection tables to Parquet.

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
`--input-filters` or customize discovery before verifying ingestion.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Treating `batch` as a list | Read source items from `batch.items`. |
| Returning an unsupported object | Return a `pyarrow.Table`, a `pandas.DataFrame`, or `None`. |
| Expecting one output file | Treat the target as a Parquet dataset root containing parts. |
| Returning the same path for two batches or groups | Give each part a unique relative path outside `.firecube`. |
| Passing a remote URI to a local-only reader | Resolve each item with `ctx.materialize(item)`. |

## Next Steps

- **[Parquet](../../concepts/output-formats/parquet.md)** — understand the persisted dataset layout
- **[Sentinel-3 Fire Detections](../../showcase/sentinel3-fire-detections.ipynb)** — download detections, create a plugin, and inspect the Parquet output
- **[Add Plugin Configuration Options](add-config-options.md)** — declare typed plugin options for the reader you just implemented
- **[Plugin Templates](../../reference/templates.md)** — look up the public template types
