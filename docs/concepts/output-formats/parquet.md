# Parquet

Use Parquet for **tabular / columnar** outputs — point records, feature tables,
detection lists. Tabular plugins subclass `GenericParquetIngestor` and run with
`--output-format parquet`.

Unlike Zarr, Parquet writes are **fully parallel** — there is no process-wide
write lock — so `pipeline_workers > 1` scales across both preprocessing and
writing.

<figure markdown="span">
  ![Firecube Parquet output layout showing batches, independent Parquet files, row groups, column chunks, and footer metadata.](../../assets/images/firecube-parquet-file-layout.svg){ width="820" }
  <figcaption markdown="span">Parquet batches write independent files; each file stores tabular data in row groups and column chunks.</figcaption>
</figure>

## The contract

Implement one required hook:

```python
from typing import ClassVar
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

    def build_dataset(self, group: str, batch: PipelineBatch, ctx: PluginContext):
        # Return a pyarrow.Table or pandas.DataFrame for this group/batch,
        # or None to skip writing.
        ...
```

- `build_dataset(group, batch, ctx)` returns a `pyarrow.Table` or
  `pandas.DataFrame` (or `None` to skip). Note this signature takes the whole
  `batch`, unlike the Zarr template's `items` form.
- The template writes one Parquet file per group/batch — `part-<batch_id>.parquet`,
  placed under a `<group>/` subdirectory when the group is not `default`.

See [GenericParquetIngestor](../plugins/generic-parquet.md) for the
full hook surface (`batch_setup`, `prepare_batch_data`, `cleanup_batch_data`,
`batch_teardown`).

## Customizing the write

Override `write_parquet(...)` for custom behavior such as GeoParquet metadata or
bespoke compression. Two template options tune the default writer (pass as
`--option key=value`):

| Option | Type | Purpose |
|--------|------|---------|
| `parquet_partition_by` | list[str] | Hive-style partition columns. |
| `parquet_row_group_size` | int | Rows per Parquet row group. |

`parquet_partition_by` is a list, so pass it as a JSON array, e.g.
`--option 'parquet_partition_by=["year","month"]'`.

Format-specific tuning guidance is in
[Performance — Parquet Tuning](../performance.md#parquet-tuning).

## Next Steps

- **[GenericParquetIngestor](../plugins/generic-parquet.md)** — implement the Parquet plugin hook
- **[Sentinel-3 FRP Plugin](../../tutorials/sentinel3-frp.md)** — build a small Parquet plugin end to end
- **[Performance Tuning](../performance.md)** — tune Parquet batch size, workers, partitions, and row groups
