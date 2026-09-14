# Plugin Extensions

## Goal

Add an optional processing capability to an existing plugin without changing
its output contract: a managed DuckDB connection for batch processing, or a
gridding function that bins irregular samples onto a regular latitude/longitude
or HEALPix axis. Use a mixin for lifecycle behavior and call a utility extension
directly for a data transformation.

| Extension | Use it when |
|---|---|
| `DuckDbMixin` | Batch processing needs a managed, thread-local DuckDB connection. |
| Lat/lon gridding | Irregular geolocated samples must be binned onto a regular latitude/longitude grid. |
| HEALPix gridding | Geolocated samples must be binned onto a fixed or data-derived HEALPix cell axis. |

These are advanced additions to an authoring class. They do not replace
`GenericZarrIngestor`, `GenericParquetIngestor`, `DirectZarrIngestor`, or the
custom pipeline contract.

## Add `DuckDbMixin`

Import `DuckDbMixin` from its extension module and place it before the Firecube
template in the class bases:

```python
from typing import Any, ClassVar

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
    PluginContext,
    register_ingestor,
)
from firecube.ingestor.extensions.duck import DuckDbMixin


@register_ingestor("my_plugin")
class MyPlugin(DuckDbMixin, GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def prepare_duckdb_schema(self, con: Any, ctx: PluginContext) -> None:
        _ = ctx
        con.execute("CREATE TABLE IF NOT EXISTS records (value DOUBLE)")

    def build_dataset(
        self,
        group: str,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> Any | None:
        if not batch.items:
            return None

        for item in batch.items:
            path = ctx.materialize(item)
            self.con.execute("INSERT INTO records SELECT * FROM read_csv_auto(?)", [str(path)])
        return self.con.execute("SELECT * FROM records").arrow()
```

`GenericZarrIngestor`, `GenericParquetIngestor`, and `DirectZarrIngestor` drive
the mixin's connection lifecycle through the cooperative
`batch_setup()`/`batch_teardown()` hooks; see
[`DuckDbMixin`](../../reference/extensions.md#firecube.ingestor.extensions.DuckDbMixin)
for the exact sequence. Use `self.con` only while the batch hook is active.

The connection is in memory by default. Enable file-backed batch state only
after checking its worker and workspace requirements for the chosen template.

### Custom Pipeline Lifecycle

A direct `BaseIngestor` subclass owns `_process_batch()` and does not receive an
automatic setup/teardown wrapper. If it uses `DuckDbMixin`, it must call the
cooperative hooks itself and guarantee teardown:

```python
def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
    self.batch_setup(ctx)
    try:
        ...
    finally:
        self.batch_teardown(ctx)
```

All three templates call teardown after batch processing, including failures,
once setup has succeeded. If `batch_setup()` itself raises, it must release
any resources it acquired before the error.

## Manage Temporary Readers

Reading archives is covered in [Discover Zipped Data](discover-zipped-data.md).
For resources that must outlive one function call, register an `ExitStack`
with [`BatchResourceRegistry`](../../reference/core-utilities.md#firecube.core.api.BatchResourceRegistry)
so readers close before their scratch files are removed.

## Use The Gridding Extensions

The gridding extensions are functions, not mixins:

```python
from firecube.ingestor.extensions import grid

binner = grid.build_latlon_binner(
    lat=latitude,
    lon=longitude,
    grid_spacing=0.1,
    bounds=product_bounds,
)
values = grid.regrid_with_binner(binner=binner, data=source_values)
```

Provide `bounds` when every batch must use the same regular grid. Without
bounds, the extension derives the grid extent from the supplied coordinates.

HEALPix helpers live in `firecube.ingestor.extensions.healpix`. Add the optional
dependency to the plugin project before using them:

```bash
uv add 'firecube[healpix]'
```

Use `target_cells` when every batch must share one HEALPix cell axis. Without
it, `build_healpix_binner()` derives the occupied cells from the input.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Listing `DuckDbMixin` after the template in the class bases | Put the mixin before the template, as shown above. |
| Using `self.con` outside the batch hooks | The connection exists only between `batch_setup()` and `batch_teardown()`. |
| A custom `_process_batch()` that never tears down | Call `batch_setup()` and `batch_teardown()` yourself, with teardown in `finally`. |
| Gridding without `bounds` when batches must share one grid | Pass `bounds`, or `target_cells` for HEALPix, so every batch uses the same axis. |
| Importing the HEALPix helpers without the extra | Add `firecube[healpix]` to the plugin project first. |

## Next Steps

- **[Custom Pipeline Plugins](base-ingestor.md)** — own the complete batch
  result and output coordination
- **[Append Datasets To Zarr](generic-zarr.md)** — use DuckDB while
  producing ordered datasets
- **[Write Tables To Parquet](generic-parquet.md)** — use DuckDB
  while producing tables
- **[Extensions](../../reference/extensions.md)** — exact signatures and
  fields for `DuckDbMixin`, the gridding functions, and `HealpixBinner`
