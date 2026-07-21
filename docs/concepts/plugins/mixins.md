# Mixins (optional)

Mixins are optional, plugin-owned composition helpers you mix into an ingestor
class to add a reusable capability. They're never required — prefer plain
composition and avoid deep mixin stacks. They live under
`firecube.ingestor.extensions`.

## DuckDbMixin

`DuckDbMixin` (`firecube.ingestor.extensions.duck`) gives a plugin a managed
DuckDB connection with lifecycle hooks — handy when you parse or aggregate with
SQL. Mix it in **before** your base class:

```python
from typing import ClassVar

from firecube.ingestor.api import GenericParquetIngestor, register_ingestor
from firecube.ingestor.extensions.duck import DuckDbMixin


@register_ingestor("my_table_plugin")
class MyTableIngestor(DuckDbMixin, GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_table_product"
    name = "my_table_plugin"
    ...
```

Hooks it adds:

| Hook | Purpose |
| :--- | :--- |
| `setup_duckdb(workspace=None, options=None)` | Open and configure the connection. |
| `prepare_duckdb_schema(con, ctx)` | Create tables/views before processing. |
| `teardown_duckdb()` | Close the connection. |

The connection is in-memory by default; set `duckdb_persist_batches=true` to
persist it across batches. A [GenericZarrIngestor](generic-zarr.md) that
enables that option **must** provide these hooks — the template validates this at
startup.

## Other extensions

`firecube.ingestor.extensions` also ships non-mixin helpers you call directly
rather than mix in — for example the grid / regridding utilities
(`build_latlon_binner`, `regrid_with_binner`, …) in
`firecube.ingestor.extensions.grid`.

## Next Steps

- **[GenericZarrIngestor](generic-zarr.md)** — use mixins with the append-Zarr template
- **[GenericParquetIngestor](generic-parquet.md)** — use mixins with the Parquet template
- **[Plugin Contract](contract.md)** — check required plugin rules
