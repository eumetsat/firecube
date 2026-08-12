# Extensions

This reference covers `firecube.ingestor.extensions`, the optional helper
surface plugins may import alongside `firecube.ingestor.api`. HEALPix helpers
require the `firecube[healpix]` extra.

{{ render_api_summary("firecube.ingestor.extensions", [
    "HealpixBinner",
    "build_healpix_binner",
    "cells_in_bbox",
    "grid_data_to_healpix",
    "grid_xarray_dataset_to_healpix",
    "LatLonBinner",
    "build_latlon_binner",
    "grid_data_to_latlon",
    "grid_xarray_dataset",
    "aggregate_by_position",
    "DuckDbMixin",
]) }}

## HEALPix Regridding

::: firecube.ingestor.extensions.HealpixBinner

::: firecube.ingestor.extensions.build_healpix_binner

::: firecube.ingestor.extensions.cells_in_bbox

::: firecube.ingestor.extensions.grid_data_to_healpix

::: firecube.ingestor.extensions.grid_xarray_dataset_to_healpix

## Lat/Lon Regridding

::: firecube.ingestor.extensions.LatLonBinner

::: firecube.ingestor.extensions.build_latlon_binner

::: firecube.ingestor.extensions.grid_data_to_latlon

::: firecube.ingestor.extensions.grid_xarray_dataset

## Positional Binning

::: firecube.ingestor.extensions.aggregate_by_position

## DuckDB Support

`DuckDbMixin` adds a per-batch DuckDB connection to a plugin through the
cooperative `batch_setup`/`batch_teardown` lifecycle hooks.

::: firecube.ingestor.extensions.DuckDbMixin
    options:
        members:
          - setup_duckdb
          - teardown_duckdb
          - prepare_duckdb_schema

## See Also

- [Plugin extensions guide](../guides/plugins/extensions.md)
- [Hooks & Lifecycle](hooks.md)
