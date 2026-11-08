# Core Utilities

This reference covers the helper functions exported from `firecube.sdk`
for use inside plugin hooks: URI and filesystem handling, source discovery,
dataset preparation, and time conversion.

`RESERVED_ARRAY_ATTRS`, `assert_attrs_safe()`, and
`FIRECUBE_STATIC_WRITTEN_ATTR` describe the array attribute keys Firecube
owns for Zarr writes. The sealing marker constants are operator-tooling
surface. `firecube zarr preallocate` and `firecube zarr consolidate-time-coord`
stamp them. They are documented with those commands in
[CLI reference](cli.md).

`compare_zarr_stores()` performs a read-only, driver-aware comparison of two
Zarr stores and returns `ZarrCompareReport`.

{{ render_api_summary("firecube.sdk", [
    "parse_uri",
    "is_remote_target",
    "infer_target_protocol",
    "local_path_from_target",
    "create_filesystem_for_uri",
    "path_stats",
    "ensure_directory",
    "delete_path",
    "discover",
    "DiscoveryRequest",
    "SourceItem",
    "discover_input_files",
    "prepare_netcdf_for_zarr",
    "clean_netcdf_encoding",
    "normalize_string_vars",
    "rename_time_dim",
    "read_hdf5_array",
    "materialize_hdf5_path",
    "extract_all_from_zips",
    "epoch_s_to_iso",
    "iso_to_epoch_s",
    "coerce_to_epoch_s",
    "normalize_epoch_iso",
    "RESERVED_ARRAY_ATTRS",
    "assert_attrs_safe",
    "FIRECUBE_STATIC_WRITTEN_ATTR",
    "BatchResourceRegistry",
    "physical_chunk_keys_for_region",
    "chunk_axis_range",
    "axis_selection_is_chunk_aligned",
    "ZarrCompareReport",
    "compare_zarr_stores",
]) }}

## URI And Filesystem

::: firecube.sdk.parse_uri

::: firecube.sdk.is_remote_target

::: firecube.sdk.infer_target_protocol

::: firecube.sdk.local_path_from_target

::: firecube.sdk.create_filesystem_for_uri

::: firecube.sdk.path_stats

::: firecube.sdk.ensure_directory

::: firecube.sdk.delete_path

## Source Discovery

::: firecube.sdk.discover

::: firecube.sdk.DiscoveryRequest

::: firecube.sdk.SourceItem

::: firecube.sdk.discover_input_files

## Dataset Preparation

::: firecube.sdk.prepare_netcdf_for_zarr

::: firecube.sdk.clean_netcdf_encoding

::: firecube.sdk.normalize_string_vars

::: firecube.sdk.rename_time_dim

::: firecube.sdk.read_hdf5_array

::: firecube.sdk.materialize_hdf5_path

## ZIP Extraction

::: firecube.sdk.extract_all_from_zips

## Time Conversion

::: firecube.sdk.epoch_s_to_iso

::: firecube.sdk.iso_to_epoch_s

::: firecube.sdk.normalize_epoch_iso

`coerce_to_epoch_s()` is documented with the direct-Zarr index types in
[Index Specification](parallelism.md).

## Reserved Array Attributes

::: firecube.sdk.RESERVED_ARRAY_ATTRS

::: firecube.sdk.assert_attrs_safe

::: firecube.sdk.FIRECUBE_STATIC_WRITTEN_ATTR

The sealing markers record who owns a time-coordinate array's values.
`firecube zarr preallocate` stamps `ATTR_PREALLOCATED` on grid-valued
coordinates and `ATTR_COORD_MANAGED` on observed-values coordinates it
materializes; `firecube zarr consolidate-time-coord` stamps
`ATTR_PREALLOCATED` together with `ATTR_CONSOLIDATED_AT`.

`ATTR_PREALLOCATED` and `ATTR_COORD_MANAGED` are documented in
[Operations](operations.md#firecube.operations.ATTR_PREALLOCATED).
`ATTR_CONSOLIDATED_AT` and `assert_coord_markers_consistent()` are not in
`firecube.sdk`. Their import path `firecube.core.api` is deprecated until v0.2.0.

::: firecube.core.api.ATTR_CONSOLIDATED_AT

::: firecube.core.api.assert_coord_markers_consistent

## Batch Resources

::: firecube.sdk.BatchResourceRegistry

## Zarr Chunk Geometry

::: firecube.sdk.physical_chunk_keys_for_region

::: firecube.sdk.chunk_axis_range

::: firecube.sdk.axis_selection_is_chunk_aligned

## Zarr Store Comparison

::: firecube.sdk.ZarrCompareReport

::: firecube.sdk.compare_zarr_stores

## See Also

- [Index Specification](parallelism.md)
- [Storage Drivers](storage-drivers.md)
