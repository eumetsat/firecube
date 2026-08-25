# Core Utilities

This reference covers the helper functions exported from `firecube.core.api`
for use inside plugin hooks: URI and filesystem handling, dataset
preparation, and time conversion.

`RESERVED_ARRAY_ATTRS`, `assert_attrs_safe()`, and
`FIRECUBE_STATIC_WRITTEN_ATTR` describe the array attribute keys Firecube
owns for Zarr writes.

`compare_zarr_stores()` performs a read-only, driver-aware comparison of two
Zarr stores and returns `ZarrCompareReport`.

{{ render_api_summary("firecube.core.api", [
    "parse_uri",
    "is_remote_target",
    "infer_target_protocol",
    "local_path_from_target",
    "create_filesystem_for_uri",
    "path_stats",
    "ensure_directory",
    "delete_path",
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

::: firecube.core.api.parse_uri

::: firecube.core.api.is_remote_target

::: firecube.core.api.infer_target_protocol

::: firecube.core.api.local_path_from_target

::: firecube.core.api.create_filesystem_for_uri

::: firecube.core.api.path_stats

::: firecube.core.api.ensure_directory

::: firecube.core.api.delete_path

## Source Discovery

::: firecube.core.api.discover_input_files

## Dataset Preparation

::: firecube.core.api.prepare_netcdf_for_zarr

::: firecube.core.api.clean_netcdf_encoding

::: firecube.core.api.normalize_string_vars

::: firecube.core.api.rename_time_dim

::: firecube.core.api.read_hdf5_array

::: firecube.core.api.materialize_hdf5_path

## ZIP Extraction

::: firecube.core.api.extract_all_from_zips

## Time Conversion

::: firecube.core.api.epoch_s_to_iso

::: firecube.core.api.iso_to_epoch_s

::: firecube.core.api.normalize_epoch_iso

## Reserved Array Attributes

::: firecube.core.api.RESERVED_ARRAY_ATTRS

::: firecube.core.api.assert_attrs_safe

::: firecube.core.api.FIRECUBE_STATIC_WRITTEN_ATTR

## Batch Resources

::: firecube.core.api.BatchResourceRegistry

## Zarr Chunk Geometry

::: firecube.core.api.physical_chunk_keys_for_region

::: firecube.core.api.chunk_axis_range

::: firecube.core.api.axis_selection_is_chunk_aligned

## Zarr Store Comparison

::: firecube.core.api.ZarrCompareReport

::: firecube.core.api.compare_zarr_stores

## Ingestor Re-Exports

The ingestor facade re-exports the same attribute guards for plugin code.

::: firecube.ingestor.api.RESERVED_ARRAY_ATTRS

::: firecube.ingestor.api.assert_attrs_safe

::: firecube.ingestor.api.FIRECUBE_STATIC_WRITTEN_ATTR

`coerce_to_epoch_s()` is documented with the direct-Zarr index types in
[Index Specification](parallelism.md).

## See Also

- [Slot-Range Parallelism](parallelism.md)
- [Storage Drivers](storage-drivers.md)
