# Core Utilities

This reference covers the helper functions exported from `firecube.core.api`
for use inside plugin hooks: URI and filesystem handling, dataset
preparation, and time conversion.

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
    "epoch_s_to_iso",
    "iso_to_epoch_s",
    "normalize_epoch_iso",
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

## Time Conversion

::: firecube.core.api.epoch_s_to_iso

::: firecube.core.api.iso_to_epoch_s

::: firecube.core.api.normalize_epoch_iso

## See Also

- [Slot-Range Parallelism](parallelism.md)
- [Storage Drivers](storage-drivers.md)
