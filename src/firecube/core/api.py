# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deprecated import path for Firecube core utilities. It will be removed in v0.2.0.

Import from `firecube.sdk` instead. The first name read through this module raises one
`DeprecationWarning` per process.
"""

from __future__ import annotations

import warnings
from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from firecube.core.archive.tensogram._compat import require_tensogram
    from firecube.core.batch_registry import BatchResourceRegistry
    from firecube.core.config import StorageConfig
    from firecube.core.controlplane import RunInfo, describe_control_plane
    from firecube.core.controlplane.types import (
        ItemManifestEntry,
        ResolvedIndexRecord,
        validate_manifest_entries,
    )
    from firecube.core.discovery import DiscoveryRequest, SourceItem, discover, discover_input_files
    from firecube.core.errors import (
        DuplicateIrregularCoordinateError,
        IndexedWriteCompilationError,
        MissingIrregularCoordinateError,
        NoDiscoveredItemsError,
    )
    from firecube.core.filesystem import (
        create_filesystem_for_uri,
        delete_path,
        ensure_directory,
        path_stats,
    )
    from firecube.core.formats import (
        clean_netcdf_encoding,
        extract_all_from_zips,
        materialize_hdf5_path,
        normalize_string_vars,
        prepare_netcdf_for_zarr,
        read_hdf5_array,
        rename_time_dim,
    )
    from firecube.core.index_resolve import (
        ExtentUnknownError,
        ResolvedIndex,
        _compute_group_identity_hash,
        coerce_to_epoch_s,
        resolve_index_spec,
    )
    from firecube.core.index_resolve import (
        _compute_group_identity_hash as compute_group_identity_hash,
    )
    from firecube.core.index_spec import (
        AUTO,
        AxisSpec,
        IndexSpec,
        IntegerAxis,
        IrregularTimeAxis,
        ItemInfo,
        RegularTimeAxis,
        TimeAxis,
    )
    from firecube.core.indexed_write import IndexedWrite
    from firecube.core.intake import CatalogGroupInfo
    from firecube.core.product import ensure_product_uri, resolve_dataset_target
    from firecube.core.slot_index import (
        SlotAxis,
        SlotIndexModel,
        epoch_s_to_iso,
        iso_to_epoch_s,
        normalize_epoch_iso,
    )
    from firecube.core.uris import (
        infer_target_protocol,
        is_remote_target,
        local_path_from_target,
        parse_uri,
    )
    from firecube.core.zarr._coord_lifecycle import assert_coord_markers_consistent
    from firecube.core.zarr._reserved_attrs import (
        FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
        FIRECUBE_STATIC_WRITTEN_ATTR,
        RESERVED_ARRAY_ATTRS,
        assert_attrs_safe,
    )
    from firecube.core.zarr._sealing_markers import (
        ATTR_CONSOLIDATED_AT,
        ATTR_COORD_MANAGED,
        ATTR_PREALLOCATED,
    )
    from firecube.core.zarr.guard.chunk_geometry import (
        axis_selection_is_chunk_aligned,
        chunk_axis_range,
        physical_chunk_keys_for_region,
    )
    from firecube.core.zarr.region_writer import RegionZarrWriterProtocol
    from firecube.core.zarr.time_decode import decode_time_array, encode_time_array
    from firecube.core.zarr.validation import (
        ZarrCompareReport,
        compare_zarr_stores,
        read_chunk_grid_with_shards,
    )

__all__ = [
    "ATTR_CONSOLIDATED_AT",
    "ATTR_COORD_MANAGED",
    "ATTR_PREALLOCATED",
    "AUTO",
    "FIRECUBE_GROUP_IDENTITY_HASH_ATTR",
    "FIRECUBE_STATIC_WRITTEN_ATTR",
    "RESERVED_ARRAY_ATTRS",
    "AxisSpec",
    "BatchResourceRegistry",
    "CatalogGroupInfo",
    "DiscoveryRequest",
    "DuplicateIrregularCoordinateError",
    "ExtentUnknownError",
    "IndexSpec",
    "IndexedWrite",
    "IndexedWriteCompilationError",
    "IntegerAxis",
    "IrregularTimeAxis",
    "ItemInfo",
    "ItemManifestEntry",
    "MissingIrregularCoordinateError",
    "NoDiscoveredItemsError",
    "RegionZarrWriterProtocol",
    "RegularTimeAxis",
    "ResolvedIndex",
    "ResolvedIndexRecord",
    "RunInfo",
    "SlotAxis",
    "SlotIndexModel",
    "SourceItem",
    "StorageConfig",
    "TimeAxis",
    "ZarrCompareReport",
    "_compute_group_identity_hash",
    "assert_attrs_safe",
    "assert_coord_markers_consistent",
    "axis_selection_is_chunk_aligned",
    "chunk_axis_range",
    "clean_netcdf_encoding",
    "coerce_to_epoch_s",
    "compare_zarr_stores",
    "compute_group_identity_hash",
    "create_filesystem_for_uri",
    "decode_time_array",
    "delete_path",
    "describe_control_plane",
    "discover",
    "discover_input_files",
    "encode_time_array",
    "ensure_directory",
    "ensure_product_uri",
    "epoch_s_to_iso",
    "extract_all_from_zips",
    "infer_target_protocol",
    "is_remote_target",
    "iso_to_epoch_s",
    "local_path_from_target",
    "materialize_hdf5_path",
    "normalize_epoch_iso",
    "normalize_string_vars",
    "parse_uri",
    "path_stats",
    "physical_chunk_keys_for_region",
    "prepare_netcdf_for_zarr",
    "read_chunk_grid_with_shards",
    "read_hdf5_array",
    "rename_time_dim",
    "require_tensogram",
    "resolve_dataset_target",
    "resolve_index_spec",
    "validate_manifest_entries",
]

# Source module of every name. A name never resolves through another deprecated module.
_SOURCES: dict[str, tuple[str, ...]] = {
    "firecube.core.archive.tensogram._compat": ("require_tensogram",),
    "firecube.core.batch_registry": ("BatchResourceRegistry",),
    "firecube.core.config": ("StorageConfig",),
    "firecube.core.controlplane": ("RunInfo", "describe_control_plane"),
    "firecube.core.controlplane.types": (
        "ItemManifestEntry",
        "ResolvedIndexRecord",
        "validate_manifest_entries",
    ),
    "firecube.core.discovery": (
        "DiscoveryRequest",
        "SourceItem",
        "discover",
        "discover_input_files",
    ),
    "firecube.core.errors": (
        "DuplicateIrregularCoordinateError",
        "IndexedWriteCompilationError",
        "MissingIrregularCoordinateError",
        "NoDiscoveredItemsError",
    ),
    "firecube.core.filesystem": (
        "create_filesystem_for_uri",
        "delete_path",
        "ensure_directory",
        "path_stats",
    ),
    "firecube.core.formats": (
        "clean_netcdf_encoding",
        "extract_all_from_zips",
        "materialize_hdf5_path",
        "normalize_string_vars",
        "prepare_netcdf_for_zarr",
        "read_hdf5_array",
        "rename_time_dim",
    ),
    "firecube.core.index_resolve": (
        "ExtentUnknownError",
        "ResolvedIndex",
        "_compute_group_identity_hash",
        "coerce_to_epoch_s",
        "compute_group_identity_hash",
        "resolve_index_spec",
    ),
    "firecube.core.index_spec": (
        "AUTO",
        "AxisSpec",
        "IndexSpec",
        "IntegerAxis",
        "IrregularTimeAxis",
        "ItemInfo",
        "RegularTimeAxis",
        "TimeAxis",
    ),
    "firecube.core.indexed_write": ("IndexedWrite",),
    "firecube.core.intake": ("CatalogGroupInfo",),
    "firecube.core.product": ("ensure_product_uri", "resolve_dataset_target"),
    "firecube.core.slot_index": (
        "SlotAxis",
        "SlotIndexModel",
        "epoch_s_to_iso",
        "iso_to_epoch_s",
        "normalize_epoch_iso",
    ),
    "firecube.core.uris": (
        "infer_target_protocol",
        "is_remote_target",
        "local_path_from_target",
        "parse_uri",
    ),
    "firecube.core.zarr._coord_lifecycle": ("assert_coord_markers_consistent",),
    "firecube.core.zarr._reserved_attrs": (
        "FIRECUBE_GROUP_IDENTITY_HASH_ATTR",
        "FIRECUBE_STATIC_WRITTEN_ATTR",
        "RESERVED_ARRAY_ATTRS",
        "assert_attrs_safe",
    ),
    "firecube.core.zarr._sealing_markers": (
        "ATTR_CONSOLIDATED_AT",
        "ATTR_COORD_MANAGED",
        "ATTR_PREALLOCATED",
    ),
    "firecube.core.zarr.guard.chunk_geometry": (
        "axis_selection_is_chunk_aligned",
        "chunk_axis_range",
        "physical_chunk_keys_for_region",
    ),
    "firecube.core.zarr.region_writer": ("RegionZarrWriterProtocol",),
    "firecube.core.zarr.time_decode": ("decode_time_array", "encode_time_array"),
    "firecube.core.zarr.validation": (
        "ZarrCompareReport",
        "compare_zarr_stores",
        "read_chunk_grid_with_shards",
    ),
}
# Names whose definition carries another name.
_RENAMED = {"compute_group_identity_hash": "_compute_group_identity_hash"}
# Names firecube.sdk does not carry, listed here because firecube.core must not import the sdk.
_NOT_IN_SDK = frozenset(
    {
        "ATTR_CONSOLIDATED_AT",
        "ATTR_COORD_MANAGED",
        "ATTR_PREALLOCATED",
        "FIRECUBE_GROUP_IDENTITY_HASH_ATTR",
        "RegionZarrWriterProtocol",
        "RunInfo",
        "SlotAxis",
        "SlotIndexModel",
        "StorageConfig",
        "_compute_group_identity_hash",
        "assert_coord_markers_consistent",
        "compute_group_identity_hash",
        "describe_control_plane",
        "encode_time_array",
        "ensure_product_uri",
        "require_tensogram",
        "resolve_dataset_target",
        "validate_manifest_entries",
    }
)
_MODULE_OF = {name: module for module, names in _SOURCES.items() for name in names}
_warned = False


def _message(name: str) -> str:
    text = (
        f"{__name__} is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
    )
    if name in _NOT_IN_SDK:
        text += f" firecube.sdk does not carry {name}; open an issue if your plugin needs it."
    return text


def __getattr__(name: str) -> Any:
    """Resolve a deprecated name from its source module; warn on the first name only."""
    global _warned
    module = _MODULE_OF.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    # The flag, not the warnings registry, keeps this to one warning per process: the
    # registry deduplicates per caller location. The resolved name is not cached in the
    # module namespace, so that no name is ever bound here outside TYPE_CHECKING.
    if not _warned:
        _warned = True
        warnings.warn(_message(name), DeprecationWarning, stacklevel=2)
    return getattr(import_module(module), _RENAMED.get(name, name))


def __dir__() -> list[str]:
    return list(__all__)
