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

"""Public API for Firecube Core Utilities.

Plugins should import from this module rather than accessing core internals directly.
"""

from firecube.core.batch_registry import BatchResourceRegistry
from firecube.core.config import StorageConfig
from firecube.core.controlplane import RunInfo, describe_control_plane
from firecube.core.controlplane.types import (
    ItemManifestEntry,
    ResolvedIndexRecord,
    validate_manifest_entries,
)
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
    discover_input_files,
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
from firecube.core.tensogram._compat import require_tensogram
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
from firecube.core.zarr.chunk_geometry import (
    axis_selection_is_chunk_aligned,
    chunk_axis_range,
    physical_chunk_keys_for_region,
)
from firecube.core.zarr.region_writer import RegionZarrWriterProtocol
from firecube.core.zarr.time_decode import decode_time_array
from firecube.core.zarr.validation import (
    ZarrCompareReport,
    compare_zarr_stores,
    read_chunk_grid_with_shards,
)

compute_group_identity_hash = _compute_group_identity_hash
"""Public alias for the per-group identity hash helper.

Underscored source stays private to ``firecube.core.index_resolve``; this
alias lets architecture-tier consumers call the helper without importing a
private symbol.
"""

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
    "discover_input_files",
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
