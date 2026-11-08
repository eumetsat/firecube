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

"""Declare the names of the deprecated `firecube.core.api` module for type checkers."""

from firecube.core._api_deprecated import (
    _compute_group_identity_hash as _compute_group_identity_hash,
)
from firecube.core._api_deprecated import assert_attrs_safe as assert_attrs_safe
from firecube.core._api_deprecated import (
    assert_coord_markers_consistent as assert_coord_markers_consistent,
)
from firecube.core._api_deprecated import (
    axis_selection_is_chunk_aligned as axis_selection_is_chunk_aligned,
)
from firecube.core._api_deprecated import chunk_axis_range as chunk_axis_range
from firecube.core._api_deprecated import clean_netcdf_encoding as clean_netcdf_encoding
from firecube.core._api_deprecated import coerce_to_epoch_s as coerce_to_epoch_s
from firecube.core._api_deprecated import compare_zarr_stores as compare_zarr_stores
from firecube.core._api_deprecated import compute_group_identity_hash as compute_group_identity_hash
from firecube.core._api_deprecated import create_filesystem_for_uri as create_filesystem_for_uri
from firecube.core._api_deprecated import decode_time_array as decode_time_array
from firecube.core._api_deprecated import delete_path as delete_path
from firecube.core._api_deprecated import describe_control_plane as describe_control_plane
from firecube.core._api_deprecated import discover as discover
from firecube.core._api_deprecated import discover_input_files as discover_input_files
from firecube.core._api_deprecated import encode_time_array as encode_time_array
from firecube.core._api_deprecated import ensure_directory as ensure_directory
from firecube.core._api_deprecated import ensure_product_uri as ensure_product_uri
from firecube.core._api_deprecated import epoch_s_to_iso as epoch_s_to_iso
from firecube.core._api_deprecated import extract_all_from_zips as extract_all_from_zips
from firecube.core._api_deprecated import infer_target_protocol as infer_target_protocol
from firecube.core._api_deprecated import is_remote_target as is_remote_target
from firecube.core._api_deprecated import iso_to_epoch_s as iso_to_epoch_s
from firecube.core._api_deprecated import local_path_from_target as local_path_from_target
from firecube.core._api_deprecated import materialize_hdf5_path as materialize_hdf5_path
from firecube.core._api_deprecated import normalize_epoch_iso as normalize_epoch_iso
from firecube.core._api_deprecated import normalize_string_vars as normalize_string_vars
from firecube.core._api_deprecated import parse_uri as parse_uri
from firecube.core._api_deprecated import path_stats as path_stats
from firecube.core._api_deprecated import (
    physical_chunk_keys_for_region as physical_chunk_keys_for_region,
)
from firecube.core._api_deprecated import prepare_netcdf_for_zarr as prepare_netcdf_for_zarr
from firecube.core._api_deprecated import read_chunk_grid_with_shards as read_chunk_grid_with_shards
from firecube.core._api_deprecated import read_hdf5_array as read_hdf5_array
from firecube.core._api_deprecated import rename_time_dim as rename_time_dim
from firecube.core._api_deprecated import require_tensogram as require_tensogram
from firecube.core._api_deprecated import resolve_dataset_target as resolve_dataset_target
from firecube.core._api_deprecated import resolve_index_spec as resolve_index_spec
from firecube.core._api_deprecated import validate_manifest_entries as validate_manifest_entries
from firecube.core.batch_registry import BatchResourceRegistry as BatchResourceRegistry
from firecube.core.config import StorageConfig as StorageConfig
from firecube.core.controlplane import RunInfo as RunInfo
from firecube.core.controlplane.types import ItemManifestEntry as ItemManifestEntry
from firecube.core.controlplane.types import ResolvedIndexRecord as ResolvedIndexRecord
from firecube.core.discovery import DiscoveryRequest as DiscoveryRequest
from firecube.core.discovery import SourceItem as SourceItem
from firecube.core.errors import (
    DuplicateIrregularCoordinateError as DuplicateIrregularCoordinateError,
)
from firecube.core.errors import IndexedWriteCompilationError as IndexedWriteCompilationError
from firecube.core.errors import MissingIrregularCoordinateError as MissingIrregularCoordinateError
from firecube.core.errors import NoDiscoveredItemsError as NoDiscoveredItemsError
from firecube.core.index_resolve import ExtentUnknownError as ExtentUnknownError
from firecube.core.index_resolve import ResolvedIndex as ResolvedIndex
from firecube.core.index_spec import AUTO as AUTO
from firecube.core.index_spec import AxisSpec as AxisSpec
from firecube.core.index_spec import IndexSpec as IndexSpec
from firecube.core.index_spec import IntegerAxis as IntegerAxis
from firecube.core.index_spec import IrregularTimeAxis as IrregularTimeAxis
from firecube.core.index_spec import ItemInfo as ItemInfo
from firecube.core.index_spec import RegularTimeAxis as RegularTimeAxis
from firecube.core.index_spec import TimeAxis as TimeAxis
from firecube.core.indexed_write import IndexedWrite as IndexedWrite
from firecube.core.intake import CatalogGroupInfo as CatalogGroupInfo
from firecube.core.slot_index import SlotAxis as SlotAxis
from firecube.core.slot_index import SlotIndexModel as SlotIndexModel
from firecube.core.zarr._reserved_attrs import (
    FIRECUBE_GROUP_IDENTITY_HASH_ATTR as FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
)
from firecube.core.zarr._reserved_attrs import (
    FIRECUBE_STATIC_WRITTEN_ATTR as FIRECUBE_STATIC_WRITTEN_ATTR,
)
from firecube.core.zarr._reserved_attrs import RESERVED_ARRAY_ATTRS as RESERVED_ARRAY_ATTRS
from firecube.core.zarr._sealing_markers import ATTR_CONSOLIDATED_AT as ATTR_CONSOLIDATED_AT
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED as ATTR_COORD_MANAGED
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED as ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriterProtocol as RegionZarrWriterProtocol
from firecube.core.zarr.validation import ZarrCompareReport as ZarrCompareReport

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
