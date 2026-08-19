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

from firecube.core.config import StorageConfig
from firecube.core.controlplane import RunInfo, describe_control_plane
from firecube.core.filesystem import (
    create_filesystem_for_uri,
    delete_path,
    ensure_directory,
    path_stats,
)
from firecube.core.formats import (
    clean_netcdf_encoding,
    discover_input_files,
    materialize_hdf5_path,
    normalize_string_vars,
    prepare_netcdf_for_zarr,
    read_hdf5_array,
    rename_time_dim,
)
from firecube.core.index_resolve import ResolvedIndex, coerce_to_epoch_s, resolve_index_spec
from firecube.core.index_spec import AxisSpec, IndexSpec, ItemInfo, RegularTimeAxis
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
from firecube.core.zarr.region_writer import RegionZarrWriterProtocol
from firecube.core.zarr.time_decode import decode_time_array
from firecube.core.zarr.validation import read_chunk_grid_with_shards

__all__ = [
    "AxisSpec",
    "CatalogGroupInfo",
    "IndexSpec",
    "ItemInfo",
    "RegionZarrWriterProtocol",
    "RegularTimeAxis",
    "ResolvedIndex",
    "RunInfo",
    "SlotAxis",
    "SlotIndexModel",
    "StorageConfig",
    "clean_netcdf_encoding",
    "coerce_to_epoch_s",
    "create_filesystem_for_uri",
    "decode_time_array",
    "delete_path",
    "describe_control_plane",
    "discover_input_files",
    "ensure_directory",
    "ensure_product_uri",
    "epoch_s_to_iso",
    "infer_target_protocol",
    "is_remote_target",
    "iso_to_epoch_s",
    "local_path_from_target",
    "materialize_hdf5_path",
    "normalize_epoch_iso",
    "normalize_string_vars",
    "parse_uri",
    "path_stats",
    "prepare_netcdf_for_zarr",
    "read_chunk_grid_with_shards",
    "read_hdf5_array",
    "rename_time_dim",
    "require_tensogram",
    "resolve_dataset_target",
    "resolve_index_spec",
]
