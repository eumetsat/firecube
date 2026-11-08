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

"""Declare the deprecated functions of `firecube.core.api` for type checkers; no runtime module exists."""

import logging
import tempfile
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from typing_extensions import deprecated

from firecube.core.controlplane.types import ItemManifestEntry
from firecube.core.discovery import DEFAULT_INCLUDE_SUFFIXES, DiscoveryRequest, SourceItem
from firecube.core.filesystem.protocol import StorageFilesystem
from firecube.core.index_resolve import ResolvedIndex
from firecube.core.index_spec import AxisSpec, IndexSpec
from firecube.core.product.resolver import WriteModePolicy
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.validation import ZarrCompareReport

@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry _compute_group_identity_hash; open an issue if your plugin needs it."
)
def _compute_group_identity_hash(axis: AxisSpec, resolved_size: int, dtype: Any) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def assert_attrs_safe(attrs: Mapping[str, Any]) -> None: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry assert_coord_markers_consistent; open an issue if your plugin needs it."
)
def assert_coord_markers_consistent(attrs: dict, coord_path: str) -> None: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def axis_selection_is_chunk_aligned(
    start: int, stop: int, axis_len: int, chunk_size: int
) -> bool: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def chunk_axis_range(start: int, stop: int, chunk_size: int) -> range: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def clean_netcdf_encoding(ds: xr.Dataset) -> xr.Dataset: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def coerce_to_epoch_s(value: Any, *, mode: str = "floor") -> int: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def compare_zarr_stores(
    a_uri: str, b_uri: str, *, storage_type: str, storage_driver: str, anonymous: bool = False
) -> ZarrCompareReport: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry compute_group_identity_hash; open an issue if your plugin needs it."
)
def compute_group_identity_hash(axis: AxisSpec, resolved_size: int, dtype: Any) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def create_filesystem_for_uri(
    uri: str, storage_config: Any, *, format: str
) -> tuple[StorageFilesystem, StorageUri]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def decode_time_array(values: np.ndarray, attrs: Mapping[str, Any]) -> np.ndarray: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def delete_path(
    base_uri: str,
    subpath: str,
    *,
    storage_config: Any | None = None,
    allow_manifest_paths: bool = False,
    dry_run: bool = False,
    filesystem: Any = None,
) -> dict[str, Any]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry describe_control_plane; open an issue if your plugin needs it."
)
def describe_control_plane(
    *, product: str | None = None, base_uri: str | None = None, product_uri: str | None = None
) -> dict[str, str]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def discover(request: DiscoveryRequest) -> list[SourceItem]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def discover_input_files(
    source: str | Path,
    *,
    storage_config: Any | None = None,
    include_suffixes: Sequence[str] = DEFAULT_INCLUDE_SUFFIXES,
    preferred_globs: Iterable[str] | None = None,
    recursive: bool = True,
    sniff_hdf5: bool = True,
    exclude: Iterable[str] | None = None,
) -> list[str]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry encode_time_array; open an issue if your plugin needs it."
)
def encode_time_array(values: np.ndarray, attrs: dict[str, Any]) -> tuple[np.ndarray, str, str]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def ensure_directory(path: Path | str) -> Path: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry ensure_product_uri; open an issue if your plugin needs it."
)
def ensure_product_uri(base_uri: str, product: str) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def epoch_s_to_iso(seconds: int) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def extract_all_from_zips(
    zip_paths: Sequence[Path], dest_dir_for: Callable[[Path], Path], *, workers: int = 1
) -> tuple[dict[Path, Path], dict[Path, str]]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def infer_target_protocol(target: str) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def is_remote_target(target: str) -> bool: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def iso_to_epoch_s(iso: str) -> int: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def local_path_from_target(target: str) -> Path: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def materialize_hdf5_path(
    file_path: Path, *, extract_root: Path | None = None, logger: logging.Logger | None = None
) -> tuple[Path, tempfile.TemporaryDirectory | None]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def normalize_epoch_iso(iso: str) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def normalize_string_vars(
    ds: xr.Dataset,
    *,
    iso_targets: Collection[str] | None = None,
    logger: logging.Logger | None = None,
    preserve_cf_time_attrs: bool = False,
) -> xr.Dataset: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def parse_uri(target: str) -> dict[str, str]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def path_stats(
    uri: str, *, storage_config: Any | None = None, exclude_substrings: Iterable[str] | None = None
) -> dict[str, int]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def physical_chunk_keys_for_region(
    *, group: str, intent: Any, shape: tuple[int, ...], chunks: tuple[int, ...], selection: Any
) -> tuple[set[tuple[str, str, tuple[int, ...]]], bool]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def prepare_netcdf_for_zarr(
    ds: xr.Dataset, time_dim: str = "time", target_time_dim: str = "timestamp"
) -> xr.Dataset: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def read_chunk_grid_with_shards(
    store_uri: str,
    group: str,
    *,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
) -> tuple[list[str], list[int], list[int], list[int] | None]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def read_hdf5_array(
    hdf5_path: Path,
    *,
    variable: str,
    dtype: np.dtype[Any] | str | None = None,
    logger: logging.Logger | None = None,
) -> np.ndarray[Any, Any]: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def rename_time_dim(
    ds: xr.Dataset, source: str = "time", target: str = "timestamp"
) -> xr.Dataset: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry require_tensogram; open an issue if your plugin needs it."
)
def require_tensogram(operation: str) -> None: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry resolve_dataset_target; open an issue if your plugin needs it."
)
def resolve_dataset_target(
    target: str,
    *,
    write_mode: str | WriteModePolicy = "direct",
    temp_root: Path | None = None,
    direct_base_uri: str | None = None,
) -> str: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def resolve_index_spec(
    spec: IndexSpec, *, time_dim_name: str, items: Sequence[ItemManifestEntry] | None = None
) -> ResolvedIndex: ...
@deprecated(
    "firecube.core.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry validate_manifest_entries; open an issue if your plugin needs it."
)
def validate_manifest_entries(entries: list[ItemManifestEntry]) -> None: ...
