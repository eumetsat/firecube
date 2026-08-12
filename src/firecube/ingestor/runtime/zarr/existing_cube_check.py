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

"""Pre-write check that an existing Zarr cube's time dim matches the plugin's declaration.

This is group-aware: production plugins write to multiple groups (e.g. opera_seviri_nordlis
has 5: SEVIRI_L15, NORDLIS, RAIN_RATE_OPERA, RAIN_ACC_OPERA, REFLECTIVITY_OPERA). Inspecting
only the top-level group / first data array is not sufficient.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, NoReturn

from firecube.core.filesystem import create_filesystem
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import storage_uri_from_target
from firecube.ingestor.errors import ConfigurationError

_INTERNAL_STATE_ARRAY = "firecube_timestamp_state"  # internal control-plane; skip during check
_LEGACY_TIME_DIM = "timestamp"
_CF_TIME_DIM = "time"


@dataclass(frozen=True)
class _ArrayMetadata:
    name: str
    metadata: dict[str, Any]
    dim_names: list[str]


def verify_dim_compatibility(
    target_uri: str,
    declared_dim: str,
    group_paths: Sequence[str],
    storage_config: Any,
) -> None:
    """Verify each group in ``group_paths`` has a time dim matching ``declared_dim``.

    No-op for groups that do not yet exist on disk (they will be created on
    first write) and for a ``target_uri`` that does not exist at all (new cube).

    Args:
        target_uri: URI of the (possibly existing) Zarr store to inspect.
        declared_dim: Time dimension name the plugin declares for its arrays.
        group_paths: Group paths to check within the store.
        storage_config: Storage configuration or binding used to open the store.

    Raises:
        ConfigurationError: If any existing group uses a different time
            dimension than ``declared_dim`` (with migration guidance), if a
            data array carries both ``time`` and ``timestamp`` dimensions,
            or if arrays within one group disagree on the time dimension.
    """
    if not target_uri:
        return

    target = storage_uri_from_target(target_uri)
    fs = create_filesystem(_binding_for_target(target, target_uri, storage_config))
    if not fs.exists(target):
        return

    for group_path in group_paths:
        _check_group(fs, target, target_uri, group_path, declared_dim)


def _check_group(
    fs: Any,
    target: StorageUri,
    target_uri: str,
    group_path: str,
    declared_dim: str,
) -> None:
    """Inspect one group's data arrays for time-dim consistency."""
    group = _normalize_group_path(group_path)
    group_uri = target if group == "." else target.join(group)
    if not fs.exists(group_uri) or not fs.exists(group_uri.join("zarr.json")):
        return

    first_array: str | None = None
    first_existing: str | None = None
    first_mismatch: tuple[str, str] | None = None

    records = _array_metadata_records(fs, group_uri)
    coordinate_names = _coordinate_array_names(records)

    for record in records:
        if record.name in coordinate_names:
            continue

        dim_names = record.dim_names
        if not dim_names:
            continue

        existing = _existing_time_dim(dim_names, declared_dim, target_uri, group)
        if existing is None:
            # Array has no time-like dimension - treat as static spatial array
            # (e.g. lat/lon with dims ['ny', 'nx']) and skip the time-dim
            # consistency check. Static arrays are not subject to time-dim
            # checks; only the time-indexed data arrays need verification.
            continue
        if first_existing is None:
            first_array = record.name
            first_existing = existing
        elif existing != first_existing:
            _raise_ambiguous_group(
                target_uri,
                group,
                first_array or "<unknown>",
                first_existing,
                record.name,
                existing,
            )
        if existing != declared_dim and first_mismatch is None:
            first_mismatch = (existing, declared_dim)

    if first_mismatch is not None:
        existing, expected = first_mismatch
        _raise_mismatch(target_uri, group, existing, expected)


def _binding_for_target(
    target: StorageUri,
    target_uri: str,
    storage_config: Any,
) -> StorageBinding:
    if isinstance(storage_config, StorageBinding):
        return storage_config
    return StorageBinding(
        identity=ProductIdentity.from_uri(target, format="zarr", product_name=target_uri),
        driver=StorageDriverConfig.from_storage_config_or_default(storage_config),
    )


def _normalize_group_path(group_path: str) -> str:
    group = str(group_path or ".").strip().strip("/")
    return group or "."


def _array_metadata_records(fs: Any, group_uri: StorageUri) -> list[_ArrayMetadata]:
    records: list[_ArrayMetadata] = []
    group_prefix = group_uri.path.rstrip("/") + "/"

    for uri in fs.find(group_uri):
        if uri.path == group_uri.join("zarr.json").path or not uri.path.endswith("/zarr.json"):
            continue
        relative = uri.path.removeprefix(group_prefix)
        parts = [part for part in relative.split("/") if part]
        if len(parts) != 2 or parts[1] != "zarr.json" or parts[0] == _INTERNAL_STATE_ARRAY:
            continue
        metadata = _read_json(fs, uri)
        records.append(
            _ArrayMetadata(
                name=_array_name(uri),
                metadata=metadata,
                dim_names=_dimension_names(metadata),
            )
        )

    return records


def _coordinate_array_names(records: Sequence[_ArrayMetadata]) -> set[str]:
    names: set[str] = set()
    for record in records:
        if _is_dimension_coordinate(record):
            names.add(record.name)
        names.update(_referenced_coordinate_names(record.metadata))
    return names


def _is_dimension_coordinate(record: _ArrayMetadata) -> bool:
    return len(record.dim_names) == 1 and record.dim_names[0] == record.name


def _referenced_coordinate_names(metadata: dict[str, Any]) -> set[str]:
    attrs = metadata.get("attributes")
    if not isinstance(attrs, dict):
        return set()

    raw = attrs.get("coordinates")
    if isinstance(raw, str):
        return {name for name in raw.split() if name}
    if isinstance(raw, list | tuple):
        return {str(name) for name in raw if isinstance(name, str) and name}
    return set()


def _read_json(fs: Any, uri: StorageUri) -> dict[str, Any]:
    # Single-shot read (NOT open().read()): parallel slot-range pods run this
    # dim-compat check at startup while other pods are concurrently creating the
    # same group's zarr.json. On s3fs, `open().read()` uses a conditional
    # (If-Match) cached fetch that raises a 412 PreconditionFailed when the ETag
    # changes mid-read; `read_bytes` issues a plain GET that returns a complete
    # old-or-new object. See StorageFilesystem.read_bytes in protocol.py.
    payload = fs.read_bytes(uri)
    return json.loads(payload.decode("utf-8") if isinstance(payload, bytes) else payload)


def _array_name(uri: StorageUri) -> str:
    parts = [part for part in uri.path.split("/") if part]
    return parts[-2] if len(parts) >= 2 else uri.path


def _dimension_names(metadata: dict[str, Any]) -> list[str]:
    raw = metadata.get("dimension_names")
    if not isinstance(raw, list):
        return []
    return [str(name) for name in raw if isinstance(name, str)]


def _existing_time_dim(
    dim_names: Sequence[str],
    declared_dim: str,
    target_uri: str,
    group_path: str,
) -> str | None:
    has_time = _CF_TIME_DIM in dim_names
    has_timestamp = _LEGACY_TIME_DIM in dim_names
    if has_time and has_timestamp:
        raise ConfigurationError(
            f"Existing cube at {target_uri} group '{group_path}' contains both 'time' and "
            "'timestamp' dimensions on the same data array. Refusing to append because "
            "the existing time dimension is ambiguous."
        )
    if declared_dim in dim_names:
        return declared_dim
    if has_timestamp:
        return _LEGACY_TIME_DIM
    if has_time:
        return _CF_TIME_DIM
    return None


def _raise_mismatch(target_uri: str, group_path: str, existing: str, declared_dim: str) -> None:
    raise ConfigurationError(
        f"Existing cube at {target_uri} group '{group_path}' uses time dimension '{existing}' "
        f"but plugin declared '{declared_dim}'.\n"
        "Refusing to append. Rebuild the cube with the declared time dimension "
        "or migrate the existing cube before appending."
    )


def _raise_unknown_time_dim(
    target_uri: str,
    group_path: str,
    array_name: str,
    dim_names: Sequence[str],
    declared_dim: str,
) -> NoReturn:
    raise ConfigurationError(
        f"Existing cube at {target_uri} group '{group_path}' array '{array_name}' "
        f"has dimensions {list(dim_names)!r}, but none match declared time dimension "
        f"'{declared_dim}' or known legacy time dimensions "
        f"('{_LEGACY_TIME_DIM}', '{_CF_TIME_DIM}'). Refusing to append because "
        "Firecube cannot determine time dimension safely."
    )


def _raise_ambiguous_group(
    target_uri: str,
    group_path: str,
    first_array: str,
    first_dim: str,
    second_array: str,
    second_dim: str,
) -> None:
    raise ConfigurationError(
        f"Existing cube at {target_uri} group '{group_path}' is ambiguous: "
        f"array '{first_array}' uses time dimension '{first_dim}' but array "
        f"'{second_array}' uses '{second_dim}'. Refusing to append because the "
        "group contains conflicting time dimensions."
    )
