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

"""Shared helpers for Firecube-managed Zarr state arrays.

This module centralizes the semantics around the per-timestamp state array
(`firecube_timestamp_state`) used by span-based deletion/resume logic.

It is intentionally small and "dumb": callers decide *when* to create/update
the state array; this module only provides safe primitives.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from firecube.core.uris import storage_uri_from_target

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig
    from firecube.core.filesystem.store_factory import ZarrStoreHandle

DEFAULT_STATE_MEANING: dict[str, str] = {
    "0": "unknown",
    "1": "present",
    "2": "deleted_by_firecube",
    "3": "failed_batch",
}


def _session_zarr_store(
    *,
    store_uri: str,
    storage_config: StorageConfig,
    mode: str,
) -> ZarrStoreHandle:
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.session import StorageSession

    uri = storage_uri_from_target(store_uri)
    session = StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(uri, format="zarr", product_name=store_uri),
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
    )
    return session.zarr.create_store(uri=uri, mode=mode)


def attach_timestamp_state_dataset(
    dataset: Any,
    *,
    dim: str = "timestamp",
    var_name: str = "firecube_timestamp_state",
    present_value: int = 1,
    meaning: dict[str, str] | None = None,
) -> Any:
    """Attach a per-timestamp state variable to an xarray Dataset (best effort).

    This is used by ingestors so span-based tooling (delete spans / scrub) can
    update state without relying on plugin-specific semantics.
    """
    meaning = meaning or DEFAULT_STATE_MEANING
    try:
        import numpy as np
    except Exception as exc:
        raise RuntimeError("numpy required for timestamp state dataset helper") from exc

    if dataset is None:
        return dataset

    length = 0
    try:
        length = int(getattr(dataset, "sizes", {}).get(dim, 0))
    except Exception:
        length = 0
    if length <= 0:
        return dataset

    try:
        state_vals = np.full((length,), np.uint8(int(present_value)), dtype=np.uint8)
        dataset[var_name] = ((dim,), state_vals)
        attrs = getattr(dataset[var_name], "attrs", None)
        if isinstance(attrs, dict):
            attrs.update({"firecube_meaning": dict(meaning)})
        else:
            dataset[var_name].attrs.update({"firecube_meaning": dict(meaning)})
    except Exception:
        return dataset

    return dataset


def _normalize_time_index_ranges(ranges: Sequence[Any]) -> list[tuple[int, int]]:
    normalized: list[tuple[int, int]] = []
    for pair in ranges:
        if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
            continue
        try:
            start = int(pair[0])
            end = int(pair[1])
        except Exception:
            continue
        if end < start:
            continue
        normalized.append((start, end))
    return normalized


def expand_time_index_ranges_to_chunk_boundaries(
    ranges: Sequence[Any],
    *,
    chunk_len: int,
    length: int,
) -> list[list[int]]:
    """Expand time index ranges to full chunk coverage boundaries.

    This is used when deleting whole chunks for spans that are not aligned:
    the state array should be updated for the full removed chunk coverage.
    """
    chunk_len = int(chunk_len or 0)
    length = int(length or 0)
    if chunk_len <= 0 or length <= 0:
        return []

    normalized = _normalize_time_index_ranges(ranges)
    if not normalized:
        return []

    chunk_indices = set()
    for start, end in normalized:
        start_idx = start // chunk_len
        end_idx = end // chunk_len
        for idx in range(start_idx, end_idx + 1):
            chunk_indices.add(idx)

    expanded: list[list[int]] = []
    for idx in sorted(i for i in chunk_indices if i >= 0):
        start = idx * chunk_len
        end = min(length - 1, (idx + 1) * chunk_len - 1)
        if end >= start:
            expanded.append([start, end])
    return expanded


def ensure_timestamp_state_array(
    *,
    store_uri: str,
    array_path: str,
    length: int,
    chunk_len: int,
    dim: str = "timestamp",
    storage_config: StorageConfig | None = None,
    storage_options: dict[str, Any] | None = None,
    present_value: int = 1,
    fill_value: int = 0,
    meaning: dict[str, str] | None = None,
) -> None:
    """Ensure a `uint8` timestamp state array exists (best effort)."""
    length = int(length or 0)
    if length <= 0:
        return

    try:
        import numpy as np
        import zarr
    except Exception as exc:
        raise RuntimeError("zarr+numpy required for timestamp state updates") from exc

    meaning = meaning or DEFAULT_STATE_MEANING
    chunk_len = int(chunk_len or length)
    chunk_len = max(1, min(chunk_len, length))

    if storage_config is not None:
        handle = _session_zarr_store(store_uri=store_uri, storage_config=storage_config, mode="a")
        root = zarr.open_group(**handle.zarr_kwargs(), mode="a", zarr_format=3)
    else:
        root = zarr.open_group(store=store_uri, mode="a", storage_options=storage_options)
    parts = str(array_path).strip("/").split("/")
    grp = root
    for part in parts[:-1]:
        grp = grp.require_group(part)

    name = parts[-1]
    if name in grp:
        return

    try:
        arr = grp.create_array(
            name,
            shape=(length,),
            dtype="u1",
            chunks=(chunk_len,),
            fill_value=int(fill_value),
            dimension_names=(dim,),
            overwrite=False,
        )
        arr[:] = np.uint8(int(present_value))
        arr.attrs.update({"firecube_meaning": dict(meaning)})
    except Exception as exc:
        # Zarr v3 raises ContainsArrayError if overwrite=False.
        # Fallback to existing if someone else created it in the meantime.
        if "ContainsArrayError" in type(exc).__name__ or "exists" in str(exc).lower():
            return
        raise


def update_timestamp_state(
    *,
    store_uri: str,
    array_path: str,
    time_index_ranges: Sequence[Any],
    value: int,
    storage_config: StorageConfig | None = None,
    storage_options: dict[str, Any] | None = None,
) -> None:
    """Best-effort update of a per-timestamp state array (uint8)."""
    normalized = _normalize_time_index_ranges(time_index_ranges)
    if not normalized:
        return

    try:
        import numpy as np
        import zarr
    except Exception as exc:
        raise RuntimeError("zarr+numpy required for timestamp state updates") from exc

    if storage_config is not None:
        handle = _session_zarr_store(store_uri=store_uri, storage_config=storage_config, mode="a")
        root = zarr.open_group(**handle.zarr_kwargs(), mode="a", zarr_format=3)
    else:
        root = zarr.open_group(store=store_uri, mode="a", storage_options=storage_options)
    parts = str(array_path).strip("/").split("/")
    grp = root
    for part in parts[:-1]:
        grp = grp.require_group(part)
    name = parts[-1]
    if name not in grp:
        return

    arr = grp[name]
    state_val = np.uint8(int(value))
    for start, end in normalized:
        cast(Any, arr)[start : end + 1] = state_val
