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

"""Focused services for append_time_groups decomposition.

Each class owns a single concern extracted from the monolithic
append_time_groups function.  The public entry-point orchestrates them.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast

import numpy as np
import pandas as pd
import xarray as xr

from firecube.core.storage.session import StorageSession, storage_config_from_binding
from firecube.core.zarr.chunk_geometry import chunk_index_to_region
from firecube.core.zarr.time_decode import decode_or_passthrough, decode_time_array
from firecube.ingestor.errors import (
    AppendOverwriteRefused,
    ConfigurationError,
    DuplicateExistingTimestampsError,
    InsertRefusedError,
    IntegrityGuardError,
)
from firecube.ingestor.runtime.zarr.alignment import AlignmentMonitor
from firecube.ingestor.runtime.zarr.resume_cache import (
    ResumeCacheEntry,
    drop_resume_cache_entry,
    get_resume_cache_entry,
    put_resume_cache_entry,
)
from firecube.ingestor.runtime.zarr.staged_metadata import _delete_workspace

if TYPE_CHECKING:
    import zarr

    from firecube.core.config import StorageConfig
    from firecube.core.filesystem.store_factory import ZarrStoreHandle

# AppendTimestampState


def _verify_dataset_has_time_dim(ds: xr.Dataset, time_dim_name: str) -> None:
    if time_dim_name not in ds.dims:
        raise ConfigurationError(
            f"Plugin declared time_dim_name={time_dim_name!r} but the dataset "
            f"from build_dataset() does not contain that dimension. "
            f"Found dims: {sorted(str(dim) for dim in ds.dims)}."
        )


class AppendTimestampState:
    """Timestamp-state array initialization and updates."""

    def __init__(self, state_var_name: str, *, time_dim_name: str) -> None:
        self._var_name = state_var_name
        self._time_dim_name = time_dim_name

    @property
    def var_name(self) -> str:
        """Return the timestamp state variable name."""
        return self._var_name

    def attach(
        self,
        ds: xr.Dataset,
        *,
        append_dim: str | None = None,
        time_dim_name: str | None = None,
    ) -> xr.Dataset:
        """Attach timestamp state variable to dataset before write."""
        from firecube.core.zarr.state import attach_timestamp_state_dataset

        dim_name = time_dim_name or append_dim or self._time_dim_name
        return attach_timestamp_state_dataset(ds, dim=dim_name, var_name=self._var_name)

    def ensure_existing(
        self,
        *,
        store_uri: str | None,
        group: str,
        existing_time: int,
        chunk_len: int | None,
        cached: ResumeCacheEntry | None,
        resume_cache_key: tuple[str, str, str] | None,
        storage_config: StorageConfig | None = None,
    ) -> None:
        """Ensure timestamp state array exists for legacy stores on resume.

        ``storage_config`` is forwarded to ``ensure_timestamp_state_array`` so
        the call routes through the driver-aware ``_session_zarr_store`` branch
        (honours ``StorageConfig.storage_driver``). When ``storage_config`` is
        None the helper falls back to its internal local-fsspec default.
        """
        if not (
            store_uri and existing_time > 0 and (cached is None or not cached.state_initialized)
        ):
            return
        from firecube.core.zarr.state import ensure_timestamp_state_array

        ensure_timestamp_state_array(
            store_uri=store_uri,
            array_path=f"{group}/{self._var_name}",
            length=existing_time,
            chunk_len=int(chunk_len or max(1, existing_time)),
            dim=self._time_dim_name,
            storage_config=storage_config,
        )
        if resume_cache_key:
            put_resume_cache_entry(
                resume_cache_key,
                ResumeCacheEntry(
                    cursor=existing_time,
                    chunk_len=chunk_len,
                    state_initialized=True,
                ),
            )


# ---------------------------------------------------------------------------
# IndexedAppendCoordinate
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class IndexedAppendCoordinate:
    """Indexed coordinate data for timestamp overlap detection and region writes.

    Produced by :meth:`AppendResumeService.read_indexed_append_coordinate`. The
    helper reports the shape of the existing coordinate but never repairs it;
    callers decide how to act on ``duplicate_diagnostics`` / ``is_sorted``.
    """

    values: np.ndarray
    """Time coordinate values (decoded, sub-second precision preserved)."""

    value_to_index: dict[Any, int]
    """Mapping from timestamp value to array index. Empty if duplicates or NaT present."""

    duplicate_diagnostics: list[str] = field(default_factory=list)
    """Human-readable descriptions of duplicate/NaT locations (empty if clean)."""

    state: np.ndarray = field(default_factory=lambda: np.empty((0,), dtype=np.uint8))
    """``firecube_timestamp_state`` array (uint8)."""

    is_sorted: bool = True
    """Whether values are monotonically non-decreasing."""


@dataclass(slots=True)
class AppendClassification:
    """Pure decision for reconciling an incoming append batch with existing slots."""

    mode: Literal["append_only", "region_overwrite", "split_region_plus_append"]
    overwrite_slice: slice | None
    new_values: list[Any]


def _contains_nat(values: np.ndarray) -> bool:
    array = np.asarray(values)
    if array.dtype.kind not in ("M", "m"):
        return False
    return bool(np.isnat(array).any())


def _flat_value_list(values: np.ndarray) -> list[Any]:
    return list(np.asarray(values).reshape(-1).tolist())


def _normalise_time_values(values: np.ndarray, target: str = "ns") -> np.ndarray:
    """Normalise a datetime64 array to a common resolution.

    Non-datetime arrays are returned unchanged so callers can decide whether a
    CF-numeric decode is required for their storage context.
    """
    array = np.asarray(values)
    if array.dtype.kind == "M":
        return array.astype(f"datetime64[{target}]")
    return array


def _time_key_list(values: np.ndarray) -> list[Any]:
    array = np.asarray(values).reshape(-1)
    if array.dtype.kind == "M":
        return [pd.Timestamp(value) for value in array]
    return list(array.tolist())


def _has_duplicate_non_nat_values(values: np.ndarray) -> bool:
    seen: set[Any] = set()
    for value in _flat_value_list(values):
        if value is None:
            continue
        if value in seen:
            return True
        seen.add(value)
    return False


def assert_incoming_monotonic(values: np.ndarray, *, display: list[Any]) -> None:
    """Require a unique, strictly increasing append coordinate with no missing values.

    ``values`` is the normalised coordinate (datetime64[ns] or numeric); ``display``
    holds the caller's original values, used verbatim in the error message. The
    append path never sorts silently: ``classify_incoming`` and every write in
    ``append_time_groups`` (including ``mode="w"``) go through this gate.
    """
    array = np.asarray(values).reshape(-1)
    if _contains_nat(array):
        raise AppendOverwriteRefused(refused_timestamps=["<NaT>"], reason="nat_incoming")
    if array.size <= 1:
        return
    if _has_duplicate_non_nat_values(array):
        raise AppendOverwriteRefused(
            refused_timestamps=[str(value) for value in display[:3]],
            reason="duplicates_incoming",
        )
    in_order = array[:-1] <= array[1:]
    if bool(np.all(in_order)):
        return
    first_descent = int(np.argmin(in_order))
    raise AppendOverwriteRefused(
        refused_timestamps=[str(value) for value in display[first_descent : first_descent + 2]],
        reason="unsorted_incoming",
    )


# ---------------------------------------------------------------------------
# AppendResumeService
# ---------------------------------------------------------------------------


class AppendResumeService:
    """Resume cache lookup, cursor inference, overlap detection."""

    def __init__(
        self,
        *,
        read_source_uri: str | None,
        read_storage_options: dict[str, Any] | None,
        resume_existing: bool,
        append_dim: str,
        chunk_shape: dict[str, int] | None,
        shard_shape: dict[str, int] | None,
        sharding: bool,
        logger: logging.Logger,
        state_var_name: str,
        session: StorageSession | None = None,
        resume_session: StorageSession | None = None,
        storage_config: StorageConfig | None = None,
        read_zarr_store: ZarrStoreHandle | None = None,
        time_dim_name: str | None = None,
    ) -> None:
        self._read_source_uri = read_source_uri
        self._write_target_uri: str | None = None
        self._read_storage_options = read_storage_options
        self._read_zarr_store = read_zarr_store
        self._resume_existing = resume_existing
        self._append_dim = time_dim_name or append_dim
        self._state_var_name = state_var_name
        self._chunk_shape = chunk_shape
        self._shard_shape = shard_shape
        self._sharding = sharding
        self._logger = logger
        self._session = session
        self._resume_session = resume_session or session
        self._storage_config = storage_config

        self.write_cursor: int = 0
        self.chunk_len: int | None = None
        self.resume_cache_key: tuple[str, str, str] | None = None
        self.coverage_arrays: list[str] = []
        self.mode: Literal["w", "a"] = "w"
        self._first_write: bool = True
        self._cached: ResumeCacheEntry | None = None
        self._existing_time: int = 0

    def init_group(self) -> None:
        """Reset per-group state before processing a new group."""
        self.write_cursor = 0
        self.chunk_len = None
        self.resume_cache_key = None
        self.coverage_arrays = []
        self.mode = "w"
        self._first_write = True
        self._cached = None
        self._existing_time = 0

    def prepare_write(
        self,
        *,
        ds: xr.Dataset,
        group: str,
        store: object,
        write_target_uri: str | None,
        arrays_for_group: Callable[[str], list[str]] | None,
        ts_state: AppendTimestampState,
    ) -> bool:
        """Prepare for a batch write.  Returns *False* to skip this batch."""
        self._write_target_uri = write_target_uri
        if not self._first_write:
            self.mode = "a"
            return True

        state_var_name = ts_state.var_name
        data_vars = [v for v in ds.data_vars if v != state_var_name]
        if not data_vars:
            return False
        primary_var = str(data_vars[0])

        if arrays_for_group is not None:
            self.coverage_arrays = list(arrays_for_group(str(group)))
        else:
            self.coverage_arrays = [f"{group}/{v}" for v in data_vars]

        primary_array_path = f"{group}/{primary_var}"
        if self._read_source_uri:
            self.resume_cache_key = (
                str(self._read_source_uri),
                str(group),
                str(self._append_dim),
            )
        self._cached = (
            get_resume_cache_entry(self.resume_cache_key) if self.resume_cache_key else None
        )

        group_already_exists, dim_names, shape, chunks = self._read_metadata(
            ds,
            store,
            primary_var,
            primary_array_path,
        )

        if group_already_exists:
            self._resolve_existing_group(
                ds,
                group,
                primary_var,
                primary_array_path,
                dim_names,
                shape,
                chunks,
            )
            storage_config = self._storage_config
            if storage_config is None and self._session is not None:
                storage_config = storage_config_from_binding(self._session._binding)
            ts_state.ensure_existing(
                store_uri=write_target_uri,
                group=group,
                existing_time=self._existing_time,
                chunk_len=self.chunk_len,
                cached=self._cached,
                resume_cache_key=self.resume_cache_key,
                storage_config=storage_config,
            )
            self.mode = "a"
        else:
            self.mode = "w"
            if self._chunk_shape and self._append_dim in self._chunk_shape:
                self.chunk_len = int(self._chunk_shape[self._append_dim])

        self._first_write = False
        return True

    def refresh_chunk_len_from_stored_array(
        self, ds: xr.Dataset, group: str, store: object
    ) -> None:
        """Read the append-dimension chunk length from the array persisted by Zarr."""
        exists, dim_names, _shape, chunks = self._read_metadata(
            ds,
            store,
            self._append_dim,
            f"{group}/{self._append_dim}",
        )
        if not exists or not chunks:
            return

        chunk_idx = list(dim_names).index(self._append_dim) if dim_names else 0
        self.chunk_len = int(chunks[chunk_idx])

    def advance_cursor(self, count: int) -> int:
        """Advance write cursor by *count*.  Returns the start index."""
        start_i = self.write_cursor
        self.write_cursor += int(count)
        return start_i

    def rewind_cursor(self, cursor: int, *, group_removed: bool = False) -> None:
        """Rewind the write cursor after a failed batch was repaired.

        Keeps the process-level resume cache consistent with the store: a
        truncated tail leaves the cached cursor pointing past the array end,
        and a deleted fresh group leaves an entry for a group that no longer
        exists.

        Args:
            cursor: Append-dimension length the group's arrays were
                truncated to.
            group_removed: ``True`` when the repair deleted the group; its
                cache entry is dropped instead of rewound.
        """
        self.write_cursor = int(cursor)
        if not self.resume_cache_key:
            return
        if group_removed:
            drop_resume_cache_entry(self.resume_cache_key)
            return
        existing = get_resume_cache_entry(self.resume_cache_key)
        if existing is not None:
            existing.cursor = int(cursor)

    def update_cache_after_write(self) -> None:
        """Update the resume cache after a successful batch write."""
        if not self.resume_cache_key:
            return
        existing = get_resume_cache_entry(self.resume_cache_key)
        if existing is None:
            put_resume_cache_entry(
                self.resume_cache_key,
                ResumeCacheEntry(
                    cursor=self.write_cursor,
                    chunk_len=self.chunk_len,
                    state_initialized=False,
                ),
            )
        else:
            existing.cursor = int(self.write_cursor)
            if self.chunk_len is not None:
                existing.chunk_len = int(self.chunk_len)

    def _read_metadata(
        self,
        ds: xr.Dataset,
        store: object,
        primary_var: str,
        primary_array_path: str,
    ) -> tuple[bool, list[str] | None, list[int] | None, list[int] | None]:
        """Read existing group metadata from URI or raw store."""
        from firecube.ingestor.runtime.zarr.append import (
            _read_array_meta_from_store,
            _read_existing_group_meta_from_source,
        )

        is_sharded = self._shard_shape is not None or self._sharding
        if self._read_source_uri:
            storage_config = self._storage_config
            if storage_config is None and self._resume_session is not None:
                storage_config = storage_config_from_binding(self._resume_session._binding)
            exists, dim_names, shape, chunks, _inner = _read_existing_group_meta_from_source(
                handle=self._read_zarr_store,
                source_uri=self._read_source_uri,
                primary_array_path=primary_array_path,
                read_options=self._read_storage_options,
                storage_config=storage_config,
                sharded=is_sharded,
            )
            return exists, dim_names, shape, chunks

        try:
            var_dims_list: list[str] = [str(dim) for dim in getattr(ds[primary_var], "dims", ())]
            dim_names, shape, chunks = _read_array_meta_from_store(
                store,
                primary_array_path,
                var_dims_list,
                sharded=self._shard_shape is not None or self._sharding,
            )
            return True, dim_names, shape, chunks
        except Exception:
            return False, None, None, None

    def _effective_chunk(
        self,
        dim: str,
        idx: int,
        chunks: list[int],
    ) -> int:
        """Expected stored chunk for ``dim`` when validating an existing group."""
        if not self._chunk_shape or dim == self._append_dim or dim not in self._chunk_shape:
            return int(chunks[idx])
        return int(self._chunk_shape[dim])

    def _resolve_existing_group(
        self,
        ds: xr.Dataset,
        group: str,
        primary_var: str,
        primary_array_path: str,
        dim_names: list[str] | None,
        shape: list[int] | None,
        chunks: list[int] | None,
    ) -> None:
        """Resolve cursor and chunk_len for an existing group."""
        from firecube.ingestor.runtime.zarr.append import _validate_shard_shape

        if self._cached is not None:
            self._existing_time = int(self._cached.cursor)
            self.write_cursor = self._existing_time
            if self._cached.chunk_len is not None:
                self.chunk_len = int(self._cached.chunk_len)
            if self._shard_shape is not None and chunks:
                var_dims_local: list[str] = [
                    str(dim) for dim in getattr(ds[primary_var], "dims", ())
                ]
                _validate_shard_shape(
                    chunks,
                    self._shard_shape,
                    var_dims_local,
                    primary_array_path,
                )
            dim_names, shape, chunks = None, None, None
        else:
            self._existing_time = int(shape[0]) if shape else 0
            self.write_cursor = self._existing_time

        var_dims: list[str] = [str(dim) for dim in getattr(ds[primary_var], "dims", ())]
        var_sizes = getattr(ds[primary_var], "sizes", {})
        if shape is not None and len(shape) == len(var_dims):
            for idx, dim in enumerate(var_dims):
                if dim == self._append_dim:
                    continue
                expected = int(shape[idx])
                actual = int(var_sizes.get(dim, -1))
                if actual != expected:
                    raise ValueError(
                        f"Existing {primary_array_path} dim '{dim}'={expected} "
                        f"does not match new dataset '{dim}'={actual}"
                    )

        if self._shard_shape is not None and chunks:
            _validate_shard_shape(
                chunks,
                self._shard_shape,
                var_dims,
                primary_array_path,
            )
        elif not self._sharding and self._chunk_shape and chunks and len(chunks) == len(var_dims):
            expected_chunks = [
                self._effective_chunk(dim, idx, chunks) for idx, dim in enumerate(var_dims)
            ]
            if tuple(int(x) for x in chunks) != tuple(int(x) for x in expected_chunks):
                raise ValueError(
                    f"Existing {primary_array_path} chunk_shape={list(chunks)} "
                    f"does not match requested {expected_chunks}"
                )

        inferred = 0
        inferred_source = ""
        if chunks:
            try:
                inferred = (
                    int(chunks[list(dim_names).index(self._append_dim)] or 0)
                    if dim_names
                    else int(chunks[0] or 0)
                )
            except (ValueError, IndexError):
                inferred = int(chunks[0] or 0)
            if inferred > 0:
                inferred_source = "stored"
        if inferred == 0 and self._chunk_shape and self._append_dim in self._chunk_shape:
            inferred = int(self._chunk_shape[self._append_dim])
            inferred_source = "configured_chunk_shape"
        if inferred == 0 and self._shard_shape and self._append_dim in self._shard_shape:
            inferred = int(self._shard_shape[self._append_dim])
            inferred_source = "configured_shard_shape"

        if inferred > 0 and self.chunk_len is None:
            self.chunk_len = inferred
            if self._cached is None:
                self._logger.debug(
                    "Resolved Zarr chunk length for append dimension",
                    extra={
                        "group": str(group),
                        "dim": self._append_dim,
                        "chunk_len": self.chunk_len,
                        "source": inferred_source,
                    },
                )

    def _reads_from_distinct_store(self) -> bool:
        """True when classification reads a store this run does not write.

        Staged runs read the final target while writing the workspace copy, so
        ``ensure_existing`` (which upgrades the write target) cannot have added
        the state array to the store being read.
        """
        read = self._read_source_uri
        write = self._write_target_uri
        if read is None or write is None:
            return False
        return _normalise_store_uri(read) != _normalise_store_uri(write)

    def _read_state_array(self, group: zarr.Group) -> np.ndarray:
        """Read the timestamp-state array named by the constructor.

        On the write target, ``prepare_write`` upgrades legacy groups through
        ``ensure_existing`` before any classification, so a missing array there
        is a real defect and raises ``AppendOverwriteRefused``.

        When reads come from a distinct source (staged runs read the final
        target, which this run never writes before upload), ``ensure_existing``
        cannot have upgraded it. A store written before the state array existed
        had no deletions, so every stored timestamp is present; that legacy case
        is answered with an all-present array and a warning. The array itself is
        created on upload or on the next direct write.
        """
        try:
            return np.asarray(cast(Any, group[self._state_var_name])[:])
        except KeyError as exc:
            array_path = f"{group.path}/{self._state_var_name}"
            if self._reads_from_distinct_store():
                # Deliberate legacy boundary: distinct read store, see docstring.
                length = int(cast(Any, group[self._append_dim]).shape[0])
                self._logger.warning(
                    "Legacy store %s has no timestamp-state array %r; treating all %d "
                    "stored timestamps as present (the array is created on the next write)",
                    self._read_source_uri,
                    array_path,
                    length,
                )
                return np.ones(length, dtype=np.uint8)
            raise AppendOverwriteRefused(
                refused_timestamps=[],
                reason="state_array_missing",
                array_path=array_path,
            ) from exc

    def read_indexed_append_coordinate(
        self,
        group: zarr.Group,
        append_dim: str,
    ) -> IndexedAppendCoordinate:
        """Read the time coordinate and state array from an existing Zarr group.

        Returns a fully-indexed coordinate with duplicate diagnostics.
        If the coordinate has duplicates or NaT values, ``value_to_index`` is
        empty and ``duplicate_diagnostics`` describes each offending slot.
        The state array is ``self._state_var_name``; its absence raises
        ``AppendOverwriteRefused(reason="state_array_missing")``.
        """
        time_arr = cast(Any, group[append_dim])
        raw_values = np.asarray(time_arr[:])
        coord_attrs = dict(time_arr.attrs)
        units = coord_attrs.get("units")
        try:
            values = decode_or_passthrough(raw_values, coord_attrs)
        except ValueError as exc:
            store_uri = self._read_source_uri or "<unknown>"
            group_name = group.path or "/"
            raise ValueError(
                "Failed to decode append coordinate "
                f"store_uri={store_uri!r} group={group_name!r} "
                f"dtype={str(raw_values.dtype)!r} units={units!r}: {exc}"
            ) from exc
        values = _normalise_time_values(values, target="ns")
        state = self._read_state_array(group)
        if values.size == 0:
            return IndexedAppendCoordinate(values=values, value_to_index={}, state=state)

        diagnostics: list[str] = []
        nat_indices: list[int] = []
        if values.dtype.kind == "M":
            nat_mask = np.isnat(values)
            if nat_mask.any():
                nat_indices = [int(i) for i in np.where(nat_mask)[0]]
                diagnostics.extend(f"index {idx}: NaT" for idx in nat_indices)

        value_list = _time_key_list(values)
        first_seen: dict[Any, int] = {}
        counts: dict[Any, int] = {}
        for i, v in enumerate(value_list):
            if v is None:
                continue
            if v in first_seen:
                counts[v] = counts.get(v, 1) + 1
            else:
                first_seen[v] = i
                counts[v] = 1

        has_duplicates = False
        for v, n_occ in counts.items():
            if n_occ > 1:
                has_duplicates = True
                diagnostics.append(f"index {first_seen[v]}: {v} appears {n_occ}x")

        has_nat = bool(nat_indices)
        if has_nat or has_duplicates:
            value_to_index: dict[Any, int] = {}
        else:
            value_to_index = dict(first_seen)

        if values.size <= 1:
            is_sorted = True
        else:
            is_sorted = bool(np.all(values[:-1] <= values[1:]))

        return IndexedAppendCoordinate(
            values=values,
            value_to_index=value_to_index,
            duplicate_diagnostics=diagnostics,
            state=state,
            is_sorted=is_sorted,
        )

    def classify_incoming(
        self,
        batch_values: np.ndarray,
        group: zarr.Group,
        batch_attrs: Mapping[str, Any] | None = None,
        *,
        allow_refill_plus_append: bool = False,
    ) -> AppendClassification:
        """Classify incoming timestamps against the existing append coordinate.

        This method is intentionally read-only: it inspects the coordinate and
        timestamp-state arrays, then returns the mode a later writer may use.
        """
        coord = self.read_indexed_append_coordinate(group, self._append_dim)
        original_batch_values = np.asarray(batch_values)

        if coord.duplicate_diagnostics and _has_duplicate_non_nat_values(coord.values):
            raise DuplicateExistingTimestampsError(
                refused_timestamps=coord.duplicate_diagnostics[:5],
                reason="duplicates_existing",
            )

        if batch_values.dtype.kind == "M":
            batch_values = _normalise_time_values(batch_values, target="ns")
        elif coord.values.dtype.kind == "M":
            attrs = batch_attrs or {}
            if not attrs.get("units"):
                raise AppendOverwriteRefused(
                    refused_timestamps=[str(value) for value in _flat_value_list(batch_values)[:3]],
                    reason="time_coord_mismatch",
                )
            try:
                batch_values = _normalise_time_values(
                    decode_time_array(batch_values, attrs),
                    target="ns",
                )
            except (KeyError, ValueError) as exc:
                raise AppendOverwriteRefused(
                    refused_timestamps=[str(value) for value in _flat_value_list(batch_values)[:3]],
                    reason="time_coord_mismatch",
                ) from exc

        if _contains_nat(batch_values):
            raise AppendOverwriteRefused(
                refused_timestamps=["<NaT>"],
                reason="nat_incoming",
            )

        if _contains_nat(coord.values):
            raise AppendOverwriteRefused(
                refused_timestamps=["<NaT in existing>"],
                reason="nat_existing",
            )

        batch_display_list = _flat_value_list(original_batch_values)
        batch_key_list = _time_key_list(batch_values)
        assert_incoming_monotonic(batch_values, display=batch_display_list)

        overlap_indices: list[tuple[int, int]] = []
        new_positions: list[int] = []
        new_values: list[Any] = []
        for batch_pos, value in enumerate(batch_key_list):
            if value in coord.value_to_index:
                overlap_indices.append((batch_pos, coord.value_to_index[value]))
            else:
                new_positions.append(batch_pos)
                new_values.append(batch_display_list[batch_pos])

        if not coord.is_sorted:
            # Refuse an unsorted coordinate rather than sorting silently. Any downstream
            # use of coord.values[-1] as "max" would misclassify.
            refused = [str(value) for value in _flat_value_list(coord.values)[:3]]
            raise AppendOverwriteRefused(
                refused_timestamps=refused,
                reason="unsorted_existing_coord",
            )

        if coord.values.size > 0:
            # A timestamp the store does not hold must sort after every stored
            # one, on every path: refill-plus-append may refill state 2/3 slots
            # (those are overlaps, handled below) but never insert new values
            # below the axis end.
            existing_max = coord.values[-1]
            for batch_pos in new_positions:
                if batch_values[batch_pos] < existing_max:
                    raise InsertRefusedError(
                        refused_timestamps=[str(batch_values[batch_pos])],
                        reason="insert",
                        existing_max=str(existing_max),
                    )

        if not overlap_indices:
            return AppendClassification(
                mode="append_only",
                overwrite_slice=None,
                new_values=batch_display_list,
            )

        store_indices = [store_index for _, store_index in overlap_indices]
        min_store_idx = min(store_indices)
        max_store_idx = max(store_indices)
        if max_store_idx - min_store_idx + 1 != len(overlap_indices):
            raise AppendOverwriteRefused(
                refused_timestamps=[str(batch_values[pos]) for pos, _ in overlap_indices[:3]],
                reason="non_contiguous",
            )

        overwrite_slice = slice(min_store_idx, max_store_idx + 1)
        overlap_state_by_index = {
            i: int(coord.state[i]) for i in range(min_store_idx, max_store_idx + 1)
        }
        if len(overlap_indices) == len(batch_key_list):
            return AppendClassification(
                mode="region_overwrite",
                overwrite_slice=overwrite_slice,
                new_values=[],
            )

        batch_positions = [batch_pos for batch_pos, _ in overlap_indices]
        overlaps_batch_prefix = batch_positions == list(range(len(overlap_indices)))
        overlaps_existing_tail = max_store_idx == int(coord.values.size) - 1
        refill_tail_allowed = allow_refill_plus_append and all(
            state in {2, 3} for state in overlap_state_by_index.values()
        )
        existing_last = coord.values[-1]
        new_positions_all_beyond_existing = all(
            batch_values[batch_pos] > existing_last for batch_pos in new_positions
        )
        if not overlaps_batch_prefix or (
            not overlaps_existing_tail
            and not refill_tail_allowed
            and not new_positions_all_beyond_existing
        ):
            raise AppendOverwriteRefused(
                refused_timestamps=[str(batch_values[pos]) for pos in batch_positions[:3]],
                reason="non_contiguous",
            )

        return AppendClassification(
            mode="split_region_plus_append",
            overwrite_slice=overwrite_slice,
            new_values=new_values,
        )

    def _open_read_root(self, *, missing_ok: bool = False) -> zarr.Group | None:
        """Open the read-side Zarr root using the configured handle or URI."""
        import zarr

        handle = self._read_zarr_store
        if (
            handle is None
            and self._read_source_uri is not None
            and self._storage_config is not None
        ):
            from firecube.core.filesystem.store_factory import create_zarr_store

            handle = create_zarr_store(
                uri=self._read_source_uri,
                storage_config=self._storage_config,
                mode="r",
            )
        if handle is None:
            if missing_ok:
                return None
            raise ValueError("A readable Zarr store is required for force_reingest classification")
        try:
            return zarr.open_group(
                **handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
            )
        except FileNotFoundError:
            if missing_ok:
                return None
            raise

    def classify_dataset(
        self,
        *,
        ds: xr.Dataset,
        group: str,
        allow_refill_plus_append: bool = False,
    ) -> AppendClassification:
        """Classify an attached dataset batch against the current Zarr group."""
        root = self._open_read_root()
        assert root is not None
        batch_values = np.asarray(ds[self._append_dim].values)
        batch_attrs = dict(ds[self._append_dim].attrs)
        return self.classify_incoming(
            batch_values,
            cast(Any, root[str(group)]),
            batch_attrs=batch_attrs,
            allow_refill_plus_append=allow_refill_plus_append,
        )

    def overlapping_values_state_aware(
        self,
        incoming_values: set[Any],
        group: zarr.Group,
    ) -> set[Any]:
        """Return the subset of ``incoming_values`` that overlap AND are state=1.

        State=2 (``deleted_by_firecube``) and state=3 (``failed_batch``) slots are
        treated as ABSENT (refillable) and are NEVER reported as overlaps. Only
        state=1 (``present``) slots count as a real duplicate that must be
        silently filtered from the incoming batch under ``resume_existing``.

        Args:
            incoming_values: Normalized incoming timestamp values (as produced
                by ``_extract_append_values`` in ``append.py``).
            group: Existing Zarr group containing the append-dim coord and the
                timestamp-state array named by the constructor.

        Returns:
            Set of incoming values already present with state=1. Empty when
            nothing overlaps or the group has no usable value→index map.

        Raises:
            AppendOverwriteRefused: ``state_array_missing`` when the group has
                no timestamp-state array.
        """
        if not incoming_values:
            return set()

        coord = self.read_indexed_append_coordinate(group, self._append_dim)
        values_arr = coord.values
        state_arr = coord.state

        if values_arr.size == 0 or state_arr.size == 0:
            return set()

        is_datetime_coord = values_arr.dtype.kind == "M"
        normalized_to_state: dict[Any, int] = {}
        for idx in range(int(values_arr.size)):
            raw = values_arr[idx]
            if is_datetime_coord:
                try:
                    normalized: Any = pd.Timestamp(raw)
                except (ValueError, TypeError):
                    continue
                if pd.isna(normalized):
                    continue
                if normalized.tzinfo is not None:
                    normalized = normalized.tz_convert("UTC").tz_localize(None)
            else:
                normalized = raw.item() if hasattr(raw, "item") else raw
            if idx < int(state_arr.size):
                normalized_to_state[normalized] = int(state_arr[idx])
        return {v for v in incoming_values if normalized_to_state.get(v) == 1}

    def compute_state_aware_skip_set(
        self,
        *,
        ds: xr.Dataset,
        group: str,
    ) -> set[Any]:
        """Return the state=1 overlap set for an incoming batch dataset.

        Convenience wrapper around :meth:`overlapping_values_state_aware` that
        opens the target group via the configured resume/read handle and
        extracts the incoming timestamp set from ``ds``. Returns an empty set
        when ``resume_existing`` is disabled, when no readable store is
        available, or when the group does not yet exist (the caller has
        nothing to filter against).
        """
        if not self._resume_existing:
            return set()

        from firecube.ingestor.runtime.zarr.append import _extract_append_values

        root = self._open_read_root(missing_ok=True)
        if root is None:
            return set()
        try:
            grp = cast(Any, root[str(group)])
        except KeyError:
            return set()

        incoming = _extract_append_values(ds, self._append_dim)
        if not incoming:
            return set()
        return self.overlapping_values_state_aware(incoming, grp)


class AppendWriteExecutor:
    """Execute one batch write to the Zarr store and report alignment.

    ``execute`` delegates to ``write_dataset_to_zarr`` with the configured
    chunk/shard/codec/sharding settings; the caller supplies the write mode
    (``"w"``/``"a"``) and optional region slice. ``check_alignment`` reports
    each write to the run's ``AlignmentMonitor`` for downstream boundary
    tracking.
    """

    def __init__(
        self,
        *,
        zarr_store: ZarrStoreHandle,
        chunk_shape: dict[str, int] | None,
        shard_shape: dict[str, int] | None,
        sharding: bool,
        compression: bool,
        append_dim: str,
        logger: logging.Logger,
        alignment: AlignmentMonitor,
        write_fn: Any = None,
        time_dim_name: str | None = None,
        state_var_name: str = "firecube_timestamp_state",
        zarr_codecs: list[dict] | None = None,
        preflight_compare_zarr_store: Any = None,
        force_reingest: bool = False,
    ) -> None:
        self._zarr_store = zarr_store
        self._chunk_shape = chunk_shape
        self._shard_shape = shard_shape
        self._sharding = sharding
        self._compression = compression
        self._append_dim = time_dim_name or append_dim
        self._logger = logger
        self._write_fn = write_fn
        self._state_var_name = state_var_name
        self._zarr_codecs = zarr_codecs
        self._alignment = alignment
        self._preflight_compare_zarr_store = preflight_compare_zarr_store
        self._force_reingest = force_reingest

    def execute(
        self,
        *,
        ds: xr.Dataset,
        group: str,
        mode: Literal["w", "a"],
        region: slice | None = None,
    ) -> None:
        """Write a single dataset batch to the Zarr store."""
        write_fn = self._write_fn
        if write_fn is None:
            from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr as write_fn

        _verify_dataset_has_time_dim(ds, self._append_dim)
        write_fn(
            ds,
            zarr_store=self._zarr_store,
            group=str(group),
            mode=mode,
            region=region,
            time_dim=self._append_dim,
            state_var_name=self._state_var_name,
            chunk_shape=self._chunk_shape,
            shard_shape=self._shard_shape,
            sharding=self._sharding,
            compression=self._compression,
            zarr_codecs=self._zarr_codecs,
            consolidate=False,
            logger=self._logger,
            preflight_compare_zarr_store=self._preflight_compare_zarr_store,
            force_reingest=self._force_reingest,
        )

    def check_alignment(
        self,
        *,
        start_i: int,
        count: int,
        chunk_len: int | None,
        group: str,
        is_final: bool | None = None,
    ) -> bool:
        """Report one write to the run's :class:`AlignmentMonitor`."""
        return self._alignment.check(
            start_i=start_i,
            count=count,
            chunk_len=chunk_len,
            group=group,
            is_final=bool(is_final),
            logger=self._logger,
        )


# ---------------------------------------------------------------------------
# AppendCoverageBuilder
# ---------------------------------------------------------------------------


class AppendCoverageBuilder:
    """Coverage entry construction, time range tracking, index range building."""

    def __init__(self, *, time_dim_name: str) -> None:
        self._written_ranges: list[list[int]] = []
        self._aligned_all: bool = True
        self._time_min: pd.Timestamp | None = None
        self._time_max: pd.Timestamp | None = None
        self._time_dim_name = time_dim_name

    def record_batch(
        self,
        *,
        start_i: int,
        count: int,
        ds: xr.Dataset,
        aligned: bool,
        append_dim: str | None = None,
        time_dim_name: str | None = None,
    ) -> None:
        """Record a written batch: index range, alignment, time bounds."""
        end_i = start_i + count - 1
        self._written_ranges.append([start_i, end_i])
        self._aligned_all = self._aligned_all and aligned

        dim_name = time_dim_name or append_dim or self._time_dim_name
        if dim_name in ds.coords or dim_name in ds.data_vars:
            ts_vals = ds[dim_name].values
            if ts_vals.size > 0:
                coord_attrs = dict(ds[dim_name].attrs)
                # Decode failures (malformed units/calendar) propagate by
                # design: silent except-swallowing here previously hid the
                # 1970-epoch coverage bug. See DESIGN.md "Risks To Avoid"
                # (bare-except removed 2026-06-18).
                decoded = decode_or_passthrough(ts_vals, coord_attrs)
                if decoded.dtype.kind == "M":
                    batch_min = cast(pd.Timestamp, pd.Timestamp(decoded.min()))
                    batch_max = cast(pd.Timestamp, pd.Timestamp(decoded.max()))
                    if pd.isna(batch_min) or pd.isna(batch_max):
                        raise ValueError(
                            f"Invalid timestamp value after decoding for dim {dim_name!r}"
                        )
                    if self._time_min is None or batch_min < self._time_min:
                        self._time_min = batch_min
                    if self._time_max is None or batch_max > self._time_max:
                        self._time_max = batch_max

    def build_entry(
        self,
        *,
        group: str,
        coverage_arrays: list[str],
        state_var_name: str,
        state_deleted_value: int,
        chunk_len_used: int | None = None,
    ) -> dict[str, Any] | None:
        """Build the coverage dict for this group, or *None* if nothing written."""
        if not self._written_ranges:
            return None
        entry: dict[str, Any] = {
            "group": str(group),
            "arrays": coverage_arrays,
            "time_index_ranges": self._written_ranges,
            "aligned": bool(self._aligned_all),
            "state_array": f"{group}/{state_var_name}",
            "state_deleted_value": int(state_deleted_value),
            "time_min": self._time_min.isoformat() if self._time_min else None,
            "time_max": self._time_max.isoformat() if self._time_max else None,
            "time_dim_name": self._time_dim_name,
        }
        if chunk_len_used is not None:
            entry["chunk_len_used"] = int(chunk_len_used)
        return entry


def _normalise_store_uri(uri: str) -> str:
    """Compare store URIs by path, tolerating ``file://`` prefixes and trailing slashes."""
    text = str(uri).strip()
    if text.startswith("file://"):
        text = text[len("file://") :]
    return text.rstrip("/")


def verify_post_write_integrity(
    *,
    temp_store_uri: str,
    final_target_uri: str,
    touched_chunks: dict[str, dict[str, list[tuple[int, ...]]]],
    session: StorageSession,
    append_dim: str,
    state_var_name: str = "firecube_timestamp_state",
) -> None:
    """Verify seeded ``state=1`` slots remain intact after a staged append write.

    Iterates ``touched_chunks[group][state_var_name]`` and, for each chunk,
    reads the same region from both the workspace and the final target. Any
    slot that was ``state=1`` in the target MUST still be ``state=1`` in the
    workspace, otherwise the workspace's state array is corrupted and would
    overwrite a valid target state on promotion.

    When ``touched_chunks[group][append_dim]`` exists, the same touched chunks
    are also checked for coordinate-value drift on the subset of slots whose
    target state is ``1``. Slots whose target state is ``0`` are ignored because
    they are legitimate new writes.

    Scope is deliberately narrow: only chunks passed via ``touched_chunks``
    are inspected. Slots outside seeded chunks read as fill (``0``) from the
    workspace, so a full-array check would false-positive on every staged
    run. This is a no-op when ``touched_chunks`` has no relevant keys for any
    group, when either store is missing, or when the checked arrays are absent
    from either store.

    On mismatch the workspace root is deleted via ``session.fs().rm`` and
    :class:`IntegrityGuardError` is raised so the run fails loudly.
    """
    import zarr as _zarr

    from firecube.core.uris import storage_uri_from_target

    if not touched_chunks:
        return

    try:
        ws_handle = session.zarr.create_store(uri=storage_uri_from_target(temp_store_uri), mode="r")
        ws_root = _zarr.open_group(
            **ws_handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
        )
    except FileNotFoundError:
        return
    try:
        target_handle = session.zarr.create_store(
            uri=storage_uri_from_target(final_target_uri), mode="r"
        )
        target_root = _zarr.open_group(
            **target_handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
        )
    except FileNotFoundError:
        return

    for group_name, arrays in touched_chunks.items():
        chunk_indices = arrays.get(state_var_name)
        if not chunk_indices:
            continue

        try:
            ws_group = cast(Any, ws_root[group_name])
        except KeyError:
            continue
        try:
            target_group = cast(Any, target_root[group_name])
        except KeyError:
            continue

        try:
            ws_arr = cast(Any, ws_group[state_var_name])
        except KeyError:
            continue
        try:
            target_arr = cast(Any, target_group[state_var_name])
        except KeyError:
            continue

        chunk_shape = tuple(int(x) for x in ws_arr.chunks)
        ws_shape = tuple(int(x) for x in ws_arr.shape)
        target_shape = tuple(int(x) for x in target_arr.shape)
        compare_shape = tuple(min(target_shape[i], ws_shape[i]) for i in range(len(target_shape)))

        for chunk_idx in chunk_indices:
            target_region = chunk_index_to_region(chunk_idx, chunk_shape, target_shape)
            if any(axis_region.stop <= axis_region.start for axis_region in target_region):
                continue
            ws_region = chunk_index_to_region(chunk_idx, chunk_shape, compare_shape)
            target_chunk = np.asarray(target_arr[target_region])
            ws_chunk = np.asarray(ws_arr[ws_region])
            was_one_mask = target_chunk == 1
            still_one_mask = ws_chunk == 1
            corrupted_mask = was_one_mask & ~still_one_mask
            if not bool(corrupted_mask.any()):
                continue

            _delete_workspace(session=session, temp_store_uri=temp_store_uri)
            corrupted_offsets = [int(i) for i in np.where(corrupted_mask.ravel())[0][:5]]
            raise IntegrityGuardError(
                f"Post-write integrity check failed for group {group_name!r} "
                f"array {state_var_name!r} chunk {chunk_idx!r}: "
                f"{int(corrupted_mask.sum())} slot(s) were state=1 in target "
                f"{final_target_uri!r} but changed in workspace "
                f"{temp_store_uri!r}. First corrupted offsets within chunk: "
                f"{corrupted_offsets}. Workspace deleted."
            )

    for group_name, arrays in touched_chunks.items():
        coord_chunk_indices = arrays.get(append_dim)
        if not coord_chunk_indices:
            continue

        try:
            ws_group = cast(Any, ws_root[group_name])
        except KeyError:
            continue
        try:
            target_group = cast(Any, target_root[group_name])
        except KeyError:
            continue

        try:
            ws_coord_arr = cast(Any, ws_group[append_dim])
        except KeyError:
            continue
        try:
            target_coord_arr = cast(Any, target_group[append_dim])
        except KeyError:
            continue
        try:
            target_state_arr = cast(Any, target_group[state_var_name])
        except KeyError:
            continue

        chunk_shape = tuple(int(x) for x in target_coord_arr.chunks)
        target_shape = tuple(int(x) for x in target_coord_arr.shape)
        ws_shape = tuple(int(x) for x in ws_coord_arr.shape)
        compare_shape = tuple(min(target_shape[i], ws_shape[i]) for i in range(len(target_shape)))

        for chunk_idx in coord_chunk_indices:
            target_region = chunk_index_to_region(chunk_idx, chunk_shape, target_shape)
            if any(axis_region.stop <= axis_region.start for axis_region in target_region):
                continue
            compare_region = chunk_index_to_region(chunk_idx, chunk_shape, compare_shape)
            if any(axis_region.stop <= axis_region.start for axis_region in compare_region):
                continue

            target_state_slice = np.asarray(target_state_arr[compare_region])
            target_coord_slice = np.asarray(target_coord_arr[compare_region])
            ws_coord_slice = np.asarray(ws_coord_arr[compare_region])
            state_one_mask = target_state_slice == 1
            if not bool(state_one_mask.any()):
                continue

            mismatch_mask = np.not_equal(target_coord_slice, ws_coord_slice) & state_one_mask
            if not bool(mismatch_mask.any()):
                continue

            _delete_workspace(session=session, temp_store_uri=temp_store_uri)
            corrupted_offsets = [int(i) for i in np.where(mismatch_mask.ravel())[0][:5]]
            target_values = target_coord_slice[mismatch_mask][:3]
            workspace_values = ws_coord_slice[mismatch_mask][:3]
            raise IntegrityGuardError(
                f"Post-write integrity check failed for group {group_name!r} "
                f"array {append_dim!r} chunk {chunk_idx!r}: coordinate drift "
                f"on {int(mismatch_mask.sum())} state=1 slot(s). "
                f"First corrupted offsets within chunk: {corrupted_offsets}. "
                f"target={target_values!r} workspace={workspace_values!r}. "
                "Workspace deleted."
            )
