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
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast

import pandas as pd
import xarray as xr

from firecube.core.storage.session import StorageSession, storage_config_from_binding
from firecube.core.zarr.time_decode import decode_time_array
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.zarr.resume_cache import (
    ResumeCacheEntry,
    get_resume_cache_entry,
    put_resume_cache_entry,
)

if TYPE_CHECKING:
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
        preexisting_values: frozenset[object] | None,
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
                    preexisting_values=preexisting_values,
                ),
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
        session: StorageSession | None = None,
        resume_session: StorageSession | None = None,
        storage_config: StorageConfig | None = None,
        read_zarr_store: ZarrStoreHandle | None = None,
        time_dim_name: str | None = None,
    ) -> None:
        self._read_source_uri = read_source_uri
        self._read_storage_options = read_storage_options
        self._read_zarr_store = read_zarr_store
        self._resume_existing = resume_existing
        self._append_dim = time_dim_name or append_dim
        self._chunk_shape = chunk_shape
        self._shard_shape = shard_shape
        self._sharding = sharding
        self._logger = logger
        self._session = session
        self._resume_session = resume_session or session
        self._storage_config = storage_config

        self.write_cursor: int = 0
        self.chunk_len: int | None = None
        self.preexisting_values: frozenset[object] | None = None
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
        self.preexisting_values = None
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
                preexisting_values=self.preexisting_values,
                storage_config=storage_config,
            )
            self._detect_overlap(ds, group)
            self.mode = "a"
        else:
            self.mode = "w"
            self.preexisting_values = frozenset()
            if self._chunk_shape and self._append_dim in self._chunk_shape:
                self.chunk_len = int(self._chunk_shape[self._append_dim])

        self._first_write = False
        return True

    def advance_cursor(self, count: int) -> int:
        """Advance write cursor by *count*.  Returns the start index."""
        start_i = self.write_cursor
        self.write_cursor += int(count)
        return start_i

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
                    preexisting_values=self.preexisting_values,
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
        shape: list[int] | None,
        chunks: list[int],
    ) -> int:
        """Expected stored chunk for ``dim``, accounting for first-write clamping.

        Two facts make the raw configured chunk the wrong thing to validate
        against on resume:

        * The append dimension's chunk is fixed when the array is created and is
          never changed by an append, so the configured value is advisory --
          trust the stored chunk (mirrors how the shape check skips this dim).
        * For other dimensions, dask clamps a configured chunk down to the array
          size on the initial write (a chunk cannot exceed the array), so the
          effective chunk is ``min(configured, size)``.

        Dimensions without a configured chunk keep their stored value. Genuine
        drift on a non-append dimension (a configured chunk that differs from
        what is actually achievable) still fails the equality check downstream.
        """
        if not self._chunk_shape or dim == self._append_dim or dim not in self._chunk_shape:
            return int(chunks[idx])
        configured = int(self._chunk_shape[dim])
        if shape is not None and idx < len(shape):
            return min(configured, int(shape[idx]))
        return min(configured, int(chunks[idx]))

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
        """Resolve cursor, chunk_len, preexisting_values for an existing group."""
        from firecube.ingestor.runtime.zarr.append import _validate_shard_shape

        if self._cached is not None:
            self._existing_time = int(self._cached.cursor)
            self.write_cursor = self._existing_time
            if self._cached.chunk_len is not None:
                self.chunk_len = int(self._cached.chunk_len)
            self.preexisting_values = self._cached.preexisting_values
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
            self.preexisting_values = None if self._resume_existing else frozenset()

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
                self._effective_chunk(dim, idx, shape, chunks) for idx, dim in enumerate(var_dims)
            ]
            if tuple(int(x) for x in chunks) != tuple(int(x) for x in expected_chunks):
                raise ValueError(
                    f"Existing {primary_array_path} chunk_shape={list(chunks)} "
                    f"does not match requested {expected_chunks}"
                )

        inferred = 0
        if self._chunk_shape and self._append_dim in self._chunk_shape:
            inferred = int(self._chunk_shape[self._append_dim])
        elif self._shard_shape and self._append_dim in self._shard_shape:
            inferred = int(self._shard_shape[self._append_dim])
        elif chunks:
            try:
                inferred = (
                    int(chunks[list(dim_names).index(self._append_dim)] or 0)
                    if dim_names
                    else int(chunks[0] or 0)
                )
            except (ValueError, IndexError):
                inferred = int(chunks[0] or 0)

        if inferred > 0:
            self.chunk_len = inferred
            if self._cached is None:
                self._logger.debug(
                    "Inferred Zarr chunk length for append dimension",
                    extra={
                        "group": str(group),
                        "dim": self._append_dim,
                        "chunk_len": self.chunk_len,
                    },
                )

    def _detect_overlap(self, ds: xr.Dataset, group: str) -> None:
        """Raise ResumeConflictError when incoming timestamps overlap existing."""
        if not self._resume_existing:
            return

        from firecube.ingestor.runtime.zarr.append import (
            _extract_append_values,
            _read_existing_append_values,
        )

        assert self._read_source_uri is not None
        preexisting = self.preexisting_values
        if preexisting is None:
            preexisting = frozenset(
                _read_existing_append_values(
                    store_uri=self._read_source_uri,
                    group=str(group),
                    append_dim=self._append_dim,
                    session=self._resume_session,
                )
            )
            if self._cached is not None:
                self._cached.preexisting_values = preexisting

        incoming_values = _extract_append_values(ds, self._append_dim)
        if preexisting and incoming_values:
            overlaps = sorted(preexisting.intersection(incoming_values), key=str)
            if overlaps:
                overlap_sample = overlaps[:3]
                sample_render = ", ".join(str(v) for v in overlap_sample)
                if len(overlaps) > len(overlap_sample):
                    sample_render += ", ..."
                from firecube.ingestor.errors import ResumeConflictError

                raise ResumeConflictError(
                    "Refusing overlapping resume append for group "
                    f"'{group}': incoming {self._append_dim} values include "
                    f"already-existing timestamps ({sample_render}). "
                    "Use --option force_reingest=true "
                    "or delete overlapping spans before retry."
                )


class AppendWriteExecutor:
    """Write loop — write_dataset_to_zarr calls, mode determination, alignment."""

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
        write_fn: Any = None,
        time_dim_name: str | None = None,
        zarr_codecs: list[dict] | None = None,
    ) -> None:
        self._zarr_store = zarr_store
        self._chunk_shape = chunk_shape
        self._shard_shape = shard_shape
        self._sharding = sharding
        self._compression = compression
        self._append_dim = time_dim_name or append_dim
        self._logger = logger
        self._write_fn = write_fn
        self._zarr_codecs = zarr_codecs

    def execute(
        self,
        *,
        ds: xr.Dataset,
        group: str,
        mode: Literal["w", "a"],
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
            append_dim=self._append_dim if mode == "a" else None,
            chunk_shape=self._chunk_shape,
            shard_shape=self._shard_shape,
            sharding=self._sharding,
            compression=self._compression,
            zarr_codecs=self._zarr_codecs,
            consolidate=False,
            logger=self._logger,
        )

    def check_alignment(
        self,
        *,
        start_i: int,
        count: int,
        chunk_len: int | None,
        group: str,
    ) -> bool:
        """Check chunk alignment and warn if misaligned.  Returns aligned flag."""
        if not chunk_len or chunk_len <= 0:
            return True
        aligned = (start_i % chunk_len == 0) and (count % chunk_len == 0)
        if not aligned:
            self._logger.warning(
                "Zarr write is unaligned with chunk layout. "
                "This may reduce performance due to Read-Modify-Write cycles. "
                "Recommendation: align pipeline_batch_size with Zarr chunk_len.",
                extra={
                    "group": str(group),
                    "chunk_len": chunk_len,
                    "batch_count": count,
                    "start_index": start_i,
                },
            )
        return aligned


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
                units = coord_attrs.get("units", "")
                if ts_vals.dtype.kind == "M" or (
                    ts_vals.dtype.kind in ("f", "i", "u") and "since" in str(units)
                ):
                    # Decode failures (malformed units/calendar) propagate by
                    # design: silent except-swallowing here previously hid the
                    # 1970-epoch coverage bug. See DESIGN.md "Risks To Avoid"
                    # (bare-except removed 2026-06-18).
                    decoded = decode_time_array(ts_vals, coord_attrs)
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
    ) -> dict[str, Any] | None:
        """Build the coverage dict for this group, or *None* if nothing written."""
        if not self._written_ranges:
            return None
        return {
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
