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

"""Append-by-time helpers for Zarr stores.

This module owns the generic 'append multiple groups over time' workflow used by
multiple products:
  - resume detection + cursor computation
  - timestamp state array creation for legacy stores
  - consistent coverage tracking for ChunkManager span bookkeeping
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
import xarray as xr
import zarr

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.uris import storage_uri_from_target
from firecube.ingestor.runtime.zarr.append_services import (
    AppendCoverageBuilder,
    AppendResumeService,
    AppendTimestampState,
    AppendWriteExecutor,
)
from firecube.ingestor.runtime.zarr.write import (
    _consolidate_metadata_best_effort,
    write_dataset_to_zarr,
)

if TYPE_CHECKING:
    from firecube.core.filesystem.store_factory import ZarrStoreHandle


def _read_array_meta_from_store(
    store: object,
    array_path: str,
    ds_var_dims: list[str],
    *,
    sharded: bool = False,
) -> tuple[list[str], list[int], list[int]]:
    """Read dim names, shape, and chunk/shard shape from a non-URI store object."""
    import zarr as _zarr

    _root = _zarr.open_group(store, mode="r")
    _array = cast(Any, _root[array_path])
    dim_names = list(ds_var_dims)
    shape = [int(x) for x in _array.shape]
    if sharded:
        cg = getattr(getattr(_array, "metadata", None), "chunk_grid", None)
        if cg is not None:
            chunks = [int(x) for x in cg.chunk_shape]
        else:
            chunks = [int(x) for x in _array.chunks]
    else:
        chunks = [int(x) for x in _array.chunks]
    return dim_names, shape, chunks


def _coerce_append_value(value: Any, attrs: Mapping[str, Any] | None = None) -> Any:
    """Normalize append-dimension values for safe comparison/logging.

    Parameters
    ----------
    value:
        A single time-coordinate scalar.
    attrs:
        Coordinate attributes from the DataArray. When *value* is numeric and *attrs*
        contains a ``units`` key with ``'since'`` (CF-style time encoding), the value
        is decoded to a ``pd.Timestamp``. Otherwise the raw value is returned.
    """
    import numpy as np

    from firecube.core.zarr.time_decode import decode_time_array

    if hasattr(value, "isoformat") or isinstance(value, (str, np.datetime64)):
        try:
            ts = pd.Timestamp(value)
        except Exception:
            return value
    elif isinstance(value, (int, float, np.integer, np.floating)):
        # Only apply CF-time decoding when attrs contain a units string with 'since'.
        # For plain integer indices (e.g. 0, 1, 2) used as slot identifiers, fall
        # through and return the raw value for comparison — no decoding needed.
        units = (attrs or {}).get("units", "")
        if units and "since" in str(units):
            decoded = decode_time_array(np.asarray([value]), attrs or {})
            ts = pd.Timestamp(decoded[0])
        else:
            return value  # raw integer/float index — use as-is for deduplication
    else:
        try:
            ts = pd.Timestamp(value)
        except Exception:
            return value
    is_na = pd.isna(ts)
    if isinstance(is_na, bool) and is_na:
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _extract_append_values(ds: xr.Dataset, append_dim: str) -> set[Any]:
    """Return normalized append-dimension values for the provided dataset batch."""
    if append_dim not in ds.coords and append_dim not in ds.data_vars:
        return set()
    if int(ds.sizes.get(append_dim, 0)) <= 0:
        return set()
    values = ds[append_dim].values
    if getattr(values, "size", 0) <= 0:
        return set()
    coord_attrs = dict(ds[append_dim].attrs)
    normalized: set[Any] = set()
    for item in values.ravel():
        try:
            coerced = _coerce_append_value(item, coord_attrs)
        except ValueError:
            coerced = item
        if coerced is not None:
            normalized.add(coerced)
    return normalized


def _read_existing_append_values(
    *,
    store_uri: str,
    group: str,
    append_dim: str,
    session: StorageSession | None = None,
) -> set[Any]:
    """Read normalized append-dimension values already present in the target group."""
    store_storage_uri = storage_uri_from_target(store_uri)
    if session is not None:
        reader_session = session
    else:
        reader_session = StorageSession(
            StorageBinding(
                identity=ProductIdentity.from_uri(
                    store_storage_uri,
                    format="zarr",
                    product_name=store_uri,
                ),
                driver=StorageDriverConfig.from_storage_config_or_default(None),
            )
        )
    existing_ds = reader_session.zarr.open_dataset(
        store_storage_uri,
        group=str(group),
    )
    try:
        if append_dim not in existing_ds.coords and append_dim not in existing_ds.data_vars:
            return set()
        if int(existing_ds.sizes.get(append_dim, 0)) <= 0:
            return set()
        values = existing_ds[append_dim].values
        if getattr(values, "size", 0) == 0:
            return set()
        coord_attrs = dict(existing_ds[append_dim].attrs)
        normalized: set[Any] = set()
        for item in values.ravel():
            try:
                coerced = _coerce_append_value(item, coord_attrs)
            except ValueError:
                coerced = item
            if coerced is not None:
                normalized.add(coerced)
        return normalized
    finally:
        with suppress(Exception):
            existing_ds.close()


def _read_existing_group_meta(
    handle: ZarrStoreHandle,
    primary_array_path: str,
    *,
    sharded: bool,
) -> tuple[bool, list[str] | None, list[int] | None, list[int] | None, list[int] | None]:
    from firecube.core.zarr.validation import (
        read_chunk_grid_from_handle,
        read_chunk_grid_with_shards_from_handle,
    )

    try:
        if sharded:
            dim_names, shape, outer_chunks, inner_chunks = read_chunk_grid_with_shards_from_handle(
                handle,
                primary_array_path,
            )
            return True, dim_names, shape, outer_chunks, inner_chunks
        dim_names, shape, chunks = read_chunk_grid_from_handle(
            handle,
            primary_array_path,
        )
        return True, dim_names, shape, chunks, None
    except (FileNotFoundError, KeyError):
        return False, None, None, None, None


def _read_existing_group_meta_from_source(
    *,
    handle: ZarrStoreHandle | None,
    source_uri: str | None,
    primary_array_path: str,
    read_options: dict[str, Any] | None,
    storage_config: Any | None,
    sharded: bool,
) -> tuple[bool, list[str] | None, list[int] | None, list[int] | None, list[int] | None]:
    if handle is None and source_uri is not None and storage_config is not None:
        from firecube.core.filesystem.store_factory import create_zarr_store

        handle = create_zarr_store(uri=source_uri, storage_config=storage_config, mode="r")
    if handle is not None:
        return _read_existing_group_meta(handle, primary_array_path, sharded=sharded)
    if source_uri is None:
        return False, None, None, None, None

    from firecube.core.zarr.validation import read_chunk_grid, read_chunk_grid_with_shards

    extra_kwargs: dict[str, Any] = (
        {"storage_options": read_options} if read_options is not None else {}
    )
    try:
        if sharded:
            dim_names, shape, chunks, inner = read_chunk_grid_with_shards(
                source_uri, primary_array_path, **extra_kwargs
            )
            return True, dim_names, shape, chunks, inner
        dim_names, shape, chunks = read_chunk_grid(source_uri, primary_array_path, **extra_kwargs)
        return True, dim_names, shape, chunks, None
    except (FileNotFoundError, KeyError):
        return False, None, None, None, None


def _validate_shard_shape(
    existing_chunks: list[int],
    shard_shape: dict[str, int],
    var_dims: list[str],
    array_path: str,
) -> None:
    if len(existing_chunks) != len(var_dims):
        return
    expected = [int(shard_shape.get(dim, existing_chunks[i])) for i, dim in enumerate(var_dims)]
    if tuple(int(x) for x in existing_chunks) != tuple(expected):
        raise ValueError(
            f"Existing {array_path} shard_shape={list(existing_chunks)} "
            f"does not match requested shard_shape {expected}"
        )


def _store_uri_from_handle(handle: ZarrStoreHandle | None) -> str | None:
    if handle is None:
        return None
    return handle.target_uri


def append_time_groups(
    *,
    store: object,
    zarr_store: ZarrStoreHandle,
    group_to_timestamps: Mapping[str, Sequence[Any]],
    dataset_for_batch: Callable[[str, Sequence[Any]], xr.Dataset | None],
    session: StorageSession | None = None,
    resume_session: StorageSession | None = None,
    resume_zarr_store: ZarrStoreHandle | None = None,
    arrays_for_group: Callable[[str], list[str]] | None = None,
    chunk_shape: dict[str, int] | None = None,
    shard_shape: dict[str, int] | None = None,
    sharding: bool = False,
    compression: bool | str = False,
    consolidate: bool = False,
    resume_existing: bool = False,
    batch_size: int = 20,
    append_dim: str = "timestamp",
    state_var_name: str = "firecube_timestamp_state",
    state_deleted_value: int = 2,
    logger: logging.Logger | None = None,
    claim_for_group: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Append datasets for multiple groups in time batches with span coverage output."""
    logger = logger or logging.getLogger("firecube.ingestor.runtime.zarr.append")
    started = time.time()

    store_uri: str | None = _store_uri_from_handle(zarr_store)
    if store_uri is None and isinstance(store, (str, Path)):
        store_uri = str(store)
    if store_uri is None and session is not None:
        store_uri = session.product.product_uri.to_str()

    storage_options = zarr_store.storage_options if zarr_store is not None else None
    if not storage_options:
        storage_options = None
    if chunk_shape is not None and not isinstance(chunk_shape, Mapping):
        chunk_shape = None
    if shard_shape is not None and not isinstance(shard_shape, Mapping):
        shard_shape = None
    if resume_existing and store_uri is None:
        raise ValueError("store_uri is required for resume_existing=True")

    if resume_zarr_store is not None:
        read_source_uri = _store_uri_from_handle(resume_zarr_store) or store_uri
        read_storage_options = resume_zarr_store.storage_options
    else:
        read_source_uri = store_uri
        read_storage_options = storage_options

    if session is not None:
        from firecube.core.storage.session import storage_config_from_binding

        storage_config = storage_config_from_binding(session._binding)
    else:
        storage_config = None

    resume_svc = AppendResumeService(
        read_source_uri=read_source_uri,
        read_storage_options=read_storage_options,
        resume_existing=resume_existing,
        append_dim=append_dim,
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=sharding,
        logger=logger,
        session=session,
        resume_session=resume_session,
        storage_config=storage_config,
        read_zarr_store=resume_zarr_store or zarr_store,
    )
    ts_state = AppendTimestampState(state_var_name, time_dim_name=append_dim)
    writer = AppendWriteExecutor(
        zarr_store=zarr_store,
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=sharding,
        compression=compression,
        append_dim=append_dim,
        logger=logger,
        write_fn=write_dataset_to_zarr,
    )
    batches_attempted = batches_written = ts_requested = ts_written = 0
    timestamps_per_group: dict[str, int] = {}
    coverage_entries: list[dict[str, Any]] = []

    for group, timestamps in group_to_timestamps.items():
        ts_list = list(timestamps or [])
        if not ts_list:
            continue
        timestamps_per_group[str(group)] = len(ts_list)
        resume_svc.init_group()
        cov = AppendCoverageBuilder(time_dim_name=append_dim)

        claim_ctx = claim_for_group(str(group)) if claim_for_group is not None else nullcontext()
        with claim_ctx:
            for start in range(0, len(ts_list), int(batch_size)):
                batch = ts_list[start : start + int(batch_size)]
                if not batch:
                    continue
                batches_attempted += 1
                ts_requested += len(batch)
                ds = dataset_for_batch(str(group), batch)
                if ds is None:
                    continue
                count = int(ds.sizes.get(append_dim, 0))
                if count <= 0:
                    continue
                ds = ts_state.attach(ds, append_dim=append_dim)
                if not resume_svc.prepare_write(
                    ds=ds,
                    group=group,
                    store=store,
                    write_target_uri=store_uri,
                    arrays_for_group=arrays_for_group,
                    ts_state=ts_state,
                ):
                    continue
                writer.execute(ds=ds, group=group, mode=resume_svc.mode)
                batches_written += 1
                ts_written += count
                start_i = resume_svc.advance_cursor(count)
                aligned = writer.check_alignment(
                    start_i=start_i,
                    count=count,
                    chunk_len=resume_svc.chunk_len,
                    group=group,
                )
                cov.record_batch(
                    start_i=start_i,
                    count=count,
                    ds=ds,
                    append_dim=append_dim,
                    aligned=aligned,
                )
                resume_svc.update_cache_after_write()

        entry = cov.build_entry(
            group=group,
            coverage_arrays=resume_svc.coverage_arrays,
            state_var_name=state_var_name,
            state_deleted_value=state_deleted_value,
        )
        if entry:
            coverage_entries.append(entry)

    if consolidate and not zarr.__version__.startswith("3"):
        _consolidate_metadata_best_effort(store, logger=logger)

    return _build_metrics(
        started=started,
        batch_size=batch_size,
        batches_attempted=batches_attempted,
        batches_written=batches_written,
        ts_requested=ts_requested,
        ts_written=ts_written,
        timestamps_per_group=timestamps_per_group,
        coverage_entries=coverage_entries,
    )


def _build_metrics(
    *,
    started: float,
    batch_size: int,
    batches_attempted: int,
    batches_written: int,
    ts_requested: int,
    ts_written: int,
    timestamps_per_group: dict[str, int],
    coverage_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    duration_s = float(time.time() - started)
    metrics: dict[str, Any] = {
        "duration_s": duration_s,
        "timestamps_per_group": timestamps_per_group,
        "batch_processing": {
            "batch_size": int(batch_size),
            "batches_attempted": int(batches_attempted),
            "batches_written": int(batches_written),
            "timestamps_requested": int(ts_requested),
            "timestamps_written": int(ts_written),
        },
    }
    if coverage_entries:
        metrics["coverage"] = coverage_entries
    return metrics
