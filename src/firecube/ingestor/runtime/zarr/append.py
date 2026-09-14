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
from typing import TYPE_CHECKING, Any, Literal, cast

import numpy as np
import pandas as pd
import xarray as xr
import zarr

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.uris import storage_uri_from_target
from firecube.ingestor.runtime.zarr.alignment import AlignmentMonitor
from firecube.ingestor.runtime.zarr.append_failure import (
    AppendBatchFailed,
    AppendBatchOutcome,
    RepairOutcome,
    TouchedSlots,
    repair_failed_group,
)
from firecube.ingestor.runtime.zarr.append_order import AppendOrder
from firecube.ingestor.runtime.zarr.append_services import (
    AppendClassification,
    AppendCoverageBuilder,
    AppendResumeService,
    AppendTimestampState,
    AppendWriteExecutor,
    _normalise_time_values,
    assert_incoming_monotonic,
)
from firecube.ingestor.runtime.zarr.schema import validate_existing_time_array_schema
from firecube.ingestor.runtime.zarr.write import (
    _consolidate_metadata_best_effort,
    write_dataset_to_zarr,
)

if TYPE_CHECKING:
    from firecube.core.filesystem.store_factory import ZarrStoreHandle


logger = logging.getLogger(__name__)


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

    Args:
        value: A single time-coordinate scalar.
        attrs: Coordinate attributes from the DataArray. When *value* is
            numeric and *attrs* contains a ``units`` key with ``'since'``
            (CF-style time encoding), the value is decoded to a
            ``pd.Timestamp``. Otherwise the raw value is returned.
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


def _apply_state_aware_skip(
    *,
    ds: xr.Dataset,
    resume_svc: AppendResumeService,
    group: str,
    append_dim: str,
) -> tuple[xr.Dataset | None, int]:
    """Drop already-present timestamps from ``ds`` during resume.

    Returns ``(filtered_ds, skipped_count)`` where ``filtered_ds`` is ``None``
    when every timestamp in the batch was skipped. Overlap with present slots
    is skipped without raising.
    """
    skip_set = resume_svc.compute_state_aware_skip_set(ds=ds, group=group)
    if not skip_set:
        return ds, 0
    coord_attrs = dict(ds[append_dim].attrs)
    values = ds[append_dim].values
    keep_mask = np.array(
        [_coerce_append_value(v, coord_attrs) not in skip_set for v in values],
        dtype=bool,
    )
    skipped = int((~keep_mask).sum())
    if skipped == 0:
        return ds, 0
    if not keep_mask.any():
        return None, skipped
    return ds.isel({append_dim: keep_mask}), skipped


def _append_coverage_entry(
    coverage_entries: list[dict[str, Any]],
    coverage: AppendCoverageBuilder,
    *,
    group: Any,
    coverage_arrays: list[str],
    state_var_name: str,
    state_deleted_value: int,
    chunk_len_used: int | None,
) -> None:
    entry = coverage.build_entry(
        group=group,
        coverage_arrays=coverage_arrays,
        state_var_name=state_var_name,
        state_deleted_value=state_deleted_value,
        chunk_len_used=chunk_len_used,
    )
    if entry:
        coverage_entries.append(entry)


def _compute_touched_data_chunks(
    *,
    classification: AppendClassification | None,
    write_cursor: int,
    count: int,
    append_dim: str,
    zarr_store: ZarrStoreHandle,
    coverage_arrays: list[str],
    group: str,
    state_var_name: str,
) -> dict[str, dict[str, list[tuple[int, ...]]]]:
    """Compute chunk-grid indices that the pending batch write will touch.

    Returns the ``{group: {array_name: [chunk_idx, ...]}}`` structure consumed
    by :func:`seed_touched_data_chunks`. The time-axis regions are derived
    from the resume classification:

    - ``region_overwrite`` — a single slice from ``classification.overwrite_slice``.
    - ``split_region_plus_append`` — the overwrite slice plus a tail append
      slice at the current write cursor (union of touched chunks).
    - ``append_only`` (or ``classification is None``) — the append slice
      ``[write_cursor, write_cursor + count)``.

    Arrays absent from the workspace store are skipped silently: seeding is
    idempotent and a missing metadata file simply means there is nothing to
    seed for this array yet.

    ``state_var_name`` is the engine-managed per-timestamp state array (e.g.
    ``firecube_timestamp_state``). It is excluded from ``coverage_arrays``
    because it is not a plugin data variable, but it IS time-indexed and its
    pre-existing chunk contents must be seeded so that a staged batch writing
    to a partial chunk does not overwrite prior slots' state markers on
    promotion.
    """
    import zarr as _zarr

    from firecube.core.zarr._drift import touched_chunks_for_slice

    time_slices: list[slice] = []
    if classification is not None and classification.overwrite_slice is not None:
        time_slices.append(classification.overwrite_slice)
        if classification.mode == "split_region_plus_append":
            region_start = int(classification.overwrite_slice.start or 0)
            region_stop = int(classification.overwrite_slice.stop or region_start)
            prefix_count = region_stop - region_start
            tail_count = max(0, count - prefix_count)
            if tail_count > 0:
                time_slices.append(slice(write_cursor, write_cursor + tail_count))
    else:
        time_slices.append(slice(write_cursor, write_cursor + count))

    try:
        root = _zarr.open_group(
            **zarr_store.zarr_kwargs(),
            mode="r",
            zarr_format=3,
            use_consolidated=False,
        )
    except FileNotFoundError:
        logger.debug(
            "_compute_touched_data_chunks: workspace store not found; returning empty chunks",
            extra={"store": zarr_store.target_uri},
        )
        return {group: {}}

    try:
        ws_group = cast(Any, root[group])
    except KeyError:
        logger.debug(
            "_compute_touched_data_chunks: workspace group missing; returning empty chunks",
            extra={"group": group},
        )
        return {group: {}}

    def _chunks_touched_by_array(name: str) -> list[tuple[int, ...]]:
        try:
            arr = cast(Any, ws_group[name])
        except KeyError:
            logger.debug(
                "_compute_touched_data_chunks: workspace array missing; skipping array",
                extra={"array": name},
            )
            return []
        dim_names = tuple(getattr(arr.metadata, "dimension_names", ()) or ())
        try:
            time_idx = dim_names.index(append_dim)
        except ValueError:
            return []
        array_shape = tuple(int(x) for x in arr.shape)
        chunk_shape = tuple(int(x) for x in arr.chunks)
        chunk_set: set[tuple[int, ...]] = set()
        for time_slice in time_slices:
            chunks = touched_chunks_for_slice(
                array_shape=array_shape,
                chunk_shape=chunk_shape,
                region={time_idx: time_slice},
            )
            chunk_set.update(chunks)
        return sorted(chunk_set)

    result: dict[str, dict[str, list[tuple[int, ...]]]] = {group: {}}
    for array_path in coverage_arrays:
        prefix = f"{group}/"
        name = array_path[len(prefix) :] if array_path.startswith(prefix) else array_path
        chunks = _chunks_touched_by_array(name)
        if chunks:
            result[group][name] = chunks

    state_chunks = _chunks_touched_by_array(state_var_name)
    if state_chunks:
        result[group][state_var_name] = state_chunks

    coord_chunks = _chunks_touched_by_array(append_dim)
    if coord_chunks:
        result[group][append_dim] = coord_chunks

    return result


def _check_and_record(
    *,
    writer: AppendWriteExecutor,
    coverage: AppendCoverageBuilder,
    resume_svc: AppendResumeService,
    start_i: int,
    count: int,
    ds: xr.Dataset,
    append_dim: str,
    group: str,
    is_final: bool,
) -> bool:
    aligned = writer.check_alignment(
        start_i=start_i,
        count=count,
        chunk_len=resume_svc.chunk_len,
        group=group,
        is_final=is_final,
    )
    coverage.record_batch(
        start_i=start_i,
        count=count,
        ds=ds,
        append_dim=append_dim,
        aligned=aligned,
    )
    return aligned


def _write_batch(
    *,
    writer: AppendWriteExecutor,
    resume_svc: AppendResumeService,
    order: AppendOrder,
    coverage: AppendCoverageBuilder,
    dataset_for_batch: Callable[[str, Sequence[Any]], xr.Dataset | None],
    batch: Sequence[Any],
    group: str,
    touched_ref: list[TouchedSlots | None],
    zarr_store: ZarrStoreHandle,
    resume_zarr_store: ZarrStoreHandle | None,
    store: object,
    store_uri: str | None,
    arrays_for_group: Callable[[str], list[str]] | None,
    ts_state: AppendTimestampState,
    append_dim: str,
    is_final: bool,
    state_var_name: str,
    resume_existing: bool,
    force_reingest: bool,
    logger: logging.Logger,
    pipeline_write_mode: str | None = None,
    final_target_uri: str | None = None,
    session: StorageSession | None = None,
) -> tuple[tuple[int, int] | None, TouchedSlots | None, int]:
    """Prepare and write one logical append batch, recording coverage ranges.

    This is the mechanical home for the four former write blocks. It preserves
    their ordering around touch tracking, physical writes, cursor advancement,
    alignment checks, and coverage recording. The returned write delta is
    consumed by :func:`_record_written`; ``None`` means the batch was skipped
    before any store write.
    """
    skipped_total = 0
    touched = touched_ref[0]
    ds = dataset_for_batch(group, batch)
    if ds is None:
        return None, touched, skipped_total
    count = int(ds.sizes.get(append_dim, 0))
    if count <= 0:
        return None, touched, skipped_total
    batch_coord = np.asarray(ds[append_dim].values)
    assert_incoming_monotonic(
        _normalise_time_values(batch_coord, target="ns"),
        display=batch_coord.reshape(-1).tolist(),
    )
    ds = ts_state.attach(ds, append_dim=append_dim)
    validate_existing_time_array_schema(
        ds,
        zarr_store,
        group,
        time_dim=append_dim,
        state_var_name=state_var_name,
    )
    if not resume_svc.prepare_write(
        ds=ds,
        group=group,
        store=store,
        write_target_uri=store_uri,
        arrays_for_group=arrays_for_group,
        ts_state=ts_state,
    ):
        return None, touched, skipped_total

    if resume_existing and not force_reingest and resume_svc.mode == "a":
        ds, skipped = _apply_state_aware_skip(
            ds=ds,
            resume_svc=resume_svc,
            group=group,
            append_dim=append_dim,
        )
        if skipped:
            skipped_total += skipped
            logger.info(
                "resume_existing state-aware skip: dropped %d already-present timestamps",
                skipped,
                extra={
                    "group": group,
                    "append_dim": append_dim,
                    "timestamps_skipped": skipped,
                },
            )
        if ds is None:
            return None, touched, skipped_total
        count = int(ds.sizes.get(append_dim, 0))
        if count <= 0:
            return None, touched, skipped_total

    classify_for_region_write = resume_svc.mode == "a" and (force_reingest or resume_existing)
    classification: AppendClassification | None = (
        resume_svc.classify_dataset(
            ds=ds,
            group=group,
            allow_refill_plus_append=resume_existing and not force_reingest,
        )
        if classify_for_region_write
        else None
    )
    write_mode = cast(Literal["w", "a"], resume_svc.mode)

    touched_data_chunks: dict[str, dict[str, list[tuple[int, ...]]]] = {}
    staged_resume_active = (
        pipeline_write_mode == "staged"
        and resume_svc.mode == "a"
        and final_target_uri is not None
        and store_uri is not None
        and session is not None
    )
    if staged_resume_active:
        touched_data_chunks = _compute_touched_data_chunks(
            classification=classification,
            write_cursor=resume_svc.write_cursor,
            count=count,
            append_dim=append_dim,
            zarr_store=zarr_store,
            coverage_arrays=resume_svc.coverage_arrays,
            group=group,
            state_var_name=state_var_name,
        )
        if touched_data_chunks.get(group):
            from firecube.ingestor.runtime.zarr.staged_metadata import (
                seed_touched_data_chunks,
            )

            seed_touched_data_chunks(
                temp_store_uri=cast(str, store_uri),
                final_target_uri=cast(str, final_target_uri),
                touched_chunks=touched_data_chunks,
                session=cast(StorageSession, session),
            )

    def _run_integrity_guard() -> None:
        if not staged_resume_active or not touched_data_chunks:
            return
        from firecube.ingestor.runtime.zarr.append_services import (
            verify_post_write_integrity,
        )

        verify_post_write_integrity(
            temp_store_uri=cast(str, store_uri),
            final_target_uri=cast(str, final_target_uri),
            touched_chunks=touched_data_chunks,
            session=cast(StorageSession, session),
            append_dim=append_dim,
            state_var_name=state_var_name,
        )

    if classification is not None and classification.mode == "region_overwrite":
        if classification.overwrite_slice is None:
            raise ValueError("force_reingest region overwrite requires a target slice")
        touched = touched or _touched_slots(group, resume_svc)
        touched_ref[0] = touched
        touched.record_region(classification.overwrite_slice, ds, append_dim)
        start_i = int(classification.overwrite_slice.start or 0)
        writer.execute(ds=ds, group=group, mode=write_mode, region=classification.overwrite_slice)
        _check_and_record(
            writer=writer,
            coverage=coverage,
            resume_svc=resume_svc,
            start_i=start_i,
            count=count,
            ds=ds,
            append_dim=append_dim,
            group=group,
            is_final=is_final,
        )
        _run_integrity_guard()
        return (1, count), touched, skipped_total

    if classification is not None and classification.mode == "split_region_plus_append":
        if classification.overwrite_slice is None:
            raise ValueError("force_reingest split overwrite requires a target slice")
        region_start = int(classification.overwrite_slice.start or 0)
        region_stop = int(classification.overwrite_slice.stop or region_start)
        prefix_count = region_stop - region_start
        ds_prefix = ds.isel({append_dim: slice(0, prefix_count)})
        ds_tail = ds.isel({append_dim: slice(prefix_count, None)})
        tail_count = int(ds_tail.sizes.get(append_dim, 0))

        order.assert_append(
            ds_tail,
            group=group,
            time_dim=append_dim,
            write_store=zarr_store,
            resume_store=resume_zarr_store,
        )
        touched = touched or _touched_slots(group, resume_svc)
        touched_ref[0] = touched
        touched.record_region(classification.overwrite_slice, ds_prefix, append_dim)
        writer.execute(
            ds=ds_prefix,
            group=group,
            mode=write_mode,
            region=classification.overwrite_slice,
        )
        _check_and_record(
            writer=writer,
            coverage=coverage,
            resume_svc=resume_svc,
            start_i=region_start,
            count=prefix_count,
            ds=ds_prefix,
            append_dim=append_dim,
            group=group,
            is_final=is_final,
        )

        if tail_count > 0:
            writer.execute(ds=ds_tail, group=group, mode="a")
            start_i = resume_svc.advance_cursor(tail_count)
            _check_and_record(
                writer=writer,
                coverage=coverage,
                resume_svc=resume_svc,
                start_i=start_i,
                count=tail_count,
                ds=ds_tail,
                append_dim=append_dim,
                group=group,
                is_final=is_final,
            )
        _run_integrity_guard()
        return (1, count), touched, skipped_total

    fresh_write = resume_svc.mode == "w"
    order.assert_append(
        ds,
        group=group,
        time_dim=append_dim,
        write_store=zarr_store,
        resume_store=resume_zarr_store,
    )
    touched = touched or _touched_slots(group, resume_svc)
    touched_ref[0] = touched
    writer.execute(ds=ds, group=group, mode=write_mode)
    if fresh_write:
        resume_svc.refresh_chunk_len_from_stored_array(ds, group, store)
    start_i = resume_svc.advance_cursor(count)
    _check_and_record(
        writer=writer,
        coverage=coverage,
        resume_svc=resume_svc,
        start_i=start_i,
        count=count,
        ds=ds,
        append_dim=append_dim,
        group=group,
        is_final=is_final,
    )
    _run_integrity_guard()
    return (1, count), touched, skipped_total


def _record_written(
    written: tuple[int, int],
    *,
    counters: dict[str, int],
    resume_svc: AppendResumeService,
    update_cache: bool = True,
) -> None:
    """Apply local write counters and refresh resume cache after a write.

    ``written`` is the ``(batches_written, timestamps_written)`` delta returned
    by :func:`_write_batch`. Cache refresh remains opt-in so split
    region-plus-append writes can update the cache once after both physical
    writes, matching the pre-refactor ordering.
    """
    batches_delta, timestamps_delta = written
    counters["batches_written"] += batches_delta
    counters["ts_written"] += timestamps_delta
    if update_cache:
        resume_svc.update_cache_after_write()


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
    compression: bool = False,
    zarr_codecs: list[dict] | None = None,
    consolidate: bool = False,
    resume_existing: bool = False,
    force_reingest: bool = False,
    batch_size: int = 20,
    append_dim: str = "timestamp",
    state_var_name: str = "firecube_timestamp_state",
    state_deleted_value: int = 2,
    logger: logging.Logger | None = None,
    claim_for_group: Callable[[str], Any] | None = None,
    is_final_batch: bool = False,
    alignment: AlignmentMonitor | None = None,
    order: AppendOrder | None = None,
    pipeline_write_mode: str | None = None,
    final_target_uri: str | None = None,
    preflight_compare_zarr_store: Any = None,
) -> dict[str, Any]:
    """Append datasets for multiple groups in time batches with span coverage output.

    Each group is written atomically with respect to this call: when a group's
    write raises, the group is repaired (region slots marked state 3, tails
    truncated to the pre-write cursor, a fresh group deleted) and
    :class:`AppendBatchFailed` is raised carrying the entries of the groups
    already committed, the failed group's region ranges, and the groups not
    attempted. A failure before any write of this call reached the store
    (a shape mismatch, an overlap refusal, ``dataset_for_batch`` raising for
    the first group) propagates unchanged: the store is untouched and the
    exception type is the contract. Groups are written in mapping order.

    Args:
        is_final_batch: ``True`` when the caller's planner marked this batch
            as the last of the run, so a short final write is not reported as
            unaligned.
        alignment: The run's :class:`AlignmentMonitor`, shared across calls so
            an unaligned layout warns once per run and is summarised at the
            end. ``None`` allocates a monitor scoped to this call.

    Raises:
        AppendBatchFailed: A group's write raised after this call had written
            to the store; ``__cause__`` is the original exception and
            ``outcome`` describes the store state.
    """
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

    ts_state = AppendTimestampState(state_var_name, time_dim_name=append_dim)
    resume_svc = AppendResumeService(
        read_source_uri=read_source_uri,
        read_storage_options=read_storage_options,
        resume_existing=resume_existing,
        append_dim=append_dim,
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=sharding,
        logger=logger,
        state_var_name=ts_state.var_name,
        session=session,
        resume_session=resume_session,
        storage_config=storage_config,
        read_zarr_store=resume_zarr_store or zarr_store,
    )
    writer = AppendWriteExecutor(
        zarr_store=zarr_store,
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=sharding,
        compression=compression,
        zarr_codecs=zarr_codecs,
        append_dim=append_dim,
        logger=logger,
        state_var_name=state_var_name,
        write_fn=write_dataset_to_zarr,
        alignment=alignment if alignment is not None else AlignmentMonitor(),
        preflight_compare_zarr_store=preflight_compare_zarr_store,
        force_reingest=force_reingest,
    )
    order = order if order is not None else AppendOrder()
    batches_attempted = ts_requested = 0
    written_counters = {"batches_written": 0, "ts_written": 0}
    ts_skipped_total = 0
    timestamps_per_group: dict[str, int] = {}
    coverage_entries: list[dict[str, Any]] = []

    group_order = [str(group) for group, ts in group_to_timestamps.items() if list(ts or [])]

    for group, timestamps in group_to_timestamps.items():
        ts_list = list(timestamps or [])
        if not ts_list:
            continue
        timestamps_per_group[str(group)] = len(ts_list)
        resume_svc.init_group()
        cov = AppendCoverageBuilder(time_dim_name=append_dim)
        touched: TouchedSlots | None = None
        touched_ref: list[TouchedSlots | None] = [None]

        try:
            claim_ctx = (
                claim_for_group(str(group)) if claim_for_group is not None else nullcontext()
            )
            with claim_ctx:
                for start in range(0, len(ts_list), int(batch_size)):
                    batch = ts_list[start : start + int(batch_size)]
                    if not batch:
                        continue
                    is_final = is_final_batch and (start + int(batch_size) >= len(ts_list))
                    batches_attempted += 1
                    ts_requested += len(batch)
                    written, touched, skipped = _write_batch(
                        writer=writer,
                        resume_svc=resume_svc,
                        order=order,
                        coverage=cov,
                        dataset_for_batch=dataset_for_batch,
                        batch=batch,
                        group=str(group),
                        touched_ref=touched_ref,
                        zarr_store=zarr_store,
                        resume_zarr_store=resume_zarr_store,
                        store=store,
                        store_uri=store_uri,
                        arrays_for_group=arrays_for_group,
                        ts_state=ts_state,
                        append_dim=append_dim,
                        is_final=is_final,
                        state_var_name=state_var_name,
                        resume_existing=resume_existing,
                        force_reingest=force_reingest,
                        logger=logger,
                        pipeline_write_mode=pipeline_write_mode,
                        final_target_uri=final_target_uri,
                        session=session,
                    )
                    touched = touched_ref[0]
                    ts_skipped_total += skipped
                    if written is None:
                        continue
                    _record_written(
                        written,
                        counters=written_counters,
                        resume_svc=resume_svc,
                    )

            _append_coverage_entry(
                coverage_entries,
                cov,
                group=group,
                coverage_arrays=resume_svc.coverage_arrays,
                state_var_name=state_var_name,
                state_deleted_value=state_deleted_value,
                chunk_len_used=resume_svc.chunk_len,
            )
        except Exception as exc:
            touched = touched_ref[0]
            if touched is None and not coverage_entries:
                # Nothing in this call reached the store: the typed refusal
                # (shape mismatch, overlap, unsorted input) stands on its own.
                raise
            raise _failed_batch(
                exc,
                group=str(group),
                touched=touched,
                zarr_store=zarr_store,
                append_dim=append_dim,
                state_var_name=state_var_name,
                state_deleted_value=state_deleted_value,
                resume_svc=resume_svc,
                logger=logger,
                committed=coverage_entries,
                not_attempted_groups=group_order[group_order.index(str(group)) + 1 :],
                counters=_build_metrics(
                    started=started,
                    batch_size=batch_size,
                    batches_attempted=batches_attempted,
                    batches_written=written_counters["batches_written"],
                    ts_requested=ts_requested,
                    ts_written=written_counters["ts_written"],
                    ts_skipped=ts_skipped_total,
                    timestamps_per_group=timestamps_per_group,
                    coverage_entries=[],
                ),
            ) from exc

    if consolidate and not zarr.__version__.startswith("3"):
        _consolidate_metadata_best_effort(store, logger=logger)

    return _build_metrics(
        started=started,
        batch_size=batch_size,
        batches_attempted=batches_attempted,
        batches_written=written_counters["batches_written"],
        ts_requested=ts_requested,
        ts_written=written_counters["ts_written"],
        ts_skipped=ts_skipped_total,
        timestamps_per_group=timestamps_per_group,
        coverage_entries=coverage_entries,
    )


def _touched_slots(group: str, resume_svc: AppendResumeService) -> TouchedSlots:
    """Start tracking a group's writes just before its first write begins.

    The cursor advances only after a write returns, so at this point it is
    still the pre-write length of the append dimension.
    """
    return TouchedSlots(
        group=group,
        batch_start_cursor=resume_svc.write_cursor,
        fresh_group=resume_svc.mode == "w",
    )


def _failed_batch(
    exc: BaseException,
    *,
    group: str,
    touched: TouchedSlots | None,
    zarr_store: ZarrStoreHandle,
    append_dim: str,
    state_var_name: str,
    state_deleted_value: int,
    resume_svc: AppendResumeService,
    logger: logging.Logger,
    committed: list[dict[str, Any]],
    not_attempted_groups: list[str],
    counters: dict[str, Any],
) -> AppendBatchFailed:
    """Repair the failed group and build the exception that reports the batch.

    Args:
        exc: The exception the group's write raised.
        group: Group whose write failed.
        touched: Slots recorded for the group, or ``None`` when the failure
            happened before its first write started (nothing to repair).
        zarr_store: Handle of the store the batch wrote into.
        append_dim: Name of the append dimension.
        state_var_name: Name of the per-timestamp state array.
        state_deleted_value: Deleted-state value recorded in coverage entries.
        resume_svc: Resume service whose cursor and cache are rewound.
        logger: Receives the failure and repair log lines.
        committed: Coverage entries of the groups flushed before this one.
        not_attempted_groups: Groups after this one in write order.
        counters: Batch counters accumulated up to the failure.

    Returns:
        The :class:`AppendBatchFailed` to raise from ``exc``.
    """
    logger.error(
        "Append batch failed in group %r: %s: %s",
        group,
        type(exc).__name__,
        exc,
        extra={"group": group, "committed_groups": [entry["group"] for entry in committed]},
    )
    failed_entry: dict[str, Any] | None = None
    if touched is None:
        repair = RepairOutcome()
    else:
        repair = repair_failed_group(
            zarr_store=zarr_store,
            group=group,
            append_dim=append_dim,
            state_var_name=state_var_name,
            touched=touched,
            logger=logger,
        )
        resume_svc.rewind_cursor(touched.batch_start_cursor, group_removed=repair.group_removed)
        if touched.region_slices:
            failed_cov = AppendCoverageBuilder(time_dim_name=append_dim)
            for region, coords in zip(touched.region_slices, touched.region_coords, strict=True):
                region_start = int(region.start or 0)
                region_stop = int(region.stop if region.stop is not None else region_start)
                if region_stop <= region_start:
                    continue
                failed_cov.record_batch(
                    start_i=region_start,
                    count=region_stop - region_start,
                    ds=coords,
                    append_dim=append_dim,
                    aligned=True,
                )
            failed_entry = failed_cov.build_entry(
                group=group,
                coverage_arrays=resume_svc.coverage_arrays,
                state_var_name=state_var_name,
                state_deleted_value=state_deleted_value,
                chunk_len_used=resume_svc.chunk_len,
            )
            if failed_entry is not None:
                failed_entry["write_strategy"] = "append_failed"
    outcome = AppendBatchOutcome(
        committed=list(committed),
        failed_group=group,
        failed_entry=failed_entry,
        repair=repair,
        not_attempted_groups=list(not_attempted_groups),
        counters=counters,
    )
    return AppendBatchFailed(outcome, exc)


def _build_metrics(
    *,
    started: float,
    batch_size: int,
    batches_attempted: int,
    batches_written: int,
    ts_requested: int,
    ts_written: int,
    ts_skipped: int,
    timestamps_per_group: dict[str, int],
    coverage_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    duration_s = float(time.time() - started)
    metrics: dict[str, Any] = {
        "duration_s": duration_s,
        "timestamps_per_group": timestamps_per_group,
        "timestamps_skipped": int(ts_skipped),
        "batch_processing": {
            "batch_size": int(batch_size),
            "batches_attempted": int(batches_attempted),
            "batches_written": int(batches_written),
            "timestamps_requested": int(ts_requested),
            "timestamps_written": int(ts_written),
            "timestamps_skipped": int(ts_skipped),
        },
    }
    if coverage_entries:
        metrics["coverage"] = coverage_entries
    return metrics
