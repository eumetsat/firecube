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

"""Zarr write helpers for Firecube ingestors.

Centralizes chunking, encoding/compression, and metadata consolidation so that
plugins/templates don't re-implement Zarr plumbing.
"""

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Literal, cast

import numpy as np
import xarray as xr
import zarr
from zarr.abc.codec import ArrayArrayCodec, ArrayBytesCodec, BytesBytesCodec
from zarr.registry import get_codec_class

if TYPE_CHECKING:
    from firecube.core.filesystem.store_factory import ZarrStoreHandle


def _consolidate_metadata_best_effort(
    opened_store: object, *, logger: logging.Logger | None
) -> None:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        try:
            zarr.consolidate_metadata(opened_store)
        except Exception as exc:  # pragma: no cover - best effort
            if logger:
                logger.warning("Failed to consolidate Zarr metadata: %s", exc)


def resolve_codec_pipeline(
    filters: list[dict] | None = None,
    serializer: dict | None = None,
    compressors: list[dict] | None = None,
) -> tuple[list[ArrayArrayCodec] | None, ArrayBytesCodec | None, list[BytesBytesCodec] | None]:
    """Resolve declared codec dicts into typed codec instances.

    Splits and validates the Zarr codec pipeline into filters
    (``ArrayArrayCodec``), a single serializer (``ArrayBytesCodec``), and
    compressors (``BytesBytesCodec``). Each input is optional; when nothing is
    declared the function returns ``(None, None, None)``.

    Args:
        filters: Codec dictionaries expected to resolve to ``ArrayArrayCodec``
            instances (array→array pipeline stages, e.g. delta/scale).
        serializer: A single codec dictionary expected to resolve to an
            ``ArrayBytesCodec`` (array→bytes, e.g. ``bytes``).
        compressors: Codec dictionaries expected to resolve to
            ``BytesBytesCodec`` instances (bytes→bytes, e.g. ``blosc``/``zstd``).

    Returns:
        A tuple ``(filters, serializer, compressors)`` where each element is a
        resolved list/single instance of the appropriate codec type, or
        ``None`` when the caller declared nothing in that position.

    Raises:
        ValueError: When a codec name is not in zarr's codec registry, or when
            a codec's configuration fails codec-specific validation.
        TypeError: When a resolved codec does not match the ABC required by
            the position it was declared in (e.g. a ``bytes`` codec passed
            under ``filters``).
    """
    if filters is None and serializer is None and compressors is None:
        return None, None, None

    resolved_filters: list[ArrayArrayCodec] | None = (
        [
            cast(ArrayArrayCodec, _resolve_codec_entry(entry, ArrayArrayCodec, "filters"))
            for entry in filters
        ]
        if filters
        else None
    )
    resolved_serializer: ArrayBytesCodec | None = (
        cast(ArrayBytesCodec, _resolve_codec_entry(serializer, ArrayBytesCodec, "serializer"))
        if serializer is not None
        else None
    )
    resolved_compressors: list[BytesBytesCodec] | None = (
        [
            cast(BytesBytesCodec, _resolve_codec_entry(entry, BytesBytesCodec, "compressors"))
            for entry in compressors
        ]
        if compressors
        else None
    )

    return resolved_filters, resolved_serializer, resolved_compressors


def _resolve_codec_entry(
    entry: dict,
    expected: type[ArrayArrayCodec] | type[ArrayBytesCodec] | type[BytesBytesCodec],
    position: str,
) -> ArrayArrayCodec | ArrayBytesCodec | BytesBytesCodec:
    name = cast(str, entry["name"])
    configuration = cast(dict[str, Any], entry.get("configuration", {}) or {})
    full_entry = {"name": name, "configuration": configuration}

    try:
        codec_class = get_codec_class(name)
    except KeyError as orig:
        raise ValueError(
            f"zarr_codecs entry name={name!r} is not a registered zarr codec. "
            "Available codecs come from zarr's [zarr.codecs] entry points. "
            "Install a codec package (e.g., 'imagecodecs' provides 'imagecodecs_openzl') "
            f"or check the name spelling. Original error: {orig}"
        ) from orig

    try:
        codec = codec_class.from_dict(full_entry)
    except (ValueError, TypeError) as orig:
        raise ValueError(
            f"zarr_codecs entry {name!r} failed codec-specific validation: {orig}"
        ) from orig

    if not isinstance(codec, expected):
        raise TypeError(
            f"zarr_codecs entry name={name!r} resolved to {type(codec).__name__}, "
            f"but position {position!r} requires a {expected.__name__}."
        )

    return codec


def derive_effective_codecs_for_spec(
    arr_spec: Any,
    template_config: Any,
) -> tuple[list[Any] | None, Any | None, list[Any] | None]:
    """Derive the effective codec pipeline for a single array spec.

    Shared helper for ``DirectZarrIngestor`` and the ``firecube zarr preallocate``
    CLI command: both materialize Zarr arrays from a ``ZarrArraySpec`` under a
    ``ZarrTemplateConfig`` and must resolve codecs with identical precedence so
    that a store created by one command remains resume-safe when the other
    command reopens it.

    Priority: per-array spec fields > template ``zarr_codecs`` > ``None`` (zarr
    default codec pipeline).

    Returns resolved codec instances (filters, serializer, compressors) ready
    to pass to ``RegionZarrWriter.ensure_group``.
    """
    from firecube.core.zarr.codec_pipeline import split_zarr_codecs

    per_array_filters = getattr(arr_spec, "filters", None)
    per_array_serializer = getattr(arr_spec, "serializer", None)
    per_array_compressors = getattr(arr_spec, "compressors", None)

    has_per_array = (
        per_array_filters is not None
        or per_array_serializer is not None
        or per_array_compressors is not None
    )

    if has_per_array:
        filters_dicts = list(per_array_filters) if per_array_filters else None
        serializer_dict = per_array_serializer
        compressors_dicts = (
            list(per_array_compressors) if per_array_compressors is not None else None
        )
        filters, serializer, compressors = resolve_codec_pipeline(
            filters=filters_dicts,
            serializer=serializer_dict,
            compressors=compressors_dicts,
        )
        if per_array_compressors is not None and len(per_array_compressors) == 0:
            compressors = []
        return filters, serializer, compressors

    zarr_compression = (
        getattr(template_config, "zarr_compression", True) if template_config else True
    )
    zarr_codecs = getattr(template_config, "zarr_codecs", None) if template_config else None

    if not zarr_compression:
        return None, None, []

    if zarr_codecs is not None:
        filters_dicts, serializer_dict, compressors_dicts = split_zarr_codecs(zarr_codecs)
        return resolve_codec_pipeline(
            filters=filters_dicts,
            serializer=serializer_dict,
            compressors=compressors_dicts,
        )

    return None, None, None


def auto_inner_chunk_size(size: int) -> int:
    """Return the largest divisor of *size* that is ≤ size//4 (minimum 1)."""
    inner = max(1, size // 4)
    while inner > 1 and size % inner != 0:
        inner -= 1
    return inner


def _build_zarr_encoding(
    ds: xr.Dataset,
    *,
    compression: bool,
    zarr_codecs: list[dict] | None = None,
    shard_shape: dict[str, int] | None = None,
    chunk_shape: dict[str, int] | None = None,
) -> dict[str, dict[str, object]]:
    from firecube.core.zarr.codec_pipeline import split_zarr_codecs

    _, _, compressor_dicts = split_zarr_codecs(zarr_codecs)
    _, _, compressor_instances = resolve_codec_pipeline(compressors=compressor_dicts)

    compressors: list[Any] | None
    if not compression and zarr_codecs is None:
        compressors = []
    elif compression and zarr_codecs is None:
        compressors = None
    else:
        compressors = list(compressor_instances) if compressor_instances else []

    encoding: dict[str, dict[str, object]] = {}
    # E5: iterate over data_vars AND coords so chunk_shape is honored uniformly
    # for the time coord and firecube_timestamp_state array. Otherwise xarray
    # auto-chunks numpy-backed coords and the on-disk chunk layout diverges
    # from data variables, breaking alignment tracking.
    all_arrays = {**ds.data_vars, **ds.coords}
    for var_name, data_array in all_arrays.items():
        var_name_str = str(var_name)
        is_data_var = var_name in ds.data_vars
        var_encoding: dict[str, object] = {}
        var_dims = tuple(str(dim) for dim in data_array.dims)

        if is_data_var and compressors is not None:
            var_encoding["compressors"] = compressors

        if shard_shape is not None and is_data_var:
            var_encoding["shards"] = tuple(shard_shape.get(dim, ds.sizes[dim]) for dim in var_dims)
            var_encoding["chunks"] = tuple((chunk_shape or {}).get(dim, 1) for dim in var_dims)
        elif chunk_shape is not None and any(dim in chunk_shape for dim in var_dims):
            # Use the configured chunk size directly. Zarr v3 accepts chunks
            # larger than the current shape; the append dimension grows across
            # later batches.
            var_encoding["chunks"] = tuple(
                int(chunk_shape.get(dim, ds.sizes[dim])) for dim in var_dims
            )

        if not is_data_var and var_encoding:
            # An explicit encoding entry replaces the variable's own encoding
            # in xarray, which would drop the CF keys a source file carried
            # (``dtype``, ``units``, ``calendar``) and re-encode the time
            # coordinate as int64 with fresh units. Carry them forward.
            for key in _INHERITED_COORD_ENCODING_KEYS:
                if key in data_array.encoding and key not in var_encoding:
                    var_encoding[key] = data_array.encoding[key]

        if is_data_var or var_encoding:
            encoding[var_name_str] = var_encoding
    return encoding


def _static_data_vars(ds: xr.Dataset, *, time_dim: str) -> set[str]:
    """Return data variables that do not carry the append/time dimension."""
    return {str(name) for name, variable in ds.data_vars.items() if time_dim not in variable.dims}


def _append_write_view(ds: xr.Dataset, *, time_dim: str) -> xr.Dataset:
    """Return the append view written by xarray: no static vars, no group attrs."""
    static_vars = _static_data_vars(ds, time_dim=time_dim)
    view = ds.drop_vars(list(static_vars)) if static_vars else ds
    view = view.copy(deep=False)
    view.attrs = {}
    return view


def _open_preflight_compare_group(
    *,
    preflight_compare_zarr_store: ZarrStoreHandle,
    group: str,
    zarr_format: int,
) -> Any | None:
    try:
        root = zarr.open_group(
            **preflight_compare_zarr_store.zarr_kwargs(),
            mode="r",
            zarr_format=cast(Literal[3], zarr_format),
            use_consolidated=False,
        )
        return cast(Any, root[str(group)])
    # FileNotFoundError: fs layer when the final target doesn't exist yet
    # (fresh staged run). KeyError: zarr group-indexing when the group doesn't
    # exist in an existing store.
    except (FileNotFoundError, KeyError):
        return None


def _preflight_compare_static_vars(
    ds: xr.Dataset,
    *,
    preflight_compare_zarr_store: ZarrStoreHandle,
    group: str,
    time_dim: str,
    force_reingest: bool,
    zarr_format: int,
) -> None:
    """Compare non-time-indexed variables against the final target."""
    from firecube.core.zarr._drift import chunk_by_chunk_equal
    from firecube.ingestor.errors import (
        STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL,
        STATIC_VAR_DRIFT_MSG_TMPL,
        STATIC_VAR_NEW_ON_APPEND_TMPL,
        SchemaDriftError,
    )

    final_group = _open_preflight_compare_group(
        preflight_compare_zarr_store=preflight_compare_zarr_store,
        group=group,
        zarr_format=zarr_format,
    )
    if final_group is None:
        return None

    store_uri = str(preflight_compare_zarr_store.target_uri)
    for var_name in sorted(_static_data_vars(ds, time_dim=time_dim)):
        if var_name not in final_group:
            raise SchemaDriftError(
                STATIC_VAR_NEW_ON_APPEND_TMPL.format(name=var_name, store_uri=store_uri)
            )
        target_arr = cast(zarr.Array, final_group[var_name])
        if not chunk_by_chunk_equal(target_arr, np.asarray(ds[var_name].values)):
            template = (
                STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL
                if force_reingest
                else STATIC_VAR_DRIFT_MSG_TMPL
            )
            format_kwargs = {"name": var_name, "store_uri": store_uri}
            if not force_reingest:
                format_kwargs["time_dim"] = time_dim
            raise SchemaDriftError(template.format(**format_kwargs))


def _preflight_compare_attrs_and_warn(
    ds: xr.Dataset,
    *,
    preflight_compare_zarr_store: ZarrStoreHandle,
    group: str,
    zarr_format: int,
    logger: logging.Logger | None,
) -> None:
    """Warn when incoming group attrs differ from first-write target attrs."""
    from firecube.core.zarr._drift import group_attrs_diff

    final_group = _open_preflight_compare_group(
        preflight_compare_zarr_store=preflight_compare_zarr_store,
        group=group,
        zarr_format=zarr_format,
    )
    if final_group is None:
        return None

    store_uri = str(preflight_compare_zarr_store.target_uri)
    stored_attrs = dict(final_group.attrs)
    diff = group_attrs_diff(stored_attrs, dict(ds.attrs))
    if not diff.is_empty and logger is not None:
        logger.warning(
            "Group attributes differ from stored; keeping first-write values",
            extra={
                "added": diff.added,
                "removed": diff.removed,
                "changed": diff.changed,
                "store_uri": store_uri,
                "group": str(group),
            },
        )


def _snapshot_group_attrs(
    *,
    zarr_store: ZarrStoreHandle,
    group: str,
    zarr_format: int,
) -> dict[str, Any] | None:
    """Snapshot attrs from the store being written, if its group exists."""
    try:
        root = zarr.open_group(
            **zarr_store.zarr_kwargs(),
            mode="r",
            zarr_format=cast(Literal[3], zarr_format),
            use_consolidated=False,
        )
        zarr_group = cast(Any, root[str(group)])
    except (FileNotFoundError, KeyError):
        return None
    return dict(zarr_group.attrs)


def _restore_group_attrs(
    *,
    zarr_store: ZarrStoreHandle,
    group: str,
    attrs: dict[str, Any],
    zarr_format: int,
) -> None:
    """Restore first-write group attrs after xarray append metadata handling."""
    root = zarr.open_group(
        **zarr_store.zarr_kwargs(),
        mode="r+",
        zarr_format=cast(Literal[3], zarr_format),
        use_consolidated=False,
    )
    zarr_group = cast(Any, root[str(group)])
    zarr_group.attrs.put(attrs)


_INHERITED_COORD_ENCODING_KEYS = ("dtype", "units", "calendar", "_FillValue")
"""CF encoding keys a coordinate keeps when the engine adds its own zarr keys."""


def write_dataset_to_zarr(
    ds: xr.Dataset,
    *,
    zarr_store: ZarrStoreHandle,
    group: str,
    mode: Literal["w", "a"] = "w",
    region: slice | None = None,
    time_dim: str = "timestamp",
    state_var_name: str = "firecube_timestamp_state",
    chunk_shape: dict[str, int] | None = None,
    shard_shape: dict[str, int] | None = None,
    sharding: bool = False,
    compression: bool = False,
    zarr_codecs: list[dict] | None = None,
    consolidate: bool = False,
    zarr_format: int = 3,
    logger: logging.Logger | None = None,
    preflight_compare_zarr_store: ZarrStoreHandle | None = None,
    force_reingest: bool = False,
) -> None:
    """Write an xarray Dataset into a Zarr V3 group with optional append semantics.

    The caller is responsible for schema validation against the existing Zarr
    group (see :mod:`firecube.ingestor.runtime.zarr.schema`); this function
    does not repeat that check.
    """

    if zarr_format != 3:
        raise ValueError("write_dataset_to_zarr only supports zarr_format=3")
    zarr_kwargs = zarr_store.zarr_kwargs()
    effective_store = zarr_kwargs["store"]

    if region is not None:
        from firecube.core.zarr.time_decode import decode_time_array
        from firecube.ingestor.errors import AppendOverwriteRefused
        from firecube.ingestor.runtime.zarr.schema import validate_time_array_schema

        group_name = str(group)
        root = zarr.open_group(
            **zarr_kwargs,
            mode="r+",
            zarr_format=zarr_format,
            use_consolidated=False,
        )
        zarr_group = cast(Any, root[group_name])

        validate_time_array_schema(
            ds,
            zarr_group,
            store_uri=zarr_store.target_uri,
            time_dim=time_dim,
            state_var_name=state_var_name,
        )

        time_arr = cast(Any, zarr_group[time_dim])
        existing_coord = decode_time_array(np.asarray(time_arr[region]), dict(time_arr.attrs))
        incoming_coord = np.asarray(ds[time_dim].values)
        if existing_coord.shape != incoming_coord.shape or not bool(
            np.all(existing_coord == incoming_coord)
        ):
            raise AppendOverwriteRefused(
                refused_timestamps=[str(v) for v in ds[time_dim].values[:3]],
                reason="time_coord_mismatch",
            )

        if preflight_compare_zarr_store is not None:
            _preflight_compare_static_vars(
                ds,
                preflight_compare_zarr_store=preflight_compare_zarr_store,
                group=group,
                time_dim=time_dim,
                force_reingest=force_reingest,
                zarr_format=zarr_format,
            )

        ds_region = ds.drop_vars(
            [
                name
                for name, variable in ds.variables.items()
                if name in {time_dim, state_var_name} or time_dim not in variable.dims
            ]
        )

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Consolidated metadata is currently not part in the Zarr format 3 specification",
            )
            ds_region.to_zarr(
                **zarr_kwargs,
                group=group_name,
                region={time_dim: region},
                mode="r+",
                zarr_format=zarr_format,
                consolidated=False,
                safe_chunks=False,
            )

        state_arr = cast(Any, zarr_group[state_var_name])
        state_arr[region] = 1

        if consolidate:
            _consolidate_metadata_best_effort(effective_store, logger=logger)
        return

    effective_shard_shape: dict[str, int] | None = shard_shape
    effective_chunk_shape: dict[str, int] | None = chunk_shape

    if sharding and shard_shape is None:
        effective_shard_shape = {str(dim): ds.sizes[dim] for dim in ds.dims}
        if chunk_shape is None:
            effective_chunk_shape = {
                dim: auto_inner_chunk_size(size) for dim, size in effective_shard_shape.items()
            }

    # If shard_shape is explicit but chunk_shape was not given, auto-derive inner chunks
    # from the shard dimensions using the same size//4 heuristic.
    if effective_shard_shape is not None and effective_chunk_shape is None:
        effective_chunk_shape = {
            dim: auto_inner_chunk_size(size) for dim, size in effective_shard_shape.items()
        }

    if effective_shard_shape:
        try:
            current_chunks = getattr(ds, "chunks", None)
            needs_rechunk = (
                current_chunks is None
                or any(
                    current_chunks.get(dim, ())
                    and any(c != target for c in current_chunks[dim][:-1])
                    for dim, target in effective_shard_shape.items()
                    if dim in current_chunks
                )
                or any(dim not in current_chunks for dim in effective_shard_shape)
            )
            if needs_rechunk:
                ds = ds.chunk(effective_shard_shape)
        except ImportError as exc:  # pragma: no cover - requires dask
            raise RuntimeError(
                "Dask is required for chunked Zarr export. Install dask[array] or disable chunking."
            ) from exc
    elif chunk_shape:
        try:
            current_chunks = getattr(ds, "chunks", None)
            needs_rechunk = (
                current_chunks is None
                or any(
                    current_chunks.get(dim, ())
                    and any(c != target for c in current_chunks[dim][:-1])
                    for dim, target in chunk_shape.items()
                    if dim in current_chunks
                )
                or any(dim not in current_chunks for dim in chunk_shape)
            )
            if needs_rechunk:
                ds = ds.chunk(chunk_shape)
        except ImportError as exc:  # pragma: no cover - requires dask
            raise RuntimeError(
                "Dask is required for chunked Zarr export. Install dask[array] or disable chunking."
            ) from exc

    ds_to_write = ds
    pre_write_group_attrs: dict[str, Any] | None = None
    if mode == "a":
        pre_write_group_attrs = _snapshot_group_attrs(
            zarr_store=zarr_store,
            group=group,
            zarr_format=zarr_format,
        )
        if preflight_compare_zarr_store is not None:
            _preflight_compare_static_vars(
                ds,
                preflight_compare_zarr_store=preflight_compare_zarr_store,
                group=group,
                time_dim=time_dim,
                force_reingest=force_reingest,
                zarr_format=zarr_format,
            )
            _preflight_compare_attrs_and_warn(
                ds,
                preflight_compare_zarr_store=preflight_compare_zarr_store,
                group=group,
                zarr_format=zarr_format,
                logger=logger,
            )
        ds_to_write = _append_write_view(ds, time_dim=time_dim)

    encoding: dict[str, dict[str, object]] | None = None
    if mode != "a":
        # Encoding is only supplied for initial writes; appends should reuse
        # existing store metadata for safety. Always build encoding so that
        # ``compressors=[]`` is explicit and zarr does not inject a default
        # compressor when the caller requested none (see
        # tests/unit/test_zarr_codec_api_assumptions.py::test_disable_compression_encoding_shape).
        encoding = _build_zarr_encoding(
            ds_to_write,
            compression=compression,
            zarr_codecs=zarr_codecs,
            shard_shape=effective_shard_shape,
            chunk_shape=effective_chunk_shape,
        )

    to_zarr_common: dict[str, Any] = {
        **zarr_kwargs,
        "group": group,
        "mode": mode,
        "zarr_format": zarr_format,
        "consolidated": False,
        "safe_chunks": False,
        "align_chunks": True,
    }

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        if mode == "a":
            ds_to_write.to_zarr(**to_zarr_common, append_dim=time_dim)
        elif encoding is not None:
            ds_to_write.to_zarr(**to_zarr_common, encoding=encoding)
        else:
            ds_to_write.to_zarr(**to_zarr_common)

    if mode == "a" and pre_write_group_attrs is not None:
        _restore_group_attrs(
            zarr_store=zarr_store,
            group=group,
            attrs=pre_write_group_attrs,
            zarr_format=zarr_format,
        )

    if consolidate:
        _consolidate_metadata_best_effort(effective_store, logger=logger)
