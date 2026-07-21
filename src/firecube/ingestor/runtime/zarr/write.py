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

import xarray as xr
import zarr
from zarr.codecs import BloscCodec

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


def auto_inner_chunk_size(size: int) -> int:
    """Return the largest divisor of *size* that is ≤ size//4 (minimum 1)."""
    inner = max(1, size // 4)
    while inner > 1 and size % inner != 0:
        inner -= 1
    return inner


def _build_zarr_encoding(
    ds: xr.Dataset,
    *,
    compression: bool | str,
    shard_shape: dict[str, int] | None = None,
    chunk_shape: dict[str, int] | None = None,
) -> dict[str, dict[str, object]]:
    if compression is False and shard_shape is None:
        return {str(var): {} for var in ds.data_vars}

    compressors: list[Any] | None = None
    if compression is not False:
        cname = compression if isinstance(compression, str) else "zstd"
        compressors = [BloscCodec(cname=cast(Any, cname), clevel=5)]

    encoding: dict[str, dict[str, object]] = {}
    for var_name, data_array in ds.data_vars.items():
        var_name_str = str(var_name)
        var_encoding: dict[str, object] = {}
        if compressors is not None:
            var_encoding["compressors"] = compressors
        if shard_shape is not None:
            var_dims = tuple(str(dim) for dim in data_array.dims)
            var_encoding["shards"] = tuple(shard_shape.get(dim, ds.sizes[dim]) for dim in var_dims)
            var_encoding["chunks"] = tuple((chunk_shape or {}).get(dim, 1) for dim in var_dims)
        encoding[var_name_str] = var_encoding
    return encoding


def write_dataset_to_zarr(
    ds: xr.Dataset,
    *,
    zarr_store: ZarrStoreHandle,
    group: str,
    mode: Literal["w", "a"] = "w",
    append_dim: str | None = None,
    chunk_shape: dict[str, int] | None = None,
    shard_shape: dict[str, int] | None = None,
    sharding: bool = False,
    compression: bool | str = False,
    consolidate: bool = False,
    zarr_format: int = 3,
    logger: logging.Logger | None = None,
) -> None:
    """Write an xarray Dataset into a Zarr V3 group with optional append semantics."""

    if zarr_format != 3:
        raise ValueError("write_dataset_to_zarr only supports zarr_format=3")
    zarr_kwargs = zarr_store.zarr_kwargs()
    effective_store = zarr_kwargs["store"]

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

    encoding: dict[str, dict[str, object]] | None = None
    if mode == "a":
        if not append_dim:
            raise ValueError("append_dim is required when mode='a'")
        append_dim_for_write = append_dim
    else:
        append_dim_for_write = None
        if effective_shard_shape is not None or chunk_shape:
            # Encoding is only supplied for initial writes; appends should reuse
            # existing store metadata for safety.
            encoding = _build_zarr_encoding(
                ds,
                compression=compression,
                shard_shape=effective_shard_shape,
                chunk_shape=effective_chunk_shape,
            )
        elif compression:
            encoding = _build_zarr_encoding(
                ds,
                compression=compression,
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
            ds.to_zarr(**to_zarr_common, append_dim=append_dim_for_write)
        elif encoding is not None:
            ds.to_zarr(**to_zarr_common, encoding=encoding)
        else:
            ds.to_zarr(**to_zarr_common)

    if consolidate:
        _consolidate_metadata_best_effort(effective_store, logger=logger)
