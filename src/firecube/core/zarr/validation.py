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

"""Zarr integrity/validation helpers used by CLI and API.

This module inspects Zarr V3 arrays at the metadata level to detect
obvious structural issues such as extra chunks whose indices are outside
the expected chunk grid implied by shape and chunk_shape.

It is intentionally read-only: callers decide whether to act on the
validation results (e.g. via ChunkManager scrub operations).
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Final, cast

import numpy as np

from firecube.core.config import StorageConfig
from firecube.core.filesystem.ops import (
    _open_fsspec_url,  # type: ignore
    create_filesystem_for_uri,
)
from firecube.core.filesystem.protocol import StorageFilesystem
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.storage.uri import StorageUri

if TYPE_CHECKING:
    from firecube.core.filesystem.store_factory import ZarrStoreHandle  # type: ignore

log = logging.getLogger("firecube.core.zarr.validation")


@dataclass
class ZarrValidationReport:
    """Summary of structural validation for a single Zarr array group."""

    product: str
    group: str
    shape: list[int]
    chunk_shape: list[int]
    expected_chunks: dict[str, int]
    max_indices: dict[str, int]
    extra_chunks: list[str]
    missing_indices: dict[str, list[int]]
    budget_exceeded: bool = False
    chunks_processed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ZarrCompareReport:
    """Summary of a read-only comparison between two Zarr stores.

    Attributes:
        equivalent: Whether every compared array path, schema field, selected
            attribute, static marker, and value payload matched.
        mismatches: Terse mismatch descriptions grouped by array path and
            category.

    Examples:
        >>> ZarrCompareReport(equivalent=True, mismatches=[]).equivalent
        True
    """

    equivalent: bool
    mismatches: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the report."""
        return asdict(self)


def _open_fs(
    store_uri: str,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
) -> tuple[Any, str]:
    """Return (fs, root_path) for a Zarr store URI using fsspec."""
    fs, root = _open_fsspec_url(
        store_uri, storage_config=storage_config, storage_options=storage_options
    )
    return fs, root.rstrip("/")


def _load_array_metadata(fs, group_path: str) -> dict[str, Any]:
    """Load zarr.json metadata for a given group path."""
    meta_path = f"{group_path}/zarr.json"
    if not fs.exists(meta_path):
        raise FileNotFoundError(f"Missing zarr.json at {meta_path}")
    with fs.open(meta_path, "r") as handle:
        return json.load(handle)


def _load_array_metadata_with_fs(fs: StorageFilesystem, group_uri: StorageUri) -> dict[str, Any]:
    """Load zarr.json metadata for a typed storage URI group path."""
    meta_uri = group_uri.join("zarr.json")
    try:
        with fs.open(meta_uri, "r") as handle:  # pyright: ignore[reportArgumentType]
            return json.load(handle)
    except FileNotFoundError:
        raise FileNotFoundError(f"Missing zarr.json at {meta_uri.to_str()}") from None


def _load_root_metadata_with_fs(fs: StorageFilesystem, store_uri: StorageUri) -> dict[str, Any]:
    meta_uri = store_uri.join("zarr.json")
    try:
        with fs.open(meta_uri, "r") as handle:  # pyright: ignore[reportArgumentType]
            return json.load(handle)
    except FileNotFoundError:
        raise FileNotFoundError(f"Missing Zarr store metadata at {meta_uri.to_str()}") from None


def _discover_arrays_with_fs(fs: StorageFilesystem, store_uri: StorageUri) -> list[str]:
    arrays: list[str] = []
    for entry in fs.find(store_uri):  # pyright: ignore[reportArgumentType]
        if not isinstance(entry, StorageUri):
            continue
        if entry.path.rsplit("/", 1)[-1] != "zarr.json":
            continue
        with fs.open(entry, "r") as handle:
            meta = json.load(handle)
        if meta.get("node_type") != "array":
            continue
        parent = entry.parent()
        rel = parent.path.removeprefix(store_uri.path.rstrip("/")).strip("/")
        if rel:
            arrays.append(rel)
    return sorted(set(arrays))


def _public_attrs(attrs: Any) -> dict[str, Any]:
    from firecube.core.api import RESERVED_ARRAY_ATTRS, assert_attrs_safe

    filtered = {
        str(key): value
        for key, value in dict(attrs or {}).items()
        if key not in RESERVED_ARRAY_ATTRS
    }
    assert_attrs_safe(filtered)
    return filtered


def _dimension_names(array: Any) -> tuple[str, ...] | None:
    metadata = getattr(array, "metadata", None)
    names = getattr(metadata, "dimension_names", None)
    if names is None:
        return None
    return tuple(str(name) for name in names)


def _static_marker(array: Any) -> Any:
    from firecube.core.api import FIRECUBE_STATIC_WRITTEN_ATTR

    return dict(getattr(array, "attrs", {}) or {}).get(FIRECUBE_STATIC_WRITTEN_ATTR)


_COMPARE_SLAB_BYTES: Final[int] = 512 * 1024 * 1024
"""Per-store byte budget for one streamed comparison slab."""


def _chunk_aligned_slabs(
    shape: tuple[int, ...], chunks: tuple[int, ...], itemsize: int, axis: int = 0
) -> Iterator[tuple[slice, ...]]:
    """Yield chunk-aligned index tuples bounded by the slab budget.

    Splits along the leading axis first; when a single chunk along that axis
    still overruns the budget, takes one chunk there and splits along the
    next axis too, recursively. Steps whole chunks on every axis: a sub-chunk
    slice still decompresses the entire chunk, so a finer walk would decode
    each chunk many times over. The irreducible floor is one chunk.
    """
    if axis >= len(shape):
        yield ()
        return
    rest_bytes = itemsize
    for size in shape[axis + 1 :]:
        rest_bytes *= size
    chunk_step = max(int(chunks[axis]) if axis < len(chunks) else 1, 1)
    step_bytes = rest_bytes * chunk_step
    if step_bytes > _COMPARE_SLAB_BYTES and axis + 1 < len(shape):
        for start in range(0, shape[axis], chunk_step):
            head = slice(start, min(start + chunk_step, shape[axis]))
            for tail in _chunk_aligned_slabs(shape, chunks, itemsize, axis + 1):
                yield (head, *tail)
        return
    multiples = max(1, _COMPARE_SLAB_BYTES // max(step_bytes, 1))
    step = chunk_step * multiples
    for start in range(0, shape[axis], step):
        yield (slice(start, min(start + step, shape[axis])),)


def _values_equal(left: Any, right: Any) -> bool:
    # Streamed in chunk-aligned slabs: a product-scale array can be tens of
    # decompressed GB per store and must never be fully resident. Ellipsis
    # indexing handles 0-d arrays (e.g. the spatial_ref grid-mapping scalar).
    shape = tuple(int(size) for size in left.shape)
    chunks = tuple(int(size) for size in (getattr(left, "chunks", ()) or ()))
    itemsize = np.dtype(left.dtype).itemsize
    # NaT and NaN are equal to themselves for comparison purposes: a dense
    # coordinate carries explicit NaT for unfilled slots.
    nan_aware = np.dtype(left.dtype).kind in {"f", "c", "M", "m"}
    for index in _chunk_aligned_slabs(shape, chunks, itemsize):
        left_values = np.asarray(left[index] if index else left[...])
        right_values = np.asarray(right[index] if index else right[...])
        if nan_aware:
            same = np.array_equal(left_values, right_values, equal_nan=True)
        else:
            same = np.array_equal(left_values, right_values)
        if not same:
            return False
    return True


def compare_zarr_stores(
    a_uri: str,
    b_uri: str,
    *,
    storage_type: str,
    storage_driver: str,
) -> ZarrCompareReport:
    """Compare two Zarr stores through the configured storage abstraction.

    The comparison is read-only and checks array paths, shape, dtype, chunks,
    native Zarr dimension names, public attrs, the Firecube static-array marker,
    and full array values. Runtime-managed attrs such as ``firecube_run_id`` and
    ``firecube_span_id`` are ignored.

    Args:
        a_uri: First Zarr store URI.
        b_uri: Second Zarr store URI.
        storage_type: Storage locality, either ``"local"`` or ``"s3"``.
        storage_driver: Storage driver, either ``"fsspec"`` or ``"obstore"``.

    Returns:
        ZarrCompareReport: ``equivalent=True`` when no mismatches were found;
        otherwise ``equivalent=False`` with one terse message per mismatch.

    Raises:
        FileNotFoundError: If either store or a discovered array is missing.
        ValueError: If the storage configuration is invalid.

    Examples:
        >>> report = compare_zarr_stores(
        ...     "file:///tmp/a.zarr",
        ...     "file:///tmp/b.zarr",
        ...     storage_type="local",
        ...     storage_driver="fsspec",
        ... )
        >>> isinstance(report.equivalent, bool)
        True
    """
    import zarr

    storage_config = StorageConfig(storage_type=storage_type, storage_driver=storage_driver)
    storage_config.validate()
    a_fs, a_store_uri = create_filesystem_for_uri(a_uri, storage_config, format="zarr")
    b_fs, b_store_uri = create_filesystem_for_uri(b_uri, storage_config, format="zarr")
    _load_root_metadata_with_fs(a_fs, a_store_uri)
    _load_root_metadata_with_fs(b_fs, b_store_uri)

    a_paths = set(_discover_arrays_with_fs(a_fs, a_store_uri))
    b_paths = set(_discover_arrays_with_fs(b_fs, b_store_uri))
    mismatches = [f"array {path}: missing from second store" for path in sorted(a_paths - b_paths)]
    mismatches.extend(
        f"array {path}: missing from first store" for path in sorted(b_paths - a_paths)
    )

    a_handle = create_zarr_store(uri=a_uri, storage_config=storage_config, mode="r")
    b_handle = create_zarr_store(uri=b_uri, storage_config=storage_config, mode="r")
    a_root = zarr.open_group(**a_handle.zarr_kwargs(), mode="r")
    b_root = zarr.open_group(**b_handle.zarr_kwargs(), mode="r")

    for path in sorted(a_paths & b_paths):
        left = cast(Any, a_root[path])
        right = cast(Any, b_root[path])
        path_prefix = f"array {path}"

        left_shape = tuple(int(size) for size in left.shape)
        right_shape = tuple(int(size) for size in right.shape)
        if left_shape != right_shape:
            mismatches.append(f"{path_prefix}: shape {left_shape} != {right_shape}")

        left_dtype = np.dtype(left.dtype)
        right_dtype = np.dtype(right.dtype)
        if left_dtype != right_dtype:
            mismatches.append(f"{path_prefix}: dtype {left_dtype} != {right_dtype}")

        left_chunks = tuple(int(size) for size in left.chunks)
        right_chunks = tuple(int(size) for size in right.chunks)
        if left_chunks != right_chunks:
            mismatches.append(f"{path_prefix}: chunks {left_chunks} != {right_chunks}")

        left_dimension_names = _dimension_names(left)
        right_dimension_names = _dimension_names(right)
        if left_dimension_names != right_dimension_names:
            mismatches.append(
                f"{path_prefix}: dimension_names {left_dimension_names} != {right_dimension_names}"
            )

        left_attrs = _public_attrs(getattr(left, "attrs", {}))
        right_attrs = _public_attrs(getattr(right, "attrs", {}))
        if left_attrs != right_attrs:
            mismatches.append(f"{path_prefix}: attrs differ")

        left_marker = _static_marker(left)
        right_marker = _static_marker(right)
        if left_marker != right_marker:
            mismatches.append(
                f"{path_prefix}: firecube_static_written {left_marker!r} != {right_marker!r}"
            )

        if (
            left_shape == right_shape
            and left_dtype == right_dtype
            and not _values_equal(left, right)
        ):
            mismatches.append(f"{path_prefix}: values differ")

    return ZarrCompareReport(equivalent=not mismatches, mismatches=mismatches)


def _chunk_entry_path(entry: StorageUri | str) -> str:
    if isinstance(entry, StorageUri):
        return entry.to_str()
    return str(entry)


def _read_chunk_grid_from_metadata(
    meta: dict[str, Any],
    group: str,
) -> tuple[list[str], list[int], list[int]]:
    node_type = meta.get("node_type")
    if node_type == "group":
        raise ValueError(f"Path {group} is a Zarr group node, not an array.")
    elif node_type != "array" and "shape" not in meta:
        # Some Zarr V3 stores might not have node_type but have shape
        raise ValueError(f"Path {group} is not a valid Zarr array node (missing shape).")

    shape = [int(x) for x in meta.get("shape", [])]
    chunk_grid = meta.get("chunk_grid", {}) or {}
    cfg = chunk_grid.get("configuration", {}) or {}
    chunk_shape = [int(x) for x in cfg.get("chunk_shape", [])]
    ndim = len(shape)

    dim_names = meta.get("dimension_names") or [f"dim{i}" for i in range(ndim)]
    if len(dim_names) != ndim:
        dim_names = [f"dim{i}" for i in range(ndim)]
    return dim_names, shape, chunk_shape


def _array_metadata_value(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _array_metadata_mapping(array: Any) -> dict[str, Any]:
    metadata = getattr(array, "metadata", None)
    if metadata is None:
        return {}
    if isinstance(metadata, dict):
        return metadata
    to_dict = getattr(metadata, "to_dict", None)
    if callable(to_dict):
        raw = to_dict()
        return raw if isinstance(raw, dict) else {}
    return {}


def _read_chunk_grid_from_zarr_array(
    array: Any,
    group: str,
) -> tuple[list[str], list[int], list[int]]:
    meta = _array_metadata_mapping(array)
    if meta:
        return _read_chunk_grid_from_metadata(meta, group)

    shape = [int(x) for x in getattr(array, "shape", ())]
    chunks = getattr(array, "chunks", None)
    chunk_shape = [int(x) for x in chunks] if chunks is not None else []
    ndim = len(shape)
    attrs = getattr(array, "attrs", {}) or {}
    dim_names = (
        list(getattr(getattr(array, "metadata", None), "dimension_names", None) or [])
        or list(attrs.get("dimension_names") or [])
        or list(attrs.get("_ARRAY_DIMENSIONS") or [])
        or [f"dim{i}" for i in range(ndim)]
    )
    if len(dim_names) != ndim:
        dim_names = [f"dim{i}" for i in range(ndim)]
    return [str(dim) for dim in dim_names], shape, chunk_shape


def read_chunk_grid_from_handle(
    handle: ZarrStoreHandle,
    group: str,
) -> tuple[list[str], list[int], list[int]]:
    """Read dimension names, shape, and chunk_shape via a driver-aware store handle."""
    import zarr

    root = zarr.open_group(**handle.zarr_kwargs(), mode="r")
    array = cast(Any, root[group.strip("/")])
    return _read_chunk_grid_from_zarr_array(array, group)


def read_chunk_grid_with_shards_from_handle(
    handle: ZarrStoreHandle,
    group: str,
) -> tuple[list[str], list[int], list[int], list[int] | None]:
    """Read chunk/shard grid via a driver-aware store handle."""
    import zarr

    root = zarr.open_group(**handle.zarr_kwargs(), mode="r")
    array = cast(Any, root[group.strip("/")])
    dim_names, shape, outer_chunk_shape = _read_chunk_grid_from_zarr_array(array, group)

    inner_chunk_shape: list[int] | None = None
    metadata = getattr(array, "metadata", None)
    codecs = _array_metadata_value(metadata, "codecs", []) or []
    for codec in codecs:
        codec_name = _array_metadata_value(codec, "name", "")
        if codec_name == "sharding_indexed":
            codec_cfg = _array_metadata_value(codec, "configuration", {}) or {}
            raw_inner = _array_metadata_value(codec_cfg, "chunk_shape", [])
            if raw_inner:
                inner_chunk_shape = [int(x) for x in raw_inner]
            break

    return dim_names, shape, outer_chunk_shape, inner_chunk_shape


def _discover_groups_with_fs(fs: StorageFilesystem, store_uri: StorageUri) -> list[str]:
    discovered: list[str] = []
    try:
        for entry in fs.find(store_uri):  # pyright: ignore[reportArgumentType]
            if not isinstance(entry, StorageUri):
                continue
            if entry.path.rsplit("/", 1)[-1] != "zarr.json":
                continue
            with fs.open(entry, "r") as handle:
                meta = json.load(handle)
            if meta.get("node_type") == "array":
                continue
            parent = entry.parent()
            rel = parent.path.removeprefix(store_uri.path.rstrip("/")).strip("/")
            discovered.append(rel or "/")
    except Exception:
        log.debug("Driver-aware group discovery failed for %s", store_uri.to_str(), exc_info=True)
    return sorted(set(discovered))


def read_chunk_grid(
    store_uri: str,
    group: str,
    *,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
) -> tuple[list[str], list[int], list[int]]:
    """Read dimension names, shape, and chunk_shape for an array without listing chunks."""
    if storage_config is not None:
        from firecube.core.filesystem.store_factory import create_zarr_store

        handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
        return read_chunk_grid_from_handle(handle, group)

    fs, root = _open_fs(store_uri, storage_config=storage_config, storage_options=storage_options)
    group_path = f"{root}/{group.strip('/')}"
    meta = _load_array_metadata(fs, group_path)

    node_type = meta.get("node_type")
    if node_type == "group":
        raise ValueError(f"Path {group} is a Zarr group node, not an array.")
    elif node_type != "array" and "shape" not in meta:
        # Some Zarr V3 stores might not have node_type but have shape
        raise ValueError(f"Path {group} is not a valid Zarr array node (missing shape).")

    shape = [int(x) for x in meta.get("shape", [])]
    chunk_grid = meta.get("chunk_grid", {}) or {}
    cfg = chunk_grid.get("configuration", {}) or {}
    chunk_shape = [int(x) for x in cfg.get("chunk_shape", [])]
    ndim = len(shape)

    dim_names = meta.get("dimension_names") or [f"dim{i}" for i in range(ndim)]
    if len(dim_names) != ndim:
        dim_names = [f"dim{i}" for i in range(ndim)]
    return dim_names, shape, chunk_shape


def read_chunk_grid_with_shards(
    store_uri: str,
    group: str,
    *,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
) -> tuple[list[str], list[int], list[int], list[int] | None]:
    """Read dimension names, shape, outer chunk shape, and inner chunk shape (if sharded).

    Returns:
        ``(dim_names, shape, outer_chunk_shape, inner_chunk_shape)``, where
        ``outer_chunk_shape`` is the shard shape for sharded arrays, or the
        regular chunk shape, and ``inner_chunk_shape`` is the inner chunk
        shape for sharded arrays, or None for non-sharded.
    """
    if storage_config is not None:
        from firecube.core.filesystem.store_factory import create_zarr_store

        handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
        return read_chunk_grid_with_shards_from_handle(handle, group)

    fs, root = _open_fs(store_uri, storage_config=storage_config, storage_options=storage_options)
    group_path = f"{root}/{group.strip('/')}"
    meta = _load_array_metadata(fs, group_path)

    node_type = meta.get("node_type")
    if node_type == "group":
        raise ValueError(f"Path {group} is a Zarr group node, not an array.")
    elif node_type != "array" and "shape" not in meta:
        raise ValueError(f"Path {group} is not a valid Zarr array node (missing shape).")

    shape = [int(x) for x in meta.get("shape", [])]
    chunk_grid = meta.get("chunk_grid", {}) or {}
    cfg = chunk_grid.get("configuration", {}) or {}
    outer_chunk_shape = [int(x) for x in cfg.get("chunk_shape", [])]
    ndim = len(shape)

    dim_names = meta.get("dimension_names") or [f"dim{i}" for i in range(ndim)]
    if len(dim_names) != ndim:
        dim_names = [f"dim{i}" for i in range(ndim)]

    inner_chunk_shape: list[int] | None = None
    codecs = meta.get("codecs", []) or []
    for codec in codecs:
        codec_name = codec.get("name", "")
        if codec_name == "sharding_indexed":
            codec_cfg = codec.get("configuration", {}) or {}
            raw_inner = codec_cfg.get("chunk_shape", [])
            if raw_inner:
                inner_chunk_shape = [int(x) for x in raw_inner]
            break

    return dim_names, shape, outer_chunk_shape, inner_chunk_shape


def group_exists(
    store_target: Any,
    group: str,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
) -> bool:
    """Best-effort check to see if a Zarr group already exists.

    Supports URI strings, local paths, and MutableMapping stores.
    Uses cheap metadata checks before falling back to opening the store.
    """
    from collections.abc import MutableMapping
    from pathlib import Path

    import zarr

    prefix = group.rstrip("/") + "/"
    try:
        # Mapping-like store (e.g. FSMap)
        if isinstance(store_target, MutableMapping):
            return any(isinstance(key, str) and key.startswith(prefix) for key in store_target)

        # URI or local path string
        if isinstance(store_target, (str, Path)):
            fs, root = _open_fsspec_url(
                str(store_target), storage_config=storage_config, storage_options=storage_options
            )
            root = root.rstrip("/")
            meta_path = f"{root}/{group}/zarr.json"
            return fs.exists(meta_path)

        # Fallback: try opening the store directly
        try:
            zarr.open_group(store=store_target, mode="r")
            return True
        except Exception:
            return False
    except Exception:
        return False


def _extract_indices_from_key(full_path: str) -> list[int]:
    """Extract integer indices from a Zarr V3 chunk key path.

    Expects keys like ".../c/0", ".../c/10/2/6". Returns the list of
    indices after the 'c/' prefix. Non-integer segments are ignored.
    """
    try:
        _, tail = full_path.split("/c/", 1)
    except ValueError:
        return []
    parts = tail.strip("/").split("/")
    indices: list[int] = []
    for part in parts:
        try:
            indices.append(int(part))
        except ValueError:
            return []
    return indices


def _walk_chunk_entries(fs, chunk_dir: str) -> list[str] | Any:
    """Yield chunk file paths under a Zarr chunk directory."""
    stack = [chunk_dir.rstrip("/")]
    while stack:
        current = stack.pop()
        try:
            entries = fs.ls(current, detail=True)
        except FileNotFoundError:
            continue

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or "").rstrip("/")
            entry_type = entry.get("type")
            if entry_type in {"directory", "dir"}:
                stack.append(name)
                continue
            if name:
                yield name


def validate_group_with_fs(
    fs: StorageFilesystem,
    store_uri: StorageUri,
    group: str,
    *,
    timeout_s: float | None = None,
    max_chunks: int | None = None,
    on_timeout: str = "warn",
) -> ZarrValidationReport:
    """Validate a Zarr array group and return a structural report.

    Args:
        store_uri: Base store URI, e.g.
            ``StorageUri.parse("s3://bucket/product.zarr")``.
        group: Array group path inside the store, e.g. ``F024/FWI`` or
            ``F048/timestamp``.
    """
    product = store_uri.path.rstrip("/").split("/")[-1] if store_uri.path != "/" else ""
    group_uri = store_uri.join(group)

    # Load metadata to check node type first
    meta = _load_array_metadata_with_fs(fs, group_uri)
    node_type = meta.get("node_type")

    if node_type == "group":
        # It's a container. Discover arrays inside it.
        sub_groups = _discover_groups_with_fs(fs, store_uri)
        # Filter groups that are children of this one
        prefix = group.strip("/") + "/"
        discovered = [g for g in sub_groups if g.startswith(prefix)]

        # For now, we return a special report or advice.
        # Ideally, we'd recursively validate, but let's start by being clear.
        raise ValueError(
            f"Path {group!r} is a Zarr group (container). "
            f"Please validate a specific array within it, e.g.:\n"
            + "\n".join([f"  --group {g}" for g in discovered[:5]])
            + (f"\n  ... ({len(discovered) - 5} more)" if len(discovered) > 5 else "")
        )

    dim_names, shape, chunk_shape = _read_chunk_grid_from_metadata(meta, group)

    ndim = len(shape)
    if not shape or len(chunk_shape) != ndim:
        raise ValueError(f"Inconsistent shape/chunk_shape for group {group_uri.to_str()}")

    # Expected chunk counts per dimension
    expected_chunks: dict[str, int] = {}
    for name, size, csize in zip(dim_names, shape, chunk_shape, strict=False):
        if csize <= 0:
            raise ValueError(f"Invalid chunk size {csize} for dimension {name}")
        expected_chunks[name] = math.ceil(size / float(csize))

    # List all chunk keys. Use a recursive find to handle nested chunk
    # directories such as c/0/0/0 (time/lat/lon) rather than only the
    # first-level prefixes returned by ls().
    chunk_dir = group_uri.join("c")
    if not fs.exists(chunk_dir):  # pyright: ignore[reportArgumentType]
        # No chunks yet; treat as empty but structurally OK.
        max_indices = dict.fromkeys(dim_names, -1)
        return ZarrValidationReport(
            product=product,
            group=group,
            shape=shape,
            chunk_shape=chunk_shape,
            expected_chunks=expected_chunks,
            max_indices=max_indices,
            extra_chunks=[],
            missing_indices={name: [] for name in dim_names},
        )

    try:
        # fs.find returns all file entries under the group, which for Zarr V3
        # chunk stores includes the actual chunk keys (e.g. c/0/0/0).
        entries = fs.find(group_uri)  # pyright: ignore[reportArgumentType]
    except Exception as exc:
        raise RuntimeError(f"Failed to list chunks under {chunk_dir.to_str()}: {exc}") from exc

    index_sets: dict[str, set[int]] = {name: set() for name in dim_names}
    max_indices: dict[str, int] = dict.fromkeys(dim_names, -1)
    extra_chunks: list[str] = []

    budget_active = timeout_s is not None or max_chunks is not None
    _started = time.time() if budget_active else 0.0
    chunks_processed = 0
    budget_exceeded = False

    for entry in entries:
        path = _chunk_entry_path(entry)
        # Budget check
        if budget_active:
            if max_chunks is not None and chunks_processed >= max_chunks:
                budget_exceeded = True
                break
            if timeout_s is not None and (time.time() - _started) >= timeout_s:
                budget_exceeded = True
                break

        indices = _extract_indices_from_key(path)
        if not indices:
            continue
        chunks_processed += 1
        if len(indices) != ndim:
            extra_chunks.append(path)
            continue

        out_of_range = False
        for name, idx in zip(dim_names, indices, strict=False):
            index_sets[name].add(idx)
            if idx > max_indices[name]:
                max_indices[name] = idx
            if idx < 0 or idx >= expected_chunks[name]:
                out_of_range = True

        if out_of_range:
            extra_chunks.append(path)

    if budget_exceeded and on_timeout == "fail":
        raise TimeoutError(
            f"validate_group_with_fs budget exceeded: {chunks_processed} chunks processed "
            f"(max_chunks={max_chunks}, timeout_s={timeout_s})"
        )

    missing_indices: dict[str, list[int]] = {}
    for name in dim_names:
        exp = expected_chunks[name]
        actual = index_sets[name]
        missing = sorted(set(range(exp)) - actual)
        missing_indices[name] = missing

    return ZarrValidationReport(
        product=product,
        group=group,
        shape=shape,
        chunk_shape=chunk_shape,
        expected_chunks=expected_chunks,
        max_indices=max_indices,
        extra_chunks=extra_chunks,
        missing_indices=missing_indices,
        budget_exceeded=budget_exceeded if budget_active else False,
        chunks_processed=chunks_processed if budget_active else 0,
    )


def discover_groups(
    store_uri: str,
    *,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
    max_depth: int = 5,
    strict: bool = False,
) -> list[str]:
    """Discover Zarr V3 groups within a store by searching for zarr.json.

    Returns:
        List of relative group paths (e.g. ["F120", "default"]).
    """
    if storage_config is not None:
        from firecube.core.filesystem.ops import create_filesystem_for_uri  # type: ignore

        fs_driver, uri_obj = create_filesystem_for_uri(store_uri, storage_config, format="zarr")
        return _discover_groups_with_fs(cast(StorageFilesystem, fs_driver), uri_obj)

    fs, root = _open_fs(store_uri, storage_config=storage_config, storage_options=storage_options)
    discovered: list[str] = []
    try:
        stack: list[tuple[str, str, int]] = [(root.rstrip("/"), "/", 0)]
        while stack:
            current_path, current_group, depth = stack.pop()
            meta_path = f"{current_path}/zarr.json"
            node_type: str | None = None

            if fs.exists(meta_path):
                with fs.open(meta_path, "r", encoding="utf-8") as handle:
                    meta = json.load(handle)
                node_type_val = meta.get("node_type")
                node_type = str(node_type_val) if node_type_val is not None else None
                if node_type != "array":
                    discovered.append(current_group)

            if depth >= max_depth:
                continue
            if current_group != "/" and node_type == "array":
                continue

            try:
                entries = fs.ls(current_path, detail=True)
            except FileNotFoundError:
                continue

            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_type = entry.get("type")
                if entry_type not in {"directory", "dir"}:
                    continue
                name = str(entry.get("name") or "").rstrip("/")
                leaf = name.split("/")[-1]
                if leaf in {"c", ".firecube"}:
                    continue
                child_group = (
                    leaf if current_group == "/" else f"{current_group.rstrip('/')}/{leaf}"
                )
                stack.append((name, child_group, depth + 1))
    except Exception as exc:
        if strict:
            raise RuntimeError(f"Failed to discover Zarr groups under {store_uri}: {exc}") from exc
        log.debug(f"Group discovery failed for {store_uri}: {exc}")

    return sorted(set(discovered))


def find_extra_chunks(
    fs: StorageFilesystem,
    store_uri: StorageUri,
    group: str,
) -> list[str]:
    """Return a list of chunk keys whose indices are outside the expected grid."""
    report = validate_group_with_fs(fs, store_uri, group)
    return report.extra_chunks
