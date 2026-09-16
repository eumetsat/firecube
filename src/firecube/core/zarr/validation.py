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
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import asdict, dataclass, field
from itertools import pairwise
from typing import TYPE_CHECKING, Any, Final, cast

import numpy as np
from zarr.abc.store import Store
from zarr.core.buffer import default_buffer_prototype

from firecube.core.config import StorageConfig
from firecube.core.filesystem.ops import (
    _open_fsspec_url,  # type: ignore
    create_filesystem_for_uri,
)
from firecube.core.filesystem.protocol import StorageFilesystem
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr._reserved_attrs import FIRECUBE_STATIC_WRITTEN_ATTR

if TYPE_CHECKING:
    from firecube.core.filesystem.store_factory import ZarrStoreHandle  # type: ignore

log = logging.getLogger("firecube.core.zarr.validation")


@dataclass
class ZarrValidationReport:
    """Summary of structural validation for a single Zarr array group.

    ``absent_chunk_indices`` maps each array (relative path) to the list of
    linear chunk indices that are missing from the store within the expected
    grid implied by ``shape`` and ``chunk_shape``. It is populated by the
    scanning pass; an empty mapping means every expected chunk was found.

    ``static_marker_failures`` lists arrays whose first dimension is not a
    time dimension (i.e. arrays treated as static coordinates such as
    ``lat``/``lon``) but which are missing the reserved
    ``firecube_static_written`` marker attribute. Each entry is a
    ``{"array": <path>, "reason": "missing_or_false_static_marker"}`` mapping.
    A non-empty list makes ``is_valid`` false so the CLI can exit non-zero.
    """

    product: str
    group: str
    shape: list[int]
    chunk_shape: list[int]
    expected_chunks: dict[str, int]
    max_indices: dict[str, int]
    extra_chunks: list[str]
    budget_exceeded: bool = False
    chunks_processed: int = 0
    arrays_checked: list[str] = field(default_factory=list)
    absent_chunk_indices: dict[str, list[int]] = field(default_factory=dict)
    info_notes: list[str] = field(default_factory=list)
    validity_issues: list[str] = field(default_factory=list)
    static_marker_failures: list[dict[str, str]] = field(default_factory=list)
    is_valid: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_STATE_ARRAY_NAME: Final[str] = "firecube_timestamp_state"


class _StorageFilesystemStore(Store):
    """Read-only Zarr Store adapter over Firecube's typed filesystem seam."""

    def __init__(
        self,
        fs: StorageFilesystem,
        store_uri: StorageUri,
        *,
        read_only: bool = True,
    ) -> None:
        super().__init__(read_only=read_only)
        self._fs = fs
        self._store_uri = store_uri

    @property
    def supports_writes(self) -> bool:
        return False

    @property
    def supports_deletes(self) -> bool:
        return False

    @property
    def supports_listing(self) -> bool:
        return True

    def __eq__(self, value: object) -> bool:
        return self is value

    def with_read_only(self, read_only: bool = False) -> Store:
        return type(self)(self._fs, self._store_uri, read_only=read_only)

    def _uri_for_key(self, key: str) -> StorageUri:
        normalized = key.strip("/")
        if not normalized:
            return self._store_uri
        return self._store_uri.join(normalized)

    def _relative_key(self, entry: StorageUri) -> str:
        root = self._store_uri.path.rstrip("/")
        return entry.path.removeprefix(root).strip("/")

    @staticmethod
    def _slice_bytes(data: bytes, byte_range: Any) -> bytes:
        if hasattr(byte_range, "start") and hasattr(byte_range, "end"):
            return data[int(byte_range.start) : int(byte_range.end)]
        if hasattr(byte_range, "offset"):
            return data[int(byte_range.offset) :]
        if hasattr(byte_range, "suffix"):
            suffix = int(byte_range.suffix)
            return b"" if suffix <= 0 else data[-suffix:]
        return data

    async def exists(self, key: str) -> bool:
        await self._ensure_open()
        return self._fs.exists(self._uri_for_key(key))

    async def get(
        self,
        key: str,
        prototype: Any | None = None,
        byte_range: Any | None = None,
    ) -> Any | None:
        await self._ensure_open()
        uri = self._uri_for_key(key)
        if not self._fs.exists(uri):
            return None
        try:
            data = self._fs.read_bytes(uri)
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            return None
        if byte_range is not None:
            data = self._slice_bytes(data, byte_range)
        buffer_prototype = cast(Any, prototype or default_buffer_prototype())
        return buffer_prototype.buffer.from_bytes(data)

    async def get_partial_values(
        self,
        prototype: Any,
        key_ranges: Iterable[tuple[str, Any | None]],
    ) -> list[Any | None]:
        return [await self.get(key, prototype, byte_range) for key, byte_range in key_ranges]

    async def list(self) -> AsyncIterator[str]:
        await self._ensure_open()
        for entry in self._fs.find(self._store_uri):  # pyright: ignore[reportArgumentType]
            rel = self._relative_key(entry)
            if rel:
                yield rel

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        await self._ensure_open()
        normalized = prefix.strip("/")
        if normalized:
            normalized = f"{normalized}/"
        async for rel in self.list():
            if rel.startswith(normalized):
                yield rel

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        await self._ensure_open()
        normalized = prefix.strip("/")
        if normalized:
            normalized = f"{normalized}/"
        seen: set[str] = set()
        async for rel in self.list():
            if normalized and not rel.startswith(normalized):
                continue
            rest = rel[len(normalized) :]
            if not rest:
                continue
            child = rest.split("/", 1)[0]
            if child in seen:
                continue
            seen.add(child)
            yield child

    async def set(self, key: str, value: Any) -> None:
        self._check_writable()
        raise NotImplementedError("Zarr validation store is read-only")

    async def delete(self, key: str) -> None:
        self._check_writable()
        raise NotImplementedError("Zarr validation store is read-only")


@dataclass
class ZarrCompareReport:
    """Summary of a read-only comparison between two Zarr stores.

    Mismatches are split into two buckets:

    * ``content_mismatches`` — differences in array paths, shape, dtype,
      dimension names, public attrs, the static-array marker, or data values.
    * ``layout_mismatches`` — differences in chunk shape, shard shape, or
      codec configuration.

    The ``mismatches`` property is a computed union of both lists.

    Attributes:
        equivalent: ``True`` when *both* buckets are empty (no differences of
            any kind).  Layout-only differences still set this to ``False``
            so that callers can inspect the report; the CLI is
            responsible for mapping layout-only to exit 0 with a warning.
        content_mismatches: Terse descriptions of content-level differences.
        layout_mismatches: Terse descriptions of layout-level differences.

    Examples:
        >>> ZarrCompareReport(equivalent=True).equivalent
        True
        >>> r = ZarrCompareReport(equivalent=False, content_mismatches=["array x: values differ"])
        >>> r.mismatches == r.content_mismatches + r.layout_mismatches
        True
    """

    equivalent: bool
    content_mismatches: list[str] = field(default_factory=list)
    layout_mismatches: list[str] = field(default_factory=list)

    @property
    def mismatches(self) -> list[str]:
        """Union of content and layout mismatches."""
        return self.content_mismatches + self.layout_mismatches

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the report."""
        return {
            "equivalent": self.equivalent,
            "content_mismatches": self.content_mismatches,
            "layout_mismatches": self.layout_mismatches,
            "mismatches": self.mismatches,
        }


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
    content_mismatches: list[str] = [
        f"array {path}: missing from second store" for path in sorted(a_paths - b_paths)
    ]
    content_mismatches.extend(
        f"array {path}: missing from first store" for path in sorted(b_paths - a_paths)
    )
    layout_mismatches: list[str] = []

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
            content_mismatches.append(f"{path_prefix}: shape {left_shape} != {right_shape}")

        left_dtype = np.dtype(left.dtype)
        right_dtype = np.dtype(right.dtype)
        if left_dtype != right_dtype:
            content_mismatches.append(f"{path_prefix}: dtype {left_dtype} != {right_dtype}")

        left_chunks = tuple(int(size) for size in left.chunks)
        right_chunks = tuple(int(size) for size in right.chunks)
        if left_chunks != right_chunks:
            layout_mismatches.append(f"{path_prefix}: chunks {left_chunks} != {right_chunks}")

        left_dimension_names = _dimension_names(left)
        right_dimension_names = _dimension_names(right)
        if left_dimension_names != right_dimension_names:
            content_mismatches.append(
                f"{path_prefix}: dimension_names {left_dimension_names} != {right_dimension_names}"
            )

        left_attrs = _public_attrs(getattr(left, "attrs", {}))
        right_attrs = _public_attrs(getattr(right, "attrs", {}))
        if left_attrs != right_attrs:
            content_mismatches.append(f"{path_prefix}: attrs differ")

        left_marker = _static_marker(left)
        right_marker = _static_marker(right)
        if left_marker != right_marker:
            content_mismatches.append(
                f"{path_prefix}: firecube_static_written {left_marker!r} != {right_marker!r}"
            )

        if (
            left_shape == right_shape
            and left_dtype == right_dtype
            and not _values_equal(left, right)
        ):
            content_mismatches.append(f"{path_prefix}: values differ")

    equivalent = not content_mismatches and not layout_mismatches
    return ZarrCompareReport(
        equivalent=equivalent,
        content_mismatches=content_mismatches,
        layout_mismatches=layout_mismatches,
    )


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


@dataclass(frozen=True)
class _ArrayInfo:
    path: str
    dim_names: list[str]
    shape: list[int]
    chunk_shape: list[int]
    array: Any | None = None


@dataclass(frozen=True)
class _ArrayChunkScan:
    path: str
    dim_names: list[str]
    shape: list[int]
    chunk_shape: list[int]
    expected_chunks: dict[str, int]
    max_indices: dict[str, int]
    extra_chunks: list[str]
    absent_time_indices: list[int]
    budget_exceeded: bool
    chunks_processed: int


def _parent_path(path: str) -> str:
    normalized = path.strip("/")
    if "/" not in normalized:
        return ""
    return normalized.rsplit("/", 1)[0]


def _leaf_name(path: str) -> str:
    return path.strip("/").rsplit("/", 1)[-1]


def _join_path(parent: str, child: str) -> str:
    return f"{parent.strip('/')}/{child.strip('/')}".strip("/")


def _relative_store_path(store_uri: StorageUri, entry: StorageUri) -> str:
    root = store_uri.path.rstrip("/")
    return entry.path.removeprefix(root).strip("/")


def _discover_array_metadata_under_with_fs(
    fs: StorageFilesystem,
    store_uri: StorageUri,
    group: str,
) -> dict[str, dict[str, Any]]:
    arrays: dict[str, dict[str, Any]] = {}
    prefix_uri = store_uri.join(group.strip("/")) if group.strip("/") else store_uri
    for entry in fs.find(prefix_uri):  # pyright: ignore[reportArgumentType]
        if not isinstance(entry, StorageUri):
            continue
        if entry.path.rsplit("/", 1)[-1] != "zarr.json":
            continue
        with fs.open(entry, "r") as handle:
            meta = json.load(handle)
        if meta.get("node_type") != "array":
            continue
        rel = _relative_store_path(store_uri, entry.parent())
        if rel:
            arrays[rel] = meta
    return arrays


def _discover_immediate_array_metadata_with_fs(
    fs: StorageFilesystem,
    store_uri: StorageUri,
    group: str,
) -> dict[str, dict[str, Any]]:
    parent = group.strip("/")
    return {
        path: meta
        for path, meta in _discover_array_metadata_under_with_fs(fs, store_uri, parent).items()
        if _parent_path(path) == parent
    }


def _open_zarr_root_from_fs(fs: StorageFilesystem, store_uri: StorageUri) -> Any | None:
    try:
        import zarr

        return zarr.open_group(
            store=_StorageFilesystemStore(fs, store_uri),
            mode="r",
            zarr_format=3,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        # Narrow on purpose: broader failures (RuntimeError, OSError, ...) must
        # propagate, not be silently downgraded to a metadata-only fallback.
        log.warning(
            "Zarr group open failed for %s: %s",
            store_uri.to_str(),
            exc,
            exc_info=True,
        )
        return None


def _zarr_array_at(root: Any | None, path: str) -> Any | None:
    if root is None:
        return None
    try:
        node = root[path.strip("/")]
    except (FileNotFoundError, KeyError, ValueError) as exc:
        # Narrow on purpose: only "array not present / metadata missing / malformed"
        # is tolerated. Broader failures must propagate (see companion catch above).
        log.warning("Zarr array lookup failed for %r: %s", path, exc)
        return None
    if hasattr(node, "shape") and hasattr(node, "chunks"):
        return node
    return None


def _array_info_from_metadata(
    path: str,
    meta: dict[str, Any],
    array: Any | None,
) -> _ArrayInfo:
    if array is not None:
        dim_names, shape, chunk_shape = _read_chunk_grid_from_zarr_array(array, path)
    else:
        dim_names, shape, chunk_shape = _read_chunk_grid_from_metadata(meta, path)
    return _ArrayInfo(
        path=path,
        dim_names=dim_names,
        shape=shape,
        chunk_shape=chunk_shape,
        array=array,
    )


def _candidate_time_dim(
    parent_arrays: list[_ArrayInfo],
    *,
    time_dim_name: str | None = None,
) -> str | None:
    by_name = {_leaf_name(info.path): info for info in parent_arrays}
    state = by_name.get(_STATE_ARRAY_NAME)
    if time_dim_name is not None:
        if state is not None and state.dim_names and state.dim_names[0] != time_dim_name:
            stored_name = state.dim_names[0]
            raise ValueError(
                "Explicit time dimension name "
                f"{time_dim_name!r} contradicts stored state dimension {stored_name!r}."
            )
        return time_dim_name

    log.info("No explicit time dimension name provided; auto-detecting from stored Zarr metadata")
    if state is not None and len(state.shape) == 1 and state.dim_names:
        return state.dim_names[0]
    return None


def _time_axis(info: _ArrayInfo, time_dim: str) -> int | None:
    if time_dim in info.dim_names:
        return info.dim_names.index(time_dim)
    if _leaf_name(info.path) == time_dim and len(info.shape) == 1:
        return 0
    return None


def _read_flat_array(array: Any, path: str) -> np.ndarray[Any, Any]:
    try:
        return np.asarray(array[...]).reshape(-1)
    except Exception as exc:
        raise RuntimeError(f"Failed to read Zarr array {path!r}: {exc}") from exc


def _validate_coord_values(
    *,
    coord_path: str,
    coord_values: np.ndarray[Any, Any],
    validity_issues: list[str],
) -> None:
    filled = np.asarray(coord_values)
    # Dense DirectZarr coordinates carry NaT (or NaN) for slots that were never
    # written; those slots are absent, not out of order, so only filled values
    # take part in the uniqueness and monotonicity checks.
    if filled.dtype.kind == "M":
        filled = filled[~np.isnat(filled)]
    elif filled.dtype.kind == "f":
        filled = filled[~np.isnan(filled)]
    values = list(filled)
    if len(set(values)) != len(values):
        validity_issues.append(f"time coordinate {coord_path}: duplicate values")
    for left, right in pairwise(values):
        try:
            monotonic = bool(left < right)
        except Exception:
            monotonic = False
        if not monotonic:
            validity_issues.append(
                f"time coordinate {coord_path}: values are not strictly monotonic"
            )
            break


def _validate_state_values(
    *,
    state_path: str,
    state_values: np.ndarray[Any, Any],
    coord_length: int,
    validity_issues: list[str],
) -> None:
    if len(state_values) != coord_length:
        validity_issues.append(
            f"state array {state_path}: length {len(state_values)} != time coordinate length {coord_length}"
        )
    valid_states = {1, 2, 3}
    try:
        observed = {int(value) for value in state_values}
    except Exception as exc:
        validity_issues.append(f"state array {state_path}: values are not integer states ({exc})")
        return
    invalid = sorted(observed - valid_states)
    if invalid:
        validity_issues.append(
            f"state array {state_path}: invalid state values {invalid}; "
            "expected states {1, 2, 3} and state=0 unknown is invalid"
        )


def _scan_array_chunks_with_fs(
    fs: StorageFilesystem,
    store_uri: StorageUri,
    info: _ArrayInfo,
    time_axis: int | None,
    *,
    timeout_s: float | None,
    max_chunks: int | None,
    on_timeout: str,
) -> _ArrayChunkScan:
    ndim = len(info.shape)
    if not info.shape or len(info.chunk_shape) != ndim:
        raise ValueError(
            f"Inconsistent shape/chunk_shape for group {store_uri.join(info.path).to_str()}"
        )

    expected_chunks: dict[str, int] = {}
    for name, size, csize in zip(info.dim_names, info.shape, info.chunk_shape, strict=False):
        if csize <= 0:
            raise ValueError(f"Invalid chunk size {csize} for dimension {name}")
        expected_chunks[name] = math.ceil(size / float(csize))

    max_indices: dict[str, int] = dict.fromkeys(info.dim_names, -1)
    extra_chunks: list[str] = []
    index_sets: dict[str, set[int]] = {name: set() for name in info.dim_names}
    group_uri = store_uri.join(info.path)
    chunk_dir = group_uri.join("c")

    budget_active = timeout_s is not None or max_chunks is not None
    started = time.time() if budget_active else 0.0
    chunks_processed = 0
    budget_exceeded = False

    if fs.exists(chunk_dir):  # pyright: ignore[reportArgumentType]
        try:
            entries = fs.find(group_uri)  # pyright: ignore[reportArgumentType]
        except Exception as exc:
            raise RuntimeError(f"Failed to list chunks under {chunk_dir.to_str()}: {exc}") from exc

        for entry in entries:
            path = _chunk_entry_path(entry)
            if budget_active:
                if max_chunks is not None and chunks_processed >= max_chunks:
                    budget_exceeded = True
                    break
                if timeout_s is not None and (time.time() - started) >= timeout_s:
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
            for name, idx in zip(info.dim_names, indices, strict=False):
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

    absent_time_indices: list[int] = []
    if time_axis is not None:
        time_dim = info.dim_names[time_axis]
        expected_time_chunks = expected_chunks[time_dim]
        absent_time_indices = sorted(set(range(expected_time_chunks)) - index_sets[time_dim])

    return _ArrayChunkScan(
        path=info.path,
        dim_names=info.dim_names,
        shape=info.shape,
        chunk_shape=info.chunk_shape,
        expected_chunks=expected_chunks,
        max_indices=max_indices,
        extra_chunks=extra_chunks,
        absent_time_indices=absent_time_indices,
        budget_exceeded=budget_exceeded if budget_active else False,
        chunks_processed=chunks_processed if budget_active else 0,
    )


def _legacy_shape(report_scans: list[_ArrayChunkScan]) -> tuple[list[int], list[int]]:
    if len(report_scans) != 1:
        return [], []
    scan = report_scans[0]
    return scan.shape, scan.chunk_shape


def _legacy_chunk_maps(
    report_scans: list[_ArrayChunkScan],
) -> tuple[dict[str, int], dict[str, int]]:
    if len(report_scans) == 1:
        scan = report_scans[0]
        return scan.expected_chunks, scan.max_indices
    expected_chunks: dict[str, int] = {}
    max_indices: dict[str, int] = {}
    for scan in report_scans:
        for dim_name, count in scan.expected_chunks.items():
            expected_chunks[f"{scan.path}:{dim_name}"] = count
        for dim_name, max_index in scan.max_indices.items():
            max_indices[f"{scan.path}:{dim_name}"] = max_index
    return expected_chunks, max_indices


def _collect_static_marker_failures(
    all_metadata: dict[str, dict[str, Any]],
    candidate_time_dims_by_parent: dict[str, str | None],
    target_paths: list[str],
    *,
    state_var_name: str,
) -> list[dict[str, str]]:
    """Return static-array marker failures for validated arrays, read-only.

    An array counts as static (and therefore must carry the
    ``firecube_static_written`` marker) when its first dimension is neither
    the effective time-dimension name nor the state-array name.
    """
    failures: list[dict[str, str]] = []
    for array_path in target_paths:
        meta = all_metadata.get(array_path)
        if meta is None:
            continue
        dimension_names = meta.get("dimension_names")
        if not dimension_names:
            continue
        first_dimension = str(dimension_names[0])
        parent = _parent_path(array_path)
        effective_time_dim = candidate_time_dims_by_parent.get(parent)
        if effective_time_dim is None:
            continue
        skip_names = {state_var_name, effective_time_dim}
        if first_dimension in skip_names:
            continue
        attrs = meta.get("attributes") or {}
        if attrs.get(FIRECUBE_STATIC_WRITTEN_ATTR):
            continue
        failures.append({"array": array_path, "reason": "missing_or_false_static_marker"})
    return failures


def validate_group_with_fs(
    fs: StorageFilesystem,
    store_uri: StorageUri,
    group: str,
    *,
    timeout_s: float | None = None,
    max_chunks: int | None = None,
    on_timeout: str = "warn",
    time_dim_name: str | None = None,
    state_var_name: str = _STATE_ARRAY_NAME,
) -> ZarrValidationReport:
    """Validate a Zarr array or container group and return a read-only report.

    Args:
        store_uri: Base store URI, e.g.
            ``StorageUri.parse("s3://bucket/product.zarr")``.
        group: Array or container-group path inside the store, e.g.
            ``F024/FWI``, ``F048/timestamp``, or ``F024``.
        time_dim_name: Explicit time-dimension name to treat as
            time-indexed when classifying arrays for the static-marker check.
            Defaults to ``None``; when ``None`` the time dimension is
            auto-detected per parent group.
        state_var_name: Leaf name of the state array whose first dimension is
            also treated as time-indexed. Defaults to the reserved
            ``firecube_timestamp_state`` name.
    """
    product = store_uri.path.rstrip("/").split("/")[-1] if store_uri.path != "/" else ""
    normalized_group = group.strip("/")
    group_uri = store_uri.join(normalized_group)
    meta = _load_array_metadata_with_fs(fs, group_uri)
    node_type = meta.get("node_type")

    if node_type == "array":
        selected_metadata = {normalized_group: meta}
    elif node_type == "group":
        selected_metadata = _discover_array_metadata_under_with_fs(fs, store_uri, normalized_group)
    else:
        selected_metadata = {normalized_group: meta}

    selected_paths = sorted(selected_metadata)
    root = _open_zarr_root_from_fs(fs, store_uri)

    all_metadata = dict(selected_metadata)
    for parent in sorted({_parent_path(path) for path in selected_paths}):
        all_metadata.update(_discover_immediate_array_metadata_with_fs(fs, store_uri, parent))

    all_infos = {
        path: _array_info_from_metadata(path, meta_item, _zarr_array_at(root, path))
        for path, meta_item in all_metadata.items()
    }

    parent_infos: dict[str, list[_ArrayInfo]] = {}
    for info in all_infos.values():
        parent_infos.setdefault(_parent_path(info.path), []).append(info)

    candidate_time_dims_by_parent = {
        parent: _candidate_time_dim(infos, time_dim_name=time_dim_name)
        for parent, infos in parent_infos.items()
    }

    arrays_checked: list[str] = []
    absent_chunk_indices: dict[str, list[int]] = {}
    validity_issues: list[str] = []
    info_notes: list[str] = []
    report_scans: list[_ArrayChunkScan] = []
    validated_parents: set[str] = set()

    for path in selected_paths:
        info = all_infos[path]
        parent = _parent_path(path)
        parent_arrays = parent_infos.get(parent, [])
        time_dim = candidate_time_dims_by_parent.get(parent)
        axis = _time_axis(info, time_dim) if time_dim is not None else None
        if axis is None:
            continue
        assert time_dim is not None

        arrays_checked.append(info.path)
        if parent not in validated_parents:
            validated_parents.add(parent)
            by_name = {_leaf_name(item.path): item for item in parent_arrays}
            coord_path = _join_path(parent, time_dim)
            coord_info = by_name.get(time_dim)
            coord_values: np.ndarray[Any, Any] | None = None
            if coord_info is None:
                validity_issues.append(f"time coordinate {coord_path}: missing")
            elif coord_info.array is None:
                validity_issues.append(f"time coordinate {coord_path}: could not be opened")
            else:
                coord_values = _read_flat_array(coord_info.array, coord_path)
                _validate_coord_values(
                    coord_path=coord_path,
                    coord_values=coord_values,
                    validity_issues=validity_issues,
                )

            state_path = _join_path(parent, _STATE_ARRAY_NAME)
            state_info = by_name.get(_STATE_ARRAY_NAME)
            if state_info is None:
                info_notes.append(
                    f"state array {state_path}: absent (DirectZarr or legacy pre-state store)"
                )
            elif coord_values is None:
                validity_issues.append(
                    f"state array {state_path}: cannot compare length without time coordinate"
                )
            elif state_info.array is None:
                validity_issues.append(f"state array {state_path}: could not be opened")
            else:
                _validate_state_values(
                    state_path=state_path,
                    state_values=_read_flat_array(state_info.array, state_path),
                    coord_length=len(coord_values),
                    validity_issues=validity_issues,
                )

        scan = _scan_array_chunks_with_fs(
            fs,
            store_uri,
            info,
            axis,
            timeout_s=timeout_s,
            max_chunks=max_chunks,
            on_timeout=on_timeout,
        )
        report_scans.append(scan)
        absent_chunk_indices[info.path] = scan.absent_time_indices

        coord_info_for_length = next(
            (item for item in parent_infos.get(parent, []) if _leaf_name(item.path) == time_dim),
            None,
        )
        if coord_info_for_length is not None:
            coord_length = coord_info_for_length.shape[0]
            if info.shape[axis] != coord_length:
                validity_issues.append(
                    f"array {info.path}: {time_dim} length {info.shape[axis]} "
                    f"!= time coordinate length {coord_length}"
                )

    if not report_scans and node_type == "array":
        info = all_infos[normalized_group]
        report_scans.append(
            _scan_array_chunks_with_fs(
                fs,
                store_uri,
                info,
                None,
                timeout_s=timeout_s,
                max_chunks=max_chunks,
                on_timeout=on_timeout,
            )
        )

    shape, chunk_shape = _legacy_shape(report_scans)
    expected_chunks, max_indices = _legacy_chunk_maps(report_scans)
    extra_chunks = [path for scan in report_scans for path in scan.extra_chunks]
    budget_exceeded = any(scan.budget_exceeded for scan in report_scans)
    chunks_processed = sum(scan.chunks_processed for scan in report_scans)

    static_target_paths = arrays_checked if arrays_checked else [normalized_group]
    for parent, time_dim in candidate_time_dims_by_parent.items():
        if time_dim is None and any(_parent_path(p) == parent for p in static_target_paths):
            info_notes.append(
                f"static-marker check skipped for {parent or '/'}: "
                "time dimension unknown (no state array); "
                "pass --time-dim to enable the check"
            )
    static_marker_failures = _collect_static_marker_failures(
        all_metadata,
        candidate_time_dims_by_parent,
        static_target_paths,
        state_var_name=state_var_name,
    )

    return ZarrValidationReport(
        product=product,
        group=normalized_group,
        shape=shape,
        chunk_shape=chunk_shape,
        expected_chunks=expected_chunks,
        max_indices=max_indices,
        extra_chunks=extra_chunks,
        budget_exceeded=budget_exceeded,
        chunks_processed=chunks_processed,
        arrays_checked=arrays_checked,
        absent_chunk_indices=absent_chunk_indices,
        info_notes=info_notes,
        validity_issues=validity_issues,
        static_marker_failures=static_marker_failures,
        is_valid=not (validity_issues or static_marker_failures),
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
