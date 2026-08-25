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

"""Reusable direct-Zarr region writer for streaming ingestors.

Provides ``RegionZarrWriter`` — a lazy-open writer that creates groups/arrays
on demand and writes spatial regions at timestamp slots. Timestamped arrays must
be preallocated before writes, usually via ``ZarrArraySpec.expected_time_count``
or the auto-computation in ``IndexedRegionStrategy``.

Extracted from the MTG FCI plugin's ``ZarrStoreWriter``; all sensor-specific
logic has been removed.  Use ``coord_names`` to specify which array names are
coordinate axes and should be skipped by :meth:`ensure_timestamp_slot`.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import numpy as np
import zarr
from zarr.errors import GroupNotFoundError, NodeNotFoundError
from zarr.storage import LocalStore

from firecube.core.errors import SchemaDriftError
from firecube.core.uris import is_remote_target, local_path_from_target
from firecube.core.zarr._reserved_attrs import _FILL_VALUE_ATTR, assert_attrs_safe

log = logging.getLogger("firecube.core.zarr.region_writer")

_DEFAULT_COORD_NAMES: frozenset[str] = frozenset({"y", "x", "channel"})


def _raise_time_capacity_error(
    group_name: str,
    array_name: str,
    *,
    length: int,
    ts_index: int,
) -> None:
    raise ValueError(
        f"Array {group_name}/{array_name} has time dimension length {length}; "
        f"ts_index={ts_index} is out of bounds. Preallocate timestamped arrays "
        "with ZarrArraySpec.expected_time_count before writing."
    )


def _fill_value_is_missing(value: Any) -> bool:
    """Return True when *value* is a not-a-value sentinel for its dtype.

    Covers floating/complex ``NaN`` and datetime64/timedelta64 ``NaT`` — the fill
    sentinels that are never equal to themselves under ``==``. Dispatch is by
    numpy dtype kind so every such type is handled uniformly; all other kinds
    (ints, bytes, object, ``None``) are never "missing".
    """
    arr = np.asarray(value)
    if arr.dtype.kind in ("f", "c"):  # floating, complex
        return bool(np.isnan(arr))
    if arr.dtype.kind in ("M", "m"):  # datetime64, timedelta64
        return bool(np.isnat(arr))
    return False


def _fill_values_equal(existing: Any, spec: Any) -> bool:
    # Missing-value fills (NaN, NaT) are never == themselves, so compare them by
    # "both missing" rather than by value; otherwise fall back to plain equality.
    existing_missing = _fill_value_is_missing(existing)
    spec_missing = _fill_value_is_missing(spec)
    if existing_missing or spec_missing:
        return existing_missing and spec_missing
    return bool(existing == spec)


def _array_is_all_fill(arr: np.ndarray, fill_value: Any) -> bool:
    """Return True iff every element of *arr* equals *fill_value* under
    missing-aware equality.

    Mirrors :func:`_fill_value_is_missing` semantics:
      - float / complex NaN fill: every element must be NaN
      - datetime64 / timedelta64 NaT fill: every element must be NaT
      - all other dtypes (int, bool, bytes/string): plain element-wise equality

    Empty arrays return True (consistent with ``np.all([])`` semantics; an
    empty static array trivially satisfies "all elements equal fill".

    Out of contract: object dtype, structured/record dtypes. Callers MUST
    not pass these.
    """
    if _fill_value_is_missing(fill_value):
        kind = np.asarray(fill_value).dtype.kind
        if kind in ("f", "c"):
            return bool(np.all(np.isnan(arr)))
        if kind in ("M", "m"):
            return bool(np.all(np.isnat(arr)))
    return bool(np.all(arr == fill_value))


def _fill_value_attr_value(fill_value: Any) -> Any:
    """Return a JSON-safe scalar for use as a ``_FillValue`` attr, or ``None``.

    Args:
        fill_value: The fill value declared in ``ZarrArraySpec.fill_value``.

    Returns:
        A JSON-finite scalar (bool, int, str, or finite float) suitable for
        writing into Zarr array attrs, or ``None`` if the value cannot be
        represented safely (NaT, NaN, datetime objects, etc.).
    """
    if isinstance(fill_value, np.generic):
        fill_value = fill_value.item()
    if isinstance(fill_value, bool | int | str):
        return fill_value
    if isinstance(fill_value, float) and math.isfinite(fill_value):
        return fill_value
    return None


_ARRAY_EQUAL_NAN_UNSAFE_KINDS = frozenset({"U", "S", "O", "V"})


def _arrays_equal_missing_aware(a1: Any, a2: Any) -> bool:
    """Element-wise array equality with NaN/NaT-aware semantics.

    Uses ``np.array_equal(..., equal_nan=True)`` for numeric and temporal
    dtypes (``i``, ``u``, ``b``, ``f``, ``c``, ``M``, ``m``), which treats
    NaN==NaN as equal for float/complex and NaT==NaT as equal for
    datetime64/timedelta64.

    Falls back to plain ``np.array_equal`` for dtypes (``U``, ``S``, ``O``,
    ``V``) where NumPy's ``equal_nan=True`` raises ``TypeError`` because
    ``np.isnan`` has no loop for those kinds. There is no NaN concept for
    strings/bytes/objects, and structured arrays need element-wise field
    comparison that is out of scope here.

    Both operand dtypes are checked because the on-disk dtype (``a1``) may
    differ from the incoming intent dtype (``a2``) when one side has been
    coerced through serialization.
    """
    left = np.asarray(a1)
    right = np.asarray(a2)
    if (
        left.dtype.kind in _ARRAY_EQUAL_NAN_UNSAFE_KINDS
        or right.dtype.kind in _ARRAY_EQUAL_NAN_UNSAFE_KINDS
    ):
        return bool(np.array_equal(left, right))
    return bool(np.array_equal(left, right, equal_nan=True))


def _array_path_exists(root: Any, group_path: str) -> bool:
    """Return True if ``group_path`` (e.g. ``data/counts``) resolves to an array."""
    parts = [p for p in group_path.split("/") if p]
    if not parts:
        return False
    current = root
    for part in parts[:-1]:
        if part not in current:
            return False
        current = current[part]
    return parts[-1] in current


def _open_root_read_only(writer: RegionZarrWriter) -> Any | None:
    """Open the root group without creating missing Zarr metadata."""
    try:
        if writer._store is not None:
            return zarr.open_group(store=writer._store, mode="r", zarr_format=3)
        if not is_remote_target(writer._store_uri):
            store = LocalStore(str(local_path_from_target(writer._store_uri)))
            return zarr.open_group(store=store, mode="r", zarr_format=3)
        return zarr.open_group(store=writer._store_uri, mode="r", zarr_format=3)
    except (FileNotFoundError, GroupNotFoundError, NodeNotFoundError):
        return None


def group_schema_satisfied(
    writer: RegionZarrWriter,
    group_name: str,
    arr_specs: Sequence[Any],
    expected_time_count: int,
) -> bool:
    """Return True iff every array already exists and matches its spec (read-only).

    A pure read check (no writes, no claims). Schema setup is idempotent, so when
    this returns True the caller can skip the exclusive setup claim entirely —
    which is what lets concurrent slot-range pods start at once without racing on
    an idempotent verify. Returns False when any array is missing or drifts; the
    caller then takes the claim and the existing create/verify path surfaces real
    drift as usual.
    """
    root = _open_root_read_only(writer)
    if root is None:
        return False
    for arr_spec in arr_specs:
        group_path = f"{group_name}/{arr_spec.name}"
        if not _array_path_exists(root, group_path):
            return False
        try:
            writer.verify_array_spec(group_path, arr_spec, expected_time_count)
        except SchemaDriftError:
            return False
    return True


@runtime_checkable
class RegionZarrWriterProtocol(Protocol):
    """Structural interface for direct-Zarr region writers.

    Plugins and templates may depend on this protocol without coupling to the
    concrete ``RegionZarrWriter`` implementation.
    """

    def ensure_group(
        self,
        group: str,
        shape: tuple[int, ...] | None = None,
        dtype: np.dtype[Any] | type[np.generic] | type[Any] | str | None = None,
        fill_value: Any | None = None,
        chunks: tuple[int, ...] | None = None,
        allow_grow: bool = False,
        shards: tuple[int, ...] | None = None,
        attrs: Mapping[str, Any] | None = None,
        dimension_names: tuple[str, ...] | None = None,
        filters: Sequence[Any] | None = None,
        serializer: Any | None = None,
        compressors: Sequence[Any] | None = None,
    ) -> Any: ...

    def ensure_timestamp_slot(self, group: str, ts_index: int) -> None: ...

    def write_region(
        self,
        group: str,
        array_name: str,
        ts_index: int,
        y_slice: slice,
        data: np.ndarray,
        *,
        channel_index: int | None = None,
    ) -> None: ...

    def write_1d(
        self,
        group: str,
        array_name: str,
        ts_index: int,
        data: np.ndarray,
    ) -> None: ...

    def resolve_timestamp_index(self, group: str, timestamp_val: Any) -> int: ...

    def write_timestamp(
        self,
        group: str,
        ts_index: int,
        timestamp_val: Any,
    ) -> None: ...

    def write_static(
        self,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None: ...


class RegionZarrWriter:
    """Write arrays directly to a Zarr store with lazy open semantics.

    Args:
        store_uri: Target store URI/path (local path, ``file://`` URI, or
            ``s3://`` URI).
        coord_names: Array names treated as coordinate axes.  These are
            skipped by :meth:`ensure_timestamp_slot` when resizing timestamped
            arrays.  Defaults to ``{"y", "x", "channel"}``.
    """

    def __init__(
        self,
        store_uri: str,
        *,
        store: object | None = None,
        coord_names: frozenset[str] = _DEFAULT_COORD_NAMES,
        time_coord_name: str = "timestamp",
    ) -> None:
        self._store_uri = store_uri
        self._store = store  # pre-built store (e.g., zarr.storage.ObjectStore for obstore)
        self._coord_names = coord_names
        self._time_coord_name = time_coord_name
        self._root: Any | None = None

    def _open_root(self) -> Any:
        """Open and memoize the root group in append mode."""
        if self._root is not None:
            return self._root

        if self._store is not None:
            self._root = zarr.open_group(store=self._store, mode="a", zarr_format=3)
            return self._root

        if not is_remote_target(self._store_uri):
            store = LocalStore(str(local_path_from_target(self._store_uri)))
            self._root = zarr.open_group(store=store, mode="a", zarr_format=3)
        else:
            self._root = zarr.open_group(store=self._store_uri, mode="a", zarr_format=3)
        return self._root

    @staticmethod
    def _normalize_timestamp_value(timestamp_val: Any) -> np.datetime64:
        """Normalize timestamp-like values to ``datetime64[s]``."""
        if isinstance(timestamp_val, np.datetime64):
            return timestamp_val.astype("datetime64[s]")
        if hasattr(timestamp_val, "isoformat"):
            return np.datetime64(str(timestamp_val.isoformat()), "s")
        return np.datetime64(str(timestamp_val), "s")

    def ensure_group(
        self,
        group: str,
        shape: tuple[int, ...] | None = None,
        dtype: np.dtype[Any] | type[np.generic] | type[Any] | str | None = None,
        fill_value: Any | None = None,
        chunks: tuple[int, ...] | None = None,
        allow_grow: bool = False,
        shards: tuple[int, ...] | None = None,
        attrs: Mapping[str, Any] | None = None,
        dimension_names: tuple[str, ...] | None = None,
        filters: Sequence[Any] | None = None,
        serializer: Any | None = None,
        compressors: Sequence[Any] | None = None,
    ) -> Any:
        """Ensure a group or array path exists.

        Args:
            group: Group path or array path (e.g. ``data_1km/counts``).
            shape: Array shape when creating array paths.
            dtype: Array dtype when creating array paths.
            fill_value: Fill value for newly created arrays.
            chunks: Chunk shape for newly created arrays.
            allow_grow: If ``True``, grow existing arrays to at least *shape*.
                Defaults to ``False`` so schema verification callers can
                inspect undersized arrays and raise their own drift errors.
            shards: Optional Zarr v3 shard shape for newly created arrays.
            attrs: Optional user attributes to write at array creation.
                Reserved keys (see
                :data:`firecube.core.zarr._reserved_attrs.RESERVED_ARRAY_ATTRS`)
                are rejected. ``ensure_group`` is creation-only — drift
                detection for the attrs of existing arrays is handled by
                :meth:`verify_array_spec`.
            dimension_names: Optional Zarr v3 ``dimension_names`` for newly
                created arrays. On resume, a mismatch against an existing
                array raises :class:`SchemaDriftError`; firecube does not
                perform in-place dimension renames.

        Returns:
            Existing or newly created group/array.

        Raises:
            ValueError: If array creation is requested without *shape*/*dtype*,
                or if *attrs* contains reserved keys.
            SchemaDriftError: If *dimension_names* differs from the existing
                array's ``dimension_names`` on resume.
        """
        if attrs is not None:
            assert_attrs_safe(attrs)

        root = self._open_root()
        parts = [p for p in Path(group).parts if p not in ("", "/")]
        if not parts:
            return root

        if len(parts) == 1 and shape is None:
            return root.require_group(parts[0])

        if shape is None or dtype is None:
            raise ValueError("shape and dtype are required when ensuring an array path")

        grp_path = "/".join(parts[:-1])
        arr_name = parts[-1]
        target_group = root if not grp_path else root.require_group(grp_path)
        if arr_name in target_group:
            arr = target_group[arr_name]
            if (
                allow_grow
                and len(arr.shape) == len(shape)
                and any(
                    current < requested for current, requested in zip(arr.shape, shape, strict=True)
                )
            ):
                arr.resize(
                    tuple(
                        max(current, requested)
                        for current, requested in zip(arr.shape, shape, strict=True)
                    )
                )
            if dimension_names is not None:
                existing_dimnames = getattr(getattr(arr, "metadata", None), "dimension_names", None)
                if existing_dimnames is not None and tuple(existing_dimnames) != tuple(
                    dimension_names
                ):
                    raise SchemaDriftError(
                        f"Schema drift for {group!r} field='dimension_names': "
                        f"existing={tuple(existing_dimnames)!r} spec={tuple(dimension_names)!r}. "
                        "Re-ingest from scratch; no in-place migration is provided."
                    )
            _fv_attr = _fill_value_attr_value(fill_value)
            if _fv_attr is not None and _FILL_VALUE_ATTR not in dict(arr.attrs):
                arr.attrs[_FILL_VALUE_ATTR] = _fv_attr
            return arr

        kwargs: dict[str, Any] = {
            "name": arr_name,
            "shape": shape,
            "dtype": dtype,
            "fill_value": fill_value,
        }
        if chunks is not None:
            kwargs["chunks"] = chunks
        if shards is not None:
            kwargs["shards"] = shards
        if attrs is not None:
            kwargs["attributes"] = dict(attrs)
        if dimension_names is not None:
            kwargs["dimension_names"] = list(dimension_names)
        if filters is not None:
            kwargs["filters"] = list(filters)
        if serializer is not None:
            kwargs["serializer"] = serializer
        if compressors is not None:
            kwargs["compressors"] = list(compressors)
        arr = target_group.create_array(**kwargs)
        _fv_attr = _fill_value_attr_value(fill_value)
        if _fv_attr is not None and _FILL_VALUE_ATTR not in dict(arr.attrs):
            arr.attrs[_FILL_VALUE_ATTR] = _fv_attr
        return arr

    def set_group_attrs(self, group: str, attrs: Mapping[str, Any] | None) -> None:
        """Stamp convention-agnostic group-level attributes onto the group metadata.

        Writes ``attrs`` verbatim onto the Zarr group's ``zarr.json``; the writer
        does not interpret the mapping. Reserved firecube-internal attribute names
        are rejected (see :func:`assert_attrs_safe`). A falsy ``attrs`` is a no-op.
        Idempotent — re-stamping the same attrs is safe.
        """
        if not attrs:
            return
        assert_attrs_safe(attrs)
        root = self._open_root()
        parts = [p for p in Path(group).parts if p not in ("", "/")]
        target = root if not parts else root.require_group("/".join(parts))
        target.attrs.update(dict(attrs))

    def verify_array_spec(self, group_path: str, spec: Any, expected_time_count: int) -> None:
        root = self._open_root()
        arr = root[group_path]

        def _fail(field: str, existing: Any, expected: Any, hint: str) -> None:
            raise SchemaDriftError(
                f"Schema drift for group_path={group_path!r} field={field!r}: "
                f"existing={existing!r} spec={expected!r}. {hint}"
            )

        existing_dtype = np.dtype(arr.dtype).str
        spec_dtype = np.dtype(spec.dtype).str
        if existing_dtype != spec_dtype:
            _fail(
                "dtype",
                existing_dtype,
                spec_dtype,
                "Align the plugin schema with the existing array.",
            )

        existing_rank = int(getattr(arr, "ndim", len(getattr(arr, "shape", ()))))
        spec_rank = len(spec.shape)
        if existing_rank != spec_rank:
            _fail("rank", existing_rank, spec_rank, "Rank must match the declared array spec.")

        existing_shape_tail = tuple(arr.shape[1:])
        spec_shape_tail = tuple(spec.shape[1:])
        if existing_shape_tail != spec_shape_tail:
            _fail(
                "shape[1:]",
                existing_shape_tail,
                spec_shape_tail,
                "Non-time dimensions must match exactly.",
            )

        if getattr(spec, "time_indexed", True):
            existing_time_count = int(arr.shape[0]) if arr.ndim else 0
            if existing_time_count < expected_time_count:
                _fail(
                    "shape[0]",
                    existing_time_count,
                    expected_time_count,
                    "Existing arrays smaller than the global expected time count must be recreated.",
                )
            if existing_time_count > expected_time_count:
                log.warning(
                    "Schema drift tolerated for group_path=%s field=shape[0]: existing=%s spec=%s; over-allocation is benign.",
                    group_path,
                    existing_time_count,
                    expected_time_count,
                )

        if spec.chunks is not None:
            existing_chunks = tuple(arr.chunks) if arr.chunks is not None else None
            if existing_chunks != tuple(spec.chunks):
                _fail(
                    "chunks",
                    existing_chunks,
                    tuple(spec.chunks),
                    "Recreate the array with matching chunking.",
                )

        if spec.fill_value is not None:
            existing_fill_value = arr.fill_value
            spec_fill_value = spec.fill_value
            if not _fill_values_equal(existing_fill_value, spec_fill_value):
                _fail(
                    "fill_value",
                    existing_fill_value,
                    spec_fill_value,
                    "Use the same fill value when declaring the schema.",
                )

        existing_shards = getattr(arr, "shards", None)
        if spec.shards is None and existing_shards is not None:
            _fail(
                "shards",
                tuple(existing_shards),
                None,
                "Existing on-disk array is sharded but the declared spec has shards=None. "
                "Concurrent region writes greater than 1 require both the declared spec and "
                "the opened array to be unsharded. Recreate the array with matching shards, "
                "or set zarr_region_write_concurrency=1 to use serial writes.",
            )

        if spec.shards is not None and existing_shards is not None:
            existing_shards_tuple = tuple(existing_shards)
            if existing_shards_tuple != tuple(spec.shards):
                _fail(
                    "shards",
                    existing_shards_tuple,
                    tuple(spec.shards),
                    "Recreate the array with matching shards.",
                )

        if spec.attrs is not None:
            assert_attrs_safe(spec.attrs)
            for k, v in spec.attrs.items():
                existing_v = arr.attrs.get(k)
                if existing_v != v:
                    _fail(
                        f"attrs[{k!r}]",
                        existing_v,
                        v,
                        "Use the same attrs when declaring the schema.",
                    )

        if spec.dimension_names is not None:
            existing_dimnames = getattr(getattr(arr, "metadata", None), "dimension_names", None)
            if existing_dimnames is not None and tuple(existing_dimnames) != tuple(
                spec.dimension_names
            ):
                _fail(
                    "dimension_names",
                    tuple(existing_dimnames),
                    tuple(spec.dimension_names),
                    "Re-ingest from scratch; no in-place dimension rename is supported.",
                )

        spec_filters = getattr(spec, "filters", None)
        spec_serializer = getattr(spec, "serializer", None)
        spec_compressors = getattr(spec, "compressors", None)

        if spec_filters is not None or spec_serializer is not None or spec_compressors is not None:
            from firecube.core.zarr.codec_pipeline import CodecPipeline, compare_pipelines

            declared = CodecPipeline(
                filters=spec_filters,
                serializer=spec_serializer,
                compressors=spec_compressors,
            )
            on_disk_codecs = getattr(getattr(arr, "metadata", None), "codecs", ())
            mismatches = compare_pipelines(declared, on_disk_codecs)
            for field_name, declared_val, on_disk_val in mismatches:
                _fail(
                    field_name,
                    on_disk_val,
                    declared_val,
                    "Re-ingest from scratch; no in-place codec migration is supported.",
                )

    def ensure_timestamp_slot(self, group: str, ts_index: int) -> None:
        """Validate that all timestamped arrays in a group include *ts_index*.

        Arrays whose names appear in ``coord_names`` (constructor param) or
        that are scalar are skipped.

        Args:
            group: Data group path (e.g. ``data_1km``).
            ts_index: Zero-based timestamp index that must exist.
        """
        grp = self._open_root()[group]
        for name in list(grp.array_keys()):
            if name in self._coord_names:
                continue
            arr = grp[name]
            if arr.ndim == 0:
                continue
            if arr.shape[0] > ts_index:
                continue
            _raise_time_capacity_error(group, name, length=int(arr.shape[0]), ts_index=ts_index)

    def write_region(
        self,
        group: str,
        array_name: str,
        ts_index: int,
        y_slice: slice,
        data: np.ndarray,
        *,
        channel_index: int | None = None,
    ) -> None:
        """Write one spatial region for one timestamp.

        Args:
            group: Data group path.
            array_name: Target array name inside *group*.
            ts_index: Timestamp slot index.
            y_slice: Spatial y-range to write.
            data: Input data with matching shape.
            channel_index: Optional channel index for 4-D arrays.

        Raises:
            ValueError: If target array rank is unsupported.
        """
        root = self._root if self._root is not None else self._open_root()
        arr = root[f"{group}/{array_name}"]
        if arr.ndim == 4:
            if channel_index is None:
                arr[ts_index, y_slice, :, :] = data
            else:
                arr[ts_index, y_slice, :, channel_index] = data
            return
        if arr.ndim == 3:
            arr[ts_index, y_slice, :] = data
            return
        raise ValueError(f"Unsupported array rank for write_region: {arr.ndim}")

    def write_1d(
        self,
        group: str,
        array_name: str,
        ts_index: int,
        data: np.ndarray,
    ) -> None:
        """Write exactly one timestamp slot.

        For 1-D target arrays, the slot is the single element at ``ts_index``
        and ``data`` must contain exactly one value (0-D scalar or 1-element
        ndarray). For higher-rank target arrays, the slot is ``arr[ts_index]``
        and ``data`` must match ``arr.shape[1:]``.

        Uses the one-element slice-assign idiom ``arr[i:i+1] = payload.reshape(1)``
        for 1-D targets: under ``numpy>=2`` (pinned in ``pyproject.toml``),
        ``arr[i] = one_element_ndarray`` raises ``ValueError`` for typed dtypes
        (misleadingly as "Could not convert object to NumPy datetime" for
        datetime64). Slice-assign is numpy 1.x AND 2.x safe. Writing multi-element
        payloads to 1-D targets is REJECTED — the control-plane records writes
        at ``(group, ts_index)`` granularity only (see DESIGN.md §"Risks To
        Avoid"), so multi-slot payloads would create silent data-integrity gaps.

        Args:
            group: Data group path.
            array_name: Target array name.
            ts_index: Timestamp slot index.
            data: One value to write at the slot. Shape/rank constraints
                depend on the target array's rank (see contract above).

        Raises:
            ValueError: If a 1-D target receives a payload with ``size != 1``,
                or if a higher-rank target receives a payload whose shape does
                not match ``arr.shape[1:]``.
        """
        arr = self._open_root()[f"{group}/{array_name}"]
        payload = np.asarray(data)

        if arr.ndim == 1:
            if payload.ndim == 0:
                arr[ts_index : ts_index + 1] = payload.reshape(1)
                return
            if payload.size != 1:
                raise ValueError(
                    f"write_1d writes exactly one slot for 1-D targets; got "
                    f"payload with size={payload.size} for "
                    f"{group}/{array_name}"
                )
            arr[ts_index : ts_index + 1] = payload.reshape(1)
            return

        if payload.shape != tuple(arr.shape[1:]):
            raise ValueError(
                f"write_1d payload shape {payload.shape} does not match "
                f"target arr.shape[1:]={tuple(arr.shape[1:])} for "
                f"{group}/{array_name}"
            )
        arr[ts_index] = payload

    def resolve_timestamp_index(self, group: str, timestamp_val: Any) -> int:
        """Return the existing or next compact timestamp index for *group*.

        If a matching timestamp already exists in the ``timestamp`` array,
        its index is returned (idempotent).  Otherwise the next available
        slot (== current length) is returned.
        """
        root = self._open_root()
        self.ensure_group(group)
        path = f"{group}/{self._time_coord_name}"
        normalized = self._normalize_timestamp_value(timestamp_val)

        if path not in root:
            return 0

        timestamp_arr = root[path]
        if timestamp_arr.shape[0] == 0:
            return 0

        existing = np.asarray(timestamp_arr[:])
        matches = np.nonzero(existing == normalized)[0]
        if matches.size:
            return int(matches[0])
        return int(timestamp_arr.shape[0])

    def write_timestamp(
        self,
        group: str,
        ts_index: int,
        timestamp_val: Any,
    ) -> None:
        """Ensure and write the timestamp coordinate for one slot."""
        normalized = self._normalize_timestamp_value(timestamp_val)
        timestamp_arr = self.ensure_group(
            f"{group}/{self._time_coord_name}",
            shape=(ts_index + 1,),
            dtype=normalized.dtype,
            fill_value=np.datetime64("NaT", "s"),
            chunks=(max(1, min(256, ts_index + 1)),),
            allow_grow=True,
            dimension_names=(self._time_coord_name,),
        )
        self.ensure_group(group)
        self.ensure_timestamp_slot(group, ts_index)
        if hasattr(timestamp_arr, "shape"):
            if timestamp_arr.shape[0] <= ts_index:
                _raise_time_capacity_error(
                    group,
                    self._time_coord_name,
                    length=int(timestamp_arr.shape[0]),
                    ts_index=ts_index,
                )
            timestamp_arr[ts_index] = normalized

    def write_static(
        self,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None:
        """Write a static (non-time-indexed) array at its declared shape.

        The array must already exist (created by schema setup).
        This method performs a full overwrite: ``arr[...] = data``.
        It does NOT call ``ensure_timestamp_slot`` — static arrays have no time axis.

        Args:
            group: Data group path.
            array_name: Target array name.
            data: Data to assign. Must have the same ndim and shape as the
                preallocated array.

        Raises:
            KeyError: If the array does not exist in the store.
            ValueError: If ``data.ndim`` or ``data.shape`` does not match the
                stored array.
        """
        root = self._open_root()
        arr_path = f"{group}/{array_name}"
        if arr_path not in root:
            raise KeyError(
                f"Array {arr_path!r} does not exist in store. Create it via schema setup first."
            )
        arr = root[arr_path]
        if arr.ndim != data.ndim:
            raise ValueError(
                f"Array {arr_path!r} ndim mismatch: stored={arr.ndim}, data={data.ndim}"
            )
        if arr.shape != data.shape:
            raise ValueError(
                f"Array {arr_path!r} shape mismatch: stored={arr.shape}, data={data.shape}"
            )
        arr[...] = data
