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
coordinate axes and should be skipped by `ensure_timestamp_slot`.
"""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import logging
import math
import operator
import struct
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable

import numpy as np
import zarr
from zarr.abc.store import Store
from zarr.errors import GroupNotFoundError, NodeNotFoundError
from zarr.storage import FsspecStore, LocalStore

from firecube.core.errors import Message, Placeholder, SchemaDriftError
from firecube.core.time.calendar import is_calendar_valued
from firecube.core.time.gregorian import gregorian_to_datetime64_ns
from firecube.core.time.guard import reject_non_gregorian_calendar_value
from firecube.core.uris import is_remote_target, local_path_from_target
from firecube.core.zarr._coord_lifecycle import (
    CoordLifecycleState,
    raise_if_invalid,
    resolve_coord_lifecycle,
)
from firecube.core.zarr._fill import _array_is_all_fill, _fill_value_is_missing
from firecube.core.zarr._reserved_attrs import _FILL_VALUE_ATTR, assert_attrs_safe
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED
from firecube.core.zarr.guard._unit_locks import _store_identity
from firecube.core.zarr.guard.chunk_geometry import Bounds
from firecube.core.zarr.guard.engine_owned import preallocate_operation
from firecube.core.zarr.guard.guarded_write import (
    ArrayHold,
    WriteFootprint,
    WriteResult,
    current_hold,
    guarded_write,
    hold_array,
)
from firecube.core.zarr.guard.metadata_authority import (
    MetadataAuthority,
    MetadataScope,
    create_or_open_array,
    set_attributes,
)
from firecube.core.zarr.guard.metadata_authority import ensure_group as _authorized_ensure_group
from firecube.core.zarr.guard.unit_ownership import _axis_index

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


def _fill_values_equal(existing: Any, spec: Any) -> bool:
    # Missing-value fills (NaN, NaT) are never == themselves, so compare them by
    # "both missing" rather than by value; otherwise fall back to plain equality.
    existing_missing = _fill_value_is_missing(existing)
    spec_missing = _fill_value_is_missing(spec)
    if existing_missing or spec_missing:
        return existing_missing and spec_missing
    return bool(existing == spec)


def _fill_value_attr_value(fill_value: Any, dtype: Any) -> Any:
    """Return a JSON-safe ``_FillValue`` attr encoding, or ``None``.

    Follows xarray's Zarr v3 attribute convention: for float dtypes the fill
    is base64-encoded little-endian IEEE-754 double — the only form
    ``xr.open_zarr`` decodes for kind ``f``; a bare number there raises
    ``TypeError`` at open time. Bool, int, and string fills pass through as
    plain JSON scalars.

    Args:
        fill_value: The fill value declared in ``ZarrArraySpec.fill_value``.
        dtype: The array's dtype; selects the attribute encoding.

    Returns:
        The encoded attr value, or ``None`` if the value cannot be
        represented safely (NaT, NaN, datetime objects, etc.), in which case
        the attribute is not stamped.
    """
    if isinstance(fill_value, np.generic):
        fill_value = fill_value.item()
    if np.dtype(dtype).kind == "f":
        if isinstance(fill_value, bool | int | float) and math.isfinite(fill_value):
            packed = struct.pack("<d", float(fill_value))
            return base64.standard_b64encode(packed).decode("ascii")
        return None
    if isinstance(fill_value, bool | int | str):
        return fill_value
    if isinstance(fill_value, float) and math.isfinite(fill_value):
        return fill_value
    return None


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
    return open_root_read_only(writer._store_uri, store=writer._store)


def open_root_read_only(
    store_uri: str,
    *,
    store: object | None = None,
    storage_options: Mapping[str, Any] | None = None,
) -> Any | None:
    """Open a store's root group read-only; ``None`` when the store has no root group.

    Any failure other than a missing store or root group propagates.

    Args:
        store_uri: Store URI or local path, used when ``store`` is not given.
        store: A pre-built zarr store (for example from a storage session), or a
            store URI that ``storage_options`` belongs to.
        storage_options: Filesystem options for a ``store`` given as a URI.
    """
    try:
        if store is not None:
            if storage_options is not None:
                return zarr.open_group(
                    store=store,
                    storage_options=dict(storage_options),
                    mode="r",
                    zarr_format=3,
                    use_consolidated=False,
                )
            return zarr.open_group(store=store, mode="r", zarr_format=3, use_consolidated=False)
        if not is_remote_target(store_uri):
            local = LocalStore(str(local_path_from_target(store_uri)))
            return zarr.open_group(store=local, mode="r", zarr_format=3, use_consolidated=False)
        return zarr.open_group(store=store_uri, mode="r", zarr_format=3, use_consolidated=False)
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
    ) -> Any:
        """Ensure a group or array path exists, creating it when absent.

        A single-segment *group* with no *shape* resolves to a group. Any
        other path resolves to an array, and *shape* and *dtype* are then
        required. Existing arrays are returned as-is unless *allow_grow* is
        set, in which case they are resized up to *shape*.

        Args:
            group: Group path or array path (e.g. ``data_1km/counts``).
            shape: Array shape when creating array paths.
            dtype: Array dtype when creating array paths.
            fill_value: Fill value for newly created arrays.
            chunks: Chunk shape for newly created arrays.
            allow_grow: If ``True``, grow an existing array to at least
                *shape* instead of leaving it undersized.
            shards: Optional Zarr v3 shard shape for newly created arrays.
            attrs: Optional user attributes written at array creation.
                Reserved keys (see `RESERVED_ARRAY_ATTRS`) are rejected.
                This is creation-only: attribute drift on existing arrays is
                reported by `verify_array_spec`, not here.
            dimension_names: Optional Zarr v3 ``dimension_names`` for newly
                created arrays.
            filters: Optional Zarr v3 filter chain for newly created arrays.
            serializer: Optional Zarr v3 serializer for newly created arrays.
            compressors: Optional Zarr v3 compressor chain for newly created
                arrays.

        Returns:
            The existing or newly created group or array.

        Raises:
            ValueError: If array creation is requested without *shape* and
                *dtype*, or if *attrs* contains reserved keys.
            SchemaDriftError: If *dimension_names* differs from an existing
                array's ``dimension_names`` on resume. Firecube performs no
                in-place dimension renames.
        """
        ...

    def ensure_timestamp_slot(self, group: str, ts_index: int) -> None:
        """Ensure every timestamped array in *group* can hold *ts_index*.

        Coordinate arrays and scalar arrays are skipped; only arrays carrying
        a leading time axis are checked or grown.

        Args:
            group: Data group path (e.g. ``data_1km``).
            ts_index: Zero-based timestamp index that must be addressable.
        """
        ...

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
        """Write one region into one timestamp slot: ``arr[ts_index, y_slice, ...]``.

        *y_slice* selects along the first axis after time; later axes are
        written in full. Arrays of rank >= 3 are supported; a rank-4 array may
        instead select one channel with *channel_index*.

        Args:
            group: Data group path.
            array_name: Target array name inside *group*.
            ts_index: Timestamp slot index to write into.
            y_slice: Range along the first axis after time covered by *data*.
            data: Region payload; its shape must match the addressed window.
            channel_index: Channel index for rank-4 arrays only; ``None`` otherwise.

        Raises:
            ValueError: If the target array rank is unsupported or *data*
                does not match the addressed window.
        """
        ...

    def write_1d(
        self,
        group: str,
        array_name: str,
        ts_index: int,
        data: np.ndarray,
    ) -> None:
        """Write exactly one timestamp slot.

        For a 1-D target the slot is the single element at *ts_index* and
        *data* must hold exactly one value. For a higher-rank target the slot
        is ``arr[ts_index]`` and *data* must match ``arr.shape[1:]``.
        Multi-slot payloads are rejected: the control plane records writes at
        ``(group, ts_index)`` granularity, so a wider write would leave an
        unrecorded data-integrity gap.

        Args:
            group: Data group path.
            array_name: Target array name.
            ts_index: Timestamp slot index.
            data: The single value to write at the slot.

        Raises:
            ValueError: If a 1-D target receives a payload with ``size != 1``,
                or a higher-rank target receives a payload whose shape does
                not match ``arr.shape[1:]``.
        """
        ...

    def resolve_timestamp_index(self, group: str, timestamp_val: Any) -> int:
        """Return the slot index *timestamp_val* occupies in *group*.

        Resolution is idempotent: a timestamp already present in the group's
        time coordinate resolves to its existing index, so a replayed write
        lands on the same slot. An unseen timestamp resolves to the next
        available slot, which equals the current coordinate length.

        Args:
            group: Data group path.
            timestamp_val: Timestamp to place, in any form the implementation
                normalizes to the group's time dtype.

        Returns:
            The zero-based slot index for *timestamp_val*.
        """
        ...

    def write_timestamp(
        self,
        group: str,
        ts_index: int,
        timestamp_val: Any,
    ) -> None:
        """Write the time coordinate value for one slot.

        Grows the time coordinate and every timestamped array in *group* as
        needed so *ts_index* is addressable before the value is stored.

        Args:
            group: Data group path.
            ts_index: Timestamp slot index to stamp.
            timestamp_val: Timestamp to write at that slot.
        """
        ...

    def write_static(
        self,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None:
        """Write a static (non-time-indexed) array at its declared shape.

        The array must already exist from schema setup. The write is a full
        overwrite of the array contents and touches no time axis.

        Args:
            group: Data group path.
            array_name: Target array name.
            data: Payload with the same ndim and shape as the stored array.

        Raises:
            KeyError: If the array does not exist in the store.
            ValueError: If ``data.ndim`` or ``data.shape`` does not match the
                stored array.
        """
        ...


class RegionZarrWriter:
    """Write arrays directly to a Zarr store with lazy open semantics.

    Args:
        store_uri: Target store URI/path (local path, ``file://`` URI, or
            ``s3://`` URI).
        coord_names: Array names treated as coordinate axes.  These are
            skipped by `ensure_timestamp_slot` when resizing timestamped
            arrays.  Defaults to ``{"y", "x", "channel"}``.
        write_empty_chunks: Applied to each array this writer writes a
            payload to, never to ``zarr.config``. ``None`` keeps the setting
            the array was opened with.
        on_write: Called on the writing thread with the `WriteResult` of
            every payload write, while the locks that write took (its stored
            units, or a whole-array hold) are still held, so the results of
            writes to one unit arrive in the order of the writes. It must not
            take a stored-unit lock or wait for another writer.
        track_units: Report in each `WriteResult` which touched units hold no
            object after the write and which the write deleted (two
            ``exists`` calls per unit). Requires ``on_write``.
        authority: The authority every ``zarr.json`` change of this writer is
            made under (`firecube.core.zarr.guard.metadata_authority`), the root
            group's creation included. ``None`` means a ``PROCESS_LOCAL``
            authority over the writer's whole store: the threads of this
            process are excluded per node, other processes are not. Only an
            authority over the whole store can create a missing root group.
        fixed_extent: Make `write_region` write through one handle per array,
            read once, so its writes read no metadata. Set it only when no
            array this writer writes changes shape while the writer is alive.
            This writer's own metadata changes still drop the handles. Growth
            by another writer goes unseen, and a write may then erase cells
            that writer wrote. `write_1d` and the other methods ignore it.

    Raises:
        ValueError: ``track_units`` is set without ``on_write``.
    """

    def __init__(
        self,
        store_uri: str,
        *,
        store: object | None = None,
        coord_names: frozenset[str] = _DEFAULT_COORD_NAMES,
        time_coord_name: str = "timestamp",
        write_empty_chunks: bool | None = None,
        on_write: Callable[[WriteResult], None] | None = None,
        track_units: bool = False,
        authority: MetadataAuthority | None = None,
        fixed_extent: bool = False,
    ) -> None:
        if track_units and on_write is None:
            raise ValueError("track_units=True reports to on_write; pass on_write as well")
        self._store_uri = store_uri
        self._store = store  # pre-built store (e.g., zarr.storage.ObjectStore for obstore)
        self._coord_names = coord_names
        self._time_coord_name = time_coord_name
        self._write_empty_chunks = write_empty_chunks
        self._on_write = on_write
        self._track_units = track_units
        self._root: Any | None = None
        # Built on first use, once: the store and the default authority bound to it.
        self._bind_lock = threading.Lock()
        self._backing: Store | None = None
        self._identity: str | None = None
        self._authority = authority
        self._fixed_extent = fixed_extent
        # One handle per array path for its rank, non-time axes and layout;
        # dropped by this writer's own metadata changes.
        self._handles_lock = threading.Lock()
        self._handles: dict[str, Any] = {}

    def _cached_array(self, array_path: str) -> Any:
        """Return this writer's handle of ``array_path``, opening it on first use.

        Never read the time-axis length from it unless ``fixed_extent`` is
        set: another writer may have grown the array since it was opened.
        """
        with self._handles_lock:
            arr = self._handles.get(array_path)
            if arr is None:
                arr = self._handles[array_path] = self._opened_root()[array_path]
            return arr

    def _opened_root(self) -> Any:
        """Return the memoized root, opening it only when no caller has.

        Concurrent region writers open the root before they fan out, so their
        worker threads read it here and never initialize it.
        """
        return self._root if self._root is not None else self._open_root()

    @contextlib.contextmanager
    def _dropping_handles(self) -> Iterator[None]:
        """Drop the cached handles when the block returns or raises."""
        try:
            yield
        finally:
            with self._handles_lock:
                self._handles.clear()

    def _reopen_at(self, array_path: str, slot: int) -> Callable[[], Any]:
        """Return a reader of ``array_path`` that refuses a handle without ``slot`` on axis 0."""

        def reopen() -> Any:
            arr = self._opened_root()[array_path]
            _axis_index(slot, int(arr.shape[0]))
            return arr

        return reopen

    def _slot_before_locks(self, array_path: str, ts_index: int) -> int:
        """Return ``ts_index`` as given, or normalised against a fresh read when negative."""
        index = operator.index(ts_index)
        if index >= 0:
            return index
        return _axis_index(index, int(self._opened_root()[array_path].shape[0]))

    def _backing_store(self) -> Store:
        """The store this writer reads and writes, built once.

        Raises:
            TypeError: ``store`` is neither a zarr ``Store`` nor a URI string.
        """
        with self._bind_lock:
            if self._backing is None:
                target = self._store if self._store is not None else self._store_uri
                if isinstance(target, Store):
                    self._backing = target
                elif not isinstance(target, str):
                    raise TypeError(
                        f"store must be a zarr Store or a URI string, got {type(target).__name__}"
                    )
                elif is_remote_target(target):
                    self._backing = FsspecStore.from_url(target, read_only=False)
                else:
                    self._backing = LocalStore(str(local_path_from_target(target)))
            return self._backing

    def _store_key(self) -> str:
        """Return `_store_identity` of this writer's store, resolved once like the store."""
        if self._identity is None:
            self._identity = _store_identity(self._store_uri, self._store)
        return self._identity

    def metadata_scope(self, prefix: str = "") -> MetadataScope:
        """The scope of an authority over *prefix* of this writer's store, bound to that store."""
        return MetadataScope(self._store_key(), prefix, backing=self._backing_store())

    @property
    def authority(self) -> MetadataAuthority:
        """The authority this writer changes ``zarr.json`` under when a call names none.

        The one given at construction, else a ``PROCESS_LOCAL`` authority over
        the whole store (scope prefix ``""``), so it may create the root group.
        """
        if self._authority is None:
            scope = self.metadata_scope()
            with self._bind_lock:
                if self._authority is None:
                    self._authority = MetadataAuthority.process_local(scope)
        return self._authority

    def _open_root(self) -> Any:
        """Open and memoize the root group; a missing one is created under `authority`."""
        return self._open_root_under(None)

    def _open_root_under(self, authority: MetadataAuthority | None) -> Any:
        """Open and memoize the root group; a missing one is created under *authority*.

        Opening writes no metadata: a store without a root ``zarr.json`` gets
        one only through `metadata_authority.ensure_group`, under *authority*
        (the writer's `authority` when ``None``), which refuses a released,
        foreign or group-scoped authority before it touches the store.

        Raises:
            MetadataAuthorityError: The root is missing and the authority may
                not create it.
        """
        if self._root is not None:
            return self._root
        store = self._backing_store()
        try:
            root = zarr.open_group(store=store, mode="r+", zarr_format=3, use_consolidated=False)
        except (FileNotFoundError, GroupNotFoundError, NodeNotFoundError):
            auth = authority if authority is not None else self.authority
            root = _authorized_ensure_group(auth, store, "")
        self._root = root
        return root

    @staticmethod
    def _normalize_timestamp_value(timestamp_val: Any) -> np.datetime64:
        """Normalize timestamp-like values to ``datetime64[s]``.

        Raises:
            ValueError: *timestamp_val* is calendar-valued on a non-Gregorian calendar.
        """
        reject_non_gregorian_calendar_value(timestamp_val)
        if isinstance(timestamp_val, np.datetime64):
            return timestamp_val.astype("datetime64[s]")
        if hasattr(timestamp_val, "isoformat"):
            return np.datetime64(str(timestamp_val.isoformat()), "s")
        return np.datetime64(str(timestamp_val), "s")

    @staticmethod
    def _normalize_timestamp_value_ns(timestamp_val: Any) -> np.datetime64:
        """Normalize timestamp-like values to ``datetime64[ns]`` for equality checks.

        Raises:
            ValueError: *timestamp_val* is calendar-valued on a non-Gregorian calendar.
            Datetime64RangeError: *timestamp_val* lies outside the ``datetime64[ns]`` range.
        """
        reject_non_gregorian_calendar_value(timestamp_val)
        return gregorian_to_datetime64_ns(timestamp_val)

    @staticmethod
    def _normalize_for_coord_compare(
        value: Any,
        target_dtype: np.dtype[Any],
    ) -> np.datetime64:
        """Normalize a datetime-like value to *target_dtype* for coord equality.

        Used by both the observed-coord materializer and the pod-write
        ``COORD_MANAGED`` verify-first branch. Comparing an in-memory
        ``datetime64[ns]`` candidate against a stored ``datetime64[s]`` slot
        used to raise a spurious ``SchemaDriftError`` because the incoming
        side kept sub-second precision the stored side had already truncated
        away. Truncating both sides to the on-disk resolution before the
        equality check removes that false drift.

        Callers must apply this helper to *both* the stored and the
        incoming value and then compare them with ``==``. NaT semantics
        are handled here: NaT-in returns NaT-in-target-dtype, so callers
        can distinguish an empty slot with ``np.isnat`` on the result.

        Args:
            value: A ``datetime64`` scalar/0-d array, a Python ``datetime``,
                a Zarr array element, or any input ``np.array`` accepts as a
                ``datetime64`` scalar.
            target_dtype: The on-disk array dtype. Must be a ``datetime64``
                dtype; other kinds are rejected because this helper is
                intentionally scoped to time coords.

        Returns:
            A 0-d ``np.datetime64`` in *target_dtype*.

        Raises:
            TypeError: If *target_dtype* is not a ``datetime64`` dtype.
            OverflowError / ValueError: Whatever numpy raises when *value*
                cannot be represented in *target_dtype* (e.g. a year outside
                the ``datetime64[s]`` range). Callers must not silence
                these — they indicate the incoming coord genuinely does
                not fit the schema and would otherwise be silently
                truncated to a wrong value.
        """
        if target_dtype.kind != "M":
            raise TypeError(
                "_normalize_for_coord_compare requires a datetime64 target dtype; "
                f"got {target_dtype!r}"
            )
        if np.datetime_data(target_dtype) == ("ns", 1) and (
            isinstance(value, str | dt.datetime | np.datetime64) or is_calendar_valued(value)
        ):
            # NumPy wraps a value outside the ns range; this conversion refuses it.
            return gregorian_to_datetime64_ns(value)
        arr = np.asarray(value)
        if arr.dtype.kind == "M" and bool(np.isnat(arr)):
            return cast(np.datetime64, np.array(np.datetime64("NaT", "ns"), dtype=target_dtype)[()])
        return cast(np.datetime64, np.array(value, dtype=target_dtype)[()])

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
        on_create: Callable[[Any], None] | None = None,
        engine_attrs: Mapping[str, Any] | None = None,
        authority: MetadataAuthority | None = None,
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
                Reserved keys (see `RESERVED_ARRAY_ATTRS`) are rejected.
                ``ensure_group`` is creation-only — drift detection for the
                attrs of existing arrays is handled by `verify_array_spec`.
            dimension_names: Optional Zarr v3 ``dimension_names`` for newly
                created arrays. On resume, a mismatch against an existing
                array raises `SchemaDriftError`; firecube does not
                perform in-place dimension renames.
            on_create: Optional callback invoked with the array only when this
                call creates it; never invoked for an existing array.
            engine_attrs: Engine-supplied attributes, reserved keys allowed,
                written in the same metadata write that creates the array
                (unlike *on_create*, which runs after it); ignored for an
                existing array. Never pass plugin-supplied attributes here.
            authority: The authority to create, grow and stamp under; the
                writer's `authority` when ``None``. The node is read from the
                store under it, never through an older handle; a new array
                gets its attributes and ``_FillValue`` in the one write that
                creates it. A missing root group is created under it too, so
                only an authority over the whole store may meet a fresh store.

        Returns:
            Existing or newly created group/array; an array handle is freshly
            read, for payload I/O.

        Raises:
            ValueError: If array creation is requested without *shape*/*dtype*,
                or if *attrs* contains reserved keys.
            SchemaDriftError: If *dimension_names* differs from the existing
                array's ``dimension_names`` on resume.
            MetadataAuthorityError: The authority is released, names another
                store, or does not cover *group*.
        """
        with self._dropping_handles():
            if attrs is not None:
                assert_attrs_safe(attrs)

            auth = authority if authority is not None else self.authority
            root = self._open_root_under(auth)
            parts = [p for p in Path(group).parts if p not in ("", "/")]
            if not parts:
                return _authorized_ensure_group(auth, root, "")

            if len(parts) == 1 and shape is None:
                return _authorized_ensure_group(auth, root, parts[0])

            if shape is None or dtype is None:
                raise ValueError("shape and dtype are required when ensuring an array path")

            def _check_dimension_names(arr: Any) -> None:
                if dimension_names is None:
                    return
                existing_dimnames = getattr(getattr(arr, "metadata", None), "dimension_names", None)
                if existing_dimnames is not None and tuple(existing_dimnames) != tuple(
                    dimension_names
                ):
                    raise SchemaDriftError(
                        f"Schema drift for {group!r} field='dimension_names': "
                        f"existing={tuple(existing_dimnames)!r} spec={tuple(dimension_names)!r}. "
                        "Re-ingest from scratch; no in-place migration is provided."
                    )

            kwargs: dict[str, Any] = {"shape": shape, "dtype": dtype, "fill_value": fill_value}
            if chunks is not None:
                kwargs["chunks"] = chunks
            if shards is not None:
                kwargs["shards"] = shards
            if attrs is not None or engine_attrs is not None:
                kwargs["attributes"] = {**(attrs or {}), **(engine_attrs or {})}
            if dimension_names is not None:
                kwargs["dimension_names"] = list(dimension_names)
            if filters is not None:
                kwargs["filters"] = list(filters)
            if serializer is not None:
                kwargs["serializer"] = serializer
            if compressors is not None:
                kwargs["compressors"] = list(compressors)
            _fv_attr = _fill_value_attr_value(fill_value, dtype)
            arr, created = create_or_open_array(
                auth,
                root,
                "/".join(parts),
                create_kwargs=kwargs,
                grow_to=tuple(shape) if allow_grow else None,
                default_attrs={_FILL_VALUE_ATTR: _fv_attr} if _fv_attr is not None else None,
                validate=_check_dimension_names,
            )
            if created and on_create is not None:
                on_create(arr)
            return arr

    def set_group_attrs(
        self,
        group: str,
        attrs: Mapping[str, Any] | None,
        *,
        authority: MetadataAuthority | None = None,
    ) -> None:
        """Stamp convention-agnostic group-level attributes onto the group metadata.

        Writes ``attrs`` verbatim onto the Zarr group's ``zarr.json``; the writer
        does not interpret the mapping. Reserved firecube-internal attribute names
        are rejected (see `assert_attrs_safe`). A falsy ``attrs`` is a no-op.
        Idempotent — re-stamping the same attrs is safe. The group is created
        when missing; both changes are made under *authority* (the writer's
        `authority` when ``None``), reading the group from the store first.
        """
        with self._dropping_handles():
            if not attrs:
                return
            assert_attrs_safe(attrs)
            auth = authority if authority is not None else self.authority
            root = self._open_root_under(auth)
            path = "/".join(p for p in Path(group).parts if p not in ("", "/"))
            _authorized_ensure_group(auth, root, path)
            written = set_attributes(auth, root, path, attrs)
            if not path:
                self._root = written  # the memoized root carries what was just written

    def update_array_attrs(
        self,
        array_path: str,
        attrs: Mapping[str, Any],
        *,
        authority: MetadataAuthority | None = None,
    ) -> Any:
        """Add or replace attributes of an existing node, engine markers included, in one write.

        The node is read from the store under *authority* (the writer's
        `authority` when ``None``), so a handle opened earlier is never
        written and a newer shape or marker is never reverted. Reserved names
        are allowed: never pass plugin-supplied attributes here.

        Args:
            array_path: Path of the array (or group) below the root.
            attrs: The attributes to add or replace.
            authority: The authority to change the node under.

        Returns:
            The node as written; read attributes back from it, not from an
            older handle.

        Raises:
            MetadataAuthorityError: The authority is released, names another
                store, or does not cover *array_path*.
            zarr.errors.GroupNotFoundError: No node at *array_path*.
        """
        with self._dropping_handles():
            auth = authority if authority is not None else self.authority
            return set_attributes(auth, self._open_root_under(auth), array_path, attrs)

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
        """Write one region for one timestamp: ``arr[ts_index, y_slice, ...]``.

        *y_slice* selects along the first axis after time and every later
        axis is written in full, so arrays of any rank >= 3 are supported
        (e.g. ``(time, member, period, level, y, x)`` one member at a time).
        Rank-4 arrays keep the optional single-channel form.

        Rank, spatial axes and layout come from this writer's cached handle.
        The write goes through the array read again under its unit locks,
        which must hold the slot. A negative *ts_index* counts from the end
        of the array as read before the locks. With ``fixed_extent`` the
        write goes through the cached handle, which must hold the slot.

        Args:
            group: Data group path.
            array_name: Target array name inside *group*.
            ts_index: Timestamp slot index.
            y_slice: Range along the first axis after time.
            data: Input data with matching shape.
            channel_index: Optional channel index, rank-4 arrays only.

        Raises:
            ValueError: If the target array has rank < 3, or *channel_index*
                is given for an array of rank > 4.
            IndexError: The slot lies outside the array's time axis.
        """
        array_path = f"{group}/{array_name}"
        arr = self._cached_array(array_path)
        if arr.ndim < 3:
            raise ValueError(f"Unsupported array rank for write_region: {arr.ndim}")
        if channel_index is not None and arr.ndim > 4:
            raise ValueError(
                f"channel_index is only supported for rank-4 arrays; "
                f"{array_path} has rank {arr.ndim}"
            )
        y_len = int(arr.shape[1])
        # Any other row selector: lock every row (never fewer than written).
        start, stop, step = y_slice.indices(y_len) if isinstance(y_slice, slice) else (0, y_len, 1)
        rows = (start, max(start, stop)) if step > 0 else (max(stop + 1, 0), start + 1)
        # Integer selections normalised as zarr does (-1 is the last), so two
        # spellings of one cell take one lock.
        slot = (
            _axis_index(ts_index, int(arr.shape[0]))
            if self._fixed_extent
            else self._slot_before_locks(array_path, ts_index)
        )
        bounds: list[tuple[int, int]] = [(slot, slot + 1), rows, (0, int(arr.shape[2]))]
        if arr.ndim == 4:
            if channel_index is None:
                bounds.append((0, int(arr.shape[3])))
            else:
                channel = _axis_index(channel_index, int(arr.shape[3]))
                bounds.append((channel, channel + 1))
        elif arr.ndim > 4:
            bounds.extend((0, int(n)) for n in arr.shape[3:])
        # Write the normalised indices the locks were taken for.
        selection: tuple[Any, ...] = (slot, y_slice, slice(None))
        if arr.ndim == 4:
            selection += (slice(None) if channel_index is None else bounds[3][0],)
        elif arr.ndim > 4:
            selection += (Ellipsis,)
        self._write_payload(
            arr,
            array_path,
            tuple(bounds),
            selection,
            data,
            reopen=None if self._fixed_extent else self._reopen_at(array_path, slot),
        )

    def _hold_array(self, array_path: str) -> AbstractContextManager[ArrayHold]:
        """Hold this process's lock on the whole array at ``array_path`` (`hold_array`)."""
        return hold_array(self._store_key(), array_path)

    def _write_payload(
        self,
        arr: Any,
        array_path: str,
        bounds: Bounds,
        selection: Any,
        payload: Any,
        *,
        reopen: Callable[[], Any] | None = None,
    ) -> None:
        """Write ``arr[selection] = payload`` through `guarded_write`, reporting to ``on_write``.

        The locks are taken for ``arr``'s stored layout. With ``reopen`` the
        write goes through the handle it reads under those locks, not ``arr``.
        ``on_write`` is called by `guarded_write` before the locks of the
        write are released, so results for one unit reach it in write order.
        Concurrent batches of one run (``pipeline_workers``) may write slots
        that share a stored chunk or shard; each write reads the object,
        merges and writes it back, so writes touching one unit of ``bounds``
        take it in turn and disjoint units never wait for each other. Inside
        a `hold_array` of the array on this thread the held lock is reused. A
        time coordinate reaching here has no marker, so ``write_timestamp``
        may create or grow it: every write to it takes the whole-array lock.
        """
        store = self._store_key()
        footprint = WriteFootprint(array_path, bounds)
        hold: AbstractContextManager[Any] = contextlib.nullcontext()
        if current_hold(store, footprint.array_path) is None and (
            footprint.array_path.rsplit("/", 1)[-1] == self._time_coord_name
        ):
            hold = hold_array(store, footprint.array_path)
        with hold:
            guarded_write(
                arr,
                footprint,
                selection,
                payload,
                store=store,
                write_empty_chunks=self._write_empty_chunks,
                track_units=self._track_units,
                on_result=self._on_write,
                reopen=reopen,
            )

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

        Rank, non-time axes and layout come from this writer's cached handle.
        The write goes through the array read again under its unit locks,
        which must hold the slot. A negative *ts_index* counts from the end
        of the array as read before the locks.

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
            IndexError: The slot lies outside the array's time axis.
        """
        array_path = f"{group}/{array_name}"
        if self.verify_engine_written_coordinate(group, array_name, data, ts_index=ts_index):
            return
        arr = self._cached_array(array_path)
        payload = np.asarray(data)

        if arr.ndim == 1:
            if payload.ndim != 0 and payload.size != 1:
                raise ValueError(
                    f"write_1d writes exactly one slot for 1-D targets; got "
                    f"payload with size={payload.size} for "
                    f"{group}/{array_name}"
                )
        elif payload.shape != tuple(arr.shape[1:]):
            raise ValueError(
                f"write_1d payload shape {payload.shape} does not match "
                f"target arr.shape[1:]={tuple(arr.shape[1:])} for "
                f"{group}/{array_name}"
            )
        slot = self._slot_before_locks(array_path, ts_index)
        bounds = ((slot, slot + 1), *((0, int(n)) for n in arr.shape[1:]))
        reopen = self._reopen_at(array_path, slot)
        if arr.ndim == 1:
            self._write_payload(
                arr, array_path, bounds, slice(slot, slot + 1), payload.reshape(1), reopen=reopen
            )
        else:
            self._write_payload(arr, array_path, bounds, slot, payload, reopen=reopen)

    def verify_engine_written_coordinate(
        self,
        group: str,
        array_name: str,
        data: Any,
        *,
        ts_index: int | None = None,
    ) -> bool:
        """Verify, instead of writing, a payload aimed at an engine-written time coordinate.

        A time coordinate whose lifecycle is ``PREALLOCATED`` or
        ``COORD_MANAGED`` is written by ``firecube zarr preallocate`` alone;
        pods only verify it, whatever the intent kind (mirrors
        ``write_timestamp``). Compares *data*, cast to the stored dtype as a
        write would cast it, with slot *ts_index* (or the whole array when
        ``None``); NaT equals NaT.

        Returns:
            ``True`` when the target is such a coordinate and the values match
            (the caller writes nothing); ``False`` for any other array, a
            coordinate without a marker included, which the caller writes.

        Raises:
            SchemaDriftError: The stored values differ, or the coordinate
                carries inconsistent markers.
        """
        if array_name != self._time_coord_name:
            return False
        array_path = f"{group}/{array_name}"
        try:
            arr = self._open_root()[array_path]
        except KeyError:
            return False
        state = resolve_coord_lifecycle(dict(arr.attrs))
        raise_if_invalid(state, array_path)
        if state not in (CoordLifecycleState.PREALLOCATED, CoordLifecycleState.COORD_MANAGED):
            return False
        where = "every slot" if ts_index is None else f"slot {ts_index}"
        stored = np.asarray(
            arr[...] if ts_index is None else arr[_axis_index(ts_index, int(arr.shape[0]))]
        )
        try:
            incoming = np.asarray(data() if callable(data) else data).astype(arr.dtype)
            incoming = incoming.reshape(stored.shape)
        except (TypeError, ValueError) as exc:
            raise SchemaDriftError(
                f"time coordinate {array_path!r} is written by preallocate; a write intent "
                f"for {where} cannot be compared with it: {exc}"
            ) from exc
        if np.array_equal(stored, incoming, equal_nan=stored.dtype.kind in "fcmM"):
            return True
        raise SchemaDriftError(
            f"time coordinate {array_path!r} ({where}) diverged: stored {stored}, incoming "
            f"{incoming}. Preallocate writes it; pods only verify it and never overwrite it."
        )

    def resolve_timestamp_index(self, group: str, timestamp_val: Any) -> int:
        """Return the existing or next compact timestamp index for *group*.

        If a matching timestamp already exists in the ``timestamp`` array,
        its index is returned (idempotent).  Otherwise the next available
        slot (== current length) is returned.

        An encoded (calendar) coordinate encodes *timestamp_val* with the
        array's own ``units`` and ``calendar`` before the lookup.
        """
        root = self._open_root()
        self.ensure_group(group)
        path = f"{group}/{self._time_coord_name}"

        if path not in root or root[path].shape[0] == 0:
            # No coordinate to dispatch on: validate as a datetime64 value, so a
            # malformed or non-Gregorian value fails instead of resolving to slot 0.
            self._normalize_timestamp_value(timestamp_val)
            return 0

        timestamp_arr = root[path]
        # Imported here: firecube.core.time.encoding imports firecube.core.zarr.
        from firecube.core.time.encoding import coordinate_encoding_from_array

        encoding = coordinate_encoding_from_array(timestamp_arr.dtype, dict(timestamp_arr.attrs))
        normalized: Any = (
            encoding.encode_scalar(timestamp_val)
            if encoding.extra_attrs
            else self._normalize_timestamp_value(timestamp_val)
        )

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
        """Ensure and write the timestamp coordinate for one slot.

        Routes on the coord array's lifecycle state, resolved from its
        reserved marker attrs by ``resolve_coord_lifecycle``:

        - ``LEGACY`` (no marker, or coord array does not exist yet):
          per-slot create-or-grow write. Unchanged from the pre-marker
          contract.
        - ``PREALLOCATED``: verify-or-error against the dense pre-materialized
          value. An exact ``datetime64[ns]``-normalized replay is a no-op;
          divergence raises ``SchemaDriftError``.
        - ``COORD_MANAGED``: verify-or-error against the engine-materialized
          value. A stored ``NaT`` means the materialization step was skipped
          and raises with the recovery command. Divergence raises
          ``SchemaDriftError``.
        - ``INVALID_BOTH_MARKERS``: the two lifecycles are mutually exclusive;
          combined presence is a corrupted state surfaced by
          ``raise_if_invalid`` before any read or write.

        An encoded (calendar) coordinate is checked before these branches: a
        ``PREALLOCATED`` one is verified and never written, and any other
        state is refused.
        """
        root = self._open_root()
        timestamp_path = f"{group}/{self._time_coord_name}"
        try:
            timestamp_arr = root[timestamp_path]
        except KeyError:
            timestamp_arr = None

        attrs: dict[str, Any] = dict(timestamp_arr.attrs) if timestamp_arr is not None else {}
        state = resolve_coord_lifecycle(attrs)
        raise_if_invalid(state, timestamp_path)

        if timestamp_arr is not None:
            # Imported here: firecube.core.time.encoding imports firecube.core.zarr.
            from firecube.core.time.encoding import coordinate_encoding_from_array

            encoding = coordinate_encoding_from_array(timestamp_arr.dtype, attrs)
            if encoding.extra_attrs:
                if state is not CoordLifecycleState.PREALLOCATED:
                    self._raise_encoded_unreachable_state(
                        timestamp_path=timestamp_path, attrs=attrs, state=state
                    )
                self._verify_preallocated_encoded_slot(
                    group=group,
                    ts_index=ts_index,
                    timestamp_val=timestamp_val,
                    timestamp_arr=timestamp_arr,
                    timestamp_path=timestamp_path,
                    encoding=encoding,
                    attrs=attrs,
                )
                return

        if state is CoordLifecycleState.PREALLOCATED and timestamp_arr is not None:
            current_ns = self._normalize_timestamp_value_ns(timestamp_arr[ts_index])
            incoming_ns = self._normalize_timestamp_value_ns(timestamp_val)
            if (np.isnat(current_ns) and np.isnat(incoming_ns)) or current_ns == incoming_ns:
                return
            raise SchemaDriftError(
                f"Preallocated timestamp coordinate {timestamp_path!r} in group {group!r} "
                f"slot {ts_index} diverged: current={current_ns!r} incoming={incoming_ns!r}."
            )

        if state is CoordLifecycleState.COORD_MANAGED and timestamp_arr is not None:
            target_dtype = timestamp_arr.dtype
            current_norm = self._normalize_for_coord_compare(timestamp_arr[ts_index], target_dtype)
            incoming_norm = self._normalize_for_coord_compare(timestamp_val, target_dtype)
            if np.isnat(current_norm):
                raise SchemaDriftError(
                    Message(
                        "coord array {array_path} slot {ts_index} is NaT under "
                        "firecube_coord_managed marker; the materialization step "
                        "must run first. Invoke: {preallocate}",
                        array_path=timestamp_path,
                        group=group,
                        ts_index=ts_index,
                        preallocate=preallocate_operation(
                            slot_start=Placeholder("<n>"), slot_end=Placeholder("<m>")
                        ),
                    )
                )
            if current_norm == incoming_norm:
                return
            raise SchemaDriftError(
                f"Coord-managed timestamp coordinate {timestamp_path!r} in group {group!r} "
                f"slot {ts_index} diverged: current={current_norm!r} incoming={incoming_norm!r}."
            )

        # Pre-marker coordinate: create, grow and element write all rewrite
        # shared objects, so concurrent batches of one run take them in turn.
        with self._hold_array(timestamp_path):
            self._write_legacy_timestamp(group, ts_index, timestamp_val)

    def _write_legacy_timestamp(self, group: str, ts_index: int, timestamp_val: Any) -> None:
        """Create or grow the unmarked time coordinate and write one slot.

        Raises:
            ValueError: *timestamp_val* is calendar-valued on a non-Gregorian calendar.
            Datetime64RangeError: The stored coordinate is ``datetime64[ns]`` and the
                value lies outside its range; nothing is written.
        """
        normalized = self._normalize_timestamp_value(timestamp_val)
        try:
            stored = self._open_root()[f"{group}/{self._time_coord_name}"]
        except KeyError:
            stored = None
        if getattr(stored, "dtype", None) == np.dtype("datetime64[ns]"):
            # The element write casts to ns, and NumPy wraps a value outside that range.
            gregorian_to_datetime64_ns(timestamp_val)
            gregorian_to_datetime64_ns(normalized)
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
            slot = _axis_index(ts_index, int(timestamp_arr.shape[0]))
            self._write_payload(
                timestamp_arr,
                f"{group}/{self._time_coord_name}",
                ((slot, slot + 1),),
                slot,
                normalized,
            )

    def _raise_encoded_unreachable_state(
        self, *, timestamp_path: str, attrs: dict[str, Any], state: CoordLifecycleState
    ) -> None:
        """Refuse an encoded (calendar) coordinate that preallocate did not mark.

        Only preallocate writes a calendar coordinate, so any other lifecycle state
        means an out-of-band edit or a skipped preallocate.

        Raises:
            SchemaDriftError: Always; the message names the path, units, calendar and state.
        """
        marker = (
            ATTR_COORD_MANAGED
            if state is CoordLifecycleState.COORD_MANAGED
            else "no sealing marker (legacy)"
        )
        raise SchemaDriftError(
            Message(
                "coordinate array {array_path!r} carries encoded time attrs "
                "(units={units!r}, calendar={calendar!r}) but its lifecycle state is "
                "{marker}; this should be unreachable because calendar axes only "
                "support the preallocated (grid) regime. Preallocate it first: {preallocate}",
                array_path=timestamp_path,
                units=str(attrs["units"]),
                calendar=str(attrs["calendar"]),
                marker=marker,
                preallocate=preallocate_operation(),
            )
        )

    def _verify_preallocated_encoded_slot(
        self,
        *,
        group: str,
        ts_index: int,
        timestamp_val: Any,
        timestamp_arr: Any,
        timestamp_path: str,
        encoding: Any,
        attrs: dict[str, Any],
    ) -> None:
        """Compare one slot of a preallocated encoded coordinate with the incoming value.

        A match returns and writes nothing.

        Args:
            group: Data group path.
            ts_index: Slot to verify.
            timestamp_val: Incoming value: a calendar-valued object or an encoded number.
            timestamp_arr: The opened coordinate array.
            timestamp_path: The coordinate's path, for messages.
            encoding: The array's ``CoordinateEncoding``.
            attrs: The array's attributes, carrying ``units`` and ``calendar``.

        Raises:
            SchemaDriftError: The slot holds the fill sentinel, or another value.
            ValueError: *timestamp_val* is on another calendar.
            TypeError: *timestamp_val* is a Gregorian-typed value.
        """
        units = str(attrs["units"])
        calendar = str(attrs["calendar"])
        incoming = encoding.encode_scalar(timestamp_val)
        stored_raw = timestamp_arr[ts_index]
        stored = stored_raw.item() if isinstance(stored_raw, np.generic) else stored_raw
        if stored == incoming:
            return
        fill_value = timestamp_arr.fill_value
        if _array_is_all_fill(np.atleast_1d(np.asarray(stored_raw)), fill_value):
            # A marked coordinate that still holds the fill sentinel was never filled:
            # report the missing preallocate, not a drift.
            raise SchemaDriftError(
                Message(
                    "Preallocated encoded timestamp coordinate {array_path!r} in group "
                    "{group!r} slot {ts_index}: preallocation incomplete (unwritten slot); "
                    "stored value equals the encoded fill sentinel {fill_value!r} "
                    "(units={units!r}, calendar={calendar!r}). Preallocate the coord array "
                    "first: {preallocate}",
                    array_path=timestamp_path,
                    group=group,
                    ts_index=ts_index,
                    fill_value=fill_value,
                    units=units,
                    calendar=calendar,
                    preallocate=preallocate_operation(),
                )
            )
        raise SchemaDriftError(
            f"Preallocated encoded timestamp coordinate {timestamp_path!r} in group "
            f"{group!r} slot {ts_index} diverged: stored={stored!r} incoming={incoming!r} "
            f"(units={units!r}, calendar={calendar!r})."
        )

    def write_static(
        self,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None:
        """Write a static (non-time-indexed) array at its declared shape.

        The array must already exist (created by schema setup).
        This method performs a full overwrite: ``arr[...] = data``, under this
        process's whole-array lock (`hold_array`; a hold of it the calling
        thread already has is reused).
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
            SchemaDriftError: The target is a preallocated or coord-managed
                time coordinate holding other values (see
                ``verify_engine_written_coordinate``).
        """
        if self.verify_engine_written_coordinate(group, array_name, data):
            return
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
        # Static arrays are written under the whole-array lock only (reused
        # inside the static dispatch transaction), never under unit locks.
        with self._hold_array(arr_path):
            self._write_payload(arr, arr_path, tuple((0, int(n)) for n in arr.shape), ..., data)
