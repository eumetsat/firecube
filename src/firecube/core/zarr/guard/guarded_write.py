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

"""The one chokepoint for Zarr payload writes (chunk and shard objects).

A partial write reads a stored object (the shard if sharded, else the chunk),
decodes it, merges the new cells, encodes it and writes it back, or deletes it
when every cell ends up equal to the fill value and ``write_empty_chunks`` is
off. Two writers doing that to one object lose an update, and Zarr leaves the
exclusion to the caller. `guarded_write` holds this process's lock on every
stored unit a footprint touches across that whole read-merge-write, applies
``write_empty_chunks`` to the array being written rather than to the process,
and can report which units hold no object afterwards. A tracked write goes
through a recording store that notes every key the write sets or deletes. A
unit the write did not reach gets one existence check after the write.

`guarded_publish` is the other payload write. It replaces the object of one
whole stored unit with bytes the caller encoded, or removes it, under the same
lock. It applies the fill rule itself and reports the same `WriteResult`. A DirectZarr payload reaches its
store through these two functions only.

Metadata (``zarr.json``: shape, attributes, markers) is a separate resource and
is not written here. The locks are in-process only.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from operator import index as operator_index
from typing import Any

import numpy as np
from zarr.buffer import default_buffer_prototype
from zarr.storage import MemoryStore, StorePath, WrapperStore

from firecube.core.zarr._zarr_compat import sync
from firecube.core.zarr.guard._unit_locks import _UNIT_LOCKS, canonical_array_path
from firecube.core.zarr.guard.chunk_geometry import Bounds, unit_bounds, units_touched
from firecube.core.zarr.guard.unit_ownership import WriteFootprint, stored_write_unit

Unit = tuple[int, ...]
"""Index of one stored unit (shard if sharded, else chunk) in the unit grid."""

_HELD = threading.local()


@dataclass(frozen=True)
class WriteResult:
    """What one guarded write did to the stored units it touched.

    Attributes:
        footprint: The cells the write was allowed to change.
        units: Every stored unit the footprint touches, in grid order.
        absent: Touched units holding no object after the write, when tracking
            was asked for. Kept empty chunks rule out deletion, not absence: a
            stepped selection or a box wider than the selection leaves units
            unwritten. Untracked and empty when the handle keeps empty chunks
            and the selection assigns every cell of the box.
        tracked: Whether ``absent`` was determined, from the write's own sets
            and deletes and one existence check per unit it did not reach. A
            publish reaches its one unit, so it never checks.
    """

    footprint: WriteFootprint
    units: tuple[Unit, ...]
    absent: tuple[Unit, ...] = ()
    tracked: bool = False


@dataclass(frozen=True)
class ArrayHold:
    """This thread's hold of the whole-array lock on one array.

    Attributes:
        store: Store identity the lock is keyed by.
        array_path: Canonical ``group/array`` path.
    """

    store: str
    array_path: str


def _held() -> set[tuple[str, str]]:
    held: set[tuple[str, str]] | None = getattr(_HELD, "keys", None)
    if held is None:
        held = set()
        _HELD.keys = held
    return held


@contextlib.contextmanager
def hold_array(store: str, array_path: str) -> Iterator[ArrayHold]:
    """Hold this process's lock on a whole array, for a transaction of several steps.

    For work larger than one payload write: a static array's write-once marker
    check, write and stamp, or the create, grow and write of an unmarked time
    coordinate. The hold is recorded for this thread, so a `guarded_write` to
    the same array inside it, and a nested `hold_array` of it, reuse the held
    lock instead of waiting on it forever. Other threads are not covered.

    The whole-array key and the unit keys `guarded_write` takes outside a hold
    do not exclude each other: an array is written either under holds or under
    unit locks, never both.

    Args:
        store: Store identity the lock is keyed by (``_store_identity``).
        array_path: ``group/array``; any spelling zarr accepts.

    Yields:
        The hold, to pass to `guarded_write` as ``hold``.
    """
    key = (store, canonical_array_path(array_path))
    hold = ArrayHold(*key)
    if key in _held():
        yield hold
        return
    with _UNIT_LOCKS.hold([(key[0], key[1], ())]):
        _held().add(key)
        try:
            yield hold
        finally:
            _held().discard(key)


def current_hold(store: str, array_path: str) -> ArrayHold | None:
    """Return this thread's hold of the whole-array lock on ``array_path``, if any."""
    key = (store, canonical_array_path(array_path))
    return ArrayHold(*key) if key in _held() else None


def array_with_write_empty_chunks(arr: Any, write_empty_chunks: bool | None) -> Any:
    """Return ``arr`` with ``write_empty_chunks`` set on the array handle only.

    ``Array.with_config`` returns a new handle over the same metadata and store
    path, without I/O; ``zarr.config`` and every other handle keep their
    setting, so two threads can write with different values at once. ``None``
    returns ``arr`` as opened, and so does a value ``arr`` already has.
    """
    if write_empty_chunks is None or arr.config.write_empty_chunks == bool(write_empty_chunks):
        return arr
    return arr.with_config({"write_empty_chunks": bool(write_empty_chunks)})


def unit_store_key(arr: Any, unit: Sequence[int]) -> str:
    """Return the store key of the object holding ``unit`` (the shard when sharded)."""
    key = arr.metadata.encode_chunk_key(tuple(int(i) for i in unit))
    return f"{arr.path}/{key}" if arr.path else key


def _unit_exists(arr: Any, unit: Sequence[int]) -> bool:
    return bool(sync(arr.store.exists(unit_store_key(arr, unit))))


def selection_box(selection: Any, shape: Sequence[int]) -> Bounds | None:
    """Return the box of cells an integer, slice or ``...`` selection covers.

    A stepped slice covers the cells from its first to its last selected
    index. ``None`` means the selection is not plain basic indexing (fancy
    indexing, ``None``/newaxis, more than one ``...``, more items than axes) or
    an integer lies outside its axis; zarr then validates it on assignment.

    Examples:
        >>> selection_box((1, slice(None)), (4, 3))
        ((1, 2), (0, 3))
        >>> selection_box((..., -1), (4, 3))
        ((0, 4), (2, 3))
    """
    axes = _selected_axes(selection, shape)
    return None if axes is None else tuple((start, stop) for start, stop, _ in axes)


def _selected_axes(selection: Any, shape: Sequence[int]) -> list[tuple[int, int, int]] | None:
    """`selection_box` per axis, with the number of cells selected on it."""
    items = list(selection) if isinstance(selection, tuple) else [selection]
    ellipses = [k for k, item in enumerate(items) if item is Ellipsis]
    if len(ellipses) > 1:
        return None
    if ellipses:
        fill = len(shape) - (len(items) - 1)
        if fill < 0:
            return None
        items[ellipses[0] : ellipses[0] + 1] = [slice(None)] * fill
    if len(items) > len(shape):
        return None
    items += [slice(None)] * (len(shape) - len(items))
    axes: list[tuple[int, int, int]] = []
    for item, extent in zip(items, shape, strict=True):
        length = int(extent)
        if isinstance(item, slice):
            start, stop, step = item.indices(length)
            cells = len(range(start, stop, step))
            if step > 0:
                axes.append((start, max(start, stop), cells))
            else:
                axes.append((max(stop + 1, 0), max(start + 1, stop + 1, 0), cells))
            continue
        if isinstance(item, (bool, np.bool_)) or not hasattr(item, "__index__"):
            return None
        try:
            index = operator_index(item)
        except TypeError:
            return None
        index = index + length if index < 0 else index
        if not 0 <= index < length:
            return None
        axes.append((index, index + 1, 1))
    return axes


def _writes_every_cell(selection: Any, shape: Sequence[int], box: Bounds) -> bool:
    """Whether ``selection`` is plain basic indexing that assigns every cell of ``box``."""
    axes = _selected_axes(selection, shape)
    return (
        axes is not None
        and tuple((start, stop) for start, stop, _ in axes) == box
        and all(stop - start == cells for start, stop, cells in axes)
    )


def _require_footprint_fits(arr: Any, footprint: WriteFootprint, selection: Any) -> None:
    path = canonical_array_path(arr.path)
    if footprint.array_path != path:
        raise ValueError(
            f"footprint names {footprint.array_path!r} but the array written is {path!r}"
        )
    shape = tuple(int(n) for n in arr.shape)
    if len(footprint.box) != len(shape) or any(
        stop > extent for (_, stop), extent in zip(footprint.box, shape, strict=True)
    ):
        raise ValueError(
            f"footprint box {footprint.box} of {path!r} does not fit its shape {shape}"
        )
    box = selection_box(selection, shape)
    if box is None or any(stop <= start for start, stop in box):
        return
    for (start, stop), (lo, hi) in zip(box, footprint.box, strict=True):
        if start < lo or stop > hi:
            raise ValueError(
                f"write to {path!r} selects {box}, outside the footprint {footprint.box} "
                "its locks were taken for"
            )


def _require_basic_selection(selection: Any) -> None:
    """Refuse a selection whose cells ``_require_footprint_fits`` cannot bound.

    Only integers (not booleans), slices and ``...`` (at most once) are
    accepted, alone or in a tuple: a fancy selection (integer array, mask,
    list) or ``None`` can land outside the units the locks are taken for.
    """
    items = selection if isinstance(selection, tuple) else (selection,)
    for item in items:
        if item is Ellipsis or isinstance(item, slice):
            continue
        if not isinstance(item, (bool, np.bool_)) and isinstance(item, (int, np.integer)):
            continue
        raise ValueError(
            "guarded_write supports int, slice and Ellipsis selections only; "
            f"got {type(item).__name__}"
        )
    if sum(item is Ellipsis for item in items) > 1:
        raise ValueError(
            "guarded_write supports int, slice and Ellipsis selections only; "
            "got more than one Ellipsis"
        )


def guarded_write(
    arr: Any,
    footprint: WriteFootprint,
    selection: Any,
    payload: Any,
    *,
    store: str,
    write_empty_chunks: bool | None = None,
    hold: ArrayHold | None = None,
    track_units: bool = False,
    on_result: Callable[[WriteResult], None] | None = None,
    reopen: Callable[[], Any] | None = None,
) -> WriteResult:
    """Run ``arr[selection] = payload`` under the lock of every unit ``footprint`` touches.

    The units are the array's stored ones (shard if sharded, else chunk), read
    from ``arr``, never from a declaration. Their locks are held across the
    whole assignment, so no other writer in this process reads, merges or
    deletes one of them in between. Inside a `hold_array` of this array on
    this thread, the held whole-array lock is used instead.

    Args:
        arr: The array whose stored layout the locks are taken for. Without
            ``reopen`` the write goes through it, so open it with its current
            shape; with ``reopen`` its shape may be stale.
        footprint: Cells the write may change; its path must be the written
            handle's and its box must fit that handle's shape. A
            ``selection`` of integers, slices and ``...`` must lie inside the
            box; any other selection is refused.
        selection: Passed to ``arr.__setitem__`` unchanged.
        payload: Passed to ``arr.__setitem__`` unchanged.
        store: Store identity the locks are keyed by (``_store_identity``).
        write_empty_chunks: Applied to this write's array handle only;
            ``None`` keeps the setting ``arr`` was opened with.
        hold: The `hold_array` hold of this array; optional, as this thread's
            hold is found without it.
        track_units: Report ``absent``, under the lock. A unit the write
            deletes is absent and one it stores is not. A unit with no
            recorded set or delete gets one ``exists`` call after the write.
            Skipped only when the handle keeps empty chunks and ``selection``
            assigns every cell of the box: every touched unit is then stored
            and none is absent.
        on_result: Called with the result after the write and its existence
            checks, before the unit locks (or the whole-array hold's lock,
            which this call does not release) are given up: results of
            writes to one unit reach it in the order of the writes. It runs
            under those locks, so it must not take a stored-unit lock or wait
            for another writer. What it raises propagates, after the write.
        reopen: Returns the handle the write goes through, read again after
            the locks are taken, so another writer's growth is seen. What it
            raises propagates, and nothing is written.

    Returns:
        The touched units and, when tracked, the absent ones.

    Raises:
        ValueError: The footprint names another array or does not fit its
            shape, the selection is not integers, slices and at most one
            ``...``, the selection leaves the footprint, ``hold`` is for
            another array or not held by this thread, or the reopened handle
            has another path or stored unit than ``arr``.
    """
    _require_basic_selection(selection)
    if reopen is None:
        _require_footprint_fits(arr, footprint, selection)
    layout = stored_write_unit(arr)
    units = tuple(units_touched(footprint.box, layout[0]))
    held = current_hold(store, footprint.array_path)
    if hold is not None and hold != held:
        raise ValueError(
            f"whole-array lock of {hold.array_path!r} on {hold.store!r} is not held by this "
            f"thread for {footprint.array_path!r} on {store!r}"
        )
    locks = (
        contextlib.nullcontext()
        if held is not None
        else _UNIT_LOCKS.hold((store, footprint.array_path, unit) for unit in units)
    )
    with locks:
        target = arr if reopen is None else _reopened(reopen, arr, layout, footprint, selection)
        configured = array_with_write_empty_chunks(target, write_empty_chunks)
        track_units = track_units and not (
            configured.config.write_empty_chunks
            and _writes_every_cell(selection, configured.shape, footprint.box)
        )
        result = _write_tracked(configured, footprint, units, selection, payload, track_units)
        if on_result is not None:
            on_result(result)
        return result


def _reopened(
    reopen: Callable[[], Any],
    arr: Any,
    layout: tuple[tuple[int, ...], str],
    footprint: WriteFootprint,
    selection: Any,
) -> Any:
    """Return the handle ``reopen`` reads, refused unless the locks taken for ``arr`` cover it."""
    fresh = reopen()
    path, fresh_path = canonical_array_path(arr.path), canonical_array_path(fresh.path)
    if fresh_path != path:
        raise ValueError(f"reopen returned {fresh_path!r}; the locks were taken for {path!r}")
    fresh_layout = stored_write_unit(fresh)
    if fresh_layout != layout:
        raise ValueError(
            f"stored unit of {path!r} is now {fresh_layout}; the locks were taken for {layout}"
        )
    _require_footprint_fits(fresh, footprint, selection)
    return fresh


def guarded_publish(
    arr: Any,
    footprint: WriteFootprint,
    payload: bytes | None,
    *,
    store: str,
    write_empty_chunks: bool | None = None,
    hold: ArrayHold | None = None,
    track_units: bool = False,
    on_result: Callable[[WriteResult], None] | None = None,
    reopen: Callable[[], Any] | None = None,
) -> WriteResult:
    """Replace the stored object of one whole unit with ``payload``, under that unit's lock.

    The caller owns every cell of the unit. The object is replaced, never
    merged, so a cell the payload does not carry is lost. The lock is
    `guarded_write`'s: the unit's lock, or this thread's `hold_array` hold.

    Encode the payload with the array's codecs and the ``write_empty_chunks``
    setting this call applies, as a write of the whole unit does. The call
    applies the fill rule itself. When empty chunks are not kept, it decodes
    the payload under the lock. A unit whose cells all equal the fill value
    then holds no object, whatever the payload. A payload that does not
    decode raises what its codecs raise, and nothing is written. When empty
    chunks are kept, nothing is decoded.

    Args:
        arr: The array whose stored layout the lock is taken for. Without
            ``reopen`` the publish goes through it, so open it with its
            current shape; with ``reopen`` its shape may be stale.
        footprint: The one stored unit replaced, clipped to the shape of the
            handle written; any other box is refused.
        payload: The encoded object, or ``None`` for no object: an existing
            object is deleted, and nothing is written when there is none.
        store: Store identity the locks are keyed by (``_store_identity``).
        write_empty_chunks: Applied to this publish only; ``None`` keeps the
            setting the handle written was opened with.
        hold: The `hold_array` hold of this array; optional, as this thread's
            hold is found without it.
        track_units: Report ``absent`` from what the call did: a set leaves
            the unit present, a delete or no object leaves it absent. No
            existence check follows the write. Skipped when the handle keeps
            empty chunks, as in `guarded_write`.
        on_result: Called with the result after the write, before the lock
            is given up, as in `guarded_write`. It must not take a stored-unit
            lock or wait for another writer. What it raises propagates.
        reopen: Returns the handle the publish goes through, read again after
            the lock is taken, so another writer's growth is seen. What it
            raises propagates, and nothing is written.

    Returns:
        The footprint and the one unit, with its absence when tracked.

    Raises:
        ValueError: The footprint names another array, is not one whole
            stored unit clipped to the handle's shape, or ``hold`` is not
            this thread's hold of the array, or the reopened handle has
            another path or stored unit than ``arr``. Nothing is written.
    """
    layout = stored_write_unit(arr)
    unit = _published_unit(footprint, layout[0])
    whole = tuple(slice(start, stop) for start, stop in footprint.box)
    if reopen is None:
        _require_whole_unit(arr, footprint, unit, whole)
    held = current_hold(store, footprint.array_path)
    if hold is not None and hold != held:
        raise ValueError(
            f"whole-array lock of {hold.array_path!r} on {hold.store!r} is not held by this "
            f"thread for {footprint.array_path!r} on {store!r}"
        )
    locks = (
        contextlib.nullcontext()
        if held is not None
        else _UNIT_LOCKS.hold([(store, footprint.array_path, unit)])
    )
    with locks:
        target = arr
        if reopen is not None:
            target = _reopened(reopen, arr, layout, footprint, whole)
            _require_whole_unit(target, footprint, unit, whole)
        configured = array_with_write_empty_chunks(target, write_empty_chunks)
        keeps_empty = bool(configured.config.write_empty_chunks)
        if (
            payload is not None
            and not keeps_empty
            and _payload_is_all_fill(configured, unit, whole, payload)
        ):
            payload = None
        present = _replace_unit_object(configured, unit, payload)
        tracked = track_units and not keeps_empty
        result = WriteResult(
            footprint=footprint,
            units=(unit,),
            absent=(unit,) if tracked and not present else (),
            tracked=tracked,
        )
        if on_result is not None:
            on_result(result)
        return result


def _published_unit(footprint: WriteFootprint, unit_shape: Sequence[int]) -> Unit:
    """Return the one stored unit ``footprint`` touches, refusing a box that touches another."""
    if len(footprint.box) != len(unit_shape):
        raise ValueError(
            f"footprint box {footprint.box} of {footprint.array_path!r} has rank "
            f"{len(footprint.box)}; its stored unit {tuple(unit_shape)} has rank {len(unit_shape)}"
        )
    touched = units_touched(footprint.box, unit_shape)
    if len(touched) != 1:
        raise ValueError(
            f"footprint box {footprint.box} of {footprint.array_path!r} touches "
            f"{len(touched)} stored units of shape {tuple(unit_shape)}; a publish replaces one"
        )
    return touched[0]


def _require_whole_unit(
    arr: Any, footprint: WriteFootprint, unit: Unit, whole: tuple[slice, ...]
) -> None:
    """Refuse a footprint that is not ``unit`` clipped to the shape of ``arr``."""
    _require_footprint_fits(arr, footprint, whole)
    shape = tuple(int(n) for n in arr.shape)
    expected = unit_bounds(unit, stored_write_unit(arr)[0], shape)
    if footprint.box != expected:
        raise ValueError(
            f"footprint box {footprint.box} of {footprint.array_path!r} is not unit {unit} "
            f"clipped to the shape {shape}, {expected}"
        )


def _payload_is_all_fill(arr: Any, unit: Unit, whole: tuple[slice, ...], payload: bytes) -> bool:
    """Decode ``payload`` as the object of ``unit`` and judge it by Zarr's own fill test.

    The bytes go into a private in-memory store, read through a handle with
    ``arr``'s metadata, path and config. Zarr's ``all_equal`` then decides, as
    it does for each chunk before it writes: ``NaN`` equals a ``NaN`` fill,
    and a zero fill is compared by bit pattern.
    """
    inner = arr.async_array
    scratch = MemoryStore()
    sync(
        scratch.set(
            unit_store_key(arr, unit), default_buffer_prototype().buffer.from_bytes(payload)
        )
    )
    handle = type(arr)(
        type(inner)(
            metadata=inner.metadata,
            store_path=StorePath(scratch, inner.store_path.path),
            config=inner.config,
        )
    )
    cells = np.asarray(handle[whole])
    return bool(
        default_buffer_prototype().nd_buffer.from_numpy_array(cells).all_equal(arr.fill_value)
    )


def _replace_unit_object(arr: Any, unit: Unit, payload: bytes | None) -> bool:
    """Set or remove the object of ``unit``; return whether the unit holds one afterwards."""
    key = unit_store_key(arr, unit)
    if payload is not None:
        sync(arr.store.set(key, default_buffer_prototype().buffer.from_bytes(payload)))
        return True
    if _unit_exists(arr, unit):
        sync(arr.store.delete(key))
    return False


def _write_tracked(
    arr: Any,
    footprint: WriteFootprint,
    units: tuple[Unit, ...],
    selection: Any,
    payload: Any,
    track_units: bool,
) -> WriteResult:
    if not (track_units and units):
        arr[selection] = payload
        return WriteResult(footprint=footprint, units=units)
    handle, recorder = _recording_handle(arr)
    handle[selection] = payload
    absent: list[Unit] = []
    for unit in units:
        kind = recorder.kinds.get(unit_store_key(arr, unit), _UNKNOWN)
        if kind == _REMOVED or (kind == _UNKNOWN and not _unit_exists(arr, unit)):
            absent.append(unit)
    return WriteResult(footprint=footprint, units=units, absent=tuple(absent), tracked=True)


_STORED, _REMOVED, _UNKNOWN = "stored", "removed", "unknown"


class _RecordingStore(WrapperStore[Any]):
    """A store wrapper that keeps, per key, the kind of the last call that returned.

    A set records ``stored`` and a delete records ``removed``. `set_if_not_exists`
    records ``unknown``, and so does `delete_dir` for each recorded key under its
    prefix. A key never touched has no entry. One instance serves one guarded write.
    """

    def __init__(self, store: Any) -> None:
        super().__init__(store)
        self.kinds: dict[str, str] = {}
        self._kinds_lock = threading.Lock()

    def _record(self, key: str, kind: str) -> None:
        with self._kinds_lock:
            self.kinds[key] = kind

    async def set(self, key: str, value: Any) -> None:
        await super().set(key, value)
        self._record(key, _STORED)

    async def _set_many(self, values: Any) -> None:
        pairs = list(values)
        await super()._set_many(pairs)
        for key, _ in pairs:
            self._record(key, _STORED)

    async def delete(self, key: str) -> None:
        await super().delete(key)
        self._record(key, _REMOVED)

    async def set_if_not_exists(self, key: str, value: Any) -> None:
        await super().set_if_not_exists(key, value)
        self._record(key, _UNKNOWN)

    async def delete_dir(self, prefix: str) -> None:
        await super().delete_dir(prefix)
        stem = prefix.rstrip("/")
        with self._kinds_lock:
            for key in self.kinds:
                if not stem or key.startswith(f"{stem}/"):
                    self.kinds[key] = _UNKNOWN

    if hasattr(WrapperStore, "set_sync"):

        def set_sync(self, key: str, value: Any) -> None:
            super().set_sync(key, value)  # pyright: ignore[reportAttributeAccessIssue]
            self._record(key, _STORED)

        def delete_sync(self, key: str) -> None:
            super().delete_sync(key)  # pyright: ignore[reportAttributeAccessIssue]
            self._record(key, _REMOVED)


def _recording_handle(arr: Any) -> tuple[Any, _RecordingStore]:
    """Return a handle over a fresh `_RecordingStore` with ``arr``'s metadata, path and config.

    Built as ``AsyncArray.with_config`` builds one: no I/O, and the write goes
    through the very metadata the guard validated.
    """
    inner = arr.async_array
    recorder = _RecordingStore(inner.store_path.store)
    rebuilt = type(inner)(
        metadata=inner.metadata,
        store_path=StorePath(recorder, inner.store_path.path),
        config=inner.config,
    )
    return type(arr)(rebuilt), recorder


__all__ = [
    "ArrayHold",
    "Unit",
    "WriteFootprint",
    "WriteResult",
    "array_with_write_empty_chunks",
    "current_hold",
    "guarded_publish",
    "guarded_write",
    "hold_array",
    "selection_box",
    "unit_store_key",
]
