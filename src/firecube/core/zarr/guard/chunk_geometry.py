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

"""Chunk-grid geometry helpers for advanced Zarr write coordination."""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Sequence
from typing import Any

from firecube.core.errors import Message, Param


def physical_chunk_keys_for_region(
    *,
    group: str,
    intent: Any,
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    selection: Any,
) -> tuple[set[tuple[str, str, tuple[int, ...]]], bool]:
    """Return physical chunk keys touched by one region write.

    Args:
        group: Logical Zarr group used as the first element of each key.
        intent: Write intent with ``group`` and ``array`` attributes for error
            messages and chunk-key construction.
        shape: Full target array shape. Rank 3 is interpreted as
            ``(time, y, x)``; rank 4 as ``(time, y, x, channel)``; rank 5 or
            more as ``(time, y, ...)`` with every axis after ``y`` written in
            full.
        chunks: Target array chunk shape with the same rank as ``shape``.
        selection: Region selection with ``ts_index``, ``y_start``,
            ``y_stop``, and optional ``channel_index`` attributes.

    Returns:
        A ``(keys, aligned)`` pair. ``keys`` contains ``(group, array,
        chunk_coords)`` tuples. ``aligned`` is true when the write is aligned
        to whole physical chunks along the selected non-X axes and the time
        chunk size is one.

    Raises:
        ValueError: If ``selection.ts_index`` is outside ``shape``.

    Examples:
        Compute the one physical chunk touched by a chunk-aligned 2-row write:

            >>> from types import SimpleNamespace
            >>> intent = SimpleNamespace(group="data", array="values")
            >>> selection = SimpleNamespace(ts_index=0, y_start=2, y_stop=4, channel_index=None)
            >>> physical_chunk_keys_for_region(
            ...     group="data",
            ...     intent=intent,
            ...     shape=(1, 6, 4),
            ...     chunks=(1, 2, 4),
            ...     selection=selection,
            ... )
            ({('data', 'values', (0, 1, 0))}, True)
    """
    rank = len(shape)
    if selection.ts_index >= shape[0]:
        raise ValueError(
            "Concurrent region write ts_index is outside the opened target array: "
            f"group={intent.group!r} array={intent.array!r} "
            f"shape={shape!r} ts_index={selection.ts_index!r}."
        )

    time_chunks = chunk_axis_range(selection.ts_index, selection.ts_index + 1, chunks[0])
    y_chunks = chunk_axis_range(selection.y_start, selection.y_stop, chunks[1])
    x_chunks = chunk_axis_range(0, shape[2], chunks[2])

    if rank == 3:
        coords = itertools.product(time_chunks, y_chunks, x_chunks)
        keys = {(group, intent.array, tuple(coord)) for coord in coords}
        aligned = chunks[0] == 1 and axis_selection_is_chunk_aligned(
            selection.y_start, selection.y_stop, shape[1], chunks[1]
        )
        return keys, aligned

    if rank > 4:
        later_axes = [chunk_axis_range(0, shape[axis], chunks[axis]) for axis in range(2, rank)]
        coords = itertools.product(time_chunks, y_chunks, *later_axes)
        keys = {(group, intent.array, tuple(coord)) for coord in coords}
        aligned = chunks[0] == 1 and axis_selection_is_chunk_aligned(
            selection.y_start, selection.y_stop, shape[1], chunks[1]
        )
        return keys, aligned

    if selection.channel_index is None:
        channel_start = 0
        channel_stop = shape[3]
    else:
        channel_start = selection.channel_index
        channel_stop = selection.channel_index + 1
    channel_chunks = chunk_axis_range(channel_start, channel_stop, chunks[3])
    coords = itertools.product(time_chunks, y_chunks, x_chunks, channel_chunks)
    keys = {(group, intent.array, tuple(coord)) for coord in coords}
    aligned = (
        chunks[0] == 1
        and axis_selection_is_chunk_aligned(
            selection.y_start, selection.y_stop, shape[1], chunks[1]
        )
        and axis_selection_is_chunk_aligned(channel_start, channel_stop, shape[3], chunks[3])
    )
    return keys, aligned


def chunk_axis_range(start: int, stop: int, chunk_size: int) -> range:
    """Return chunk indices intersected by a half-open axis selection.

    Args:
        start: Inclusive selected element index.
        stop: Exclusive selected element index.
        chunk_size: Physical chunk length along the axis.

    Returns:
        A ``range`` of intersected chunk indices. Empty or zero-length
        selections return an empty range.

    Raises:
        ZeroDivisionError: If ``chunk_size`` is zero.

    Examples:
        A misaligned ``[1, 5)`` selection with chunk size two touches three
        chunks:

            >>> list(chunk_axis_range(1, 5, 2))
            [0, 1, 2]

        An empty selection touches no chunk, wherever it sits:

            >>> list(chunk_axis_range(1, 1, 4))
            []
    """
    if stop <= start:
        return range(0)
    return range(start // chunk_size, ((stop - 1) // chunk_size) + 1)


def chunk_index_to_region(
    chunk_idx: tuple[int, ...],
    chunk_shape: tuple[int, ...],
    array_shape: tuple[int, ...],
) -> tuple[slice, ...]:
    """Return the slice tuple for a chunk index, clamping the last chunk to array_shape.

    Sits alongside ``chunk_axis_range`` (element-selection → chunk-index range) and
    ``physical_chunk_keys_for_region`` (region-selection → physical chunk keys); this
    one goes the other direction: chunk-index → element region.
    """
    return tuple(
        slice(
            chunk_idx[i] * chunk_shape[i],
            min((chunk_idx[i] + 1) * chunk_shape[i], array_shape[i]),
        )
        for i in range(len(chunk_idx))
    )


Bounds = tuple[tuple[int, int], ...]
"""Half-open ``(start, stop)`` per dimension: a box of array cells."""


def write_unit_shape(
    chunks: Sequence[int], shards: Sequence[int] | None
) -> tuple[tuple[int, ...], str]:
    """Return the stored object shape a write replaces, and what it is called.

    A sharded array stores one object per shard: a write to any inner chunk
    reads and rewrites the whole shard. Otherwise the object is the chunk.

    Args:
        chunks: Chunk shape of the stored array (inner chunks when sharded).
        shards: Shard shape of the stored array, or ``None`` when unsharded.

    Returns:
        ``(unit_shape, "shard" | "chunk")``.
    """
    if shards is not None:
        return tuple(int(axis) for axis in shards), "shard"
    return tuple(int(axis) for axis in chunks), "chunk"


def units_touched(bounds: Bounds, unit_shape: Sequence[int]) -> list[tuple[int, ...]]:
    """Return the index of every unit a box of cells intersects.

    Examples:
        >>> units_touched(((11, 12), (0, 4)), (12, 2))
        [(0, 0), (0, 1)]

        A box with an empty interval holds no cells and touches no unit:

        >>> units_touched(((1, 1),), (4,))
        []
    """
    ranges = [
        chunk_axis_range(start, stop, size)
        for (start, stop), size in zip(bounds, unit_shape, strict=True)
    ]
    return [tuple(index) for index in itertools.product(*ranges)]


def unit_bounds(
    unit_index: Sequence[int], unit_shape: Sequence[int], array_shape: Sequence[int]
) -> Bounds:
    """Return the cells of one unit, clipped to the array extent.

    Examples:
        >>> unit_bounds((1, 1), (12, 2), (20, 3))
        ((12, 20), (2, 3))
    """
    return tuple(
        (index * size, min((index + 1) * size, extent))
        for index, size, extent in zip(unit_index, unit_shape, array_shape, strict=True)
    )


def bounds_cover(target: Bounds, boxes: Iterable[Bounds]) -> bool:
    """Return whether the union of ``boxes`` contains every cell of ``target``.

    Exact for any number of overlapping boxes: the target is cut at every box
    edge, and each resulting cell block must lie inside some box. The work
    grows with the number of boxes, never with the number of array cells.

    Examples:
        >>> bounds_cover(((0, 1), (0, 4)), [((0, 1), (0, 2)), ((0, 1), (2, 4))])
        True
        >>> bounds_cover(((0, 1), (0, 4)), [((0, 1), (0, 3))])
        False
    """
    clipped = []
    for box in boxes:
        inter = tuple(
            (max(b0, t0), min(b1, t1)) for (b0, b1), (t0, t1) in zip(box, target, strict=True)
        )
        if all(start < stop for start, stop in inter):
            clipped.append(inter)
    if not clipped:
        return all(start >= stop for start, stop in target)
    cuts = [
        sorted({start, stop, *(edge for box in clipped for edge in box[dim])})
        for dim, (start, stop) in enumerate(target)
    ]
    blocks = itertools.product(
        *[list(itertools.pairwise(dim_cuts)) for dim_cuts in cuts],
    )
    for block in blocks:
        if not any(
            all(b0 <= c0 and c1 <= b1 for (c0, c1), (b0, b1) in zip(block, box, strict=True))
            for box in clipped
        ):
            return False
    return True


def window_straddling_units(
    *, time_unit: int, time_len: int, window: tuple[int, int]
) -> list[tuple[int, int]]:
    """Return the time spans of the units a slot window covers only partly.

    A unit is clipped to the array's time extent, so a terminal partial unit
    that a window covers to the end of the array is owned by that window.

    Args:
        time_unit: Time extent of one stored unit (chunk or shard).
        time_len: Time extent of the stored array.
        window: Half-open ``(slot_start, slot_end)`` the writer owns.

    Returns:
        ``(first_slot, end_slot)`` of every unit that overlaps the window but
        also holds slots outside it, in slot order.

    Only the unit holding the window's first slot and the unit holding its
    last slot can hold slots outside the window, so only those two are
    inspected and the cost does not depend on the window's length.

    Examples:
        >>> window_straddling_units(time_unit=12, time_len=12, window=(3, 4))
        [(0, 12)]
        >>> window_straddling_units(time_unit=100, time_len=1000, window=(100, 200))
        []
    """
    start, stop = max(window[0], 0), min(window[1], time_len)
    if start >= stop:
        return []
    first_index = start // time_unit
    last_index = (stop - 1) // time_unit
    indices = (first_index,) if first_index == last_index else (first_index, last_index)
    straddled = []
    for index in indices:
        first, end = index * time_unit, min((index + 1) * time_unit, time_len)
        if first < window[0] or end > window[1]:
            straddled.append((first, end))
    return straddled


def _one_slot_unit_advice(unit_kind: str) -> str:
    """Return how to give an array a one-slot write unit, for ``unit_kind``.

    Inner chunks do not decide what a write replaces in a sharded array, so a
    shard refusal is not cured by smaller chunks alone.
    """
    if unit_kind == "shard":
        return (
            "has a time shard of 1 (shards=(1, ...) with chunks=(1, ...)), or no shards and "
            "a time chunk of 1 (chunks=(1, ...)), on the ZarrArraySpec; "
            "smaller inner chunks alone leave the shard shared"
        )
    return (
        "has a time chunk of 1 (chunks=(1, ...) on the ZarrArraySpec, "
        "or leave chunks and shards undeclared)"
    )


def unit_ownership_refusal(
    *,
    array_path: str,
    unit_kind: str,
    unit_shape: Sequence[int],
    unit_slots: tuple[int, int],
    window: tuple[int, int],
) -> Message:
    """Return the refusal message for a slot window that shares a stored unit.

    Examples:
        >>> "slots [0, 12)" in unit_ownership_refusal(
        ...     array_path="data/vec", unit_kind="chunk", unit_shape=(12,),
        ...     unit_slots=(0, 12), window=(3, 4))
        True
    """
    first, end = unit_slots
    size = int(unit_shape[0])
    return Message(
        "{array_path}: the stored {unit_kind} shape {unit_shape!r} puts slots "
        "[{first}, {end}) in one {unit_kind} object, but this run's slot window "
        "[{slot_window[0]}, {slot_window[1]}) holds only part of them. A writer of the other slots "
        "rewrites the same object, and one of the two writes is lost. Use "
        "{slot_start}/{slot_end} windows aligned to {size} slots (here [{first}, {end})), "
        "or ingest into a new store whose array {advice}.",
        array_path=array_path,
        unit_kind=unit_kind,
        unit_shape=tuple(unit_shape),
        first=first,
        end=end,
        slot_window=tuple(window),
        slot_start=Param("slot_start"),
        slot_end=Param("slot_end"),
        size=size,
        advice=_one_slot_unit_advice(unit_kind),
    )


def incompletely_written_units(
    *,
    array_shape: Sequence[int],
    unit_shape: Sequence[int],
    touched: Iterable[Bounds],
    written: Iterable[Bounds],
) -> list[tuple[int, ...]]:
    """Return the units a run touched but did not write in every cell.

    Args:
        array_shape: Shape of the stored array.
        unit_shape: Stored write unit (chunk, or shard when sharded).
        touched: Every box the run wrote or attempted.
        written: Every box the run is known to have written.

    Returns:
        Sorted unit indices with at least one cell outside ``written``.

    Examples:
        >>> incompletely_written_units(
        ...     array_shape=(12,), unit_shape=(12,),
        ...     touched=[((0, 1),), ((1, 2),)], written=[((0, 1),), ((1, 2),)])
        [(0,)]
    """
    by_unit: dict[tuple[int, ...], list[Bounds]] = {}
    for box in touched:
        for unit in units_touched(box, unit_shape):
            by_unit.setdefault(unit, [])
    for box in written:
        for unit in units_touched(box, unit_shape):
            if unit in by_unit:
                by_unit[unit].append(box)
    return sorted(
        unit
        for unit, boxes in by_unit.items()
        if not bounds_cover(unit_bounds(unit, unit_shape, array_shape), boxes)
    )


def staged_partial_unit_refusal(
    *,
    array_path: str,
    unit_kind: str,
    unit_shape: Sequence[int],
    unit_cells: Bounds,
    detail: str,
) -> Message:
    """Return the refusal message for a staged run that would promote a partial unit.

    Examples:
        >>> "slots [0, 12)" in staged_partial_unit_refusal(
        ...     array_path="data/vec", unit_kind="chunk", unit_shape=(12,),
        ...     unit_cells=((0, 12),), detail="slots 6-11 are not written")
        True
    """
    first, end = unit_cells[0]
    return Message(
        "{array_path}: staged promotion replaces whole {unit_kind} objects, and the "
        "stored {unit_kind} shape {unit_shape!r} puts slots [{first}, {end}) "
        "(cells {cells!r}) in one object, but this run does not write all "
        "of it: {detail}. Promoting it would reset the unwritten cells in the target "
        "to the fill value. Write every slot and cell of the {unit_kind} in one run, "
        "ingest with {write_mode}, or use a store whose array "
        "{advice}.",
        array_path=array_path,
        unit_kind=unit_kind,
        unit_shape=tuple(unit_shape),
        first=first,
        end=end,
        cells=list(unit_cells),
        detail=detail,
        write_mode=Param("write_mode", "direct"),
        advice=_one_slot_unit_advice(unit_kind),
    )


def axis_selection_is_chunk_aligned(
    start: int,
    stop: int,
    axis_len: int,
    chunk_size: int,
) -> bool:
    """Return whether a half-open axis selection aligns to chunk boundaries.

    Args:
        start: Inclusive selected element index.
        stop: Exclusive selected element index.
        axis_len: Full axis length. A selection ending at this value is aligned
            even when the final chunk is partial.
        chunk_size: Physical chunk length along the axis.

    Returns:
        ``True`` when ``start`` begins on a chunk boundary and ``stop`` is
        either a chunk boundary or the axis end; otherwise ``False``.

    Raises:
        ZeroDivisionError: If ``chunk_size`` is zero.

    Examples:
        A whole final partial chunk is aligned when it reaches the axis end:

            >>> axis_selection_is_chunk_aligned(4, 5, 5, 2)
            True
    """
    return start % chunk_size == 0 and (stop == axis_len or stop % chunk_size == 0)


__all__ = [
    "Bounds",
    "axis_selection_is_chunk_aligned",
    "bounds_cover",
    "chunk_axis_range",
    "chunk_index_to_region",
    "incompletely_written_units",
    "physical_chunk_keys_for_region",
    "staged_partial_unit_refusal",
    "unit_bounds",
    "unit_ownership_refusal",
    "units_touched",
    "window_straddling_units",
    "write_unit_shape",
]
