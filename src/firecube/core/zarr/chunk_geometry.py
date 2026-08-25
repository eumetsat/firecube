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
from typing import Any


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
            ``(time, y, x)``; rank 4 as ``(time, y, x, channel)``.
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
    """
    return range(start // chunk_size, ((stop - 1) // chunk_size) + 1)


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
    "axis_selection_is_chunk_aligned",
    "chunk_axis_range",
    "physical_chunk_keys_for_region",
]
