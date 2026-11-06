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

"""Feed the region intents of one DirectZarr slot to a `UnitWriteBuffer`.

`write_slot_buffered` takes one slot's intents in dispatch order. An eligible
region intent becomes one piece of the buffer: its box is the box
`RegionZarrWriter.write_region` writes, and its data is the resolved payload
with the slot axis, and a named channel axis, added back. Every other intent
goes the serial loop's way, after the open units it could touch are flushed.

The buffer publishes each complete stored unit once through the writer and
writes every other unit piece by piece. Coverage of an intent is recorded once
all its cells have landed.
"""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from typing import Any

import numpy as np

from firecube.core.zarr.guard._unit_locks import canonical_array_path
from firecube.core.zarr.guard.chunk_geometry import Bounds
from firecube.core.zarr.guard.guarded_write import Unit
from firecube.core.zarr.guard.unit_buffer import (
    ArrayDescription,
    UnitWriteBuffer,
    has_lossless_codecs,
)
from firecube.core.zarr.region_writer import RegionZarrWriter, region_box

ArrayKey = tuple[str, str]
"""``(group, array name)`` as the intent spells them."""


class _WriterSink:
    """A `RegionZarrWriter` as the `UnitSink` of a buffer, keyed by `ArrayKey`.

    The sink describes each array once and publishes every unit of it against
    that description, so a payload never meets a description read later.
    """

    def __init__(self, writer: RegionZarrWriter) -> None:
        self._writer = writer
        self._descriptions: dict[ArrayKey, ArrayDescription] = {}

    def describe(self, key: ArrayKey) -> ArrayDescription:
        """Return the array's description, read through the writer on the first call."""
        description = self._descriptions.get(key)
        if description is None:
            description = self._descriptions[key] = self._writer.describe_array(*key)
        return description

    def publish(self, key: ArrayKey, unit: Unit, payload: bytes | None) -> None:
        """Publish one whole unit against the description `describe` returned."""
        self._writer.publish_unit(
            key[0], key[1], unit, payload, description=self._descriptions[key]
        )

    def write_box(self, key: ArrayKey, box: Bounds, data: np.ndarray) -> None:
        """Write data into exactly the cells of *box*."""
        self._writer.write_box(key[0], key[1], box, data)


def write_slot_buffered(
    writer: RegionZarrWriter,
    intents: Iterable[Any],
    *,
    time_coord_name: str,
    dispatch: Callable[[Any], None],
    record: Callable[[Any], None],
) -> None:
    """Write one slot's intents, assembling eligible region pieces into whole stored units.

    Call it inside the slot's claim, from one thread. It makes one buffer and
    closes it after the last intent. An exception discards the buffer without
    a flush, so no held piece is written or recorded.

    Args:
        writer: The group's writer; the buffer publishes and writes through it.
        intents: The slot's timed intents, in dispatch order.
        time_coord_name: Name of the array a ``timestamp`` intent writes.
        dispatch: Writes one intent the serial loop's way.
        record: Records one intent's coverage once all its cells have landed.
    """
    feeder = _SlotFeeder(writer, time_coord_name=time_coord_name, dispatch=dispatch, record=record)
    for intent in intents:
        feeder.take(intent)
    feeder.close()


class _SlotFeeder:
    """Route one slot's intents to a `UnitWriteBuffer` or to the serial dispatch.

    An eligible region intent goes to the buffer. Every other intent flushes
    the open units of the array it writes, then goes to *dispatch*. An intent
    of an unknown kind flushes every open unit first.
    """

    def __init__(
        self,
        writer: RegionZarrWriter,
        *,
        time_coord_name: str,
        dispatch: Callable[[Any], None],
        record: Callable[[Any], None],
    ) -> None:
        self._writer = writer
        self._time_coord_name = time_coord_name
        self._dispatch = dispatch
        self._record = record
        self._sink = _WriterSink(writer)
        self._buffer = UnitWriteBuffer[ArrayKey](self._sink, on_landed=record)
        # Canonical array path to the key its open units are held under. One
        # spelling per path at a time, so no unit is assembled twice.
        self._held: dict[str, ArrayKey] = {}

    def take(self, intent: Any) -> None:
        """Write or hold one intent, in dispatch order."""
        kind = getattr(intent, "kind", None)
        if kind == "region":
            self._take_region(intent)
            return
        if kind == "1d":
            self._flush_path(_canonical_path(intent.group, intent.array))
        elif kind == "timestamp":
            self._flush_path(_canonical_path(intent.group, self._time_coord_name))
        else:
            self._flush_path(None)
        self._dispatch(intent)
        self._record(intent)

    def close(self) -> None:
        """Write every unit still open as box writes."""
        self._buffer.close()

    def _take_region(self, intent: Any) -> None:
        key: ArrayKey = (intent.group, intent.array)
        path = _canonical_path(*key)
        piece = None if path is None else self._piece(key, intent)
        if path is None or piece is None:
            self._flush_path(path)
            self._dispatch(intent)
            self._record(intent)
            return
        box, added_axes = piece
        held = self._held.get(path)
        if held is not None and held != key:
            self._buffer.flush(held)
        self._held[path] = key
        data: Any = intent.data() if callable(intent.data) else intent.data
        piece_shape = tuple(
            stop - start for axis, (start, stop) in enumerate(box) if axis not in added_axes
        )
        if type(data) is not np.ndarray or data.shape != piece_shape:
            # write_region takes this payload and may broadcast or convert it;
            # the box does not. Written once, already resolved.
            self._buffer.flush(key)
            self._writer.write_region(
                group=intent.group,
                array_name=intent.array,
                ts_index=intent.ts_index,
                y_slice=intent.y_slice,
                data=data,
                channel_index=intent.channel_index,
            )
            self._record(intent)
            return
        self._buffer.put(key, box, np.expand_dims(data, added_axes), intent)

    def _piece(self, key: ArrayKey, intent: Any) -> tuple[Bounds, tuple[int, ...]] | None:
        """Return the box of an eligible region intent and the axes its payload lacks.

        Returns ``None`` for an ineligible intent, which keeps the serial path
        and its refusals.
        """
        y_slice = intent.y_slice
        if not isinstance(y_slice, slice) or y_slice.step not in (None, 1):
            return None
        try:
            slot = operator.index(intent.ts_index)
        except TypeError:
            return None
        if slot < 0:
            return None
        description = self._sink.describe(key)
        shape, unit_shape = description.shape, description.unit_shape
        rank = len(shape)
        channel = intent.channel_index
        if rank < 3 or unit_shape[0] != 1 or slot >= shape[0]:
            return None
        if channel is not None and (rank > 4 or (rank == 4 and unit_shape[3] != 1)):
            return None
        if not has_lossless_codecs(description.metadata):
            return None
        try:
            box = region_box(shape, slot, y_slice, channel)
        except (TypeError, ValueError, IndexError):
            return None
        if box[1][0] >= box[1][1]:
            return None
        added_axes = (0, 3) if rank == 4 and channel is not None else (0,)
        return box, added_axes

    def _flush_path(self, path: str | None) -> None:
        """Flush the open units of the array at *path*; ``None`` flushes every array."""
        if path is None:
            for key in self._held.values():
                self._buffer.flush(key)
            return
        key = self._held.get(path)
        if key is not None:
            self._buffer.flush(key)


def _canonical_path(group: str, array: str) -> str | None:
    """Return the path zarr stores the array under, or ``None`` when zarr refuses it."""
    try:
        return canonical_array_path(f"{group}/{array}")
    except ValueError:
        return None


__all__ = ["ArrayKey", "write_slot_buffered"]
