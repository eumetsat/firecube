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

"""Assemble whole stored units from pieces of an array write, and write each unit once.

A writer that sends many small pieces into one stored unit makes the store
read, merge and rewrite that unit once per piece. `UnitWriteBuffer` takes the
pieces instead and encodes each one on arrival into an in-memory scratch array
per unit. The scratch has the destination's own metadata, so zarr builds the
object exactly as a write of the whole unit would.

A unit whose pieces supply every cell exactly once goes to the sink as one
encoded object, or as no object when every cell is the fill value. Any other
unit goes to the sink as box writes, one per piece, in the order the pieces
arrived. The buffer decides completeness from the volume supplied and never
from the scratch content.

A piece that overlaps cells already held for a unit flushes the units it
touches and taints them. A tainted unit takes every later piece as a box write
until the buffer closes.

A piece that the scratch could store with other bits goes the same way. When
the destination drops empty chunks, zarr drops a chunk whose cells all pass its
fill test. That test counts every NaN as equal to a NaN fill, and on complex
and structured dtypes ``-0.0`` as equal to ``0.0``. The scratch lacks the
destination's other cells, so it can drop a chunk the destination keeps, and
the supplied bits are lost. The buffer therefore refuses the scratch to a piece
whose cells in one chunk all pass the fill test while some differ from the fill
in bits. Only such a piece can lose bits: a chunk holding any other cell is kept.

The buffer touches no store but its own scratch: a `UnitSink` reads, writes
and publishes the destination. It accepts only arrays whose codecs are on the
lossless list of `has_lossless_codecs`, because a flush decodes held cells and
encodes them again.

The buffer is not thread-safe. The caller guarantees that no other buffer
assembles the same unit at the same time. The caller owns every cell of a
unit it supplies completely, because the publish replaces the stored object.
"""

from __future__ import annotations

import math
import operator
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np
import zarr
from zarr.buffer import default_buffer_prototype
from zarr.storage import MemoryStore, StorePath

from firecube.core.zarr._fill import _fill_mask
from firecube.core.zarr._zarr_compat import sync
from firecube.core.zarr.guard.chunk_geometry import Bounds, unit_bounds, units_touched
from firecube.core.zarr.guard.guarded_write import Unit, array_with_write_empty_chunks
from firecube.core.zarr.guard.unit_ownership import stored_write_unit

_LOSSLESS_CODECS = frozenset({"bytes", "zstd", "crc32c"})
_SHARD_INDEX_CODECS = ("bytes", "crc32c")


@dataclass(frozen=True)
class ArrayDescription:
    """What a `UnitWriteBuffer` needs to know about one destination array.

    Attributes:
        shape: Extent of the array on every axis.
        unit_shape: Shape of one stored unit: the shard when sharded, else the chunk.
        metadata: The array's zarr v3 metadata, from which each scratch is built.
        write_empty_chunks: Whether the destination keeps all-fill chunks; the scratch follows it.
    """

    shape: tuple[int, ...]
    unit_shape: tuple[int, ...]
    metadata: Any
    write_empty_chunks: bool


def describe_array(arr: Any, write_empty_chunks: bool | None = None) -> ArrayDescription:
    """Return the `ArrayDescription` of an open zarr array.

    Args:
        arr: The array the sink writes through.
        write_empty_chunks: The setting the sink applies to its writes;
            ``None`` takes the setting ``arr`` was opened with.

    Returns:
        The array's shape, stored unit, metadata and effective ``write_empty_chunks``.
    """
    configured = array_with_write_empty_chunks(arr, write_empty_chunks)
    unit_shape, _ = stored_write_unit(arr)
    return ArrayDescription(
        shape=tuple(int(n) for n in arr.shape),
        unit_shape=tuple(int(n) for n in unit_shape),
        metadata=arr.metadata,
        write_empty_chunks=bool(configured.config.write_empty_chunks),
    )


def has_lossless_codecs(metadata: Any) -> bool:
    """Return whether every codec of a zarr v3 array is on the buffer's lossless list.

    The list holds ``bytes``, ``zstd`` and ``crc32c``. It also holds
    ``sharding_indexed`` when its inner codecs are on the list and its index
    codecs are exactly ``bytes`` then ``crc32c``. Zarr v2 metadata and any
    storage transformer are off the list.

    Args:
        metadata: The array's metadata, as ``zarr.Array.metadata`` returns it.

    Returns:
        ``True`` when every codec is on the list.
    """
    if getattr(metadata, "zarr_format", None) != 3:
        return False
    document = metadata.to_dict()
    if document.get("storage_transformers"):
        return False
    return all(_codec_is_lossless(codec) for codec in document.get("codecs", ()))


def _codec_document(codec: Any) -> tuple[str | None, dict[str, Any]]:
    """Return the name and configuration of a codec given as a dict or a codec object."""
    document = codec if isinstance(codec, dict) else codec.to_dict()
    return document.get("name"), dict(document.get("configuration") or {})


def _codec_is_lossless(codec: Any) -> bool:
    name, configuration = _codec_document(codec)
    if name in _LOSSLESS_CODECS:
        return True
    if name != "sharding_indexed":
        return False
    index = tuple(_codec_document(c)[0] for c in configuration.get("index_codecs", ()))
    inner = [_codec_document(c)[0] for c in configuration.get("codecs", ())]
    return index == _SHARD_INDEX_CODECS and all(n in _LOSSLESS_CODECS for n in inner)


class UnitSink[K: Hashable](Protocol):
    """The destination of a `UnitWriteBuffer`, one array per key.

    The sink owns the store, its locks and what is reported about each write.
    The buffer calls it only through these three operations.
    """

    def describe(self, key: K) -> ArrayDescription:
        """Return the shape, stored unit, metadata and ``write_empty_chunks`` of the array."""
        ...

    def publish(self, key: K, unit: Unit, payload: bytes | None) -> None:
        """Replace the object of one whole stored unit with *payload*; ``None`` means no object."""
        ...

    def write_box(self, key: K, box: Bounds, data: np.ndarray) -> None:
        """Write *data*, whose shape is the box's shape, into exactly the cells of *box*."""
        ...


@dataclass
class _Piece:
    """One `UnitWriteBuffer.put` call: its token, arrival number and units not yet landed."""

    token: object
    seq: int
    pending: int = 0


@dataclass(frozen=True)
class _Fragment:
    """The part of one piece that falls inside one unit, in array coordinates."""

    piece: _Piece
    box: Bounds


@dataclass
class _OpenUnit:
    """One unit being assembled: its scratch and the fragments it holds."""

    unit: Unit
    bounds: Bounds
    volume: int
    store: MemoryStore
    scratch: Any
    object_key: str
    fragments: list[_Fragment] = field(default_factory=list)
    supplied: int = 0

    def local(self, box: Bounds) -> tuple[slice, ...]:
        """Return the scratch selection of a box of array cells inside this unit."""
        return tuple(
            slice(start - origin, stop - origin)
            for (start, stop), (origin, _) in zip(box, self.bounds, strict=True)
        )


@dataclass(frozen=True)
class _Array:
    """A described array, its scratch metadata and what its chunk elision compares.

    Attributes:
        description: What the sink described.
        metadata: The array's metadata with attributes dropped; its fill keeps the array's bits.
        chunk_shape: The shape zarr drops at once: the inner chunk when sharded, else the unit.
        elision_fill: The scratch fill as a 0-d array when elision can change bits, else ``None``.
    """

    description: ArrayDescription
    metadata: Any
    chunk_shape: tuple[int, ...]
    elision_fill: np.ndarray | None


class UnitWriteBuffer[K: Hashable]:
    """Collect pieces of array writes per stored unit and write each unit through a sink.

    One buffer serves one caller on one thread. It is not thread-safe. The
    caller guarantees that no other buffer assembles the same unit, and owns
    every cell of a unit it supplies completely.

    Each array is described once, on its first piece. A failure while the
    buffer writes leaves it unusable: discard it. It reports no further token.

    Args:
        sink: The destination of every publish and box write.
        on_landed: Called with a piece's token once every unit the piece
            touched has been published or written.
    """

    def __init__(
        self,
        sink: UnitSink[K],
        *,
        on_landed: Callable[[object], None] | None = None,
    ) -> None:
        self._sink = sink
        self._on_landed = on_landed
        self._arrays: dict[K, _Array] = {}
        self._open: dict[tuple[K, Unit], _OpenUnit] = {}
        self._tainted: set[tuple[K, Unit]] = set()
        self._next_seq = 0
        self._closed = False
        self._failed = False

    def put(self, key: K, box: Bounds, data: Any, token: object) -> None:
        """Take one piece of a write to the array *key*.

        The piece is encoded into the scratch of every unit it touches. A unit
        it completes is published at once. Some pieces go out as one box
        write instead, after the open units they touch are flushed. These are
        a piece that overlaps held cells, one whose bits the scratch could
        change, and one that touches a tainted unit.

        Args:
            key: The array, as the sink knows it.
            box: Half-open ``(start, stop)`` per axis, inside the array.
            data: The cells of *box*; its shape is the box's shape.
            token: Reported to ``on_landed`` once this piece has landed.

        Raises:
            ValueError: The box does not fit the array, *data* has another
                shape, or the array's codecs are off the lossless list.
                Nothing is held.
            RuntimeError: The buffer is closed or failed earlier.
        """
        self._require_usable()
        if self._closed:
            raise RuntimeError("unit write buffer is closed; it takes no more pieces")
        array = self._array(key)
        bounds = _checked_box(box, array.description.shape, key)
        payload = np.asarray(data)
        box_shape = tuple(stop - start for start, stop in bounds)
        if payload.shape != box_shape:
            raise ValueError(
                f"piece data shape {payload.shape} does not match the shape {box_shape} "
                f"of box {bounds} for {key!r}"
            )
        piece = _Piece(token=token, seq=self._next_seq)
        self._next_seq += 1
        try:
            self._place(key, array, bounds, payload, piece)
        except BaseException:
            self._failed = True
            raise

    def flush(self, key: K) -> None:
        """Write every open unit of the array *key* as box writes, one per held piece.

        Each fragment is read back from the scratch with its own box and
        written in the order the pieces arrived. Other arrays' units stay open.

        Raises:
            RuntimeError: The buffer failed earlier.
        """
        self._require_usable()
        try:
            self._flush_units([held for held in self._open if held[0] == key])
        except BaseException:
            self._failed = True
            raise

    def close(self) -> None:
        """Write every open unit of every array as box writes, then refuse further pieces.

        Raises:
            RuntimeError: The buffer failed earlier.
        """
        self._require_usable()
        try:
            self._flush_units(list(self._open))
        except BaseException:
            self._failed = True
            raise
        self._closed = True

    def _require_usable(self) -> None:
        if self._failed:
            raise RuntimeError("unit write buffer failed earlier; discard it")

    def _array(self, key: K) -> _Array:
        """Return the described array *key*, describing and checking it on first use."""
        known = self._arrays.get(key)
        if known is not None:
            return known
        description = self._sink.describe(key)
        metadata_shape = tuple(int(n) for n in description.metadata.shape)
        if metadata_shape != description.shape or len(description.unit_shape) != len(
            description.shape
        ):
            raise ValueError(
                f"description of {key!r} is inconsistent: shape {description.shape}, "
                f"metadata shape {metadata_shape}, stored unit {description.unit_shape}"
            )
        if not has_lossless_codecs(description.metadata):
            raise ValueError(
                f"array {key!r} has codecs outside the lossless list; write it without the buffer"
            )
        # The JSON document writes every NaN fill as "NaN", so a scratch built
        # from it would pad and drop chunks with canonical NaN bits. The
        # metadata's own update methods keep the fill object, bit for bit.
        metadata = description.metadata.update_attributes({})
        if _scalar_bytes(metadata.fill_value) != _scalar_bytes(description.metadata.fill_value):
            raise ValueError(
                f"scratch metadata of {key!r} does not keep the fill's bits; "
                "write it without the buffer"
            )
        known = _Array(
            description=description,
            metadata=metadata,
            chunk_shape=_chunk_shape(metadata.to_dict(), description.unit_shape),
            elision_fill=_elision_fill(metadata.fill_value, description.write_empty_chunks),
        )
        self._arrays[key] = known
        return known

    def _place(
        self, key: K, array: _Array, bounds: Bounds, payload: np.ndarray, piece: _Piece
    ) -> None:
        touched = [(key, unit) for unit in units_touched(bounds, array.description.unit_shape)]
        if not touched:
            self._report([piece])
            return
        overlapped = [
            held
            for held in touched
            if held in self._open
            and any(_overlaps(bounds, fragment.box) for fragment in self._open[held].fragments)
        ]
        loses_bits = not overlapped and _elision_changes_bits(array, bounds, payload)
        if overlapped or loses_bits or any(held in self._tainted for held in touched):
            if overlapped or loses_bits:
                self._tainted.update(touched)
            self._flush_units([held for held in touched if held in self._open])
            self._sink.write_box(key, bounds, payload)
            self._report([piece])
            return
        piece.pending = len(touched)
        for held in touched:
            open_unit = self._open.get(held)
            if open_unit is None:
                open_unit = self._open_unit(array, held)
            part = _intersection(bounds, open_unit.bounds)
            open_unit.scratch[open_unit.local(part)] = payload[_relative(part, bounds)]
            open_unit.fragments.append(_Fragment(piece=piece, box=part))
            open_unit.supplied += _volume(part)
        for held in touched:
            open_unit = self._open[held]
            if open_unit.supplied == open_unit.volume:
                self._publish(held, open_unit)

    def _open_unit(self, array: _Array, held: tuple[K, Unit]) -> _OpenUnit:
        """Open a scratch for one unit: the array's metadata, its shape the clipped unit."""
        description = array.description
        bounds = unit_bounds(held[1], description.unit_shape, description.shape)
        metadata = array.metadata.update_shape(tuple(stop - start for start, stop in bounds))
        store = MemoryStore()
        scratch = zarr.Array(
            zarr.AsyncArray(
                metadata=metadata,
                store_path=StorePath(store, ""),
                config={"write_empty_chunks": description.write_empty_chunks},
            )
        )
        open_unit = _OpenUnit(
            unit=held[1],
            bounds=bounds,
            volume=_volume(bounds),
            store=store,
            scratch=scratch,
            object_key=scratch.metadata.encode_chunk_key((0,) * len(bounds)),
        )
        self._open[held] = open_unit
        return open_unit

    def _publish(self, held: tuple[K, Unit], open_unit: _OpenUnit) -> None:
        """Hand the scratch object of a complete unit to the sink, or ``None`` when it has none."""
        stored = sync(
            open_unit.store.get(open_unit.object_key, prototype=default_buffer_prototype())
        )
        payload = None if stored is None else stored.to_bytes()
        del self._open[held]
        self._sink.publish(held[0], held[1], payload)
        self._report(self._land([open_unit]))

    def _flush_units(self, held: Sequence[tuple[K, Unit]]) -> None:
        """Write the fragments of the given open units as box writes, in arrival order."""
        units = [(key, self._open.pop(key)) for key in held]
        ordered = sorted(
            (
                (fragment.piece.seq, open_unit.unit),
                index,
                fragment,
            )
            for index, (_, open_unit) in enumerate(units)
            for fragment in open_unit.fragments
        )
        for _, index, fragment in ordered:
            (key, _), open_unit = units[index]
            cells = np.asarray(open_unit.scratch[open_unit.local(fragment.box)])
            self._sink.write_box(key, fragment.box, cells)
        self._report(self._land([open_unit for _, open_unit in units]))

    def _land(self, units: Sequence[_OpenUnit]) -> list[_Piece]:
        """Count one landed unit for each fragment; return the pieces that have fully landed."""
        landed: list[_Piece] = []
        for open_unit in units:
            for fragment in open_unit.fragments:
                fragment.piece.pending -= 1
                if fragment.piece.pending == 0:
                    landed.append(fragment.piece)
        return landed

    def _report(self, pieces: Sequence[_Piece]) -> None:
        if self._on_landed is None:
            return
        for piece in sorted(pieces, key=lambda p: p.seq):
            self._on_landed(piece.token)


def _checked_box(box: Bounds, shape: tuple[int, ...], key: Hashable) -> Bounds:
    """Return *box* with integer bounds, refused unless it lies inside *shape*."""
    if len(box) != len(shape):
        raise ValueError(f"box {box} has rank {len(box)}; {key!r} has shape {shape}")
    checked = tuple((operator.index(start), operator.index(stop)) for start, stop in box)
    for (start, stop), extent in zip(checked, shape, strict=True):
        if not 0 <= start <= stop <= extent:
            raise ValueError(f"box {checked} does not fit {key!r} of shape {shape}")
    return checked


def _chunk_shape(document: dict[str, Any], unit_shape: tuple[int, ...]) -> tuple[int, ...]:
    """Return the shape zarr drops as one empty chunk: the inner chunk when sharded."""
    codecs = document.get("codecs", ())
    if codecs:
        name, configuration = _codec_document(codecs[0])
        if name == "sharding_indexed":
            return tuple(int(n) for n in configuration["chunk_shape"])
    return unit_shape


def _scalar_bytes(value: Any) -> bytes | None:
    """Return the bytes of a scalar as numpy holds it; ``None`` stays ``None``."""
    return None if value is None else np.asarray(value).tobytes()


def _elision_fill(scratch_fill: Any, write_empty_chunks: bool) -> np.ndarray | None:
    """Return the scratch fill as a 0-d array when zarr's fill test can pass cells of other bits.

    Zarr drops nothing when empty chunks are kept, and passes nothing against a
    ``None`` fill. A float fill that is not NaN passes only its own bits: zarr
    compares a zero by bit pattern, and every other value has one encoding.
    Integer, boolean, string and datetime cells that pass carry the fill's bits.
    """
    if write_empty_chunks or scratch_fill is None:
        return None
    fill = np.asarray(scratch_fill)
    kind = fill.dtype.kind
    if kind == "f":
        return fill if bool(np.isnan(fill)) else None
    return fill if kind in ("c", "V") else None


def _elision_changes_bits(array: _Array, bounds: Bounds, payload: np.ndarray) -> bool:
    """Return whether a scratch write of *payload* could drop a chunk holding supplied bits.

    A chunk is dropped only when all its cells pass zarr's fill test, so the
    piece's own cells in that chunk all pass. Cells of earlier pieces are never
    the lost ones. A piece enters the scratch only when each chunk holding one
    of its differing cells also holds one of its failing cells, which stays.
    """
    fill = array.elision_fill
    if fill is None:
        return False
    cells = np.asarray(payload, dtype=fill.dtype)
    passes = np.asarray(_fill_mask(cells, fill), dtype=bool)
    if not passes.any():
        return False
    raw = np.dtype(f"V{fill.dtype.itemsize}")
    differs = passes & (cells.view(raw) != fill.view(raw))
    if not differs.any():
        return False
    shape, chunk_shape = array.description.shape, array.chunk_shape
    for chunk in units_touched(bounds, chunk_shape):
        cells_box = _intersection(bounds, unit_bounds(chunk, chunk_shape, shape))
        region = _relative(cells_box, bounds)
        if differs[region].any() and passes[region].all():
            return True
    return False


def _volume(box: Bounds) -> int:
    return math.prod(stop - start for start, stop in box)


def _intersection(a: Bounds, b: Bounds) -> Bounds:
    return tuple((max(a0, b0), min(a1, b1)) for (a0, a1), (b0, b1) in zip(a, b, strict=True))


def _overlaps(a: Bounds, b: Bounds) -> bool:
    return all(start < stop for start, stop in _intersection(a, b))


def _relative(part: Bounds, box: Bounds) -> tuple[slice, ...]:
    """Return the selection of *part* inside data laid out over *box*."""
    return tuple(
        slice(start - origin, stop - origin)
        for (start, stop), (origin, _) in zip(part, box, strict=True)
    )


__all__ = [
    "ArrayDescription",
    "UnitSink",
    "UnitWriteBuffer",
    "describe_array",
    "has_lossless_codecs",
]
