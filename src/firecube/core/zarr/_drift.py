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

"""Dtype-tolerant, NaN/NaT-aware array equality helpers for drift detection.

Consolidates helpers used by the direct-Zarr and staged-Zarr append paths to
decide whether two arrays or two group-attribute dictionaries are equivalent
for the purposes of resume-safety and preflight-compare checks. Kept
domain-agnostic: no dependency on plugin runtime, template layer, or CLI.

Public surface:

- :func:`arrays_equal_missing_aware` — dtype-tolerant element-wise equality
  mirroring xarray's ``compat="equals"`` semantics (float32 vs float64 with
  equal values compares equal; NaN==NaN and NaT==NaT).
- :class:`GroupAttrsDiff` + :func:`group_attrs_diff` — compare two group
  attribute dictionaries, reporting added/removed/changed keys without baking
  in a volatile-attr filter (engine is generic first-write-wins).
- :func:`touched_chunks_for_slice` — pure numeric helper computing the set
  of chunk-grid index tuples touched by a region write.
- :func:`chunk_by_chunk_equal` — memory-bounded chunk-by-chunk comparison of
  two ``zarr.Array`` (or array-like) values using
  :func:`arrays_equal_missing_aware` per chunk.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
import zarr

from firecube.core.zarr.chunk_geometry import chunk_index_to_region

__all__ = [
    "GroupAttrsDiff",
    "arrays_equal_missing_aware",
    "chunk_by_chunk_equal",
    "group_attrs_diff",
    "touched_chunks_for_slice",
]


def arrays_equal_missing_aware(a: np.ndarray, b: np.ndarray) -> bool:
    """Return True if ``a`` and ``b`` are equal, dtype-tolerant and NaN/NaT-aware.

    Shape mismatch returns False. Dtype tolerance is provided by promoting
    both operands to their common dtype via :func:`numpy.promote_types`; if
    promotion fails (for example float vs bytes), the function returns
    False. This mirrors xarray's ``compat="equals"`` behaviour: a
    ``float32`` array and a ``float64`` array with the same values compare
    equal.

    NaN and NaT positions are treated as equal when they occur at identical
    positions in both operands. Non-numeric kinds (object, string, bytes)
    use strict equality — there is no NaN concept for those kinds.

    Args:
        a: First array to compare.
        b: Second array to compare.

    Returns:
        True if the two arrays are equal under the rules above.

    Examples:
        >>> import numpy as np
        >>> arrays_equal_missing_aware(
        ...     np.array([1.0, 2.0], dtype=np.float32),
        ...     np.array([1.0, 2.0], dtype=np.float64),
        ... )
        True
        >>> arrays_equal_missing_aware(
        ...     np.array([1.0, float("nan")]),
        ...     np.array([1.0, float("nan")]),
        ... )
        True
    """
    if a.shape != b.shape:
        return False
    try:
        common = np.promote_types(a.dtype, b.dtype)
    except TypeError:
        return False
    try:
        a_cast = a.astype(common, copy=False)
        b_cast = b.astype(common, copy=False)
    except (TypeError, ValueError):
        return False
    kind = common.kind
    if kind in ("f", "c"):
        return bool(np.array_equal(a_cast, b_cast, equal_nan=True))
    if kind in ("M", "m"):
        a_nat = np.isnat(a_cast)
        b_nat = np.isnat(b_cast)
        if not np.array_equal(a_nat, b_nat):
            return False
        mask = ~a_nat
        return bool(np.array_equal(a_cast[mask], b_cast[mask]))
    return bool(np.array_equal(a_cast, b_cast))


def _normalise_for_attr_compare(v: Any) -> Any:
    if isinstance(v, (tuple, list)):
        return [_normalise_for_attr_compare(x) for x in v]
    if isinstance(v, dict):
        return {k: _normalise_for_attr_compare(x) for k, x in v.items()}
    return v


@dataclass(frozen=True)
class GroupAttrsDiff:
    """Result of comparing two group-attribute dicts.

    Attributes:
        added: Attribute keys present in ``incoming`` but not in ``stored``.
        removed: Attribute keys present in ``stored`` but not in ``incoming``.
        changed: Attribute keys present in both dicts with differing values.
    """

    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[str, ...]

    @property
    def is_empty(self) -> bool:
        """Return True when ``stored`` and ``incoming`` attrs are identical."""
        return not (self.added or self.removed or self.changed)


def group_attrs_diff(
    stored: Mapping[str, Any],
    incoming: Mapping[str, Any],
) -> GroupAttrsDiff:
    """Compare two group-attribute dicts, reporting added/removed/changed keys.

    No volatile-attr filter is applied — the engine treats group attributes as
    generic first-write-wins for every key. Value comparison uses
    :func:`arrays_equal_missing_aware` for :class:`numpy.ndarray` values and
    plain ``==`` otherwise. Any equality that raises falls back to "changed"
    to remain conservative (drift is more useful reported than silently
    dropped).

    Args:
        stored: Attribute dict currently persisted on the target group.
        incoming: Attribute dict the runtime intends to write.

    Returns:
        A :class:`GroupAttrsDiff` describing the differences between the two
        dicts.

    Examples:
        >>> d = group_attrs_diff({"title": "A"}, {"title": "A", "history": "x"})
        >>> d.added
        ('history',)
        >>> d.is_empty
        False
    """
    stored_keys = set(stored)
    incoming_keys = set(incoming)
    added = tuple(sorted(incoming_keys - stored_keys))
    removed = tuple(sorted(stored_keys - incoming_keys))
    changed: list[str] = []
    for key in sorted(stored_keys & incoming_keys):
        sv, iv = stored[key], incoming[key]
        if isinstance(sv, np.ndarray) and isinstance(iv, np.ndarray):
            equal = arrays_equal_missing_aware(sv, iv)
        else:
            try:
                equal = bool(_normalise_for_attr_compare(sv) == _normalise_for_attr_compare(iv))
            except Exception:
                equal = False
        if not equal:
            changed.append(key)
    return GroupAttrsDiff(added=added, removed=removed, changed=tuple(changed))


def touched_chunks_for_slice(
    *,
    array_shape: tuple[int, ...],
    chunk_shape: tuple[int, ...],
    region: dict[int, slice],
) -> list[tuple[int, ...]]:
    """Return chunk-grid index tuples touched by a region write.

    For each dimension present in ``region``, the touched chunk range is
    ``[start // chunk_size, ceil(stop / chunk_size))`` clamped to the number
    of chunks along that dimension. For dimensions absent from ``region``,
    every chunk along that dimension is included.

    Pure numeric helper — no dependency on ``zarr.Array``. Callers wanting
    per-chunk keys should apply their own store-key convention.

    Args:
        array_shape: Full array shape.
        chunk_shape: Chunk shape aligned with ``array_shape``.
        region: Mapping of axis index to write slice for that axis. A
            missing axis is treated as "all chunks on this axis".

    Returns:
        List of chunk-grid index tuples (``ndim``-tuples of ``int``) covering
        every chunk touched by the region write.

    Examples:
        >>> touched_chunks_for_slice(
        ...     array_shape=(10,), chunk_shape=(5,), region={0: slice(0, 3)}
        ... )
        [(0,)]
        >>> sorted(
        ...     touched_chunks_for_slice(
        ...         array_shape=(10,), chunk_shape=(2,), region={0: slice(1, 4)}
        ...     )
        ... )
        [(0,), (1,)]
    """
    ndim = len(array_shape)
    ranges: list[range] = []
    for dim in range(ndim):
        chunk_size = chunk_shape[dim]
        if chunk_size <= 0:
            raise ValueError(f"chunk_shape[{dim}] must be positive; got {chunk_size!r}")
        n_chunks = math.ceil(array_shape[dim] / chunk_size)
        if dim in region:
            sl = region[dim]
            start_elem = sl.start if sl.start is not None else 0
            stop_elem = sl.stop if sl.stop is not None else array_shape[dim]
            start = start_elem // chunk_size
            stop = math.ceil(stop_elem / chunk_size)
            ranges.append(range(start, min(stop, n_chunks)))
        else:
            ranges.append(range(n_chunks))
    return list(itertools.product(*ranges))


def chunk_by_chunk_equal(
    target: zarr.Array,
    incoming: np.ndarray | zarr.Array,
    *,
    chunk_shape: tuple[int, ...] | None = None,
) -> bool:
    """Compare two arrays chunk by chunk, bounded to one chunk at a time.

    Uses :func:`arrays_equal_missing_aware` on each chunk and short-circuits
    on the first mismatch. For sharded targets, zarr may decode a whole
    shard behind the scenes; memory stays bounded by the shard/chunk codec
    behaviour rather than the full array size.

    Args:
        target: ``zarr.Array`` currently persisted on the target store. The
            array's declared chunk shape is used unless ``chunk_shape`` is
            explicitly passed.
        incoming: In-memory ``numpy.ndarray`` or another ``zarr.Array``
            being compared against ``target``. Must be shape-compatible.
        chunk_shape: Optional override for the iteration chunk shape.
            Defaults to ``target.chunks``.

    Returns:
        True when every chunk in ``target`` compares equal to the same
        region in ``incoming`` under :func:`arrays_equal_missing_aware`.

    Examples:
        >>> import numpy as np, tempfile, zarr
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     a = zarr.open_array(
        ...         f"{tmp}/a.zarr", mode="w", shape=(4,), dtype="f8", chunks=(2,)
        ...     )
        ...     b = zarr.open_array(
        ...         f"{tmp}/b.zarr", mode="w", shape=(4,), dtype="f8", chunks=(2,)
        ...     )
        ...     _ = a.__setitem__(slice(None), np.arange(4.0))
        ...     _ = b.__setitem__(slice(None), np.arange(4.0))
        ...     chunk_by_chunk_equal(a, b)
        True
    """
    shape = tuple(target.shape)
    if tuple(getattr(incoming, "shape", ())) != shape:
        return False
    if chunk_shape is None:
        cs = tuple(int(c) for c in target.chunks)
    else:
        cs = chunk_shape
    ndim = len(shape)
    if len(cs) != ndim:
        raise ValueError(f"chunk_shape length {len(cs)} does not match array ndim {ndim}")
    grid_shape = tuple(math.ceil(shape[i] / cs[i]) for i in range(ndim))
    for chunk_idx in itertools.product(*[range(g) for g in grid_shape]):
        slices = chunk_index_to_region(chunk_idx, cs, shape)
        t_chunk = np.asarray(target[slices])
        i_chunk = np.asarray(incoming[slices])
        if not arrays_equal_missing_aware(t_chunk, i_chunk):
            return False
    return True
