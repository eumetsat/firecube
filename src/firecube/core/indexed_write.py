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

"""Coordinate-keyed indexed write intent for :class:`DirectZarrIngestor`.

Defines :class:`IndexedWrite`, a frozen dataclass produced by
``DirectZarrIngestor.build_indexed_write()``. Lives in ``firecube.core``
so both the ``firecube.core.api`` and ``firecube.ingestor.api`` façades
can re-export it without violating the core-below-ingestor layering rule
enforced by ``tests/architecture/test_core_independence.py``.

The compile step from :class:`IndexedWrite` to :class:`WriteIntent` is
ingestor-owned and stays in ``firecube.ingestor.templates.direct_zarr``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True, kw_only=True)
class IndexedWrite:
    """A coordinate-keyed write intent whose slot index is resolved at compile time.

    Produced by ``DirectZarrIngestor.build_indexed_write()`` to describe a
    write whose target slot is expressed as a raw coordinate value (typically
    a ``datetime``, ``numpy.datetime64``, or integer) rather than a pre-resolved
    integer index. The engine maps ``coordinate`` to a slot index at compile
    time against the plugin's declared ``IndexSpec``, then materializes an
    equivalent ``WriteIntent``.

    Use the :meth:`region` classmethod for 2-D spatial region writes and
    :meth:`slot` for 1-D per-slot writes. These are the only two builders by
    design — there is intentionally no ``.static()`` or ``.coordinate()``
    factory:

    - Static (non-time-indexed) arrays remain ``WriteIntent.static`` because
      they carry no slot coordinate.
    - Time-coordinate writes are engine-owned; plugins never emit them.

    ``data`` may be an eager ``numpy.ndarray`` or a zero-arg callable
    ``Callable[[], np.ndarray]``. The callable is resolved exactly once at
    dispatch time, in dispatch order, on the same terms as ``WriteIntent``.
    """

    group: str
    """Zarr group name matching a ``ZarrGroupSpec`` in the schema."""

    array: str
    """Array name within the group."""

    coordinate: Any
    """Raw slot key resolved to an integer index at compile time.

    Typically a ``datetime``, ``numpy.datetime64``, or ``int``. The engine
    dispatches the value through the plugin's declared ``IndexSpec``;
    unresolvable coordinates raise at compile time. Must not be ``None``.
    """

    data: np.ndarray | Callable[[], np.ndarray] | Any
    """Array payload, or a zero-arg callable that returns one.

    Callable payloads are resolved exactly once at dispatch time under the
    same rules as ``WriteIntent`` — the callable must close over stable
    inputs (paths, configuration), not open file handles or per-batch scratch.
    """

    y_slice: slice | None = None
    """Row slice within the array; required for :meth:`region`, ``None`` for :meth:`slot`."""

    channel_index: int | None = None
    """Channel dimension index for :meth:`region` writes, ``None`` otherwise."""

    _kind: str = "region"
    """Private discriminator selecting region vs slot compile behavior.

    Set by the :meth:`region` / :meth:`slot` classmethods. Not part of the
    public constructor contract — pass builders, not raw kwargs.
    """

    def __post_init__(self) -> None:
        if self.coordinate is None:
            raise ValueError("coordinate must not be None")
        if self.y_slice is not None and not isinstance(self.y_slice, slice):
            raise ValueError(
                f"y_slice must be a slice or None, got {type(self.y_slice).__name__!r}"
            )

    @classmethod
    def region(
        cls,
        *,
        group: str,
        array: str,
        coordinate: Any,
        data: np.ndarray | Callable[[], np.ndarray] | Any,
        y_slice: slice,
        channel_index: int | None = None,
    ) -> IndexedWrite:
        """Build an indexed 2-D spatial region write.

        Use for the main image arrays — counts, radiances, quality flags,
        pixel times — where each slot contributes a spatial tile. The array
        must be declared with ``time_indexed=True`` in ``ZarrArraySpec``.

        Args:
            group: Zarr group name matching a ``ZarrGroupSpec`` in the schema.
            array: Array name within the group.
            coordinate: Raw slot key (e.g. ``datetime``, ``numpy.datetime64``,
                or ``int``). Resolved to a slot index by the engine at compile
                time. Must not be ``None``.
            data: 2-D array data, or a zero-arg callable that returns it.
            y_slice: Row slice within the array.
            channel_index: Channel dimension index, or ``None`` for
                non-channel arrays.

        Returns:
            An :class:`IndexedWrite` with ``_kind="region"``.

        Examples:
            >>> import numpy as np
            >>> from datetime import datetime, timezone
            >>> iw = IndexedWrite.region(
            ...     group="data", array="counts",
            ...     coordinate=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ...     data=np.zeros((100, 2048)), y_slice=slice(0, 100),
            ... )
            >>> iw.y_slice
            slice(0, 100, None)
        """
        return cls(
            group=group,
            array=array,
            coordinate=coordinate,
            data=data,
            y_slice=y_slice,
            channel_index=channel_index,
            _kind="region",
        )

    @classmethod
    def slot(
        cls,
        *,
        group: str,
        array: str,
        coordinate: Any,
        data: np.ndarray | Callable[[], np.ndarray] | Any,
    ) -> IndexedWrite:
        """Build an indexed 1-D per-slot write.

        Use for 1-D arrays that grow along the time axis — per-slot scalars,
        per-slot vectors, or any array where each slot contributes one row.
        The array must be declared with ``time_indexed=True`` in
        ``ZarrArraySpec``.

        Args:
            group: Zarr group name matching a ``ZarrGroupSpec`` in the schema.
            array: Array name within the group.
            coordinate: Raw slot key (e.g. ``datetime``, ``numpy.datetime64``,
                or ``int``). Resolved to a slot index by the engine at compile
                time. Must not be ``None``.
            data: 1-D array payload, or a zero-arg callable that returns it.

        Returns:
            An :class:`IndexedWrite` with ``_kind="slot"``.

        Examples:
            >>> import numpy as np
            >>> from datetime import datetime, timezone
            >>> iw = IndexedWrite.slot(
            ...     group="data", array="counts",
            ...     coordinate=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ...     data=np.zeros((4,)),
            ... )
            >>> iw.array
            'counts'
        """
        return cls(
            group=group,
            array=array,
            coordinate=coordinate,
            data=data,
            _kind="slot",
        )
