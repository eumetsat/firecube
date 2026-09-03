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

"""Declarative index specifications for DirectZarrIngestor parallelism.

Plugin authors declare an ``IndexSpec`` describing the time-axis structure of
their product. The engine resolves it once into a ``ResolvedIndex`` (see
``firecube.core.index_resolve``) and caches it for the run.

``RegularTimeAxis``, ``IntegerAxis``, and ``IrregularTimeAxis`` are currently
shipped. Additional axis types are planned as additive extensions in future
releases.
"""

from __future__ import annotations

import datetime as dt
import numbers
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal

import numpy as np

from firecube.core.slot_index import (
    epoch_s_to_iso,
    iso_to_epoch_s,
    normalize_epoch_iso,
)


class AxisSpec:
    """Marker base for axis specifications.

    ``RegularTimeAxis``, ``IntegerAxis``, and ``IrregularTimeAxis`` are the
    current implementations. Additional axis types are planned as additive
    extensions in future releases. The resolver in ``index_resolve.py``
    dispatches on ``isinstance`` -- no abstract methods are required here.
    """


class _AutoSentinel:
    """Sentinel type for the ``AUTO`` discovery mode.

    Pass the module-level ``AUTO`` singleton as ``values`` to
    ``IrregularTimeAxis`` to let the engine discover coordinate values at
    planning time by calling ``inspect_item`` on every source item before
    preallocate. Do not construct this class directly; use ``AUTO``.
    """

    def __repr__(self) -> str:
        return "AUTO"


# Like ``dataclasses.MISSING``. PEP 661 discussed sentinel objects and rejected
# a heavier enum-style approach for this sort of module-level singleton.
AUTO: Final[_AutoSentinel] = _AutoSentinel()
"""Sentinel that tells ``IrregularTimeAxis`` to discover coordinate values at planning time.

Set ``IrregularTimeAxis(coordinate=..., values=AUTO)`` to let the engine call
``inspect_item`` on every source item before preallocate and build the axis
from the returned coordinates. The engine sorts discovered coordinates and
assigns each a slot index in ascending order.

Importable from ``firecube.core.api`` and ``firecube.ingestor.api``.
"""


def _canonical_coordinate_value(value: Any) -> Any:
    if isinstance(value, np.datetime64):
        return np.datetime_as_string(value, unit="ns", timezone="UTC")
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        else:
            value = value.astimezone(dt.UTC)
        return value.isoformat().replace("+00:00", "Z")
    return value


@dataclass(frozen=True, kw_only=True)
class RegularTimeAxis(AxisSpec):
    """A regularly-spaced time axis with a fixed epoch and cadence.

    Attributes:
        coordinate: Name of the time coordinate dimension (e.g. ``"time"``).
        epoch: UTC-explicit ISO 8601 string for the axis origin (e.g.
            ``"2024-01-01T00:00:00Z"``). Naive strings are rejected.
        cadence_s: Slot cadence in seconds. Must be a positive integer.
        mode: Alignment mode. ``"exact"`` requires timestamps to fall exactly
            on slot boundaries; ``"floor"`` maps each timestamp to the nearest
            preceding boundary.
        end_date: Optional UTC-explicit ISO 8601 string for the axis end
            (exclusive). Must be aligned to the cadence and strictly after
            ``epoch``. At most one of ``end_date`` and ``slot_count`` may
            be set.
        slot_count: Optional total number of slots. Must be positive.
            At most one of ``end_date`` and ``slot_count`` may be set.

    Note:
        Both ``end_date`` and ``slot_count`` may be ``None`` for serial-mode
        plugins that do not declare a fixed horizon. The parallel gate will
        raise ``ConfigurationError`` if it cannot determine the extent.
    """

    coordinate: str
    """Name of the time coordinate dimension."""

    epoch: str
    """UTC-explicit ISO 8601 epoch string."""

    cadence_s: int
    """Slot cadence in seconds (positive integer)."""

    mode: Literal["exact", "floor"] = "exact"
    """Alignment mode: ``"exact"`` or ``"floor"``."""

    end_date: str | None = None
    """UTC-explicit ISO 8601 timestamp for the axis end (exclusive).

    The last written slot's left edge is at ``end_date - cadence_s``; the axis
    covers the half-open interval ``[epoch, end_date)``.

    Must be strictly after ``epoch`` and aligned to ``cadence_s`` (i.e.
    ``(end_date - epoch)`` must be a whole multiple of ``cadence_s``).
    Misalignment raises ``ValueError`` with the two nearest aligned
    boundaries in the error message.

    Mutually exclusive with ``slot_count``. Leave both ``None`` for
    serial-mode plugins without a fixed horizon.
    """

    slot_count: int | None = None
    """Total number of slots on the axis (positive integer).

    Equivalent to ``(end_date - epoch) // cadence_s`` when ``end_date`` is
    provided instead; the two spellings are algebraically identical, pick
    whichever expresses the product horizon most naturally.

    Sets the shape of the time dimension in the preallocated Zarr store:
    time-indexed arrays are created with shape ``(slot_count, ...)``.

    Mutually exclusive with ``end_date``. Leave both ``None`` for
    serial-mode plugins without a fixed horizon.
    """

    def __post_init__(self) -> None:
        if not isinstance(self.cadence_s, numbers.Integral) or isinstance(self.cadence_s, bool):
            raise TypeError(
                "cadence_s must be an integral type (int or numpy.integer); "
                f"got {type(self.cadence_s).__name__!r}"
            )
        if self.cadence_s <= 0:
            raise ValueError(f"cadence_s must be positive; got {self.cadence_s}")

        valid_modes = {"exact", "floor"}
        if self.mode not in valid_modes:
            raise ValueError(f"mode must be one of {sorted(valid_modes)!r}; got {self.mode!r}")

        epoch_s = _utc_explicit_epoch_s(self.epoch, "epoch")

        if self.end_date is not None and self.slot_count is not None:
            raise ValueError(
                "at most one of end_date/slot_count may be set; "
                "leave both None for serial-mode without a fixed horizon"
            )

        if self.end_date is not None:
            end_s = _utc_explicit_epoch_s(self.end_date, "end_date")
            if end_s <= epoch_s:
                raise ValueError(
                    f"end_date must be strictly after epoch; "
                    f"epoch={self.epoch!r}, end_date={self.end_date!r}"
                )
            remainder = (end_s - epoch_s) % self.cadence_s
            if remainder != 0:
                floor_count = (end_s - epoch_s) // self.cadence_s
                nearest_lo = epoch_s_to_iso(epoch_s + floor_count * self.cadence_s)
                nearest_hi = epoch_s_to_iso(epoch_s + (floor_count + 1) * self.cadence_s)
                raise ValueError(
                    f"end_date is not aligned to cadence_s={self.cadence_s}; "
                    f"nearest aligned boundaries are {nearest_lo!r} and {nearest_hi!r}"
                )

        if self.slot_count is not None and self.slot_count <= 0:
            raise ValueError(f"slot_count must be positive; got {self.slot_count}")


def effective_regular_time_policy(axis: RegularTimeAxis) -> Literal["grid", "observed"]:
    """Return the stored-coordinate-values policy for a regular time axis.

    Derives ``"grid"`` from ``mode="exact"`` and ``"observed"`` from
    ``mode="floor"``. Engine code should call this instead of reading
    ``axis.mode`` when deciding whether stored time-coordinate values are
    knowable before ingest.
    """
    return "grid" if axis.mode == "exact" else "observed"


class TimeAxis:
    """Intent-named constructors for time-axis declarations.

    The four constructors cover every supported time-axis shape and are the
    recommended way to declare one; each returns a plain axis dataclass, so
    the explicit ``RegularTimeAxis`` / ``IrregularTimeAxis`` forms remain
    available as an escape hatch.

    Pick by answering one question -- what defines your product's timeline?

    * A fixed cadence, and the coordinate should carry the grid labels:
      :meth:`grid`.
    * A fixed cadence, but the coordinate should carry each slot's real
      observation time: :meth:`observed`.
    * A known list of timestamps with no fixed cadence: :meth:`explicit`.
    * Timestamps only your source items can reveal: :meth:`discovered`.

    Examples:
        >>> axis = TimeAxis.observed(
        ...     coordinate="time",
        ...     epoch="2025-07-01T00:00:00Z",
        ...     cadence_s=600,
        ...     slot_count=4320,
        ... )
        >>> spec = IndexSpec(name="my_product_v1", groups={"data": axis})
    """

    @staticmethod
    def grid(
        *,
        coordinate: str,
        epoch: str,
        cadence_s: int,
        slot_count: int | None = None,
        end_date: str | None = None,
        placement: Literal["exact", "floor"] = "exact",
    ) -> RegularTimeAxis:
        """A fixed-cadence axis whose coordinate carries the grid labels.

        The coordinate values are ``epoch + n * cadence_s``, known before
        ingest, so the engine materializes and seals them at preallocate.
        Item timestamps must sit exactly on grid boundaries; off-grid
        observation times need :meth:`observed` instead, because sealed grid
        values cannot pass drift verification against off-grid writes.

        Args:
            coordinate: Name of the time coordinate dimension.
            epoch: UTC-explicit ISO 8601 axis origin.
            cadence_s: Slot cadence in seconds (positive integer).
            slot_count: Total slots; mutually exclusive with ``end_date``.
            end_date: Exclusive UTC-explicit axis end; mutually exclusive
                with ``slot_count``. Leave both ``None`` for serial mode.
            placement: Must be ``"exact"``; ``"floor"`` raises ``ValueError``
                (use :meth:`observed` for floor placement).
        """
        if placement == "floor":
            raise ValueError(
                "TimeAxis.grid(placement='floor') is not supported: "
                "the floor+grid combination cannot pass drift verification. "
                "Use TimeAxis.observed() for floor placement with real observation times."
            )
        return RegularTimeAxis(
            coordinate=coordinate,
            epoch=epoch,
            cadence_s=cadence_s,
            mode=placement,
            slot_count=slot_count,
            end_date=end_date,
        )

    @staticmethod
    def observed(
        *,
        coordinate: str,
        epoch: str,
        cadence_s: int,
        slot_count: int | None = None,
        end_date: str | None = None,
    ) -> RegularTimeAxis:
        """A fixed-cadence axis whose coordinate carries real observation times.

        Items are placed on the grid by flooring, but the coordinate stores
        each slot's actual observation time (for example a sensing start such
        as ``00:00:02`` in a ten-minute cadence). Those values only exist once
        the source items are known, so the engine writes them in a
        single-writer materialization step; ingest verifies them.

        Args:
            coordinate: Name of the time coordinate dimension.
            epoch: UTC-explicit ISO 8601 axis origin.
            cadence_s: Slot cadence in seconds (positive integer).
            slot_count: Total slots; mutually exclusive with ``end_date``.
            end_date: Exclusive UTC-explicit axis end; mutually exclusive
                with ``slot_count``. Leave both ``None`` for serial mode.
        """
        return RegularTimeAxis(
            coordinate=coordinate,
            epoch=epoch,
            cadence_s=cadence_s,
            mode="floor",
            slot_count=slot_count,
            end_date=end_date,
        )

    @staticmethod
    def explicit(*, coordinate: str, values: Sequence[Any]) -> IrregularTimeAxis:
        """An axis whose timeline is a known, explicit list of timestamps.

        Use when the product has no fixed cadence but the full timeline is
        known when the plugin is configured. The engine sorts the values
        ascending, assigns slot indices in that order, and materializes the
        coordinate at preallocate. The rest of the plugin is unchanged:
        ``resolved_index(ctx).position(group, timestamp)`` maps each value to
        its declared slot.

        Args:
            coordinate: Name of the time coordinate dimension.
            values: Non-empty sequence of distinct coordinate values
                (``datetime``, ``numpy.datetime64``, or integers). Strings,
                bytes, duplicates, and empty input are rejected.

        Examples:
            >>> import numpy as np
            >>> axis = TimeAxis.explicit(
            ...     coordinate="timestamp",
            ...     values=(
            ...         np.datetime64("2026-01-01T00:00:00", "ns"),
            ...         np.datetime64("2026-01-01T00:17:30", "ns"),
            ...         np.datetime64("2026-01-01T00:42:00", "ns"),
            ...     ),
            ... )
        """
        return IrregularTimeAxis(coordinate=coordinate, values=values)

    @staticmethod
    def discovered(*, coordinate: str) -> IrregularTimeAxis:
        """An axis whose timeline is discovered from the source items.

        Use when only the inputs can reveal the timestamps. Before
        preallocate the engine calls ``inspect_item`` on every discovered
        source item, sorts the coordinates ascending, and freezes the
        resulting axis. That costs one full source pass at planning time.
        The declaration is ``IrregularTimeAxis(values=AUTO)``, the spelling
        error messages use.

        Discovery makes ``inspect_item`` load-bearing:

        * Return ``ItemInfo(coordinate=timestamp)`` for every item that
          belongs on the axis.
        * Return ``None`` to skip an item entirely.
        * Return ``ItemInfo(coordinate=None)`` only when the item exists but
          its coordinate cannot be resolved; discovery fails with
          ``MissingIrregularCoordinateError`` for it.

        ``inspect_item`` runs during discovery and again during the write
        phase, so it must be idempotent and independent of discovery order,
        and items must resolve through references that stay valid for the
        run because the discovered axis is handed to parallel workers.
        Duplicate coordinates raise ``DuplicateIrregularCoordinateError``;
        zero discovered items raise ``NoDiscoveredItemsError``.

        Args:
            coordinate: Name of the time coordinate dimension.
        """
        return IrregularTimeAxis(coordinate=coordinate, values=AUTO)


@dataclass(frozen=True, kw_only=True)
class IntegerAxis(AxisSpec):
    """A zero-based integer axis with a fixed slot count.

    Declares an integer position with no epoch or cadence. ``inspect_item``
    returns the integer position as the item's coordinate, and
    ``resolved_index(ctx).position(group, key)`` maps it to the slot index.
    A single ``IndexSpec`` may mix ``IntegerAxis`` and time-axis groups;
    each group resolves its own axis independently.

    Attributes:
        slot_count: Total number of integer positions on the axis
            (positive integer). Sets the shape of the axis dimension
            in the preallocated Zarr store: arrays indexed by this
            axis are created with shape ``(slot_count, ...)``.

    Examples:
        >>> spec = IndexSpec(
        ...     name="my_mixed_product_v1",
        ...     groups={
        ...         "data": TimeAxis.observed(
        ...             coordinate="timestamp",
        ...             epoch="2024-01-01T00:00:00Z",
        ...             cadence_s=600,
        ...             end_date="2024-01-08T00:00:00Z",
        ...         ),
        ...         "lookup": IntegerAxis(slot_count=64),
        ...     },
        ... )
    """

    slot_count: int
    """Total number of integer positions on the axis (positive integer)."""

    def __post_init__(self) -> None:
        if isinstance(self.slot_count, bool) or not isinstance(self.slot_count, numbers.Integral):
            raise TypeError(
                "slot_count must be an integral type (Python int or numpy.integer subclass); "
                f"bool is explicitly rejected. Got: {type(self.slot_count).__name__}"
                f"({self.slot_count!r})"
            )
        object.__setattr__(self, "slot_count", int(self.slot_count))
        if self.slot_count < 1:
            raise ValueError(f"slot_count must be >= 1, got {self.slot_count!r}")


@dataclass(frozen=True, kw_only=True)
class IrregularTimeAxis(AxisSpec):
    """A time axis with explicit coordinate values.

    Attributes:
        coordinate: Name of the time coordinate dimension. Must match
            ``time_dim_name`` when the axis is resolved.
        values: Explicit coordinate values for the axis, or ``AUTO`` to
            discover them later. Accepts any non-empty ``Sequence`` of
            hashable comparable values, typically ``datetime`` objects,
            ``numpy.datetime64`` values, or integers. Duplicate values raise
            ``ValueError``. A non-``Sequence`` raises ``TypeError``. Empty input
            raises ``ValueError``. The engine sorts concrete values ascending
            and assigns slot indices in that order.

    Note:
        ``AUTO`` triggers planning-time discovery: the engine scans the full
        source set via ``inspect_item`` before preallocate so it can discover
        coordinates and sort them. That costs a full source pass.
    """

    coordinate: str
    """Name of the time coordinate dimension."""

    values: Sequence[Any] | _AutoSentinel
    """Explicit coordinate values, or ``AUTO`` for planning-time discovery."""

    def __post_init__(self) -> None:
        if self.values is AUTO:
            return
        if isinstance(self.values, (str, bytes)):
            raise TypeError("values must be a sequence of coordinate values, not a string or bytes")
        if not isinstance(self.values, Sequence):
            raise TypeError("values must be AUTO or a non-empty Sequence")
        if not self.values:
            raise ValueError("values must be a non-empty Sequence")
        if len(set(self.values)) != len(self.values):
            raise ValueError("values must not contain duplicates")
        object.__setattr__(self, "values", tuple(self.values))


@dataclass(frozen=True, kw_only=True)
class IndexSpec:
    """Declarative index specification for a multi-group DirectZarr product.

    When ``groups`` has more than one entry and the ingestion caller does not
    pass ``slot_group`` to name one authoritative group, every group must
    reference the same axis object (Python ``is`` identity, not value
    equality): slot-range filtering operates on one canonical axis, and two
    structurally equal axes give the engine no way to pick one. Separately
    constructed axes with identical fields are rejected with
    ``ConfigurationError`` at bind time; share the instance instead, or pass
    ``--slot-group`` on the command line.

    Attributes:
        name: Stable identifier for this index configuration. Used as the
            ``SlotIndexModel.name`` for byte-identity checks.
        groups: Mapping from group name to axis specification. Must be non-empty.
            Normalized to a sorted tuple internally for hashability.
        time_unit: Optional time unit string forwarded to the legacy
            ``SlotIndexModel`` unchanged (e.g. ``None`` for the default).

    Examples:
        >>> axis = RegularTimeAxis(
        ...     coordinate="timestamp",
        ...     epoch="2024-01-01T00:00:00Z",
        ...     cadence_s=600,
        ...     end_date="2024-01-08T00:00:00Z",  # or slot_count=1008
        ... )
        >>> spec = IndexSpec(
        ...     name="my_product_v1",
        ...     groups={"data_1km": axis, "data_2km": axis},  # same object
        ... )
    """

    name: str
    """Stable identifier for this index configuration."""

    groups: Mapping[str, AxisSpec]
    """Mapping from group name to axis specification."""

    time_unit: str | None = None
    """Optional time unit forwarded to the legacy SlotIndexModel."""

    _groups_tuple: tuple[tuple[str, AxisSpec], ...] = field(
        init=False, repr=False, compare=False, hash=False
    )

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("IndexSpec.name must be non-empty")
        if not self.groups:
            raise ValueError("IndexSpec.groups must be non-empty")
        object.__setattr__(self, "_groups_tuple", tuple(sorted(self.groups.items())))

    def __hash__(self) -> int:
        return hash((self.name, self._groups_tuple, self.time_unit))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IndexSpec):
            return NotImplemented
        return (
            self.name == other.name
            and self._groups_tuple == other._groups_tuple
            and self.time_unit == other.time_unit
        )


@dataclass(frozen=True, kw_only=True)
class ItemInfo:
    """Metadata returned by ``inspect_item`` describing a single source item.

    The engine uses ``coordinate`` to map the item to a position on its
    assigned axis.

    Attributes:
        coordinate: The item's coordinate value. For ``RegularTimeAxis`` this
            is a UTC-explicit timestamp. For ``IntegerAxis`` this is an integer
            coordinate.
    """

    coordinate: Any = None
    """Coordinate value used by the resolver to compute a position."""


def _utc_explicit_epoch_s(value: str, field_name: str) -> int:
    """Validate a UTC-explicit ISO timestamp and return whole epoch seconds."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty UTC ISO string")
    text = value.strip()
    if not (text.endswith("Z") or text.endswith("+00:00") or text.endswith("-00:00")):
        raise ValueError(
            f"{field_name} must be UTC-explicit ('Z', '+00:00', or '-00:00' offset); got {value!r}"
        )
    normalize_epoch_iso(text)
    return iso_to_epoch_s(text)
