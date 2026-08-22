"""Declarative index specifications for DirectZarrIngestor parallelism.

Plugin authors declare an ``IndexSpec`` describing the time-axis structure of
their product. The engine resolves it once into a ``ResolvedIndex`` (see
``firecube.core.index_resolve``) and caches it for the run.

``RegularTimeAxis`` and ``IntegerAxis`` are currently shipped. Additional axis
types are planned as additive extensions in future releases.
"""

from __future__ import annotations

import numbers
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from firecube.core.slot_index import (
    epoch_s_to_iso,
    iso_to_epoch_s,
    normalize_epoch_iso,
)


class AxisSpec:
    """Marker base for axis specifications.

    ``RegularTimeAxis`` and ``IntegerAxis`` are the current implementations.
    Additional axis types are planned as additive extensions in future
    releases. The resolver in ``index_resolve.py`` dispatches on
    ``isinstance`` -- no abstract methods are required here.
    """


@dataclass(frozen=True, kw_only=True)
class RegularTimeAxis(AxisSpec):
    """A regularly-spaced time axis with a fixed epoch and cadence.

    Args:
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


@dataclass(frozen=True, kw_only=True)
class IntegerAxis(AxisSpec):
    """A zero-based integer axis with a fixed slot count.

    Args:
        slot_count: Total number of integer positions on the axis
            (positive integer). Sets the shape of the axis dimension
            in the preallocated Zarr store: arrays indexed by this
            axis are created with shape ``(slot_count, ...)``.
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
class IndexSpec:
    """Declarative index specification for a multi-group DirectZarr product.

    Args:
        name: Stable identifier for this index configuration. Used as the
            ``SlotIndexModel.name`` for byte-identity checks.
        groups: Mapping from group name to axis specification. Must be non-empty.
            Normalized to a sorted tuple internally for hashability.
        time_unit: Optional time unit string forwarded to the legacy
            ``SlotIndexModel`` unchanged (e.g. ``None`` for the default).

    Examples:
        >>> axis = RegularTimeAxis(
        ...     coordinate="time",
        ...     epoch="2024-01-01T00:00:00Z",
        ...     cadence_s=600,
        ...     end_date="2024-01-08T00:00:00Z",  # or slot_count=1008
        ... )
        >>> spec = IndexSpec(name="my_product_v1", groups={"data": axis})
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

    Args:
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
