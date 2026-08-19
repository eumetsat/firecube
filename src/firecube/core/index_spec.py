"""Declarative index specifications for DirectZarrIngestor parallelism.

Plugin authors declare an ``IndexSpec`` describing the time-axis structure of
their product. The engine resolves it once into a ``ResolvedIndex`` (see
``firecube.core.index_resolve``) and caches it for the run.

Only ``RegularTimeAxis`` is currently shipped. Additional axis types are
planned as additive extensions in future releases.
"""

from __future__ import annotations

from collections.abc import Hashable, Mapping
from dataclasses import dataclass, field
from numbers import Integral
from typing import Any, Literal

from firecube.core.slot_index import (
    epoch_s_to_iso,
    iso_to_epoch_s,
    normalize_epoch_iso,
)


class AxisSpec:
    """Marker base for axis specifications.

    ``RegularTimeAxis`` is the only current implementation. Additional axis
    types are planned as additive extensions in future releases. The resolver
    in ``index_resolve.py`` dispatches on ``isinstance`` -- no abstract methods
    are required here.
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
        end: Optional UTC-explicit ISO 8601 string for the axis end (exclusive).
            Must be aligned to the cadence and strictly after ``epoch``.
            At most one of ``end`` and ``size`` may be set.
        size: Optional number of slots. Must be positive.
            At most one of ``end`` and ``size`` may be set.

    Note:
        Both ``end`` and ``size`` may be ``None`` for serial-mode plugins that
        do not declare a fixed horizon. The parallel gate will raise
        ``ConfigurationError`` if it cannot determine the extent.
    """

    coordinate: str
    """Name of the time coordinate dimension."""

    epoch: str
    """UTC-explicit ISO 8601 epoch string."""

    cadence_s: int
    """Slot cadence in seconds (positive integer)."""

    mode: Literal["exact", "floor"] = "exact"
    """Alignment mode: ``"exact"`` or ``"floor"``."""

    end: str | None = None
    """Optional UTC-explicit ISO 8601 end string (exclusive, cadence-aligned)."""

    size: int | None = None
    """Optional number of slots (positive integer)."""

    def __post_init__(self) -> None:
        if not isinstance(self.cadence_s, Integral) or isinstance(self.cadence_s, bool):
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

        if self.end is not None and self.size is not None:
            raise ValueError(
                "at most one of end/size may be set; "
                "leave both None for serial-mode without a fixed horizon"
            )

        if self.end is not None:
            end_s = _utc_explicit_epoch_s(self.end, "end")
            if end_s <= epoch_s:
                raise ValueError(
                    f"end must be strictly after epoch; epoch={self.epoch!r}, end={self.end!r}"
                )
            remainder = (end_s - epoch_s) % self.cadence_s
            if remainder != 0:
                floor_count = (end_s - epoch_s) // self.cadence_s
                nearest_lo = epoch_s_to_iso(epoch_s + floor_count * self.cadence_s)
                nearest_hi = epoch_s_to_iso(epoch_s + (floor_count + 1) * self.cadence_s)
                raise ValueError(
                    f"end is not aligned to cadence_s={self.cadence_s}; "
                    f"nearest aligned boundaries are {nearest_lo!r} and {nearest_hi!r}"
                )

        if self.size is not None and self.size <= 0:
            raise ValueError(f"size must be positive; got {self.size}")


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
        ...     size=1008,
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

    All fields are optional. The engine uses ``coordinate`` to map the item
    to a time-slot index; ``key`` and ``group`` are available for plugins that
    need to route items to specific groups or use a non-default key.

    Args:
        coordinate: The item's time coordinate value (e.g. a UTC ISO string,
            ``datetime``, or ``numpy.datetime64``). Used by the resolver to
            compute ``ts_index``.
        key: Optional hashable key for deduplication or routing.
        group: Optional group name override. When set, the engine routes this
            item to the named group only.
    """

    coordinate: Any = None
    """Time coordinate value for slot-index resolution."""

    key: Hashable | None = None
    """Optional hashable key for deduplication or routing."""

    group: str | None = None
    """Optional group name override."""


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
