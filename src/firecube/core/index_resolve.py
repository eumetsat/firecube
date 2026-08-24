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

"""Pure resolvers for declarative index specifications.

This module is I/O-free and has no imports from ``firecube.ingestor``.
The engine binding (``ingestor/runtime/index_binding.py``) wraps these
resolvers with caching and context threading.
"""

from __future__ import annotations

import datetime as dt
import functools
import numbers
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast, runtime_checkable

import numpy as np

from firecube.core.controlplane.types import (
    ItemManifestEntry,
    ResolvedIndexRecord,
    compute_resolved_index_identity_hash,
)
from firecube.core.index_spec import (
    AUTO,
    AxisSpec,
    IndexSpec,
    IntegerAxis,
    IrregularTimeAxis,
    RegularTimeAxis,
    _canonical_coordinate_value,
)
from firecube.core.slot_index import (
    SlotAxis,
    SlotIndexModel,
    epoch_s_to_iso,
    iso_to_epoch_s,
    normalize_epoch_iso,
)


class ExtentUnknownError(ValueError):
    """Raised when a regular axis has no fixed extent (end and size are both None).

    The parallel gate catches this and re-raises as ``ConfigurationError``
    naming the group and the missing field.
    """


@runtime_checkable
class AxisResolver(Protocol):
    """Protocol satisfied by all per-axis resolvers.

    ``RegularTimeResolver`` is the only shipped implementation. Additional resolvers
    like an integer resolver or an irregular resolver could also satisfy it --
    dispatch is via ``_resolver_for``.
    """

    @property
    def size(self) -> int:
        """Total number of slots on this axis."""
        ...

    def position(self, coordinate: Any) -> int:
        """Map a coordinate value to its zero-based slot index."""
        ...

    def coordinate(self, index: int) -> Any:
        """Map a slot index back to its coordinate value."""
        ...


def coerce_to_epoch_s(value: Any, *, mode: str = "floor") -> int:
    """Coerce a coordinate value to seconds since the Unix epoch (UTC).

    Accepts the following types:

    - ``str``: UTC-explicit ISO 8601 string (via ``iso_to_epoch_s``).
    - ``datetime.datetime``: naive treated as UTC (D6 default; FCI pattern);
      aware converted to UTC.
    - ``numpy.datetime64``: any unit, converted to seconds.
    - ``pandas.Timestamp``: naive treated as UTC; aware converted to UTC.

    Args:
        value: The coordinate value to coerce.
        mode: ``"floor"`` (default) or ``"exact"``. In ``"exact"`` mode,
            fractional seconds raise ``ValueError``.

    Returns:
        Integer seconds since the Unix epoch (UTC).

    Raises:
        TypeError: If ``value`` is not one of the accepted types.
        ValueError: In ``"exact"`` mode, if the value has sub-second precision.
    """
    import pandas as pd  # lazy import: keep at function scope to defer import cost

    if isinstance(value, str):
        return iso_to_epoch_s(value)

    if isinstance(value, pd.Timestamp):
        if value.tz is None:
            value = value.tz_localize("UTC")
        else:
            value = value.tz_convert("UTC")
        ts = value.timestamp()
        if mode == "exact" and ts != int(ts):
            raise ValueError(
                f"coordinate {value!r} has sub-second precision; "
                "use mode='floor' or provide a whole-second value"
            )
        return int(ts)

    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            # D6 default: naive datetime treated as UTC (FCI pattern)
            value = value.replace(tzinfo=dt.UTC)
        else:
            value = value.astimezone(dt.UTC)
        ts = value.timestamp()
        if mode == "exact" and ts != int(ts):
            raise ValueError(
                f"coordinate {value!r} has sub-second precision; "
                "use mode='floor' or provide a whole-second value"
            )
        return int(ts)

    if isinstance(value, np.datetime64):
        return int(value.astype("datetime64[s]").astype("int64"))

    raise TypeError(
        "coordinate must be str, datetime, numpy.datetime64, or pandas.Timestamp; "
        f"got {type(value).__name__!r}"
    )


@dataclass(frozen=True)
class RegularTimeResolver:
    """Resolver for a ``RegularTimeAxis``.

    Satisfies the ``AxisResolver`` Protocol. Cached properties avoid
    repeated epoch parsing.

    Args:
        axis: The ``RegularTimeAxis`` specification to resolve.
    """

    axis: RegularTimeAxis

    @functools.cached_property
    def _epoch_s(self) -> int:
        """Epoch in seconds since Unix epoch."""
        return iso_to_epoch_s(self.axis.epoch)

    @property
    def size(self) -> int:
        """Total number of slots.

        Raises:
            ExtentUnknownError: If neither ``end_date`` nor ``slot_count`` is
                set on the axis.
        """
        if self.axis.slot_count is not None:
            return self.axis.slot_count
        if self.axis.end_date is not None:
            end_s = iso_to_epoch_s(self.axis.end_date)
            return (end_s - self._epoch_s) // self.axis.cadence_s
        raise ExtentUnknownError(
            "regular axis has no fixed extent: set either end_date or slot_count "
            "to enable parallel ingestion"
        )

    def position(self, coordinate: Any) -> int:
        """Map a coordinate value to its zero-based slot index.

        Args:
            coordinate: The coordinate value (datetime, ISO string, etc.).

        Returns:
            Zero-based slot index.

        Raises:
            ValueError: If the coordinate predates the epoch, or (in exact
                mode) is not cadence-aligned.
        """
        ts_s = coerce_to_epoch_s(coordinate, mode=self.axis.mode)
        epoch_s = self._epoch_s
        if ts_s < epoch_s:
            raise ValueError(f"coordinate {coordinate!r} predates epoch {self.axis.epoch!r}")
        raw = ts_s - epoch_s
        index, rem = divmod(raw, self.axis.cadence_s)
        if self.axis.mode == "exact" and rem != 0:
            raise ValueError(
                f"coordinate {coordinate!r} is not cadence-aligned "
                f"(mode=exact, cadence={self.axis.cadence_s}s; "
                "nearest slot boundaries: "
                f"{epoch_s_to_iso(epoch_s + index * self.axis.cadence_s)!r}, "
                f"{epoch_s_to_iso(epoch_s + (index + 1) * self.axis.cadence_s)!r})"
            )
        return int(index)

    def coordinate(self, index: int) -> np.datetime64:
        """Map a slot index to its coordinate value.

        Args:
            index: Zero-based slot index.

        Returns:
            ``numpy.datetime64`` in seconds resolution.
        """
        return np.datetime64(self._epoch_s + index * self.axis.cadence_s, "s")


@dataclass(frozen=True)
class IntegerResolver:
    """Resolver for an ``IntegerAxis``.

    Satisfies the ``AxisResolver`` Protocol. Coordinates and indexes are the
    same zero-based integral values.

    Args:
        axis: The ``IntegerAxis`` specification to resolve.
    """

    axis: IntegerAxis

    @property
    def size(self) -> int:
        """Total number of slots on this axis."""
        return self.axis.slot_count

    def position(self, coordinate: Any) -> int:
        """Map an integer coordinate value to its zero-based slot index.

        Args:
            coordinate: The integral coordinate value.

        Returns:
            The zero-based slot index.

        Raises:
            TypeError: If ``coordinate`` is not an integral type, or is bool.
            IndexError: If ``coordinate`` is outside ``[0, size)``.
        """
        return self._validate_integral_coordinate(coordinate)

    def coordinate(self, index: int) -> int:
        """Map a slot index back to its integer coordinate value.

        Args:
            index: The integral zero-based slot index.

        Returns:
            The integer coordinate value.

        Raises:
            TypeError: If ``index`` is not an integral type, or is bool.
            IndexError: If ``index`` is outside ``[0, size)``.
        """
        return self._validate_integral_coordinate(index)

    def _validate_integral_coordinate(self, coordinate: Any) -> int:
        if isinstance(coordinate, bool) or not isinstance(coordinate, numbers.Integral):
            raise TypeError(
                "coordinate must be an integral type "
                "(Python int or numpy.integer subclass); bool is explicitly rejected. "
                f"Got: {type(coordinate).__name__}({coordinate!r})"
            )
        coord = int(coordinate)
        if coord < 0 or coord >= self.axis.slot_count:
            raise IndexError(f"coordinate {coord} out of range [0, {self.axis.slot_count})")
        return coord


@dataclass(frozen=True)
class IrregularTimeResolver:
    """Resolver for an ``IrregularTimeAxis``.

    Coordinates map to explicit axis values.
    """

    axis: IrregularTimeAxis

    def _values(self) -> tuple[Any, ...]:
        values = self.axis.values
        if values is AUTO:
            raise ExtentUnknownError("irregular axis has no explicit values")
        return tuple(cast(Sequence[Any], values))

    @property
    def size(self) -> int:
        return len(self._values())

    def position(self, coordinate: Any) -> int:
        # O(n) scan; acceptable for hundreds of items. For tens of thousands,
        # replace with a cached dict[value, index] lookup.
        try:
            return self._values().index(coordinate)
        except ValueError as exc:
            raise ValueError(f"coordinate {coordinate!r} is not present in axis values") from exc

    def coordinate(self, index: int) -> Any:
        return self._values()[index]


def _resolver_for(axis: AxisSpec) -> AxisResolver:
    """Dispatch an axis specification to its resolver.

    This is the single extension point for new axis kinds. Additional axis
    types are planned as additive extensions in future releases.

    Args:
        axis: An ``AxisSpec`` instance.

    Returns:
        An ``AxisResolver`` for the given axis.

    Raises:
        NotImplementedError: If the axis kind is not supported in this version.
    """
    if isinstance(axis, RegularTimeAxis):
        return RegularTimeResolver(axis=axis)
    if isinstance(axis, IrregularTimeAxis):
        return IrregularTimeResolver(axis=axis)
    if isinstance(axis, IntegerAxis):
        return IntegerResolver(axis=axis)
    raise NotImplementedError(
        f"No resolver for axis type {type(axis).__name__!r}; "
        "currently supported: RegularTimeAxis, IntegerAxis. "
        "Additional axis types are planned as additive extensions in future releases."
    )


class ResolvedIndex:
    """A resolved, immutable index for a multi-group DirectZarr product.

    Built by ``resolve_index_spec``; cached per ``(id(ctx._ctx), spec)``
    on the ``DirectZarrIngestor`` instance.

    The ``identity_hash`` is content-addressed from the canonical
    resolved-index payload. It intentionally does not track the legacy
    slot-index model hash.
    """

    def __init__(
        self,
        spec: IndexSpec,
        resolvers: dict[str, AxisResolver],
        *,
        items: Sequence[ItemManifestEntry] | None = None,
    ) -> None:
        self._spec = spec
        self._resolvers = resolvers
        self._groups: tuple[str, ...] = tuple(sorted(resolvers))
        self.items: tuple[ItemManifestEntry, ...] | None = (
            tuple(items) if items is not None else None
        )

    @property
    def groups(self) -> tuple[str, ...]:
        """Sorted tuple of group names."""
        return self._groups

    def canonical_index_payload(self) -> dict[str, Any]:
        """Return the canonical resolved-index payload."""

        groups: dict[str, dict[str, Any]] = {}
        for group in self._groups:
            axis = self._spec.groups[group]
            resolver = self._resolvers[group]
            if isinstance(axis, RegularTimeAxis):
                params: dict[str, Any] = {
                    "epoch": axis.epoch,
                    "cadence_s": axis.cadence_s,
                    "mode": axis.mode,
                }
                if axis.end_date is not None:
                    params["end_date"] = axis.end_date
                groups[group] = {
                    "kind": "regular_time",
                    "size": resolver.size,
                    "params": params,
                }
                continue
            if isinstance(axis, IntegerAxis):
                groups[group] = {"kind": "integer", "size": resolver.size, "params": {}}
                continue
            if isinstance(axis, IrregularTimeAxis):
                values = axis.values
                if values is AUTO:
                    raise ExtentUnknownError("irregular axis has no explicit values")
                groups[group] = {
                    "kind": "irregular_time",
                    "size": resolver.size,
                    "params": {
                        "coordinate": axis.coordinate,
                        "values": [
                            _canonical_coordinate_value(value)
                            for value in cast(Sequence[Any], values)
                        ],
                    },
                }
                continue
            raise NotImplementedError(
                f"No canonical resolved-index payload for axis type {type(axis).__name__!r}"
            )

        return {
            "schema_version": "v1",
            "name": self._spec.name,
            "groups": groups,
        }

    def as_resolved_index_record(
        self, *, run_id: str, recorded_at: str | None = None
    ) -> ResolvedIndexRecord:
        """Build the on-disk resolved-index record for this resolved spec."""

        if recorded_at is None:
            recorded_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
        index = self.canonical_index_payload()
        identity_hash = compute_resolved_index_identity_hash(index, self.items)
        return ResolvedIndexRecord(
            schema_version="v1",
            recorded_at=recorded_at,
            recorded_by_run_id=run_id,
            identity_hash=identity_hash,
            index=index,
            items=self.items,
        )

    @functools.cached_property
    def identity_hash(self) -> str:
        """Content-addressed hash of the canonical resolved-index payload."""

        return compute_resolved_index_identity_hash(self.canonical_index_payload(), self.items)

    def size(self, group: str) -> int:
        """Total number of slots for the given group.

        Args:
            group: Group name.

        Returns:
            Total slot count.

        Raises:
            KeyError: If the group is not in this index.
            ExtentUnknownError: If the axis has no fixed extent.
        """
        return self._resolvers[group].size

    def position(self, group: str, coordinate: Any) -> int:
        """Map a coordinate to its slot index within the given group.

        Args:
            group: Group name.
            coordinate: The coordinate value.

        Returns:
            Zero-based slot index.
        """
        return self._resolvers[group].position(coordinate)

    def coordinate(self, group: str, index: int) -> Any:
        """Map a slot index to its coordinate value within the given group.

        Args:
            group: Group name.
            index: Zero-based slot index.

        Returns:
            Coordinate value (type depends on axis kind).
        """
        return self._resolvers[group].coordinate(index)

    def axis_for(self, group: str) -> AxisSpec | None:
        """Return the axis spec for the named group, or None if not present."""

        return self._spec.groups.get(group)

    def as_legacy_slot_index_model(self) -> SlotIndexModel | None:
        """Build a ``SlotIndexModel`` for byte-parity with existing cubes.

        Returns non-None ONLY when every axis is a ``RegularTimeAxis``.
        Mixed-kind specs return ``None``; persistence for those
        specs is handled by a future release.

        Returns:
            A ``SlotIndexModel`` byte-identical to what the legacy mixin
            produced, or ``None`` for non-regular specs.
        """
        groups: dict[str, SlotAxis] = {}
        for group in self._groups:
            axis = self._spec.groups[group]
            if not isinstance(axis, RegularTimeAxis):
                return None
            groups[group] = SlotAxis(cadence_s=axis.cadence_s, mode=axis.mode)

        # Use the first group's axis for epoch (all regular axes share the same
        # epoch -- validated by resolve_index_spec).
        first_axis = next(
            axis for axis in self._spec.groups.values() if isinstance(axis, RegularTimeAxis)
        )
        return SlotIndexModel(
            name=self._spec.name,
            epoch=normalize_epoch_iso(first_axis.epoch),
            groups=groups,
            time_unit=self._spec.time_unit,
        )


def resolve_index_spec(
    spec: IndexSpec,
    *,
    time_dim_name: str,
    items: Sequence[ItemManifestEntry] | None = None,
) -> ResolvedIndex:
    """Resolve an ``IndexSpec`` into a ``ResolvedIndex``.

    Validates that each ``RegularTimeAxis.coordinate`` matches
    ``time_dim_name``. Builds per-group resolvers via ``_resolver_for``.

    Args:
        spec: The index specification to resolve.
        time_dim_name: The expected time-coordinate dimension name
            (e.g. ``"time"`` or ``"timestamp"``).

    Returns:
        A ``ResolvedIndex`` ready for slot-index computation.

    Raises:
        ConfigurationError: If any axis coordinate does not match
            ``time_dim_name``.
        NotImplementedError: If any axis kind is not supported.
    """
    from firecube.core.errors import ConfigurationError

    resolvers: dict[str, AxisResolver] = {}
    for group, axis in spec.groups.items():
        if (
            isinstance(axis, (RegularTimeAxis, IrregularTimeAxis))
            and axis.coordinate != time_dim_name
        ):
            raise ConfigurationError(
                f"Group {group!r}: {type(axis).__name__}.coordinate={axis.coordinate!r} "
                f"does not match time_dim_name={time_dim_name!r}. "
                "Set coordinate to match the plugin's time dimension name."
            )
        resolvers[group] = _resolver_for(axis)

    return ResolvedIndex(spec=spec, resolvers=resolvers, items=items)
