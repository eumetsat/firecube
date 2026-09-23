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

"""DirectZarr fixture plugins for calendar-declared time axes.

Every ingestor here uses pure in-memory integer items (no network I/O, no
private data). The payload written for item ``i`` is the number ``i`` itself
(as a ``float32``), so a test can prove placement purely by reading the
``data/values`` array back from the store: ``values[slot] == i`` means item
``i`` landed at ``slot``.

Ingestors:

* ``CalendarAxisRegularIngestor`` (``calendar_axis_regular``) -- a regular
  daily ``RegularTimeAxis`` whose ``calendar`` and ``slot_count`` are plugin
  options (defaults ``"360_day"`` / ``90``). Hands raw ``cftime`` objects to
  ``inspect_item`` / ``IndexedWrite.region``. Setting ``calendar`` to a
  Gregorian-like name (e.g. ``"proleptic_gregorian"``) hands plain
  ``datetime.datetime`` objects instead, exercising the axis as an
  explicitly Gregorian-declared one rather than an unset one.
* ``CalendarAxisRegularEncodedIngestor`` (``calendar_axis_regular_encoded``)
  -- identical axis, but hands already-encoded integers (seconds since the
  axis epoch) instead of ``cftime`` objects.
* ``CalendarAxisIrregularExplicitIngestor`` (``calendar_axis_irregular_explicit``)
  -- an ``IrregularTimeAxis`` with explicit ``cftime`` values that have gaps
  (non-uniform day offsets), declared in ascending order.
* ``CalendarAxisIrregularAutoIngestor`` (``calendar_axis_irregular_auto``) --
  the same gapped timeline, discovered via ``values=AUTO`` with items
  presented out of order; AUTO discovery sorts them back into place.
* ``CalendarAxisWrongCalendarIngestor`` (``calendar_axis_wrong_calendar``) --
  a single item whose coordinate is a ``cftime`` value on a *different*
  calendar than the declared axis.
* ``CalendarAxisGregorianValueIngestor`` (``calendar_axis_gregorian_value``)
  -- a single item whose coordinate is a plain ``datetime.datetime``
  (Gregorian) handed to a calendar axis.
* ``CalendarAxisUncalendaredRegularIngestor``
  (``calendar_axis_uncalendared_regular``) -- a ``RegularTimeAxis`` with NO
  ``calendar`` declared, fed a raw ``cftime`` coordinate (the A1 guard).
* ``CalendarAxisUncalendaredAutoIngestor``
  (``calendar_axis_uncalendared_auto``) -- an AUTO ``IrregularTimeAxis`` with
  NO ``calendar`` declared, discovering a raw ``cftime`` coordinate (the A2
  discovery guard).
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, ClassVar

import cftime
import numpy as np

from firecube.core.api import (
    AUTO,
    IndexedWrite,
    IndexSpec,
    IrregularTimeAxis,
    ItemInfo,
    RegularTimeAxis,
)
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginConfig,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)

_GROUP = "data"
_COORD = "time"
_EPOCH = "2049-01-01T00:00:00Z"
_CADENCE_S = 86400
_Y_ROWS = 1
_X_COLS = 1
_FILL_VALUE = -1.0

# CF names that address plain Gregorian (``datetime64``) time, matching the
# canonical spelling ``RegularTimeAxis`` normalises to. Kept local to this
# fixture rather than imported: a plugin only ever sees the calendar name it
# declared, not firecube's internal normalisation helper.
_GREGORIAN_LIKE_CALENDARS = ("standard", "gregorian", "proleptic_gregorian")
_EPOCH_DT = dt.datetime.fromisoformat(_EPOCH.replace("Z", "+00:00"))

# Ascending day offsets with gaps, shared by the irregular explicit/AUTO
# fixtures: 0, 2, 5, 9, 14 (gaps of 2, 3, 4, 5 days).
_GAP_DAY_OFFSETS: tuple[int, ...] = (0, 2, 5, 9, 14)
_IRREGULAR_UNITS = "days since 2049-01-01 00:00:00"


def _values_group(slot_count: int) -> ZarrGroupSpec:
    return ZarrGroupSpec(
        group=_GROUP,
        arrays=[
            ZarrArraySpec(
                name="values",
                shape=(slot_count, _Y_ROWS, _X_COLS),
                dtype="float32",
                chunks=(1, _Y_ROWS, _X_COLS),
                fill_value=_FILL_VALUE,
                expected_time_count=slot_count,
                time_indexed=True,
                dimension_names=(_COORD, "y", "x"),
            )
        ],
    )


def _payload(item: int) -> np.ndarray:
    return np.array([[float(item)]], dtype=np.float32)


# ---------------------------------------------------------------------------
# Regular axis: cftime and already-encoded variants.
# ---------------------------------------------------------------------------


@dataclass
class CalendarAxisRegularConfig(PluginConfig):
    """Options for the regular calendar-axis fixture.

    Attributes:
        calendar: CF calendar name for the time axis.
        slot_count: Total number of daily slots.
        encoded: When ``True``, hand already-encoded integers (seconds since
            the axis epoch) instead of raw ``cftime`` objects.
    """

    calendar: str = "360_day"
    slot_count: int = 90
    encoded: bool = False


@register_ingestor("calendar_axis_regular")
class CalendarAxisRegularIngestor(DirectZarrIngestor):
    """Regular daily calendar axis; slot count spans a 02-29/02-30 boundary."""

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_regular"
    time_dim_name: ClassVar[str] = _COORD
    plugin_config_class = CalendarAxisRegularConfig

    def _config(self) -> CalendarAxisRegularConfig:
        config = self.plugin_config
        assert isinstance(config, CalendarAxisRegularConfig)
        return config

    def _axis(self) -> RegularTimeAxis:
        config = self._config()
        return RegularTimeAxis(
            coordinate=_COORD,
            epoch=_EPOCH,
            cadence_s=_CADENCE_S,
            slot_count=config.slot_count,
            calendar=config.calendar,
        )

    def _coordinate_for(self, item: int) -> Any:
        axis = self._axis()
        encoded = item * axis.cadence_s
        if self._config().encoded:
            return encoded
        if axis.calendar in _GREGORIAN_LIKE_CALENDARS:
            # An explicitly Gregorian-like ``calendar`` (e.g.
            # "proleptic_gregorian") addresses plain datetime64 time, same as
            # an axis with no calendar declared: hand the axis a plain
            # ``datetime.datetime``, not a ``cftime`` object.
            return _EPOCH_DT + dt.timedelta(seconds=encoded)
        assert axis.encoded_units is not None
        return cftime.num2date(encoded, units=axis.encoded_units, calendar=axis.calendar)

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(self._config().slot_count))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(name="calendar_axis_regular_v1", groups={_GROUP: self._axis()})

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=self._coordinate_for(item))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(self._config().slot_count)]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for item in batch.items:
            assert isinstance(item, int)
            out.append(
                IndexedWrite.region(
                    group=_GROUP,
                    array="values",
                    coordinate=self._coordinate_for(item),
                    data=_payload(item),
                    y_slice=slice(0, _Y_ROWS),
                )
            )
        return out


@dataclass
class _CalendarAxisRegularEncodedConfig(CalendarAxisRegularConfig):
    encoded: bool = True


@register_ingestor("calendar_axis_regular_encoded")
class CalendarAxisRegularEncodedIngestor(CalendarAxisRegularIngestor):
    """Same axis as ``calendar_axis_regular``, but hands already-encoded numbers."""

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_regular_encoded"
    plugin_config_class = _CalendarAxisRegularEncodedConfig


# ---------------------------------------------------------------------------
# Irregular axis: explicit (gaps) and AUTO (gaps, discovered out of order).
# ---------------------------------------------------------------------------


@dataclass
class CalendarAxisIrregularConfig(PluginConfig):
    """Options for the irregular calendar-axis fixtures.

    Attributes:
        calendar: CF calendar name for the time axis.
    """

    calendar: str = "360_day"


def _gap_coordinate(day_offset: int, *, calendar: str) -> Any:
    return cftime.num2date(day_offset, units=_IRREGULAR_UNITS, calendar=calendar)


@register_ingestor("calendar_axis_irregular_explicit")
class CalendarAxisIrregularExplicitIngestor(DirectZarrIngestor):
    """Irregular calendar axis with explicit, gapped ``cftime`` values."""

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_irregular_explicit"
    time_dim_name: ClassVar[str] = _COORD
    plugin_config_class = CalendarAxisIrregularConfig

    def _config(self) -> CalendarAxisIrregularConfig:
        config = self.plugin_config
        assert isinstance(config, CalendarAxisIrregularConfig)
        return config

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(len(_GAP_DAY_OFFSETS)))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        calendar = self._config().calendar
        values = tuple(_gap_coordinate(offset, calendar=calendar) for offset in _GAP_DAY_OFFSETS)
        return IndexSpec(
            name="calendar_axis_irregular_explicit_v1",
            groups={
                _GROUP: IrregularTimeAxis(
                    coordinate=_COORD,
                    values=values,
                    calendar=calendar,
                    units=_IRREGULAR_UNITS,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(
            coordinate=_gap_coordinate(_GAP_DAY_OFFSETS[item], calendar=self._config().calendar)
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(len(_GAP_DAY_OFFSETS))]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for item in batch.items:
            assert isinstance(item, int)
            info = self.inspect_item(item, ctx)
            assert info is not None
            out.append(
                IndexedWrite.region(
                    group=_GROUP,
                    array="values",
                    coordinate=info.coordinate,
                    data=_payload(item),
                    y_slice=slice(0, _Y_ROWS),
                )
            )
        return out


# Discovery order deliberately not ascending: item i's coordinate is at
# _GAP_DAY_OFFSETS[i] (ascending in i), but discover_source_files presents
# items out of order so AUTO's sort-by-coordinate step is load-bearing.
_AUTO_DISCOVERY_ORDER: tuple[int, ...] = (2, 0, 4, 1, 3)


@register_ingestor("calendar_axis_irregular_auto")
class CalendarAxisIrregularAutoIngestor(DirectZarrIngestor):
    """Irregular calendar axis with AUTO-discovered, gapped ``cftime`` values."""

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_irregular_auto"
    time_dim_name: ClassVar[str] = _COORD
    plugin_config_class = CalendarAxisIrregularConfig

    def _config(self) -> CalendarAxisIrregularConfig:
        config = self.plugin_config
        assert isinstance(config, CalendarAxisIrregularConfig)
        return config

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(_AUTO_DISCOVERY_ORDER)

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="calendar_axis_irregular_auto_v1",
            groups={
                _GROUP: IrregularTimeAxis(
                    coordinate=_COORD,
                    values=AUTO,
                    calendar=self._config().calendar,
                    units=_IRREGULAR_UNITS,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(
            coordinate=_gap_coordinate(_GAP_DAY_OFFSETS[item], calendar=self._config().calendar)
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(len(_GAP_DAY_OFFSETS))]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for item in batch.items:
            assert isinstance(item, int)
            info = self.inspect_item(item, ctx)
            assert info is not None
            out.append(
                IndexedWrite.region(
                    group=_GROUP,
                    array="values",
                    coordinate=info.coordinate,
                    data=_payload(item),
                    y_slice=slice(0, _Y_ROWS),
                )
            )
        return out


# ---------------------------------------------------------------------------
# Negative fixtures: wrong calendar, Gregorian value, no calendar declared.
# ---------------------------------------------------------------------------

_NEGATIVE_SLOT_COUNT = 3


@register_ingestor("calendar_axis_wrong_calendar")
class CalendarAxisWrongCalendarIngestor(DirectZarrIngestor):
    """Single item whose ``cftime`` coordinate is on a calendar the axis does not declare.

    The axis declares ``calendar="360_day"``; the item's coordinate is a
    ``cftime.DatetimeNoLeap`` value. Compilation must raise with both
    calendars named in the message.
    """

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_wrong_calendar"
    time_dim_name: ClassVar[str] = _COORD

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [0]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="calendar_axis_wrong_calendar_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    slot_count=_NEGATIVE_SLOT_COUNT,
                    calendar="360_day",
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=cftime.DatetimeNoLeap(2049, 1, 1, 0, 0, 0))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_NEGATIVE_SLOT_COUNT)]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        return [
            IndexedWrite.region(
                group=_GROUP,
                array="values",
                coordinate=cftime.DatetimeNoLeap(2049, 1, 1, 0, 0, 0),
                data=_payload(item),
                y_slice=slice(0, _Y_ROWS),
            )
            for item in batch.items
            if isinstance(item, int)
        ]


@register_ingestor("calendar_axis_gregorian_value")
class CalendarAxisGregorianValueIngestor(DirectZarrIngestor):
    """Single item whose coordinate is a plain (Gregorian) ``datetime`` on a calendar axis."""

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_gregorian_value"
    time_dim_name: ClassVar[str] = _COORD

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [0]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="calendar_axis_gregorian_value_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    slot_count=_NEGATIVE_SLOT_COUNT,
                    calendar="360_day",
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=dt.datetime(2049, 1, 1, tzinfo=dt.UTC))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_NEGATIVE_SLOT_COUNT)]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        return [
            IndexedWrite.region(
                group=_GROUP,
                array="values",
                coordinate=dt.datetime(2049, 1, 1, tzinfo=dt.UTC),
                data=_payload(item),
                y_slice=slice(0, _Y_ROWS),
            )
            for item in batch.items
            if isinstance(item, int)
        ]


@register_ingestor("calendar_axis_uncalendared_regular")
class CalendarAxisUncalendaredRegularIngestor(DirectZarrIngestor):
    """A ``RegularTimeAxis`` with NO ``calendar``, fed a raw ``cftime`` coordinate.

    Exercises the A1 guard in ``firecube.core.index_resolve.coerce_to_epoch_s``:
    the resulting ``TypeError`` must name the value's calendar and tell the
    plugin author to declare ``calendar=``.
    """

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_uncalendared_regular"
    time_dim_name: ClassVar[str] = _COORD

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [0]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="calendar_axis_uncalendared_regular_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    slot_count=_NEGATIVE_SLOT_COUNT,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=cftime.Datetime360Day(2049, 1, 1, 0, 0, 0))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_NEGATIVE_SLOT_COUNT)]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        return [
            IndexedWrite.region(
                group=_GROUP,
                array="values",
                coordinate=cftime.Datetime360Day(2049, 1, 1, 0, 0, 0),
                data=_payload(item),
                y_slice=slice(0, _Y_ROWS),
            )
            for item in batch.items
            if isinstance(item, int)
        ]


@register_ingestor("calendar_axis_uncalendared_auto")
class CalendarAxisUncalendaredAutoIngestor(DirectZarrIngestor):
    """An AUTO ``IrregularTimeAxis`` with NO ``calendar``, discovering a raw ``cftime`` value.

    Exercises the A2 discovery guard in
    ``firecube.ingestor.runtime.index_binding._discover_auto_irregular_axis``.
    """

    PRODUCT_NAME: ClassVar[str] = "calendar_axis_uncalendared_auto"
    time_dim_name: ClassVar[str] = _COORD

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [0]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="calendar_axis_uncalendared_auto_v1",
            groups={_GROUP: IrregularTimeAxis(coordinate=_COORD, values=AUTO)},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=cftime.Datetime360Day(2049, 1, 1, 0, 0, 0))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(1)]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        return [
            IndexedWrite.region(
                group=_GROUP,
                array="values",
                coordinate=cftime.Datetime360Day(2049, 1, 1, 0, 0, 0),
                data=_payload(item),
                y_slice=slice(0, _Y_ROWS),
            )
            for item in batch.items
            if isinstance(item, int)
        ]
