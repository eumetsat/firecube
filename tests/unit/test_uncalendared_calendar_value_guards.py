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

"""Guards for a calendar-valued coordinate handed to an axis without ``calendar=``.

Two end-to-end runs surfaced confusing failures for this mistake:

* A ``RegularTimeAxis`` without ``calendar=`` fed a raw ``cftime`` object
  failed deep inside `coerce_to_epoch_s` with a ``TypeError`` that gave no
  hint the fix was to declare a calendar (see ``firecube.core.index_resolve``).
* An ``IrregularTimeAxis`` without ``calendar=`` fed ``cftime`` objects (an
  explicit sequence, or discovered via ``AUTO``) accepted them silently and
  only failed much later, at preallocate, with an opaque
  ``TypeError: Object of type Datetime360Day is not JSON serializable``.

Both are now loud at the point of first contact. Gregorian-like
calendar-valued objects (``cftime.DatetimeGregorian`` and friends) are
unaffected in either case -- they keep converting to Gregorian time exactly
as before.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import cftime
import numpy as np
import pytest

from firecube.core.index_resolve import RegularTimeResolver, coerce_to_epoch_s
from firecube.core.index_spec import AUTO, IndexSpec, IrregularTimeAxis, ItemInfo, RegularTimeAxis
from firecube.ingestor.runtime.index_binding import resolve_index_spec_for_ingestor

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# A1: RegularTimeAxis without calendar=, calendar-valued coordinate.
# ---------------------------------------------------------------------------


class TestCoerceToEpochSCalendarGuidance:
    def test_non_gregorian_cftime_value_gets_calendar_guidance(self) -> None:
        value = cftime.Datetime360Day(2049, 2, 30, 0, 0, 0)
        with pytest.raises(TypeError) as excinfo:
            coerce_to_epoch_s(value)
        message = str(excinfo.value)
        # The original sentence is kept verbatim...
        assert "coordinate must be str, datetime, numpy.datetime64, or pandas.Timestamp" in message
        assert "got 'Datetime360Day'" in message
        # ...with guidance appended naming the value's calendar.
        assert "calendar" in message and "360_day" in message
        assert "declare calendar=" in message

    def test_gregorian_cftime_value_also_gets_guidance(self) -> None:
        # DatetimeGregorian duck-types as calendar-valued too; it is not
        # accepted by coerce_to_epoch_s either (only str/datetime/
        # datetime64/Timestamp are), so it gets the same guidance.
        value = cftime.DatetimeGregorian(2024, 1, 1, 0, 0, 0)
        with pytest.raises(TypeError, match="declare calendar="):
            coerce_to_epoch_s(value)

    def test_plain_unsupported_type_keeps_original_message_unchanged(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            coerce_to_epoch_s(12345)
        message = str(excinfo.value)
        assert message == (
            "coordinate must be str, datetime, numpy.datetime64, or pandas.Timestamp; got 'int'"
        )
        assert "calendar" not in message

    def test_regular_time_resolver_position_surfaces_same_guidance(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
        )
        resolver = RegularTimeResolver(axis=axis)
        value = cftime.DatetimeNoLeap(2049, 1, 2, 0, 0, 0)
        with pytest.raises(ValueError, match="declare calendar="):
            resolver.position(value)


# ---------------------------------------------------------------------------
# A2: IrregularTimeAxis without calendar=, calendar-valued coordinates.
# ---------------------------------------------------------------------------


class TestIrregularTimeAxisUncalendaredExplicitValues:
    def test_non_gregorian_cftime_values_raise_at_construction(self) -> None:
        with pytest.raises(ValueError) as excinfo:
            IrregularTimeAxis(
                coordinate="time",
                values=[
                    cftime.Datetime360Day(2049, 1, 1, 0, 0, 0),
                    cftime.Datetime360Day(2049, 1, 3, 0, 0, 0),
                ],
            )
        message = str(excinfo.value)
        assert "360_day" in message
        assert "calendar=" in message
        assert "units=" in message

    def test_noleap_cftime_values_raise_at_construction(self) -> None:
        with pytest.raises(ValueError, match="noleap"):
            IrregularTimeAxis(
                coordinate="time",
                values=[cftime.DatetimeNoLeap(2049, 1, 1, 0, 0, 0)],
            )

    def test_gregorian_like_cftime_values_keep_working(self) -> None:
        # Today's behaviour, unchanged: a Gregorian-like cftime object is not
        # rejected here -- it is out of scope for this guard (the write-path
        # guard in firecube.core.zarr._calendar_guard converts it normally).
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[
                cftime.DatetimeGregorian(2024, 1, 1, 0, 0, 0),
                cftime.DatetimeGregorian(2024, 1, 2, 0, 0, 0),
            ],
        )
        assert axis.calendar == "proleptic_gregorian"
        assert len(cast(tuple, axis.values)) == 2

    def test_plain_datetime64_values_still_work(self) -> None:
        # Control: ordinary datetime64 values (no calendar involved at all)
        # are unaffected by the new duck-typed check.
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[
                np.datetime64("2024-01-01T00:00:00", "ns"),
                np.datetime64("2024-01-02T00:00:00", "ns"),
            ],
        )
        assert axis.calendar == "proleptic_gregorian"


# ---------------------------------------------------------------------------
# A2: AUTO discovery, no calendar declared.
# ---------------------------------------------------------------------------


class _AutoIngestor:
    def __init__(self, spec: IndexSpec, coordinates: dict[str, Any]) -> None:
        self._spec = spec
        self._coordinates = coordinates

    def index_spec(self, ctx: Any) -> IndexSpec:
        return self._spec

    def _resolve_time_dim_name(self) -> str:
        return "time"

    def discover_source_files(self, ctx: Any) -> list[Any]:
        return list(self._coordinates)

    def filter_item(self, item: Any, ctx: Any) -> bool:
        return True

    def inspect_item(self, item: Any, ctx: Any) -> ItemInfo | None:
        return ItemInfo(coordinate=self._coordinates[item])


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(source="source")


class TestAutoDiscoveryUncalendaredValues:
    def test_non_gregorian_cftime_discovered_value_raises(self) -> None:
        spec = IndexSpec(
            name="auto_uncalendared_v1",
            groups={"data": IrregularTimeAxis(coordinate="time", values=AUTO)},
        )
        ingestor = _AutoIngestor(
            spec,
            {"a": cftime.Datetime360Day(2049, 1, 1, 0, 0, 0)},
        )
        with pytest.raises(ValueError) as excinfo:
            resolve_index_spec_for_ingestor(ingestor, _ctx())
        message = str(excinfo.value)
        assert "360_day" in message
        assert "calendar=" in message
        assert "units=" in message

    def test_gregorian_like_cftime_discovered_value_keeps_working(self) -> None:
        spec = IndexSpec(
            name="auto_gregorian_v1",
            groups={"data": IrregularTimeAxis(coordinate="time", values=AUTO)},
        )
        ingestor = _AutoIngestor(
            spec,
            {"a": cftime.DatetimeGregorian(2024, 1, 1, 0, 0, 0)},
        )
        binding = resolve_index_spec_for_ingestor(ingestor, _ctx())
        assert binding is not None
        assert binding.resolved.size("data") == 1
