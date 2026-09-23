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

"""Behavioral tests for calendar-declared (non-Gregorian) time axes.

Covers the ``calendar=`` extension to ``RegularTimeAxis`` and
``IrregularTimeAxis`` end to end: axis construction validation, resolver
placement, and the persisted resolved-index payload / identity hash.
"""

from __future__ import annotations

import json

import cftime
import numpy as np
import pytest

from firecube.core.index_resolve import (
    IrregularTimeResolver,
    RegularTimeResolver,
    _compute_group_identity_hash,
    resolve_index_spec,
)
from firecube.core.index_spec import AUTO, IndexSpec, IrregularTimeAxis, RegularTimeAxis

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# RegularTimeAxis validation matrix
# ---------------------------------------------------------------------------


class TestRegularTimeAxisCalendarValidation:
    def test_accepts_valid_calendar_axis_and_normalises_calendar(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_Day",
        )
        assert axis.calendar == "360_day"
        assert axis.encoded_units == "seconds since 2049-01-01 12:00:00"

    def test_calendar_alias_is_normalised(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=1,
            calendar="365_day",
        )
        assert axis.calendar == "noleap"

    def test_calendar_defaults_to_proleptic_gregorian(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time", epoch="2049-01-01T00:00:00Z", cadence_s=600, slot_count=10
        )
        assert axis.calendar == "proleptic_gregorian"
        assert axis.encoded_units == "seconds since 2049-01-01 00:00:00"

    @pytest.mark.parametrize("calendar", ["standard", "gregorian", "proleptic_gregorian"])
    def test_gregorian_like_calendar_is_accepted_and_normalised(self, calendar: str) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=600,
            slot_count=1,
            calendar=calendar,
        )
        assert axis.calendar == "proleptic_gregorian"

    def test_rejects_mode_floor_with_calendar(self) -> None:
        with pytest.raises(ValueError, match='mode="exact"'):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00Z",
                cadence_s=600,
                mode="floor",
                slot_count=1,
                calendar="360_day",
            )

    def test_rejects_end_date_with_calendar(self) -> None:
        with pytest.raises(ValueError, match="slot_count"):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00Z",
                cadence_s=600,
                end_date="2049-01-02T00:00:00Z",
                calendar="360_day",
            )

    def test_regular_calendar_axis_requires_slot_count_early(self) -> None:
        with pytest.raises(ValueError, match="slot_count"):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00Z",
                cadence_s=86400,
                calendar="360_day",
            )

    def test_rejects_epoch_invalid_in_calendar(self) -> None:
        # 360_day months always have 30 days; February 31st cannot exist.
        with pytest.raises(ValueError, match="360_day"):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-02-31T00:00:00Z",
                cadence_s=86400,
                slot_count=1,
                calendar="360_day",
            )

    def test_rejects_unknown_calendar(self) -> None:
        with pytest.raises(ValueError, match="not_a_calendar"):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00Z",
                cadence_s=86400,
                slot_count=1,
                calendar="not_a_calendar",
            )

    def test_still_requires_utc_explicit_epoch_with_calendar(self) -> None:
        with pytest.raises(ValueError, match="UTC-explicit"):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00",
                cadence_s=86400,
                slot_count=1,
                calendar="360_day",
            )

    def test_epoch_invalid_in_gregorian_but_valid_in_360_day_is_accepted(self) -> None:
        # The engine never parses the epoch as a Gregorian date; it is only
        # placed inside the derived `units` string.
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="1850-02-30T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        assert axis.encoded_units == "seconds since 1850-02-30 00:00:00"

    def test_rejects_non_positive_slot_count_with_calendar(self) -> None:
        with pytest.raises(ValueError, match="slot_count must be positive"):
            RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00Z",
                cadence_s=86400,
                slot_count=0,
                calendar="360_day",
            )


# ---------------------------------------------------------------------------
# IrregularTimeAxis validation matrix
# ---------------------------------------------------------------------------


class TestIrregularTimeAxisCalendarValidation:
    def test_units_without_calendar_raises(self) -> None:
        with pytest.raises(ValueError, match="calendar"):
            IrregularTimeAxis(
                coordinate="time",
                values=[cftime.Datetime360Day(2049, 1, 1)],
                units="days since 1850-01-01",
            )

    def test_calendar_without_units_raises(self) -> None:
        with pytest.raises(ValueError, match="units"):
            IrregularTimeAxis(
                coordinate="time",
                values=[cftime.Datetime360Day(2049, 1, 1)],
                calendar="360_day",
            )

    @pytest.mark.parametrize("calendar", ["standard", "gregorian", "proleptic_gregorian"])
    def test_gregorian_like_calendar_is_accepted_and_normalised(self, calendar: str) -> None:
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[cftime.DatetimeGregorian(2024, 1, 1, 0, 0, 0)],
            calendar=calendar,
        )
        assert axis.calendar == "proleptic_gregorian"

    @pytest.mark.parametrize("calendar", ["standard", "gregorian", "proleptic_gregorian"])
    def test_units_on_gregorian_like_calendar_raises(self, calendar: str) -> None:
        with pytest.raises(ValueError, match="units is only used with a non-Gregorian calendar"):
            IrregularTimeAxis(
                coordinate="time",
                values=[cftime.DatetimeGregorian(2024, 1, 1, 0, 0, 0)],
                calendar=calendar,
                units="seconds since 2049-01-01 00:00:00",
            )

    def test_auto_with_calendar_and_units_is_accepted_and_deferred(self) -> None:
        axis = IrregularTimeAxis(
            coordinate="time",
            values=AUTO,
            calendar="360_day",
            units="days since 1850-01-01",
        )
        assert axis.values is AUTO
        assert axis.calendar == "360_day"

    def test_explicit_values_are_canonicalised_to_encoded_numbers(self) -> None:
        units = "seconds since 2049-01-01 12:00:00"
        values = [
            cftime.Datetime360Day(2049, 2, 30, 12, 0, 0),
            cftime.Datetime360Day(2049, 1, 1, 12, 0, 0),
        ]
        axis = IrregularTimeAxis(coordinate="time", values=values, calendar="360_day", units=units)
        assert axis.values == (5097600, 0)  # order preserved, not sorted

    def test_already_encoded_numbers_are_accepted_as_values(self) -> None:
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[0, 86400, 172800],
            calendar="360_day",
            units="seconds since 2049-01-01 00:00:00",
        )
        assert axis.values == (0, 86400, 172800)

    def test_duplicate_detection_operates_on_canonical_values(self) -> None:
        units = "seconds since 2049-01-01 00:00:00"
        # Two distinct cftime instances that encode to the same second are a
        # duplicate on this axis, even though they are not `==` as objects.
        same_instant = [
            cftime.Datetime360Day(2049, 1, 2, 0, 0, 0),
            cftime.Datetime360Day(2049, 1, 2, 0, 0, 0),
        ]
        with pytest.raises(ValueError, match="duplicates"):
            IrregularTimeAxis(
                coordinate="time", values=same_instant, calendar="360_day", units=units
            )

    def test_calendar_default_values_are_unaffected(self) -> None:
        axis = IrregularTimeAxis(coordinate="time", values=[1, 2, 3])
        assert axis.values == (1, 2, 3)
        assert axis.calendar == "proleptic_gregorian"
        assert axis.units is None


# ---------------------------------------------------------------------------
# RegularTimeResolver on a calendar axis
# ---------------------------------------------------------------------------


class TestRegularTimeResolverCalendar:
    @pytest.mark.parametrize("calendar", ["360_day", "noleap", "all_leap"])
    def test_epoch_encodes_to_slot_zero(self, calendar: str) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar=calendar,
        )
        resolver = RegularTimeResolver(axis=axis)
        epoch_value = cftime.num2date(0, units="days since 2049-01-01", calendar=calendar)
        assert resolver.position(epoch_value) == 0

    def test_epoch_invalid_in_gregorian_still_resolves_positions(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="1850-02-30T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        resolver = RegularTimeResolver(axis=axis)
        assert resolver.position(cftime.Datetime360Day(1850, 2, 30, 0, 0, 0)) == 0
        assert resolver.position(cftime.Datetime360Day(1850, 3, 1, 0, 0, 0)) == 1

    def test_real_world_hadgem3_values_resolve_to_documented_slots(self) -> None:
        # repro/06-shaped gate from the design: days since 1850-01-01 values
        # against an axis epoch of 2049-01-01T12:00:00Z resolve to slots
        # 0, 59, 89 -- 2049-01-01, 2049-02-30, 2049-03-30.
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        resolver = RegularTimeResolver(axis=axis)
        values = cftime.num2date(
            [71640.5, 71699.5, 71729.5], units="days since 1850-01-01", calendar="360_day"
        )
        assert [resolver.position(value) for value in values] == [0, 59, 89]

    def test_off_grid_coordinate_raises_value_error(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        resolver = RegularTimeResolver(axis=axis)
        off_grid = cftime.Datetime360Day(2049, 1, 1, 18, 0, 0)  # 6h off a daily grid
        with pytest.raises(ValueError, match="not cadence-aligned"):
            resolver.position(off_grid)

    def test_coordinate_before_epoch_raises_value_error(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        resolver = RegularTimeResolver(axis=axis)
        before_epoch = cftime.Datetime360Day(2048, 12, 30, 12, 0, 0)
        with pytest.raises(ValueError, match="predates epoch"):
            resolver.position(before_epoch)

    def test_coordinate_by_index_returns_int_multiple_of_cadence(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        resolver = RegularTimeResolver(axis=axis)
        assert resolver.coordinate(59) == 59 * 86400
        assert type(resolver.coordinate(59)) is int

    def test_already_encoded_number_resolves_directly(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        resolver = RegularTimeResolver(axis=axis)
        assert resolver.position(2 * 86400) == 2


# ---------------------------------------------------------------------------
# IrregularTimeResolver on a calendar axis
# ---------------------------------------------------------------------------


class TestIrregularTimeResolverCalendar:
    def _resolver(self) -> IrregularTimeResolver:
        units = "seconds since 2049-01-01 00:00:00"
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[
                cftime.Datetime360Day(2049, 1, 1, 0, 0, 0),
                cftime.Datetime360Day(2049, 1, 3, 0, 0, 0),
                cftime.Datetime360Day(2049, 2, 1, 0, 0, 0),
            ],
            calendar="360_day",
            units=units,
        )
        return IrregularTimeResolver(axis=axis)

    def test_explicit_cftime_value_resolves(self) -> None:
        resolver = self._resolver()
        assert resolver.position(cftime.Datetime360Day(2049, 1, 3, 0, 0, 0)) == 1

    def test_already_encoded_number_resolves(self) -> None:
        resolver = self._resolver()
        assert resolver.position(2 * 86400) == 1

    def test_coordinate_by_index_returns_canonical_number(self) -> None:
        resolver = self._resolver()
        assert resolver.coordinate(0) == 0

    def test_unknown_coordinate_raises_value_error(self) -> None:
        resolver = self._resolver()
        with pytest.raises(ValueError, match="not present"):
            resolver.position(cftime.Datetime360Day(2049, 3, 1, 0, 0, 0))


# ---------------------------------------------------------------------------
# Persistence: resolved-index payload and identity hash
# ---------------------------------------------------------------------------


class TestResolvedIndexPayloadCalendar:
    def test_calendar_axis_payload_is_json_dumpable(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        resolved = resolve_index_spec(
            IndexSpec(name="calendar_v1", groups={"data": axis}), time_dim_name="time"
        )
        payload = resolved.canonical_index_payload()
        json.dumps(payload)  # must not raise
        assert payload["groups"]["data"]["params"]["calendar"] == "360_day"
        assert payload["groups"]["data"]["params"]["epoch"] == "2049-01-01T12:00:00Z"
        assert payload["groups"]["data"]["params"]["cadence_s"] == 86400
        assert payload["groups"]["data"]["params"]["mode"] == "exact"

    def test_irregular_calendar_axis_payload_carries_calendar_and_units(self) -> None:
        units = "seconds since 2049-01-01 00:00:00"
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[cftime.Datetime360Day(2049, 1, 1, 0, 0, 0)],
            calendar="360_day",
            units=units,
        )
        resolved = resolve_index_spec(
            IndexSpec(name="irregular_calendar_v1", groups={"data": axis}), time_dim_name="time"
        )
        payload = resolved.canonical_index_payload()
        json.dumps(payload)  # must not raise
        params = payload["groups"]["data"]["params"]
        assert params["calendar"] == "360_day"
        assert params["units"] == units
        assert params["values"] == [0]

    def test_calendar_none_payload_has_no_calendar_key(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=4
        )
        resolved = resolve_index_spec(
            IndexSpec(name="plain_v1", groups={"data": axis}), time_dim_name="time"
        )
        payload = resolved.canonical_index_payload()
        # Exact dict-literal comparison: no `calendar` key sneaks in when unset.
        assert payload == {
            "schema_version": "v1",
            "name": "plain_v1",
            "groups": {
                "data": {
                    "kind": "regular_time",
                    "size": 4,
                    "params": {
                        "epoch": "2024-01-01T00:00:00Z",
                        "cadence_s": 600,
                        "mode": "exact",
                    },
                }
            },
        }

    def test_identity_hash_differs_by_calendar(self) -> None:
        def _resolved(calendar: str) -> str:
            axis = RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T12:00:00Z",
                cadence_s=86400,
                slot_count=90,
                calendar=calendar,
            )
            return resolve_index_spec(
                IndexSpec(name="cal_v1", groups={"data": axis}), time_dim_name="time"
            ).identity_hash

        assert _resolved("360_day") != _resolved("noleap")

    def test_identity_hash_unaffected_when_calendar_unset(self) -> None:
        def _resolved() -> str:
            axis = RegularTimeAxis(
                coordinate="time", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=4
            )
            return resolve_index_spec(
                IndexSpec(name="plain_v1", groups={"data": axis}), time_dim_name="time"
            ).identity_hash

        assert _resolved() == _resolved()

    def test_legacy_slot_index_model_is_none_for_calendar_axis(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        resolved = resolve_index_spec(
            IndexSpec(name="cal_v1", groups={"data": axis}), time_dim_name="time"
        )
        assert resolved.as_legacy_slot_index_model() is None


class TestGroupIdentityHashCalendar:
    def test_regular_group_hash_differs_by_calendar(self) -> None:
        shared_kwargs = {
            "coordinate": "time",
            "epoch": "2049-01-01T12:00:00Z",
            "cadence_s": 86400,
            "slot_count": 90,
        }
        a = RegularTimeAxis(**shared_kwargs, calendar="360_day")
        b = RegularTimeAxis(**shared_kwargs, calendar="noleap")
        assert _compute_group_identity_hash(a, 90, "int64") != _compute_group_identity_hash(
            b, 90, "int64"
        )

    def test_regular_group_hash_survives_a_gregorian_invalid_epoch(self) -> None:
        # This would raise inside `normalize_epoch_iso` if the calendar
        # branch fell back to Gregorian epoch normalisation.
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="1850-02-30T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        digest = _compute_group_identity_hash(axis, 5, "int64")
        assert len(digest) == 64

    def test_irregular_group_hash_differs_by_units(self) -> None:
        # Two axes on the same calendar but a different `units` reference
        # date place the same raw cftime value at a different encoded
        # number; the group hash must tell them apart.
        a = IrregularTimeAxis(
            coordinate="time",
            values=[cftime.Datetime360Day(2049, 1, 1, 0, 0, 0)],
            calendar="360_day",
            units="seconds since 2049-01-01 00:00:00",
        )
        b = IrregularTimeAxis(
            coordinate="time",
            values=[cftime.Datetime360Day(2049, 1, 1, 0, 0, 0)],
            calendar="360_day",
            units="seconds since 2000-01-01 00:00:00",
        )
        assert _compute_group_identity_hash(a, 1, "int64") != _compute_group_identity_hash(
            b, 1, "int64"
        )

    def test_irregular_group_hash_unaffected_when_calendar_unset(self) -> None:
        a = IrregularTimeAxis(coordinate="time", values=(1, 2, 3))
        b = IrregularTimeAxis(coordinate="time", values=(1, 2, 3))
        assert _compute_group_identity_hash(a, 3, "int64") == _compute_group_identity_hash(
            b, 3, "int64"
        )


class TestFilteredSpecPreservesCalendar:
    def test_filtered_spec_carries_calendar_forward(self) -> None:
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        resolved = resolve_index_spec(
            IndexSpec(name="cal_v1", groups={"data": axis}), time_dim_name="time"
        )
        filtered = resolved.filtered_spec(groups=["data"])
        rebuilt_axis = filtered.groups["data"]
        assert isinstance(rebuilt_axis, RegularTimeAxis)
        assert rebuilt_axis.calendar == "360_day"

    def test_filtered_spec_carries_irregular_calendar_and_units_forward(self) -> None:
        units = "seconds since 2049-01-01 00:00:00"
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[cftime.Datetime360Day(2049, 1, 1, 0, 0, 0)],
            calendar="360_day",
            units=units,
        )
        resolved = resolve_index_spec(
            IndexSpec(name="irregular_cal_v1", groups={"data": axis}), time_dim_name="time"
        )
        filtered = resolved.filtered_spec(groups=["data"])
        rebuilt_axis = filtered.groups["data"]
        assert isinstance(rebuilt_axis, IrregularTimeAxis)
        assert rebuilt_axis.calendar == "360_day"
        assert rebuilt_axis.units == units


def test_np_array_of_cftime_objects_round_trips_through_resolver() -> None:
    # Sanity check that numpy object arrays of cftime values (the shape
    # `xarray`/`cftime.num2date` hand back for a vectorised decode) resolve
    # the same way as scalar cftime values do.
    axis = RegularTimeAxis(
        coordinate="time",
        epoch="2049-01-01T00:00:00Z",
        cadence_s=86400,
        slot_count=5,
        calendar="360_day",
    )
    resolver = RegularTimeResolver(axis=axis)
    values = np.array(cftime.num2date([0, 1, 2], units="days since 2049-01-01", calendar="360_day"))
    assert [resolver.position(value) for value in values] == [0, 1, 2]
