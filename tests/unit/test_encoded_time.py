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

"""Behavioral tests for the domain-neutral calendar-time helper module."""

from __future__ import annotations

import datetime as dt

import cftime
import numpy as np
import pytest

from firecube.core.encoded_time import (
    _canonicalise_encoded_number,
    derive_regular_axis_units,
    encode_coordinate,
    is_calendar_valued,
    is_gregorian_like,
    normalise_calendar,
    validate_calendar_units,
)

pytestmark = pytest.mark.unit


class TestNormaliseCalendar:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("360_day", "360_day"),
            ("360_Day", "360_day"),
            ("NoLeap", "noleap"),
            ("365_day", "noleap"),
            ("365_Day", "noleap"),
            ("366_day", "all_leap"),
            ("ALL_LEAP", "all_leap"),
            ("julian", "julian"),
            ("Standard", "proleptic_gregorian"),
            ("GREGORIAN", "proleptic_gregorian"),
        ],
    )
    def test_normalises_case_and_aliases(self, raw: str, expected: str) -> None:
        assert normalise_calendar(raw) == expected


class TestIsGregorianLike:
    @pytest.mark.parametrize(
        "calendar", ["standard", "gregorian", "proleptic_gregorian", "STANDARD", "Gregorian"]
    )
    def test_gregorian_like_names(self, calendar: str) -> None:
        assert is_gregorian_like(calendar) is True

    @pytest.mark.parametrize(
        "calendar", ["360_day", "noleap", "365_day", "all_leap", "366_day", "julian"]
    )
    def test_non_gregorian_names(self, calendar: str) -> None:
        assert is_gregorian_like(calendar) is False


class TestIsCalendarValued:
    def test_cftime_object_is_calendar_valued(self) -> None:
        assert is_calendar_valued(cftime.Datetime360Day(2049, 1, 1)) is True

    @pytest.mark.parametrize(
        "value",
        [
            "2049-01-01T00:00:00Z",
            dt.datetime(2049, 1, 1),
            np.datetime64("2049-01-01"),
            42,
            4.2,
            None,
        ],
    )
    def test_gregorian_and_plain_values_are_not_calendar_valued(self, value: object) -> None:
        assert is_calendar_valued(value) is False


class TestDeriveRegularAxisUnits:
    @pytest.mark.parametrize(
        ("epoch", "expected"),
        [
            ("2049-01-01T12:00:00Z", "seconds since 2049-01-01 12:00:00"),
            ("2049-01-01T12:00:00+00:00", "seconds since 2049-01-01 12:00:00"),
            ("2049-01-01T12:00:00-00:00", "seconds since 2049-01-01 12:00:00"),
            # Never parsed as a date: a day that does not exist in the
            # Gregorian calendar still derives units cleanly.
            ("1850-02-30T00:00:00Z", "seconds since 1850-02-30 00:00:00"),
        ],
    )
    def test_derives_units_from_utc_explicit_epoch(self, epoch: str, expected: str) -> None:
        assert derive_regular_axis_units(epoch) == expected

    @pytest.mark.parametrize("epoch", ["2049-01-01T12:00:00", "not-a-date"])
    def test_rejects_epoch_missing_utc_suffix(self, epoch: str) -> None:
        with pytest.raises(ValueError, match="UTC-explicit"):
            derive_regular_axis_units(epoch)

    @pytest.mark.parametrize("epoch", ["", "  "])
    def test_rejects_empty_epoch(self, epoch: str) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            derive_regular_axis_units(epoch)


class TestValidateCalendarUnits:
    @pytest.mark.parametrize("calendar", ["360_day", "noleap", "all_leap", "julian"])
    def test_accepts_valid_units_and_calendar(self, calendar: str) -> None:
        validate_calendar_units(units="seconds since 2049-01-01 12:00:00", calendar=calendar)

    def test_rejects_unknown_calendar(self) -> None:
        with pytest.raises(ValueError, match="not_a_calendar"):
            validate_calendar_units(
                units="seconds since 2049-01-01 12:00:00", calendar="not_a_calendar"
            )

    def test_rejects_epoch_invalid_in_calendar(self) -> None:
        # 360_day months always have 30 days; 2049-02-31 cannot exist.
        with pytest.raises(ValueError, match="360_day"):
            validate_calendar_units(units="seconds since 2049-02-31 00:00:00", calendar="360_day")


class TestEncodeCoordinate:
    _UNITS = "seconds since 2049-01-01 12:00:00"

    def test_encodes_matching_calendar_valued_object(self) -> None:
        value = cftime.Datetime360Day(2049, 2, 30, 12, 0, 0)
        assert encode_coordinate(value, units=self._UNITS, calendar="360_day") == 5097600

    def test_encodes_epoch_itself_to_zero(self) -> None:
        value = cftime.Datetime360Day(2049, 1, 1, 12, 0, 0)
        assert encode_coordinate(value, units=self._UNITS, calendar="360_day") == 0

    def test_mismatched_calendar_raises_value_error_naming_both(self) -> None:
        noleap_value = cftime.DatetimeNoLeap(2049, 1, 2)
        with pytest.raises(ValueError, match="noleap") as exc_info:
            encode_coordinate(noleap_value, units=self._UNITS, calendar="360_day")
        assert "360_day" in str(exc_info.value)

    def test_alias_365_day_matches_noleap_value(self) -> None:
        value = cftime.DatetimeNoLeap(2049, 1, 2)
        # calendar="365_day" is an alias for "noleap"; a DatetimeNoLeap value
        # (value.calendar == "noleap") must be accepted, not rejected as a
        # calendar mismatch.
        assert (
            encode_coordinate(value, units="seconds since 2049-01-01 00:00:00", calendar="365_day")
            == 86400
        )

    def test_bool_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="bool"):
            encode_coordinate(True, units=self._UNITS, calendar="360_day")

    @pytest.mark.parametrize(
        "value",
        [
            "2049-01-01T00:00:00Z",
            dt.datetime(2049, 1, 1),
            np.datetime64("2049-01-01"),
        ],
    )
    def test_gregorian_shaped_values_raise_type_error(self, value: object) -> None:
        with pytest.raises(TypeError, match="Gregorian"):
            encode_coordinate(value, units=self._UNITS, calendar="360_day")

    def test_pandas_timestamp_raises_type_error(self) -> None:
        import pandas as pd

        with pytest.raises(TypeError, match="Gregorian"):
            encode_coordinate(pd.Timestamp("2049-01-01"), units=self._UNITS, calendar="360_day")

    @pytest.mark.parametrize(
        ("value", "expected", "expected_type"),
        [
            (5097600, 5097600, int),
            (5097600.0, 5097600, int),
            (np.int64(5097600), 5097600, int),
            (np.float64(5097600.5), 5097600.5, float),
            (5097600.5, 5097600.5, float),
        ],
    )
    def test_already_encoded_numbers_pass_through_canonicalised(
        self, value: object, expected: object, expected_type: type
    ) -> None:
        result = encode_coordinate(value, units=self._UNITS, calendar="360_day")
        assert result == expected
        assert type(result) is expected_type

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_number_raises_value_error(self, value: float) -> None:
        with pytest.raises(ValueError, match="finite"):
            encode_coordinate(value, units=self._UNITS, calendar="360_day")

    def test_real_world_hadgem3_values_resolve_to_expected_seconds(self) -> None:
        # days since 1850-01-01, 360_day calendar; matches the design's
        # empirical gate (repro/06): 2049-01-01, 2049-02-30, 2049-03-30 are
        # 0, 59, and 89 cadence-days after the axis epoch.
        source_units = "days since 1850-01-01"
        decoded = np.array(
            cftime.num2date([71640.5, 71699.5, 71729.5], units=source_units, calendar="360_day")
        )
        encoded = [
            encode_coordinate(value, units=self._UNITS, calendar="360_day") for value in decoded
        ]
        assert encoded == [0, 5097600, 7689600]
        assert [value // 86400 for value in encoded] == [0, 59, 89]

    def test_result_is_json_serialisable(self) -> None:
        import json

        value = cftime.Datetime360Day(2049, 2, 30, 12, 0, 0)
        encoded = encode_coordinate(value, units=self._UNITS, calendar="360_day")
        assert json.dumps(encoded) == "5097600"


@pytest.mark.parametrize(
    ("value", "expected_type_name"),
    [
        (True, "bool"),
        (np.bool_(True), "np.bool_"),
    ],
    ids=["python-bool", "numpy-bool"],
)
def test_encode_coordinate_bool_shape_message_names_type(
    value: object, expected_type_name: str
) -> None:
    """The rejection message names the bool type that was passed.

    ``type(np.bool_(True)).__name__`` is ``"bool"``, so a message built from
    the type name alone cannot tell the two apart; the numpy alias must be
    named explicitly.
    """
    with pytest.raises(TypeError) as excinfo:
        encode_coordinate(
            value,  # type: ignore[arg-type]
            units="seconds since 2049-01-01 00:00:00",
            calendar="360_day",
        )
    assert expected_type_name.lower() in str(excinfo.value).lower(), (
        f"expected {expected_type_name!r} in error message, got: {excinfo.value}"
    )
    # Make sure the OTHER type name does not appear (diagnostic message)
    other = "np.bool_" if expected_type_name == "bool" else "bool"
    if other == "bool":
        # Cannot check for "bool" absence in an "np.bool_" message, since
        # "bool" is a substring of "np.bool_". Skip cross-check for np.bool_.
        return
    assert other not in str(excinfo.value), (
        f"unexpected {other!r} in {expected_type_name} message: {excinfo.value}"
    )


class TestCanonicaliseEncodedNumber:
    @pytest.mark.parametrize("value", [np.bool_(True), np.bool_(False)])
    def test_np_bool_is_rejected(self, value: np.bool_) -> None:
        with pytest.raises(TypeError, match="bool"):
            _canonicalise_encoded_number(value)

    def test_int64_still_passes_through(self) -> None:
        assert _canonicalise_encoded_number(np.int64(42)) == 42
