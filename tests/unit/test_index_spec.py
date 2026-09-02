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

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from firecube.core.api import AUTO, IrregularTimeAxis, RegularTimeAxis
from firecube.core.index_spec import _canonical_coordinate_value
from firecube.ingestor.api import AUTO as INGESTOR_AUTO


def test_irregular_time_axis_accepts_auto() -> None:
    axis = IrregularTimeAxis(coordinate="time", values=AUTO)

    assert axis.coordinate == "time"
    assert axis.values is AUTO


def test_irregular_time_axis_accepts_concrete_sequence() -> None:
    axis = IrregularTimeAxis(coordinate="obs_time", values=[1, 2, 3])

    assert axis.coordinate == "obs_time"
    assert axis.values == (1, 2, 3)


def test_irregular_time_axis_rejects_empty_sequence() -> None:
    try:
        IrregularTimeAxis(coordinate="time", values=[])
    except ValueError as exc:
        assert "non-empty" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("empty sequence was accepted")


def test_irregular_time_axis_rejects_duplicate_values() -> None:
    try:
        IrregularTimeAxis(coordinate="time", values=[1, 1])
    except ValueError as exc:
        assert "duplicates" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("duplicate values were accepted")


def test_irregular_time_axis_normalizes_list_to_tuple() -> None:
    axis = IrregularTimeAxis(coordinate="time", values=["2024-01-01T00:00:00Z"])

    assert axis.values == ("2024-01-01T00:00:00Z",)


def test_regular_time_axis_rejects_zero_slot_count() -> None:
    with pytest.raises(ValueError, match="slot_count must be positive"):
        RegularTimeAxis(
            coordinate="time",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=600,
            mode="floor",
            slot_count=0,
        )


def test_auto_is_singleton_across_facades() -> None:
    assert AUTO is INGESTOR_AUTO


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2024, 1, 1, 12, 0, 0), "2024-01-01T12:00:00Z"),
        (
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            "2024-01-01T12:00:00Z",
        ),
        (
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5))),
            "2024-01-01T17:00:00Z",
        ),
        ("2024-01-01T12:00:00Z", "2024-01-01T12:00:00Z"),
        (42, 42),
    ],
)
def test_canonical_coordinate_value_timezone_canonicalization(
    value: object,
    expected: object,
) -> None:
    assert _canonical_coordinate_value(value) == expected
