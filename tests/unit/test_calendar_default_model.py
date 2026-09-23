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

"""Contract tests for the default calendar model.

Every time axis has a calendar; ``"proleptic_gregorian"`` is the default
*value*, not the absence of one. ``"standard"`` and ``"gregorian"`` are
aliases normalised to ``"proleptic_gregorian"``. These tests pin the
byte-identity of the default, alias, and explicit-value spellings against
each other, and the acceptance/rejection matrix that differs between
Gregorian-like and non-Gregorian calendars.
"""

from __future__ import annotations

import json

import cftime
import numpy as np
import pytest

from firecube.core.index_resolve import (
    RegularTimeResolver,
    _compute_group_identity_hash,
    resolve_index_spec,
)
from firecube.core.index_spec import IndexSpec, IrregularTimeAxis, RegularTimeAxis

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Default calendar value
# ---------------------------------------------------------------------------


def test_regular_axis_default_calendar_is_proleptic_gregorian() -> None:
    axis = RegularTimeAxis(
        coordinate="time", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=4
    )
    assert axis.calendar == "proleptic_gregorian"


def test_irregular_axis_default_calendar_is_proleptic_gregorian() -> None:
    axis = IrregularTimeAxis(coordinate="time", values=[1, 2, 3])
    assert axis.calendar == "proleptic_gregorian"


# ---------------------------------------------------------------------------
# Default / alias / explicit spellings are byte-identical
# ---------------------------------------------------------------------------


def _regular_axis(**overrides: object) -> RegularTimeAxis:
    kwargs: dict[str, object] = {
        "coordinate": "time",
        "epoch": "2024-01-01T00:00:00Z",
        "cadence_s": 600,
        "slot_count": 4,
    }
    kwargs.update(overrides)
    return RegularTimeAxis(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "calendar_kwargs",
    [
        pytest.param({}, id="default-unset"),
        pytest.param({"calendar": "standard"}, id="alias-standard"),
        pytest.param({"calendar": "gregorian"}, id="alias-gregorian"),
        pytest.param({"calendar": "proleptic_gregorian"}, id="explicit-canonical"),
    ],
)
def test_gregorian_spellings_are_byte_identical(calendar_kwargs: dict[str, object]) -> None:
    default_axis = _regular_axis()
    axis = _regular_axis(**calendar_kwargs)
    assert axis.calendar == "proleptic_gregorian"

    default_resolved = resolve_index_spec(
        IndexSpec(name="cal_default_v1", groups={"data": default_axis}), time_dim_name="time"
    )
    resolved = resolve_index_spec(
        IndexSpec(name="cal_default_v1", groups={"data": axis}), time_dim_name="time"
    )

    default_payload = default_resolved.canonical_index_payload()
    payload = resolved.canonical_index_payload()
    assert json.dumps(payload, sort_keys=True) == json.dumps(default_payload, sort_keys=True)
    assert "calendar" not in payload["groups"]["data"]["params"]

    assert _compute_group_identity_hash(axis, 4, "datetime64[ns]") == _compute_group_identity_hash(
        default_axis, 4, "datetime64[ns]"
    )

    default_legacy = default_resolved.as_legacy_slot_index_model()
    legacy = resolved.as_legacy_slot_index_model()
    assert legacy is not None and default_legacy is not None
    assert legacy.canonical_bytes() == default_legacy.canonical_bytes()


# ---------------------------------------------------------------------------
# Gregorian-like axes accept mode="floor" and end_date
# ---------------------------------------------------------------------------


def test_gregorian_calendar_accepts_mode_floor() -> None:
    axis = RegularTimeAxis(
        coordinate="time",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        mode="floor",
        slot_count=4,
        calendar="proleptic_gregorian",
    )
    assert axis.mode == "floor"


def test_gregorian_calendar_accepts_end_date() -> None:
    axis = RegularTimeAxis(
        coordinate="time",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        end_date="2024-01-01T01:00:00Z",
        calendar="proleptic_gregorian",
    )
    assert axis.end_date == "2024-01-01T01:00:00Z"


# ---------------------------------------------------------------------------
# Gregorian regular axis position() accepts cftime alongside ISO strings
# ---------------------------------------------------------------------------


def test_gregorian_regular_position_accepts_cftime_and_iso_equally() -> None:
    axis = RegularTimeAxis(
        coordinate="time", epoch="2024-01-01T00:00:00Z", cadence_s=3600, slot_count=2000
    )
    resolver = RegularTimeResolver(axis=axis)
    cftime_slot = resolver.position(cftime.DatetimeGregorian(2024, 3, 1, 6))
    iso_slot = resolver.position("2024-03-01T06:00:00Z")
    assert cftime_slot == iso_slot


# ---------------------------------------------------------------------------
# Irregular Gregorian axis: cftime and equivalent datetime64 hash identically
# ---------------------------------------------------------------------------


def test_irregular_gregorian_cftime_matches_equivalent_datetime64() -> None:
    cftime_axis = IrregularTimeAxis(
        coordinate="time", values=[cftime.DatetimeGregorian(2024, 1, 1, 0, 0, 0)]
    )
    datetime64_axis = IrregularTimeAxis(
        coordinate="time", values=[np.datetime64("2024-01-01T00:00:00", "ns")]
    )

    cftime_resolved = resolve_index_spec(
        IndexSpec(name="irr_v1", groups={"data": cftime_axis}), time_dim_name="time"
    )
    datetime64_resolved = resolve_index_spec(
        IndexSpec(name="irr_v1", groups={"data": datetime64_axis}), time_dim_name="time"
    )
    assert cftime_resolved.identity_hash == datetime64_resolved.identity_hash
    assert (
        cftime_resolved.canonical_index_payload() == datetime64_resolved.canonical_index_payload()
    )


# ---------------------------------------------------------------------------
# RegularTimeResolver.coordinate() return type differs by calendar
# ---------------------------------------------------------------------------


def test_regular_coordinate_return_type_differs_by_calendar() -> None:
    gregorian_axis = RegularTimeAxis(
        coordinate="time", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=4
    )
    gregorian_resolver = RegularTimeResolver(axis=gregorian_axis)
    assert isinstance(gregorian_resolver.coordinate(1), np.datetime64)

    calendar_axis = RegularTimeAxis(
        coordinate="time",
        epoch="2049-01-01T00:00:00Z",
        cadence_s=86400,
        slot_count=5,
        calendar="360_day",
    )
    calendar_resolver = RegularTimeResolver(axis=calendar_axis)
    assert type(calendar_resolver.coordinate(1)) is int


# ---------------------------------------------------------------------------
# Non-Gregorian axes keep their existing rejection matrix
# ---------------------------------------------------------------------------


def test_non_gregorian_still_rejects_mode_floor() -> None:
    with pytest.raises(ValueError, match='mode="exact"'):
        RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=600,
            mode="floor",
            slot_count=1,
            calendar="360_day",
        )


def test_non_gregorian_still_rejects_end_date() -> None:
    with pytest.raises(ValueError, match="slot_count"):
        RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=600,
            end_date="2049-01-02T00:00:00Z",
            calendar="360_day",
        )


def test_non_gregorian_still_requires_slot_count() -> None:
    with pytest.raises(ValueError, match="slot_count"):
        RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            calendar="360_day",
        )


def test_non_gregorian_irregular_still_requires_units() -> None:
    with pytest.raises(ValueError, match="units"):
        IrregularTimeAxis(
            coordinate="time",
            values=[cftime.Datetime360Day(2049, 1, 1)],
            calendar="360_day",
        )


# ---------------------------------------------------------------------------
# units is only meaningful on a non-Gregorian irregular axis
# ---------------------------------------------------------------------------


def test_irregular_units_on_gregorian_axis_raises() -> None:
    with pytest.raises(ValueError, match="units is only used with a non-Gregorian calendar"):
        IrregularTimeAxis(
            coordinate="time",
            values=[1, 2, 3],
            units="days since 1850-01-01",
        )
