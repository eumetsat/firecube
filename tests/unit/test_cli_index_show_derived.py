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

"""Unit tests for ``firecube zarr index show --derived`` on calendar groups.

``_derived_coordinates_for_group`` computes what ``index show --derived``
prints; this exercises it directly against real resolved-index payloads
(built via ``resolve_index_spec``, not hand-typed dicts) for both a
calendar-declared group and the unchanged Gregorian case.

Covers ``_derived_coordinates_for_group`` for a calendar payload: the
``firecube zarr index show --derived`` CLI must render calendar-encoded
axes as calendar dates on the axis's declared calendar, not raw encoded
seconds. The Gregorian case must be unaffected.
"""

from __future__ import annotations

import pytest

from firecube.cli.index import _derived_coordinates_for_group
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, IrregularTimeAxis, RegularTimeAxis

pytestmark = pytest.mark.unit


def _payload_for(axis: RegularTimeAxis | IrregularTimeAxis) -> dict:
    spec = IndexSpec(name="derived-test-v1", groups={"data": axis})
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")
    return resolved.canonical_index_payload()["groups"]["data"]


class TestDerivedCoordinatesRegularCalendar:
    def test_360_day_epoch_noon_returns_february_30_at_index_59(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2049-01-01T12:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        payload = _payload_for(axis)

        coords = _derived_coordinates_for_group("data", payload)

        assert coords is not None
        assert len(coords) == 90
        assert coords[0].startswith("2049-01-01T12:00:00")
        assert coords[59].startswith("2049-02-30T12:00:00")

    def test_noleap_never_produces_february_29(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=60,
            calendar="noleap",
        )
        payload = _payload_for(axis)

        coords = _derived_coordinates_for_group("data", payload)

        assert coords is not None
        assert not any(c.startswith("2049-02-29") for c in coords)
        assert coords[31].startswith("2049-02-01")


class TestDerivedCoordinatesIrregularCalendar:
    def test_half_day_values_decode_to_calendar_isoformats(self) -> None:
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=[71640.5, 71699.5, 71729.5],
            calendar="360_day",
            units="days since 1850-01-01",
        )
        payload = _payload_for(axis)

        coords = _derived_coordinates_for_group("data", payload)

        assert coords is not None
        assert len(coords) == 3
        assert coords[0].startswith("2049-01-01T12:00:00")


class TestDerivedCoordinatesGregorianUnchanged:
    def test_regular_time_without_calendar_uses_gregorian_arithmetic(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=600,
            slot_count=3,
        )
        payload = _payload_for(axis)

        coords = _derived_coordinates_for_group("data", payload)

        assert coords == [
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:10:00Z",
            "2024-01-01T00:20:00Z",
        ]

    def test_irregular_time_without_calendar_is_still_a_noop(self) -> None:
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=["2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"],
        )
        payload = _payload_for(axis)

        assert _derived_coordinates_for_group("data", payload) is None

    def test_integer_kind_is_still_a_noop(self) -> None:
        payload = {"kind": "integer", "size": 4, "params": {}}

        assert _derived_coordinates_for_group("data", payload) is None


class TestDerivedCoordinatesExplicitGregorianMatchesUnset:
    """An explicitly declared Gregorian-like ``calendar`` behaves like an unset one."""

    def test_regular_time_explicit_proleptic_gregorian_matches_unset(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=600,
            slot_count=3,
            calendar="proleptic_gregorian",
        )
        payload = _payload_for(axis)

        coords = _derived_coordinates_for_group("data", payload)

        assert coords == [
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:10:00Z",
            "2024-01-01T00:20:00Z",
        ]

    def test_irregular_time_explicit_proleptic_gregorian_is_still_a_noop(self) -> None:
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=["2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"],
            calendar="proleptic_gregorian",
        )
        payload = _payload_for(axis)

        assert _derived_coordinates_for_group("data", payload) is None
