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

from typing import Literal

import numpy as np
import pytest

from firecube.core.index_resolve import _compute_group_identity_hash
from firecube.core.index_spec import IntegerAxis, IrregularTimeAxis, RegularTimeAxis

pytestmark = [pytest.mark.unit, pytest.mark.contract]


# Contract pin: hash of an undeclared regular axis (calendar=None), computed at
# firecube v0.1.7 = 1ba0550bcd6e093e4b9602f03917da15321393dc via:
#   uv run --directory /tmp/cf-r5-v017 python -c "
#     from firecube.core.index_resolve import _compute_group_identity_hash
#     from firecube.core.index_spec import RegularTimeAxis
#     axis = RegularTimeAxis(coordinate='timestamp', epoch='2024-01-01T00:00:00Z',
#                            cadence_s=600, mode='exact', slot_count=100, end_date=None)
#     print(_compute_group_identity_hash(axis, 100, 'datetime64[ns]'))"
# Any change to _compute_group_identity_hash that shifts this literal is a
# persisted-format break for every store built by v0.1.7 and every subsequent
# non-calendar regular axis; treat it as a hard incompatibility, NOT a
# "just regenerate the pin" refactor.
_V017_UNDECLARED_REGULAR_HASH: str = (
    "04e7861a55f053efca8b8fab188aec5a0cfd63ca130c844407d0d880237e775a"
)

# Contract pin: hash of a calendar-declared regular axis, computed on the
# current branch tip (v0.1.7 did not have the `calendar` field on
# RegularTimeAxis; the field was added by the CF-calendar handoff patch 0001).
# Command:
#   uv run python -c "
#     from firecube.core.index_resolve import _compute_group_identity_hash
#     from firecube.core.index_spec import RegularTimeAxis
#     axis = RegularTimeAxis(coordinate='timestamp', epoch='2049-01-01T00:00:00Z',
#                            cadence_s=86400, slot_count=90, calendar='360_day')
#     print(_compute_group_identity_hash(axis, 90, 'int64'))"
# Any change to _compute_group_identity_hash that shifts this literal is a
# persisted-format break for every calendar store built by this branch and
# forward; do NOT "just regenerate" it -- migrate.
_CURRENT_CALENDAR_360DAY_HASH: str = (
    "105bacab35c891da511a2b5841869dff9b66c7f3a13fee6ff2bc7a485b2672f1"
)


def _make_regular(
    *,
    coordinate: str = "timestamp",
    epoch: str = "2024-01-01T00:00:00Z",
    cadence_s: int = 600,
    mode: Literal["exact", "floor"] = "exact",
    slot_count: int | None = 100,
    end_date: str | None = None,
) -> RegularTimeAxis:
    return RegularTimeAxis(
        coordinate=coordinate,
        epoch=epoch,
        cadence_s=cadence_s,
        mode=mode,
        slot_count=slot_count,
        end_date=end_date,
    )


def test_regular_axis_hash_is_deterministic() -> None:
    axis = _make_regular()
    a = _compute_group_identity_hash(axis, 100, "datetime64[ns]")
    b = _compute_group_identity_hash(axis, 100, "datetime64[ns]")
    assert a == b
    assert len(a) == 64


def test_regular_axis_hash_differs_when_slot_count_differs() -> None:
    axis = _make_regular()
    assert _compute_group_identity_hash(axis, 100, "datetime64[ns]") != (
        _compute_group_identity_hash(axis, 101, "datetime64[ns]")
    )


def test_regular_axis_hash_differs_when_cadence_differs() -> None:
    small = _make_regular(cadence_s=600)
    large = _make_regular(cadence_s=900)
    assert _compute_group_identity_hash(small, 100, "datetime64[ns]") != (
        _compute_group_identity_hash(large, 100, "datetime64[ns]")
    )


def test_regular_axis_hash_differs_when_epoch_differs() -> None:
    a = _make_regular(epoch="2024-01-01T00:00:00Z")
    b = _make_regular(epoch="2025-01-01T00:00:00Z")
    assert _compute_group_identity_hash(a, 100, "datetime64[ns]") != (
        _compute_group_identity_hash(b, 100, "datetime64[ns]")
    )


def test_regular_axis_hash_differs_when_mode_differs() -> None:
    exact = _make_regular(mode="exact")
    floor = _make_regular(mode="floor")
    assert _compute_group_identity_hash(exact, 100, "datetime64[ns]") != (
        _compute_group_identity_hash(floor, 100, "datetime64[ns]")
    )


def test_regular_axis_hash_differs_when_dtype_differs() -> None:
    axis = _make_regular()
    assert _compute_group_identity_hash(axis, 100, "datetime64[ns]") != (
        _compute_group_identity_hash(axis, 100, "datetime64[s]")
    )


def test_regular_axis_hash_accepts_numpy_dtype_and_string_equivalently() -> None:
    axis = _make_regular()
    from_string = _compute_group_identity_hash(axis, 100, "datetime64[ns]")
    from_np_dtype = _compute_group_identity_hash(axis, 100, np.dtype("datetime64[ns]"))
    assert from_string == from_np_dtype


def test_integer_axis_hash_deterministic() -> None:
    axis = IntegerAxis(slot_count=42)
    a = _compute_group_identity_hash(axis, 42, "int64")
    b = _compute_group_identity_hash(axis, 42, "int64")
    assert a == b
    assert len(a) == 64


def test_integer_axis_hash_differs_by_size() -> None:
    axis = IntegerAxis(slot_count=42)
    assert _compute_group_identity_hash(axis, 42, "int64") != (
        _compute_group_identity_hash(axis, 43, "int64")
    )


def test_irregular_axis_hash_deterministic() -> None:
    axis = IrregularTimeAxis(
        coordinate="timestamp",
        values=("2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"),
    )
    a = _compute_group_identity_hash(axis, 2, "datetime64[ns]")
    b = _compute_group_identity_hash(axis, 2, "datetime64[ns]")
    assert a == b
    assert len(a) == 64


def test_regular_and_integer_axes_produce_different_hashes() -> None:
    reg = _make_regular()
    integer = IntegerAxis(slot_count=100)
    assert _compute_group_identity_hash(reg, 100, "datetime64[ns]") != (
        _compute_group_identity_hash(integer, 100, "datetime64[ns]")
    )


def test_unsupported_axis_type_raises_not_implemented() -> None:
    class _Fake:
        pass

    with pytest.raises(NotImplementedError, match="No group identity hash"):
        _compute_group_identity_hash(_Fake(), 10, "float32")  # type: ignore[arg-type]


def test_regular_axis_end_date_and_slot_count_yield_same_hash_at_same_size() -> None:
    a = _make_regular(slot_count=100)
    b = _make_regular(slot_count=None, end_date="2024-01-01T16:40:00Z")
    assert _compute_group_identity_hash(a, 100, "datetime64[ns]") == (
        _compute_group_identity_hash(b, 100, "datetime64[ns]")
    )


def test_regular_axis_hash_unaffected_when_calendar_is_none() -> None:
    # `calendar` defaults to None, so no "calendar" key should ever enter the
    # hashed payload. Confirmed by equality against the v0.1.7 literal below --
    # if this fails, the base-plan invariance guarantee has drifted.
    axis = _make_regular()
    got = _compute_group_identity_hash(axis, 100, "datetime64[ns]")
    assert got == _V017_UNDECLARED_REGULAR_HASH, (
        "regular-axis hash for calendar=None must match v0.1.7 exactly; a mismatch "
        "means the base-plan byte-identical guarantee for undeclared axes has drifted"
    )


def test_regular_axis_hash_differs_when_calendar_is_set_vs_unset() -> None:
    plain = RegularTimeAxis(
        coordinate="timestamp", epoch="2049-01-01T00:00:00Z", cadence_s=86400, slot_count=90
    )
    calendared = RegularTimeAxis(
        coordinate="timestamp",
        epoch="2049-01-01T00:00:00Z",
        cadence_s=86400,
        slot_count=90,
        calendar="360_day",
    )
    assert _compute_group_identity_hash(plain, 90, "int64") != _compute_group_identity_hash(
        calendared, 90, "int64"
    )


def test_regular_axis_hash_explicit_proleptic_gregorian_matches_default() -> None:
    # An explicit calendar="proleptic_gregorian" is the same declared value
    # as the default (unset) calendar, not a different axis: both must
    # produce the exact v0.1.7 pinned hash.
    axis = RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        mode="exact",
        slot_count=100,
        end_date=None,
        calendar="proleptic_gregorian",
    )
    got = _compute_group_identity_hash(axis, 100, "datetime64[ns]")
    assert got == _V017_UNDECLARED_REGULAR_HASH


def test_calendar_axis_hash_pinned() -> None:
    # Contract pin: a calendar-declared regular axis must produce the exact
    # hash computed at the current branch tip. This is the persisted-format
    # contract for the new field; a mismatch here means future code changed
    # `_compute_group_identity_hash` in a way that silently invalidates every
    # calendar store already on disk.
    axis = RegularTimeAxis(
        coordinate="timestamp",
        epoch="2049-01-01T00:00:00Z",
        cadence_s=86400,
        slot_count=90,
        calendar="360_day",
    )
    got = _compute_group_identity_hash(axis, 90, "int64")
    assert got == _CURRENT_CALENDAR_360DAY_HASH, (
        "calendar-axis hash drift: `_compute_group_identity_hash` changed in a way "
        "that breaks the persisted-format contract for every calendar store on disk"
    )
