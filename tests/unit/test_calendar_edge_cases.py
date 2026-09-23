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

"""Regression-hunt edge cases for the calendar-declared time axis work.

This branch made every time axis carry a calendar (default
``proleptic_gregorian``, aliases ``standard``/``gregorian``), unified the
resolver on ``firecube.core.encoded_time.encode_coordinate``, and introduced
``CoordinateEncoding``. The promise under test: for Gregorian axes nothing
observable changed versus the v0.1.7 reference implementation (same slot for
the same input, same errors, same bytes).

``_oracle_coerce_to_epoch_s`` and ``_oracle_position`` below are copied
verbatim (not re-derived) from ``git show 1ba0550:src/firecube/core/index_resolve.py``
and serve as an independent oracle for the Gregorian resolver -- comparing
against them is parity pinning, not a mirror test, because the production
code no longer contains this logic at all (it now goes through
``encode_coordinate``).
"""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

import cftime
import numpy as np
import pandas as pd
import pytest

from firecube.core.encoded_time import normalise_calendar, validate_calendar_units
from firecube.core.index_resolve import (
    IrregularTimeResolver,
    RegularTimeResolver,
    resolve_index_spec,
)
from firecube.core.index_spec import (
    AUTO,
    IndexSpec,
    IntegerAxis,
    IrregularTimeAxis,
    ItemInfo,
    RegularTimeAxis,
)
from firecube.core.zarr.coord_materialization import (
    coordinate_encoding_for,
    coordinate_encoding_from_array,
)
from firecube.core.zarr.time_decode import decode_time_array
from firecube.ingestor.runtime.index_binding import (
    _discover_auto_irregular_axis,
    resolve_index_spec_for_ingestor,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# v0.1.7 oracle (git show 1ba0550:src/firecube/core/index_resolve.py), copied
# verbatim as the independent parity reference for the Gregorian fast path.
# ---------------------------------------------------------------------------


def _oracle_coerce_to_epoch_s(value: Any, *, mode: str = "floor") -> int:
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            bare = text[:-1]
        elif text.endswith("+00:00"):
            bare = text[: -len("+00:00")]
        elif text.endswith("-00:00"):
            bare = text[: -len("-00:00")]
        else:
            raise ValueError(
                f"iso_to_epoch_s requires UTC-explicit ISO 8601 input "
                f"(end with 'Z', '+00:00', or '-00:00'). Got: {value!r}. "
                f"To fix: use 'YYYY-MM-DDTHH:MM:SSZ'."
            )
        when = np.datetime64(bare, "s")
        epoch = np.datetime64("1970-01-01T00:00:00", "s")
        return int((when - epoch).astype("int64"))

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


def _oracle_position(*, epoch: str, cadence_s: int, mode: str, value: Any) -> int:
    epoch_s = _oracle_coerce_to_epoch_s(epoch, mode="floor")
    ts_s = _oracle_coerce_to_epoch_s(value, mode=mode)
    if ts_s < epoch_s:
        raise ValueError(f"coordinate {value!r} predates epoch {epoch!r}")
    raw = ts_s - epoch_s
    index, rem = divmod(raw, cadence_s)
    if mode == "exact" and rem != 0:
        raise ValueError(
            f"coordinate {value!r} is not cadence-aligned (mode=exact, cadence={cadence_s}s)"
        )
    return int(index)


# ---------------------------------------------------------------------------
# 1. RegularTimeResolver.position on a Gregorian axis
# ---------------------------------------------------------------------------


class TestGregorianPositionParity:
    """For every v0.1.7-accepted input type, the resolved slot matches the oracle."""

    @pytest.fixture
    def axis(self) -> RegularTimeAxis:
        return RegularTimeAxis(
            coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=100
        )

    @pytest.mark.parametrize(
        ("value", "expected_slot"),
        [
            pytest.param("2024-01-01T00:00:00Z", 0, id="iso-Z-epoch"),
            pytest.param("2024-01-01T00:00:00+00:00", 0, id="iso-offset-epoch"),
            pytest.param(dt.datetime(2024, 1, 1, 0, 10, 0), 1, id="naive-datetime"),
            pytest.param(
                dt.datetime(2024, 1, 1, 0, 10, 0, tzinfo=dt.UTC), 1, id="aware-utc-datetime"
            ),
            pytest.param(pd.Timestamp("2024-01-01T00:20:00Z"), 2, id="timestamp-utc"),
            pytest.param(
                pd.Timestamp("2024-01-01T05:20:00", tz="America/New_York"),
                62,
                id="timestamp-non-utc-tz",
            ),
            pytest.param(np.datetime64("2024-01-01T00:30:00", "s"), 3, id="datetime64-s"),
            pytest.param(np.datetime64("2024-01-01T00:30:00.000", "ms"), 3, id="datetime64-ms"),
            pytest.param(np.datetime64("2024-01-01T00:30:00.000000", "us"), 3, id="datetime64-us"),
            pytest.param(
                np.datetime64("2024-01-01T00:30:00.000000000", "ns"), 3, id="datetime64-ns"
            ),
        ],
    )
    def test_slot_matches_oracle(
        self, axis: RegularTimeAxis, value: Any, expected_slot: int
    ) -> None:
        resolver = RegularTimeResolver(axis=axis)
        oracle_slot = _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )
        assert oracle_slot == expected_slot  # sanity: oracle itself is correct
        assert resolver.position(value) == expected_slot

    def test_cftime_gregorian_and_proleptic_gregorian_accepted(self, axis: RegularTimeAxis) -> None:
        """New capability on this branch: v0.1.7's oracle has no accept path for
        cftime input at all (it falls through to the final ``TypeError``); the
        current resolver accepts a Gregorian-like cftime value and resolves it
        the same as the equivalent ISO string.
        """
        resolver = RegularTimeResolver(axis=axis)
        assert resolver.position(cftime.DatetimeGregorian(2024, 1, 1, 0, 40, 0)) == 4
        assert resolver.position(cftime.DatetimeProlepticGregorian(2024, 1, 1, 0, 40, 0)) == 4
        with pytest.raises(TypeError):
            _oracle_coerce_to_epoch_s(cftime.DatetimeGregorian(2024, 1, 1, 0, 40, 0))

    def test_epoch_itself_is_slot_zero(self, axis: RegularTimeAxis) -> None:
        resolver = RegularTimeResolver(axis=axis)
        assert resolver.position(axis.epoch) == 0

    def test_last_slot_boundary_exact_match(self, axis: RegularTimeAxis) -> None:
        resolver = RegularTimeResolver(axis=axis)
        # slot 99 (last of slot_count=100) left edge is epoch + 99*600s.
        value = "2024-01-01T16:30:00Z"
        assert resolver.position(value) == 99
        assert resolver.position(value) == _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )

    def test_one_second_before_boundary_exact_mode_misaligned(self, axis: RegularTimeAxis) -> None:
        resolver = RegularTimeResolver(axis=axis)
        value = "2024-01-01T00:09:59Z"
        with pytest.raises(ValueError, match="not cadence-aligned"):
            resolver.position(value)
        with pytest.raises(ValueError):
            _oracle_position(
                epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
            )

    def test_one_second_before_boundary_floor_mode_floors(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=600,
            mode="floor",
            slot_count=100,
        )
        resolver = RegularTimeResolver(axis=axis)
        value = "2024-01-01T00:09:59Z"
        assert resolver.position(value) == 0
        assert resolver.position(value) == _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )

    def test_before_epoch_error_text_unchanged(self, axis: RegularTimeAxis) -> None:
        resolver = RegularTimeResolver(axis=axis)
        value = "2023-12-31T23:59:00Z"
        with pytest.raises(ValueError) as actual_exc:
            resolver.position(value)
        with pytest.raises(ValueError) as oracle_exc:
            _oracle_position(
                epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
            )
        assert str(actual_exc.value) == str(oracle_exc.value)

    def test_beyond_slot_count_is_not_bounds_checked_by_position(
        self, axis: RegularTimeAxis
    ) -> None:
        """``position()`` never consulted ``slot_count``/``end_date`` in v0.1.7
        either -- extent enforcement is the caller's job. Pin that this is
        still true: a coordinate far past the declared ``slot_count`` still
        resolves rather than raising ``IndexError``.
        """
        resolver = RegularTimeResolver(axis=axis)
        value = "2024-01-02T00:00:00Z"  # index 144, slot_count=100
        assert resolver.position(value) == 144
        assert resolver.position(value) == _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )

    def test_pre_1582_epoch_and_values(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp", epoch="1500-01-01T00:00:00Z", cadence_s=86400, slot_count=5
        )
        resolver = RegularTimeResolver(axis=axis)
        value = "1500-01-03T00:00:00Z"
        assert resolver.position(value) == 2
        assert resolver.position(value) == _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )

    def test_year_1970_epoch(self) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp", epoch="1970-01-01T00:00:00Z", cadence_s=1, slot_count=5
        )
        resolver = RegularTimeResolver(axis=axis)
        value = "1970-01-01T00:00:03Z"
        assert resolver.position(value) == 3
        assert resolver.position(value) == _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )

    @pytest.mark.parametrize("cadence_s", [1, 3600, 86400])
    def test_cadence_variants(self, cadence_s: int) -> None:
        axis = RegularTimeAxis(
            coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=cadence_s, slot_count=10
        )
        resolver = RegularTimeResolver(axis=axis)
        value = np.datetime64("2024-01-01T00:00:00", "s") + np.timedelta64(3 * cadence_s, "s")
        assert resolver.position(value) == 3
        assert resolver.position(value) == _oracle_position(
            epoch=axis.epoch, cadence_s=axis.cadence_s, mode=axis.mode, value=value
        )


class TestSubSecondHandling:
    """Sub-second coordinates under mode='floor' (truncate) and 'exact'.

    ``np.datetime64`` input always floors regardless of mode (unchanged from
    v0.1.7: ``coerce_to_epoch_s`` never sub-second-checks a ``datetime64``
    argument). ``str``/``datetime``/``pandas.Timestamp`` input under
    ``mode="exact"`` raises ``ValueError``.
    """

    @pytest.fixture
    def floor_axis(self) -> RegularTimeAxis:
        return RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=1,
            mode="floor",
            slot_count=100,
        )

    @pytest.fixture
    def exact_axis(self) -> RegularTimeAxis:
        return RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=1,
            mode="exact",
            slot_count=100,
        )

    def test_floor_mode_truncates_fractional_string(self, floor_axis: RegularTimeAxis) -> None:
        resolver = RegularTimeResolver(axis=floor_axis)
        assert resolver.position("2024-01-01T00:00:03.789Z") == 3
        assert resolver.position("2024-01-01T00:00:03.789Z") == _oracle_position(
            epoch=floor_axis.epoch,
            cadence_s=floor_axis.cadence_s,
            mode=floor_axis.mode,
            value="2024-01-01T00:00:03.789Z",
        )

    def test_exact_mode_datetime64_always_floors(self, exact_axis: RegularTimeAxis) -> None:
        """v0.1.7 parity: a ``datetime64`` value never sub-second-checks, even
        under ``mode="exact"``.
        """
        resolver = RegularTimeResolver(axis=exact_axis)
        value = np.datetime64("2024-01-01T00:00:03.500", "ms")
        assert resolver.position(value) == 3
        assert resolver.position(value) == _oracle_position(
            epoch=exact_axis.epoch,
            cadence_s=exact_axis.cadence_s,
            mode=exact_axis.mode,
            value=value,
        )

    def test_exact_mode_fractional_string_truncates_like_v017(
        self, exact_axis: RegularTimeAxis
    ) -> None:
        """A fractional-second ISO string is truncated in exact mode.

        ``coerce_to_epoch_s`` has never checked a ``str`` for sub-second
        precision (only ``datetime``/``Timestamp``), so the resolver keeps
        placing such a string on the truncated slot, exactly as v0.1.7 did.
        """
        resolver = RegularTimeResolver(axis=exact_axis)
        value = "2024-01-01T00:00:03.500Z"
        expected = _oracle_position(
            epoch=exact_axis.epoch,
            cadence_s=exact_axis.cadence_s,
            mode=exact_axis.mode,
            value=value,
        )
        assert resolver.position(value) == expected == 3

    def test_exact_mode_fractional_datetime_raises_value_error(
        self, exact_axis: RegularTimeAxis
    ) -> None:
        """Parity case (both v0.1.7 and current raise): a sub-second aware
        ``datetime`` under ``mode="exact"`` was already rejected in v0.1.7.
        """
        resolver = RegularTimeResolver(axis=exact_axis)
        value = dt.datetime(2024, 1, 1, 0, 0, 3, 500000, tzinfo=dt.UTC)
        with pytest.raises(ValueError, match="sub-second precision"):
            _oracle_position(
                epoch=exact_axis.epoch,
                cadence_s=exact_axis.cadence_s,
                mode=exact_axis.mode,
                value=value,
            )
        with pytest.raises(ValueError, match="sub-second precision"):
            resolver.position(value)

    def test_exact_mode_fractional_timestamp_raises_value_error(
        self, exact_axis: RegularTimeAxis
    ) -> None:
        resolver = RegularTimeResolver(axis=exact_axis)
        value = pd.Timestamp("2024-01-01T00:00:03.500Z")
        with pytest.raises(ValueError, match="sub-second precision"):
            _oracle_position(
                epoch=exact_axis.epoch,
                cadence_s=exact_axis.cadence_s,
                mode=exact_axis.mode,
                value=value,
            )
        with pytest.raises(ValueError, match="sub-second precision"):
            resolver.position(value)


# ---------------------------------------------------------------------------
# 2. IrregularTimeAxis Gregorian
# ---------------------------------------------------------------------------


class TestIrregularTimeAxisGregorian:
    def test_mixed_accepted_types_stored_unchanged(self) -> None:
        """str/datetime/np.datetime64 values are each stored as their own
        type -- Gregorian canonicalisation is a no-op for non-calendar-valued
        input, matching v0.1.7 (which had no canonicalisation step at all).
        """
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=[
                "2024-01-01T00:00:00Z",
                dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
                np.datetime64("2024-01-03T00:00:00", "ns"),
            ],
        )
        values = cast(Sequence[Any], axis.values)
        assert [type(v) for v in values] == [str, dt.datetime, np.datetime64]

    def test_cftime_gregorian_values_canonicalised_to_datetime64(self) -> None:
        """New on this branch: v0.1.7 stored a cftime value as-is (whatever
        ``set()``/hashing did with it); the current axis converts a
        Gregorian-like cftime value to ``datetime64[ns]`` at construction.
        """
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=[
                cftime.DatetimeGregorian(2024, 1, 1),
                cftime.DatetimeProlepticGregorian(2024, 1, 2),
            ],
        )
        values = cast(Sequence[Any], axis.values)
        assert all(isinstance(v, np.datetime64) for v in values)
        assert axis.values == (
            np.datetime64("2024-01-01T00:00:00", "ns"),
            np.datetime64("2024-01-02T00:00:00", "ns"),
        )

    def test_duplicate_same_type_values_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicates"):
            IrregularTimeAxis(
                coordinate="timestamp",
                values=["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            )

    def test_duplicate_detection_does_not_cross_types(self) -> None:
        """Same instant, different accepted types: NOT deduplicated. Matches
        v0.1.7 exactly -- the old ``__post_init__`` ran ``set(self.values)``
        on the raw values with no canonicalisation, so a ``str`` and a
        ``datetime`` naming the same instant never compared equal either.
        """
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=[
                "2024-01-01T00:00:00Z",
                dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
            ],
        )
        values = cast(Sequence[Any], axis.values)
        assert len(values) == 2

    def test_position_with_different_type_than_declared_does_not_match(self) -> None:
        """v0.1.7 parity: the resolver used ``tuple.index()`` equality with no
        canonicalisation, so a coordinate of a different accepted type never
        matched a stored value naming the same instant. This is unchanged:
        the branch's canonicalisation only normalises calendar-valued
        (cftime) input, not str/datetime/np.datetime64 against each other.
        """
        axis = IrregularTimeAxis(
            coordinate="timestamp",
            values=["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
        )
        resolver = IrregularTimeResolver(axis=axis)
        assert resolver.position("2024-01-01T00:00:00Z") == 0
        with pytest.raises(ValueError, match="is not present in axis values"):
            resolver.position(np.datetime64("2024-01-01T00:00:00", "ns"))


# ---------------------------------------------------------------------------
# 3. AUTO discovery
# ---------------------------------------------------------------------------


@dataclass
class _FakeItem:
    path: str
    coord: Any


class _FakeIngestor:
    """Minimal real (non-mock) stand-in satisfying the duck-typed contract
    ``resolve_index_spec_for_ingestor``/``_discover_auto_irregular_axis``
    require: ``index_spec``, ``_resolve_time_dim_name``,
    ``discover_source_files``, ``filter_item``, ``inspect_item``.
    """

    def __init__(self, items: list[_FakeItem], axis: IrregularTimeAxis, time_dim_name: str) -> None:
        self._items = items
        self._axis = axis
        self._time_dim_name = time_dim_name

    def discover_source_files(self, ctx: Any) -> list[_FakeItem]:
        return self._items

    def filter_item(self, item: _FakeItem, ctx: Any) -> bool:
        return True

    def inspect_item(self, item: _FakeItem, ctx: Any) -> ItemInfo:
        return ItemInfo(coordinate=item.coord)

    def index_spec(self, ctx: Any) -> IndexSpec:
        return IndexSpec(name="demo", groups={"g": self._axis})

    def _resolve_time_dim_name(self) -> str:
        return self._time_dim_name


class _FakeCtx:
    source = "fake-source"


class TestAutoDiscovery:
    def test_gregorian_auto_discovery_unchanged(self) -> None:
        axis = IrregularTimeAxis(coordinate="timestamp", values=AUTO)
        items = [
            _FakeItem("a", "2024-01-02T00:00:00Z"),
            _FakeItem("b", "2024-01-01T00:00:00Z"),
        ]
        ingestor = _FakeIngestor(items, axis, "timestamp")
        binding = resolve_index_spec_for_ingestor(ingestor, _FakeCtx())
        assert binding is not None
        assert binding.resolved.size("g") == 2
        # discovery sorts ascending
        assert binding.resolved.position("g", "2024-01-01T00:00:00Z") == 0
        assert binding.resolved.position("g", "2024-01-02T00:00:00Z") == 1

    def test_noleap_value_on_gregorian_auto_axis_refused_naming_calendar(self) -> None:
        axis = IrregularTimeAxis(coordinate="timestamp", values=AUTO)
        items = [
            _FakeItem("a", cftime.DatetimeNoLeap(2000, 1, 1)),
            _FakeItem("b", "2024-01-02T00:00:00Z"),
        ]
        ingestor = _FakeIngestor(items, axis, "timestamp")
        with pytest.raises(ValueError) as excinfo:
            resolve_index_spec_for_ingestor(ingestor, _FakeCtx())
        message = str(excinfo.value)
        # The value's own calendar is named explicitly, and callers are told
        # to declare `calendar=`. Note (see report): the message does NOT
        # separately spell out the axis's own calendar name
        # ("proleptic_gregorian") anywhere in the text -- only the value's
        # calendar ("noleap") is named. This is the documented contract in
        # `_canonicalise_irregular_value`'s docstring, not a bug.
        assert "noleap" in message
        assert "calendar=" in message

    def test_discover_auto_irregular_axis_directly_callable(self) -> None:
        """Same refusal, called one level down without the ingestor round-trip."""
        axis = IrregularTimeAxis(coordinate="timestamp", values=AUTO)
        items = [_FakeItem("a", cftime.DatetimeNoLeap(2000, 1, 1))]
        ingestor = _FakeIngestor(items, axis, "timestamp")
        with pytest.raises(ValueError, match="noleap"):
            _discover_auto_irregular_axis(axis, ingestor, _FakeCtx())


# ---------------------------------------------------------------------------
# 4. Aliases
# ---------------------------------------------------------------------------


class TestCalendarAliases:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Standard", "proleptic_gregorian"),
            ("GREGORIAN", "proleptic_gregorian"),
            (" proleptic_gregorian ", "proleptic_gregorian"),
            ("365_day", "noleap"),
        ],
    )
    def test_alias_normalises(self, raw: str, expected: str) -> None:
        assert normalise_calendar(raw) == expected
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=3,
            calendar=raw,
        )
        assert axis.calendar == expected

    def test_unknown_calendar_name_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="calendar must be one of"):
            validate_calendar_units(
                units="seconds since 2024-01-01 00:00:00", calendar="not_a_real_calendar"
            )
        with pytest.raises(ValueError):
            RegularTimeAxis(
                coordinate="t",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=86400,
                slot_count=3,
                calendar="not_a_real_calendar",
            )

    def test_empty_string_calendar_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            RegularTimeAxis(
                coordinate="t",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=86400,
                slot_count=3,
                calendar="",
            )

    def test_none_calendar_does_not_raise_a_clean_error(self) -> None:
        """REGRESSION (see report): `calendar` was `str | None = None` before
        this branch, with an explicit `if self.calendar is not None:` guard
        in `RegularTimeAxis.__post_init__` -- `calendar=None` was the
        documented way to get Gregorian/no-calendar behavior. Commit
        65c8d5f74 changed the field to `calendar: str = "proleptic_gregorian"`
        and dropped the `is not None` guard, so `calendar=None` (still
        reachable at runtime -- dataclasses do not enforce type hints) now
        falls straight into `normalise_calendar(None)` ->
        `None.strip()` and blows up with an undocumented, unhelpful
        `AttributeError` instead of failing clearly with `TypeError`/
        `ValueError`. This test intentionally asserts the CORRECT contract
        (a clear, typed migration error) and is expected to FAIL against the
        current code, pinning the regression without patching source.
        """
        with pytest.raises((TypeError, ValueError)):
            RegularTimeAxis(
                coordinate="t",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=86400,
                slot_count=3,
                calendar=None,  # type: ignore[arg-type]
            )

    def test_none_calendar_on_irregular_axis_does_not_raise_a_clean_error(self) -> None:
        """Same regression as `test_none_calendar_does_not_raise_a_clean_error`,
        on `IrregularTimeAxis.__post_init__`'s identical
        `normalise_calendar(self.calendar)` call.
        """
        with pytest.raises((TypeError, ValueError)):
            IrregularTimeAxis(
                coordinate="t",
                values=["2024-01-01T00:00:00Z"],
                calendar=None,  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# 5. Hash / persistence
# ---------------------------------------------------------------------------


class TestHashAndPersistence:
    def test_gregorian_regular_and_irregular_payload_has_no_calendar_or_units(self) -> None:
        axis_reg = RegularTimeAxis(
            coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )
        axis_irr = IrregularTimeAxis(
            coordinate="timestamp", values=["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]
        )
        spec = IndexSpec(name="demo", groups={"reg": axis_reg, "irr": axis_irr})
        resolved = resolve_index_spec(spec, time_dim_name="timestamp")
        payload = resolved.canonical_index_payload()
        assert "calendar" not in payload["groups"]["reg"]["params"]
        assert "calendar" not in payload["groups"]["irr"]["params"]
        assert "units" not in payload["groups"]["irr"]["params"]

    def test_integer_axis_payload_unchanged(self) -> None:
        spec = IndexSpec(name="ints", groups={"a": IntegerAxis(slot_count=7)})
        resolved = resolve_index_spec(spec, time_dim_name="timestamp")
        payload = resolved.canonical_index_payload()
        assert payload["groups"]["a"] == {"kind": "integer", "size": 7, "params": {}}

    def test_mixed_bounded_unbounded_regular_axes_size_none_for_unbounded(self) -> None:
        axis_bounded = RegularTimeAxis(
            coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )
        axis_unbounded = RegularTimeAxis(
            coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=600
        )
        spec = IndexSpec(
            name="mixed", groups={"bounded": axis_bounded, "unbounded": axis_unbounded}
        )
        resolved = resolve_index_spec(spec, time_dim_name="timestamp")
        payload = resolved.canonical_index_payload()
        assert payload["groups"]["bounded"]["size"] == 5
        assert payload["groups"]["unbounded"]["size"] is None

    def test_legacy_slot_index_model_same_bytes_default_and_standard_alias(self) -> None:
        axis_default = RegularTimeAxis(
            coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )
        axis_standard = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=600,
            slot_count=5,
            calendar="standard",
        )
        spec_default = IndexSpec(name="demo", groups={"a": axis_default})
        spec_standard = IndexSpec(name="demo", groups={"a": axis_standard})
        resolved_default = resolve_index_spec(spec_default, time_dim_name="timestamp")
        resolved_standard = resolve_index_spec(spec_standard, time_dim_name="timestamp")
        model_default = resolved_default.as_legacy_slot_index_model()
        model_standard = resolved_standard.as_legacy_slot_index_model()
        assert model_default is not None
        assert model_standard is not None
        assert model_default.canonical_bytes() == model_standard.canonical_bytes()

    def test_legacy_slot_index_model_none_for_360_day_axis(self) -> None:
        axis_360 = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=3,
            calendar="360_day",
        )
        spec = IndexSpec(name="demo", groups={"a": axis_360})
        resolved = resolve_index_spec(spec, time_dim_name="timestamp")
        assert resolved.as_legacy_slot_index_model() is None


# ---------------------------------------------------------------------------
# 6. Non-Gregorian calendars
# ---------------------------------------------------------------------------


class TestNonGregorianCalendars:
    def test_360_day_epoch_valid_in_that_calendar_is_accepted(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="1850-02-30T00:00:00Z",
            cadence_s=86400,
            slot_count=3,
            calendar="360_day",
        )
        assert axis.calendar == "360_day"

    def test_360_day_epoch_refused_for_noleap(self) -> None:
        """1850-02-30 is not a valid date in noleap (no such day)."""
        with pytest.raises(ValueError, match="not valid for calendar"):
            RegularTimeAxis(
                coordinate="t",
                epoch="1850-02-30T00:00:00Z",
                cadence_s=86400,
                slot_count=3,
                calendar="noleap",
            )

    def test_julian_regular_axis_positions_and_coordinates_round_trip(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2000-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="julian",
        )
        resolver = RegularTimeResolver(axis=axis)
        pos = resolver.position(cftime.DatetimeJulian(2000, 1, 3))
        assert pos == 2
        coord_value = resolver.coordinate(pos)
        assert coord_value == 2 * 86400
        decoded = decode_time_array(
            np.array([coord_value], dtype=np.int64),
            {"units": axis.encoded_units, "calendar": axis.calendar},
        )
        assert decoded[0] == cftime.DatetimeJulian(2000, 1, 3)

    def test_all_leap_calendar_accepted(self) -> None:
        """all_leap always has Feb 29, unlike Gregorian/noleap."""
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2001-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=366,
            calendar="all_leap",
        )
        resolver = RegularTimeResolver(axis=axis)
        # 0-based day-of-year: Jan (31 days, indices 0-30) then Feb 29 at index 59.
        assert resolver.position(cftime.DatetimeAllLeap(2001, 2, 29)) == 59

    def test_cftime_value_on_wrong_calendar_refused(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2000-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="julian",
        )
        resolver = RegularTimeResolver(axis=axis)
        with pytest.raises(ValueError, match="does not match axis calendar"):
            resolver.position(cftime.DatetimeNoLeap(2000, 1, 3))

    def test_already_encoded_int_accepted(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2000-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="julian",
        )
        resolver = RegularTimeResolver(axis=axis)
        assert resolver.position(2 * 86400) == 2

    def test_fractional_encoded_value_refused_in_exact_mode(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2000-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="julian",
        )
        resolver = RegularTimeResolver(axis=axis)
        with pytest.raises(ValueError, match="non-integral"):
            resolver.position(86400.5)

    def test_negative_encoded_value_refused_predates_epoch(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="2000-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="julian",
        )
        resolver = RegularTimeResolver(axis=axis)
        with pytest.raises(ValueError, match="predates epoch"):
            resolver.position(-1)


# ---------------------------------------------------------------------------
# 7. CoordinateEncoding
# ---------------------------------------------------------------------------


class TestCoordinateEncoding:
    def test_gregorian_encoding_defaults_datetime64_ns_and_nat_fill(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )
        encoding = coordinate_encoding_for(axis, None)
        assert encoding.dtype == np.dtype("datetime64[ns]")
        assert np.isnat(encoding.fill_value)
        assert encoding.extra_attrs == {}

    def test_gregorian_encoding_honours_spec_dtype(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )

        @dataclass
        class _Spec:
            dtype: str = "datetime64[s]"

        encoding = coordinate_encoding_for(axis, _Spec())
        assert encoding.dtype == np.dtype("datetime64[s]")

    def test_360_day_encoding_is_int64_with_min_fill_and_both_attrs(self) -> None:
        axis = RegularTimeAxis(
            coordinate="t",
            epoch="1850-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=3,
            calendar="360_day",
        )
        encoding = coordinate_encoding_for(axis, None)
        assert encoding.dtype == np.dtype("int64")
        assert encoding.fill_value == np.iinfo(np.int64).min
        assert encoding.extra_attrs == {
            "units": "seconds since 1850-01-01 00:00:00",
            "calendar": "360_day",
        }

    def test_encoding_from_array_int64_without_attrs_is_gregorian_legacy(self) -> None:
        encoding = coordinate_encoding_from_array(np.dtype("int64"), {})
        assert encoding.extra_attrs == {}
        # Gregorian branch: fill_value is NaT cast to the array's own dtype
        # (int64 here), which lands on the same bit pattern as int64's own
        # min-sentinel -- coincidence of representation, not a calendar attr.
        assert int(encoding.fill_value) == np.iinfo(np.int64).min

    def test_encoding_from_array_datetime64_with_stray_units_attr_is_gregorian(self) -> None:
        encoding = coordinate_encoding_from_array(
            np.dtype("datetime64[ns]"), {"units": "days since 1970-01-01"}
        )
        assert encoding.extra_attrs == {}
        assert np.isnat(encoding.fill_value)

    def test_encoding_from_array_int64_with_both_attrs_is_encoded(self) -> None:
        encoding = coordinate_encoding_from_array(
            np.dtype("int64"),
            {"units": "seconds since 1850-01-01", "calendar": "360_day"},
        )
        assert encoding.extra_attrs == {
            "units": "seconds since 1850-01-01",
            "calendar": "360_day",
        }
        assert encoding.fill_value == np.iinfo(np.int64).min

    def test_is_fill_on_empty_arrays(self) -> None:
        axis_greg = RegularTimeAxis(
            coordinate="t", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )
        axis_360 = RegularTimeAxis(
            coordinate="t",
            epoch="1850-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=3,
            calendar="360_day",
        )
        greg_encoding = coordinate_encoding_for(axis_greg, None)
        encoded_encoding = coordinate_encoding_for(axis_360, None)
        assert greg_encoding.is_fill(np.array([], dtype="datetime64[ns]")).shape == (0,)
        assert encoded_encoding.is_fill(np.array([], dtype="int64")).shape == (0,)

    def test_values_equal_nat_nat_matches_v017_intent(self) -> None:
        """`_normalize_for_coord_compare`-based call sites in v0.1.7 never
        directly compared two NaT scalars with `==` (they special-cased
        `np.isnat` first, since `NaT == NaT` is `False` under raw numpy
        equality). `CoordinateEncoding.values_equal` gives the same
        "both empty counts as equal" answer through one helper instead of
        two call sites each doing their own `isnat`-then-`==` dance.
        """
        axis_greg = RegularTimeAxis(
            coordinate="t", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
        )
        encoding = coordinate_encoding_for(axis_greg, None)
        nat = np.datetime64("NaT", "ns")
        assert encoding.values_equal(nat, nat) is True
        # Pin the raw-numpy contrast this helper is deliberately avoiding.
        assert bool(nat == nat) is False

    def test_values_equal_encoded_fill_vs_fill(self) -> None:
        axis_360 = RegularTimeAxis(
            coordinate="t",
            epoch="1850-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=3,
            calendar="360_day",
        )
        encoding = coordinate_encoding_for(axis_360, None)
        assert encoding.values_equal(encoding.fill_value, encoding.fill_value) is True


# ---------------------------------------------------------------------------
# 8. Import cost
# ---------------------------------------------------------------------------


class TestImportCost:
    def _run(self, code: str) -> tuple[bool, bool]:
        script = "import sys\n" + code + "\nprint('cftime' in sys.modules, 'xarray' in sys.modules)"
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=True
        )
        cftime_imported_str, xarray_imported_str = result.stdout.strip().split()
        return cftime_imported_str == "True", xarray_imported_str == "True"

    def test_constructing_gregorian_axis_does_not_import_cftime(self) -> None:
        code = textwrap.dedent(
            """
            from firecube.core import index_spec
            axis = index_spec.RegularTimeAxis(
                coordinate="t", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
            )
            """
        )
        cftime_imported, _ = self._run(code)
        assert cftime_imported is False

    def test_resolving_gregorian_axis_currently_imports_cftime_via_controlplane(self) -> None:
        """NOTE (see report, not a regression from this branch): resolving a
        spec requires ``firecube.core.index_resolve``, which imports
        ``firecube.core.controlplane.types`` -- and importing any submodule
        of the ``firecube.core.controlplane`` package runs that package's
        ``__init__.py``, which eagerly imports ``ChunkManager`` and friends,
        pulling in the zarr/xarray stack (and therefore ``cftime``)
        transitively. Diffed byte-identical against
        ``git show 1ba0550:src/firecube/core/controlplane/__init__.py`` and
        ``.../types.py`` -- this eager import predates the calendar work and
        is not something this branch introduced or could avoid by itself.
        `encode_coordinate`'s "no xarray/cftime" promise (encoded_time.py's
        module docstring) is about its own arithmetic fast path, not about
        the transitive import graph of the module that calls it.
        """
        code = textwrap.dedent(
            """
            from firecube.core import index_spec, index_resolve
            axis = index_spec.RegularTimeAxis(
                coordinate="t", epoch="2024-01-01T00:00:00Z", cadence_s=600, slot_count=5
            )
            resolver = index_resolve.RegularTimeResolver(axis=axis)
            resolver.position("2024-01-01T00:10:00Z")
            """
        )
        cftime_imported, xarray_imported = self._run(code)
        assert cftime_imported is True
        assert xarray_imported is True
