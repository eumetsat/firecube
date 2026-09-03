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

import datetime as _dt

import numpy as np
import pytest

from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.unit

_normalize = RegionZarrWriter._normalize_for_coord_compare


class TestNormalizeForCoordCompareNaTSemantics:
    def test_two_nats_compare_equal_at_seconds_dtype(self) -> None:
        target = np.dtype("datetime64[s]")
        left = _normalize(np.datetime64("NaT", "ns"), target)
        right = _normalize(np.datetime64("NaT", "s"), target)
        assert np.isnat(left)
        assert np.isnat(right)
        assert left.dtype == target
        assert right.dtype == target

    def test_nat_input_returns_nat_in_target_dtype(self) -> None:
        for target_unit in ("s", "ms", "us", "ns"):
            target = np.dtype(f"datetime64[{target_unit}]")
            result = _normalize(np.datetime64("NaT", "ns"), target)
            assert np.isnat(result)
            assert result.dtype == target

    def test_nat_vs_real_value_are_not_equal(self) -> None:
        target = np.dtype("datetime64[s]")
        nat = _normalize(np.datetime64("NaT", "ns"), target)
        real = _normalize(np.datetime64("2024-01-01T00:00:00", "s"), target)
        assert np.isnat(nat)
        assert not np.isnat(real)
        assert nat != real


class TestNormalizeForCoordCompareTruncation:
    def test_subsecond_incoming_truncated_to_seconds_matches_stored_seconds(self) -> None:
        target = np.dtype("datetime64[s]")
        stored = np.datetime64("2026-03-15T10:20:30", "s")
        incoming = np.datetime64("2026-03-15T10:20:30.123456789", "ns")
        stored_norm = _normalize(stored, target)
        incoming_norm = _normalize(incoming, target)
        assert stored_norm == incoming_norm
        assert stored_norm.dtype == target
        assert incoming_norm.dtype == target

    def test_both_sides_truncated_stored_ns_vs_incoming_s(self) -> None:
        target = np.dtype("datetime64[s]")
        stored_ns = np.datetime64("2026-03-15T10:20:30.999999999", "ns")
        incoming_s = np.datetime64("2026-03-15T10:20:30", "s")
        assert _normalize(stored_ns, target) == _normalize(incoming_s, target)

    def test_python_datetime_input_normalized(self) -> None:
        target = np.dtype("datetime64[s]")
        py_dt = _dt.datetime(2026, 3, 15, 10, 20, 30, 500000)
        expected = np.datetime64("2026-03-15T10:20:30", "s")
        assert _normalize(py_dt, target) == expected

    def test_return_dtype_matches_target_across_units(self) -> None:
        value = np.datetime64("2026-03-15T10:20:30.123456789", "ns")
        for unit in ("s", "ms", "us", "ns"):
            target = np.dtype(f"datetime64[{unit}]")
            result = _normalize(value, target)
            assert result.dtype == target


class TestNormalizeForCoordCompareDivergent:
    def test_different_seconds_do_not_equate_after_truncation(self) -> None:
        target = np.dtype("datetime64[s]")
        a = np.datetime64("2026-03-15T10:20:30", "s")
        b = np.datetime64("2026-03-15T10:20:31", "s")
        assert _normalize(a, target) != _normalize(b, target)

    def test_subsecond_divergence_hidden_under_seconds_dtype(self) -> None:
        target = np.dtype("datetime64[s]")
        a = np.datetime64("2026-03-15T10:20:30.100000000", "ns")
        b = np.datetime64("2026-03-15T10:20:30.900000000", "ns")
        assert _normalize(a, target) == _normalize(b, target)

    def test_subsecond_divergence_preserved_under_ns_dtype(self) -> None:
        target = np.dtype("datetime64[ns]")
        a = np.datetime64("2026-03-15T10:20:30.100000000", "ns")
        b = np.datetime64("2026-03-15T10:20:30.900000000", "ns")
        assert _normalize(a, target) != _normalize(b, target)


class TestNormalizeForCoordCompareContract:
    def test_non_datetime_target_raises_typeerror(self) -> None:
        with pytest.raises(TypeError):
            _normalize(np.datetime64("2026-03-15T10:20:30", "s"), np.dtype("int64"))

    def test_unparseable_string_input_propagates_valueerror(self) -> None:
        target = np.dtype("datetime64[s]")
        with pytest.raises(ValueError):
            _normalize("not-a-date", target)

    def test_non_datetime_object_input_propagates_valueerror(self) -> None:
        target = np.dtype("datetime64[s]")
        with pytest.raises(ValueError):
            _normalize(object(), target)
