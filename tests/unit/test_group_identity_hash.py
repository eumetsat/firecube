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

pytestmark = pytest.mark.unit


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
