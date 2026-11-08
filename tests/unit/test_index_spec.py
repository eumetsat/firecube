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
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from firecube.core import index_spec
from firecube.core.index_spec import _canonical_coordinate_value
from firecube.sdk import AUTO, IrregularTimeAxis, ItemInfo, RegularTimeAxis
from tests.helpers.rows import table_row


def test_irregular_time_axis_accepts_auto() -> None:
    axis = IrregularTimeAxis(coordinate="time", values=AUTO)

    assert axis.coordinate == "time"
    assert axis.values is AUTO


def test_irregular_time_axis_accepts_concrete_sequence() -> None:
    axis = IrregularTimeAxis(coordinate="obs_time", values=[1, 2, 3])

    assert axis.coordinate == "obs_time"
    assert axis.values == (1, 2, 3)


def test_irregular_time_axis_rejects_invalid_values(subtests: Any, tmp_path: Path) -> None:
    rows = (
        ("test_irregular_time_axis_rejects_empty_sequence", [], "non-empty"),
        ("test_irregular_time_axis_rejects_duplicate_values", [1, 1], "duplicates"),
    )
    for row_id, values, message in rows:
        with table_row(subtests, row_id, tmp_path), pytest.raises(ValueError, match=message):
            IrregularTimeAxis(coordinate="time", values=values)


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


def test_sdk_auto_is_the_index_spec_singleton() -> None:
    assert AUTO is index_spec.AUTO


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


def test_item_info_values_are_copied_read_only_and_outside_the_hash():
    buffer = np.array(1.5)
    info = ItemInfo(coordinate=0, values={"lon": buffer})
    buffer[...] = 9.0
    assert info == ItemInfo(coordinate=0, values={"lon": 1.5})
    assert hash(info) == hash(ItemInfo(coordinate=0))
    with pytest.raises(TypeError):
        info.values["lon"] = 2.0  # pyright: ignore[reportIndexIssue, reportOptionalSubscript]
