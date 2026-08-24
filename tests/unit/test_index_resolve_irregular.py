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

"""Validation tests for irregular index resolution."""

from __future__ import annotations

import pytest

from firecube.core.errors import ConfigurationError
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, IrregularTimeAxis


def _spec(*, coordinate: str) -> IndexSpec:
    return IndexSpec(
        name="irregular_v1",
        groups={
            "data": IrregularTimeAxis(
                coordinate=coordinate,
                values=("2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"),
            )
        },
    )


def test_irregular_time_axis_coordinate_mismatch_raises_configuration_error() -> None:
    with pytest.raises(
        ConfigurationError, match=r"IrregularTimeAxis\.coordinate='timestamp'|IrregularTimeAxis"
    ) as exc_info:
        resolve_index_spec(_spec(coordinate="timestamp"), time_dim_name="time")

    message = str(exc_info.value)
    assert "IrregularTimeAxis.coordinate='timestamp'" in message
    assert "time_dim_name='time'" in message


def test_irregular_time_axis_coordinate_match_resolves() -> None:
    resolved = resolve_index_spec(_spec(coordinate="time"), time_dim_name="time")

    assert resolved.groups == ("data",)
