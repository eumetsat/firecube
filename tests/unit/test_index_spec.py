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

from firecube.core.api import AUTO, IrregularTimeAxis
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


def test_auto_is_singleton_across_facades() -> None:
    assert AUTO is INGESTOR_AUTO
