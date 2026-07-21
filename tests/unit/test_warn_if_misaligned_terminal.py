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

import logging

import pytest

from firecube.ingestor.types.planned_range import warn_if_misaligned

pytestmark = pytest.mark.unit


def test_terminal_partial_silent(caplog: pytest.LogCaptureFixture) -> None:
    """[900, 950) with total=950 — accepted terminal, no warning."""
    with caplog.at_level(logging.WARNING):
        warn_if_misaligned(
            900, 950, {"g": [(100,)]}, logging.getLogger("test"), global_expected={"g": 950}
        )
    assert not any("g" in r.message for r in caplog.records)


def test_non_terminal_misaligned_still_warns(caplog: pytest.LogCaptureFixture) -> None:
    """[100, 250) with total=500 — non-terminal misalignment, must warn."""
    with caplog.at_level(logging.WARNING):
        warn_if_misaligned(
            100, 250, {"g": [(100,)]}, logging.getLogger("test"), global_expected={"g": 500}
        )
    assert any("g" in r.message for r in caplog.records)


def test_multi_group_mixed_one_terminal_one_misaligned(caplog: pytest.LogCaptureFixture) -> None:
    """Group A terminal-partial (silent), Group B non-terminal misaligned (must warn)."""
    chunk_shapes_per_group = {"a": [(100,)], "b": [(100,)]}
    with caplog.at_level(logging.WARNING):
        warn_if_misaligned(
            100,
            150,
            chunk_shapes_per_group,
            logging.getLogger("test"),
            global_expected={"a": 150, "b": 500},
        )
    messages = [r.message for r in caplog.records]
    assert any("b" in m for m in messages), "Group B should warn"
    assert not any("Group 'a'" in m for m in messages), "Group A should not warn (terminal)"


def test_backward_compat_without_global_expected_warns_for_terminal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without global_expected, [900, 950) still warns (backward compat)."""
    with caplog.at_level(logging.WARNING):
        warn_if_misaligned(900, 950, {"g": [(100,)]}, logging.getLogger("test"))
    assert any("g" in r.message for r in caplog.records)
