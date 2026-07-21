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

import pytest

from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.types.planned_range import validate_chunk_alignment

pytestmark = pytest.mark.unit


def test_aligned_range_passes() -> None:
    validate_chunk_alignment(0, 100, {"g": [(100,)]})


def test_misaligned_start_fails() -> None:
    with pytest.raises(ConfigurationError):
        validate_chunk_alignment(50, 200, {"g": [(100,)]}, global_expected={"g": 300})


def test_misaligned_end_non_terminal_fails() -> None:
    with pytest.raises(ConfigurationError):
        validate_chunk_alignment(100, 250, {"g": [(100,)]}, global_expected={"g": 500})


def test_misaligned_end_terminal_passes() -> None:
    validate_chunk_alignment(900, 950, {"g": [(100,)]}, global_expected={"g": 950})


def test_misaligned_end_terminal_without_global_expected_fails() -> None:
    with pytest.raises(ConfigurationError):
        validate_chunk_alignment(900, 950, {"g": [(100,)]})


def test_multiple_groups_one_terminal_one_not() -> None:
    with pytest.raises(ConfigurationError):
        validate_chunk_alignment(
            100,
            150,
            {"a": [(50,)], "b": [(100,)]},
            global_expected={"a": 150, "b": 500},
        )
