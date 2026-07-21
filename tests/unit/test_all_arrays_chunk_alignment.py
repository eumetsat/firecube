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

from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.types.planned_range import validate_chunk_alignment, warn_if_misaligned


def test_heterogeneous_chunks_all_validated() -> None:
    with pytest.raises(ConfigurationError, match=r"Group 'g'.*\(50, 10\).*\(100, 10\)"):
        validate_chunk_alignment(0, 75, {"g": [(50, 10), (100, 10)]})


def test_aligned_for_all_arrays_passes() -> None:
    validate_chunk_alignment(0, 200, {"g": [(100, 10), (50, 10)]})


def test_misaligned_second_array_caught() -> None:
    with pytest.raises(ConfigurationError, match=r"Group 'g'.*\(100, 10\)"):
        validate_chunk_alignment(0, 50, {"g": [(50, 10), (100, 10)]})


def test_misaligned_error_names_problem_array() -> None:
    with pytest.raises(ConfigurationError) as excinfo:
        validate_chunk_alignment(0, 50, {"g": [(30, 10), (40, 10)]})

    message = str(excinfo.value)
    assert "Group 'g'" in message
    assert "(30, 10)" in message
    assert "(40, 10)" in message


def test_no_chunks_arrays_skipped(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("firecube.tests.slot_range")

    with caplog.at_level(logging.WARNING, logger="firecube.tests.slot_range"):
        warn_if_misaligned(0, 50, {"g": []}, logger)

    validate_chunk_alignment(0, 50, {"g": []})
    assert caplog.messages == []
