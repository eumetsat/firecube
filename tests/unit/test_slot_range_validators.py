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
from firecube.ingestor.types.planned_range import (
    validate_chunk_alignment,
    validate_slot_range,
    warn_if_misaligned,
)


def test_validate_slot_range_valid() -> None:
    validate_slot_range(0, 100)


def test_validate_slot_range_equal_fails() -> None:
    with pytest.raises(ValueError, match="slot_start must be < slot_end"):
        validate_slot_range(10, 10)


def test_validate_slot_range_negative_fails() -> None:
    with pytest.raises(ValueError, match="slot_start must be >= 0"):
        validate_slot_range(-1, 10)


def test_validate_chunk_alignment_fails_with_suggestion() -> None:
    with pytest.raises(ConfigurationError, match=r"data.*Suggested aligned range"):
        validate_chunk_alignment(50, 150, {"data": [(100,)]})


def test_validate_chunk_alignment_passes() -> None:
    validate_chunk_alignment(0, 100, {"data": [(100,)]})


def test_warn_if_misaligned_warns_not_raises(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("firecube.tests.slot_range")

    with caplog.at_level(logging.WARNING, logger="firecube.tests.slot_range"):
        warn_if_misaligned(50, 150, {"data": [(100,)]}, logger)

    assert any("Suggested aligned range" in message for message in caplog.messages)
