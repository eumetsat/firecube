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

from firecube.ingestor.config.engine import EngineConfig


def test_slot_range_valid() -> None:
    cfg = EngineConfig(slot_start=0, slot_end=100)

    assert cfg.slot_start == 0
    assert cfg.slot_end == 100


def test_slot_range_unpaired_start_only() -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        EngineConfig(slot_start=0)


def test_slot_range_inverted() -> None:
    with pytest.raises(ValueError, match="slot_start must be < slot_end"):
        EngineConfig(slot_start=100, slot_end=50)


def test_slot_range_none_is_default() -> None:
    cfg = EngineConfig()

    assert cfg.slot_start is None
    assert cfg.slot_end is None
