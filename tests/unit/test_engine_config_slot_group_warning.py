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

import warnings

import pytest

from firecube.ingestor.config.engine import EngineConfig

pytestmark = pytest.mark.unit


def test_unsafe_chars_emit_warning() -> None:
    with pytest.warns(UserWarning, match="URL-encoded"):
        config = EngineConfig(slot_group="multires/0.5deg")

    assert config.slot_group == "multires/0.5deg"


def test_safe_chars_no_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        config = EngineConfig(slot_group="group_a")

    assert config.slot_group == "group_a"
    assert caught == []


def test_warning_does_not_raise() -> None:
    with pytest.warns(UserWarning, match="URL-encoded"):
        config = EngineConfig(slot_group="grp/with/slash", slot_start=0, slot_end=100)

    assert config.slot_group == "grp/with/slash"
    assert config.slot_start == 0
    assert config.slot_end == 100
