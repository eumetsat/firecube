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
from pathlib import Path
from typing import Any

import pytest

from firecube.ingestor.types.context import ExecutionParams
from tests.helpers.rows import table_row

pytestmark = pytest.mark.unit


def test_slot_group_character_warning(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "unsafe_chars_emit_warning", tmp_path):
        with pytest.warns(UserWarning, match="URL-encoded"):
            config = ExecutionParams(slot_group="multires/0.5deg")

        assert config.slot_group == "multires/0.5deg"

    with table_row(subtests, "safe_chars_no_warning", tmp_path):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            config = ExecutionParams(slot_group="group_a")

        assert config.slot_group == "group_a"
        assert caught == []

    with table_row(subtests, "warning_does_not_raise", tmp_path):
        with pytest.warns(UserWarning, match="URL-encoded"):
            config = ExecutionParams(slot_group="grp/with/slash", slot_start=0, slot_end=100)

        assert config.slot_group == "grp/with/slash"
        assert config.slot_start == 0
        assert config.slot_end == 100
