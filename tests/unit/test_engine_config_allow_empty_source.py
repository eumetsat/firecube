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


@pytest.mark.unit
def test_default_false():
    cfg = EngineConfig()
    assert cfg.allow_empty_source is False


@pytest.mark.unit
def test_from_options_true():
    cfg = EngineConfig.from_options({"allow_empty_source": "true"})
    assert cfg.allow_empty_source is True


@pytest.mark.unit
def test_from_options_false():
    cfg = EngineConfig.from_options({"allow_empty_source": "false"})
    assert cfg.allow_empty_source is False
