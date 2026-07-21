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

from dataclasses import dataclass

import pytest

from firecube.ingestor.types.config import PluginConfig


@dataclass
class FutureAnnotatedConfig(PluginConfig):
    flag: bool | None = None
    count: int = 0
    ratio: float | None = None


def test_plugin_config_coerces_scalar_types_under_future_annotations():
    cfg = FutureAnnotatedConfig.from_options({"flag": "true", "count": "42", "ratio": "1.5"})
    assert cfg.flag is True
    assert cfg.count == 42
    assert cfg.ratio == 1.5


def test_plugin_config_rejects_unknown_keys():
    with pytest.raises(ValueError, match=r"Unknown configuration keys"):
        FutureAnnotatedConfig.from_options({"nope": "x"})


def test_plugin_config_rejects_invalid_booleans():
    with pytest.raises(ValueError, match=r"Invalid boolean"):
        FutureAnnotatedConfig.from_options({"flag": "maybe"})
