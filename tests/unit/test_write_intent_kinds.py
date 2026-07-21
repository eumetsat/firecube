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

"""Tests for the WriteIntent kind discriminator."""

from __future__ import annotations

import numpy as np

from firecube.ingestor.api import WriteIntent


def test_static_kind_constructs() -> None:
    """WriteIntent with kind='static' should construct without error."""
    intent = WriteIntent(group="g", array="lat", ts_index=0, data=np.zeros((4, 5)), kind="static")
    assert intent.kind == "static"


def test_static_kind_ts_index_ignored_semantically() -> None:
    """For static intents, ts_index is meaningless but accepted without error."""
    intent = WriteIntent(group="g", array="lat", ts_index=999, data=np.zeros((4, 5)), kind="static")
    assert intent.kind == "static"
    assert intent.ts_index == 999
