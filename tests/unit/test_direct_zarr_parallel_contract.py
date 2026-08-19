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

from typing import cast
from unittest.mock import MagicMock

import pytest

from firecube.ingestor.api import DirectZarrIngestor, IngestResult, PipelineBatch, PluginContext


class _DirectZarrStub(DirectZarrIngestor):
    PRODUCT_NAME = "direct_stub"
    name = "direct_stub"

    def zarr_schema(self, ctx):
        return []

    def build_write_intents(self, batch, ctx):
        return []

    def ingest(self, *args, **kwargs) -> IngestResult:
        return cast(IngestResult, MagicMock())


def test_default_supports_flag_is_false() -> None:
    assert _DirectZarrStub().index_spec(cast(PluginContext, MagicMock(spec=PluginContext))) is None


def test_default_inspect_item_raises_not_implemented() -> None:
    ingestor = _DirectZarrStub()
    ctx = cast(PluginContext, MagicMock(spec=PluginContext))
    with pytest.raises(NotImplementedError, match=r"inspect_item"):
        ingestor.inspect_item(object(), ctx)


def test_simple_subclass_works_without_implementing_new_methods() -> None:
    ingestor = _DirectZarrStub()
    ctx = cast(PluginContext, MagicMock(spec=PluginContext))
    batch = cast(PipelineBatch, MagicMock(spec=PipelineBatch))
    assert ingestor.name == "direct_stub"
    assert ingestor.zarr_schema(ctx) == []
    assert ingestor.build_write_intents(batch, ctx) == []
