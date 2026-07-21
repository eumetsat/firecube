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

from unittest.mock import patch

import pytest

from firecube.ingestor.api import IngestContext, IngestResult
from firecube.ingestor.registry import loader


@pytest.fixture(autouse=True)
def _restore_plugin_registry():
    """Snapshot and restore the global plugin registry around each test.

    The loader-cache tests deliberately clear ``loader.AVAILABLE_INGESTORS`` and
    repopulate it from mocked entry points. Without restoration, subsequent
    tests (notably ``test_typed_options_param.py``) see a polluted registry
    that no longer contains ``cli_test_plugin``, causing silent
    ``TypedOptionsParam`` schema-lookup failures.
    """
    saved_ingestors = loader.AVAILABLE_INGESTORS.copy()
    saved_loaded = loader._LOADED
    try:
        yield
    finally:
        loader.AVAILABLE_INGESTORS.clear()
        loader.AVAILABLE_INGESTORS.update(saved_ingestors)
        loader._LOADED = saved_loaded


class _DummyIngestorA:
    def run(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        return IngestResult(output_format="test")

    def ingest(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        return IngestResult(output_format="test")


class _DummyIngestorB:
    def run(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        return IngestResult(output_format="test")

    def ingest(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        return IngestResult(output_format="test")


class _FakeEntryPoint:
    def __init__(self, name, cls):
        self.name = name
        self._cls = cls

    def load(self):
        loader.register_ingestor(self.name)(self._cls)
        return self._cls


def test_plugin_discovery_cache():
    """Verify loader caches and returns stable plugin mappings."""

    loader.reset_plugin_discovery_cache()

    with patch("firecube.ingestor.registry.loader.entry_points") as mock_eps:
        mock_eps.return_value = [_FakeEntryPoint("demo_a", _DummyIngestorA)]
        first = loader.discover_ingestors()
        assert mock_eps.call_count == 1
        assert "demo_a" in first
        assert first["demo_a"] is _DummyIngestorA

        # Change discovered entry points; cache should still be served.
        mock_eps.return_value = [_FakeEntryPoint("demo_b", _DummyIngestorB)]
        second = loader.discover_ingestors()
        assert mock_eps.call_count == 1
        assert "demo_a" in second
        assert "demo_b" not in second


def test_cache_reset_reloads_plugins():
    """Verify reset clears cache and next call reflects current entry points."""
    loader.reset_plugin_discovery_cache()

    with patch("firecube.ingestor.registry.loader.entry_points") as mock_eps:
        mock_eps.return_value = [_FakeEntryPoint("demo_a", _DummyIngestorA)]
        first = loader.discover_ingestors()
        assert mock_eps.call_count == 1
        assert set(first) == {"demo_a"}

        loader.reset_plugin_discovery_cache()

        mock_eps.return_value = [_FakeEntryPoint("demo_b", _DummyIngestorB)]
        second = loader.discover_ingestors()
        assert mock_eps.call_count == 2
        assert set(second) == {"demo_b"}
