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

"""Tests for PluginContext boundary enforcement."""

import pytest

from firecube.ingestor.types.context import (
    IngestContext,
    PluginContext,
    RuntimeIngestContext,
)


def _make_plugin_ctx(**options):
    ictx = IngestContext(source="/tmp", options=options or {"key": "value"})
    rctx = RuntimeIngestContext.from_ingest_context(
        ictx, run_id="test", temp_root=None, materializer=None
    )
    return PluginContext(rctx), rctx


def test_options_are_immutable():
    pctx, _ = _make_plugin_ctx(foo="bar")
    with pytest.raises(TypeError):
        pctx.options["foo"] = "mutated"


def test_options_detached_from_runtime_context():
    pctx, rctx = _make_plugin_ctx(original="value")
    rctx.options["injected"] = "after_creation"
    assert "injected" not in pctx.options
    assert pctx.option("injected") is None


def test_option_and_options_are_consistent():
    pctx, rctx = _make_plugin_ctx(stable="yes")
    rctx.options["drift"] = "live"
    assert pctx.options.get("stable") == "yes"
    assert pctx.option("stable") == "yes"
    assert pctx.options.get("drift") is None
    assert pctx.option("drift") is None


def test_option_returns_default_for_missing_key():
    pctx, _ = _make_plugin_ctx()
    assert pctx.option("nonexistent", "fallback") == "fallback"
    assert pctx.option("nonexistent") is None


def test_forbidden_attributes_raise():
    pctx, _ = _make_plugin_ctx()
    assert pctx.storage is None
    with pytest.raises(AttributeError, match="forbidden"):
        _ = pctx._chunk_manager
    with pytest.raises(AttributeError, match="forbidden"):
        _ = pctx._materializer


def test_plugin_context_exposes_expected_properties():
    pctx, _ = _make_plugin_ctx()
    assert pctx.source == "/tmp"
    assert pctx.run_id == "test"
    assert hasattr(pctx.options, "get") and hasattr(pctx.options, "keys")
