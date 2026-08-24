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

"""Tests for the derived parallel capability gate (IndexBinding-based)."""

from __future__ import annotations

from typing import Any

import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor


class _MockCtx:
    _ctx = object()

    def option(self, *args, **kwargs):
        return None

    run_id = None


class _SerialPlugin(DirectZarrIngestor):
    PRODUCT_NAME = "serial_probe"

    def zarr_schema(self, ctx):
        return []

    def build_write_intents(self, batch, ctx):
        return []


class _NoInspectPlugin(DirectZarrIngestor):
    PRODUCT_NAME = "no_inspect_probe"

    def zarr_schema(self, ctx):
        return []

    def build_write_intents(self, batch, ctx):
        return []

    def index_spec(self, ctx):
        return IndexSpec(
            name="v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    slot_count=10,
                )
            },
        )


class _FullPlugin(DirectZarrIngestor):
    PRODUCT_NAME = "full_probe"

    def zarr_schema(self, ctx):
        return []

    def build_write_intents(self, batch, ctx):
        return []

    def index_spec(self, ctx):
        return IndexSpec(
            name="v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    slot_count=10,
                )
            },
        )

    def inspect_item(self, item, ctx):
        return ItemInfo(coordinate="2024-01-01T00:00:00Z")


def test_gate_rejects_serial_plugin():
    """Gate rejects plugin whose index_spec returns None."""
    plugin = _SerialPlugin.__new__(_SerialPlugin)
    ctx: Any = _MockCtx()
    with pytest.raises(ConfigurationError, match="index_spec"):
        validate_parallel_capability(plugin, 0, 10, ctx)


def test_gate_rejects_plugin_without_inspect_item():
    """Gate rejects plugin that has index_spec but no inspect_item override."""
    plugin = _NoInspectPlugin.__new__(_NoInspectPlugin)
    ctx: Any = _MockCtx()
    with pytest.raises(ConfigurationError, match="inspect_item"):
        validate_parallel_capability(plugin, 0, 10, ctx)


def test_gate_accepts_full_plugin():
    """Gate accepts plugin with both index_spec and inspect_item."""
    plugin = _FullPlugin.__new__(_FullPlugin)
    ctx: Any = _MockCtx()
    result = validate_parallel_capability(plugin, 0, 10, ctx)
    assert result is not None
