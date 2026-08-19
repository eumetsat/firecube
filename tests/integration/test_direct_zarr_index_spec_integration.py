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

"""Integration test for DirectZarrIngestor with index_spec.

Tests the index_spec binding flow:
1. resolves a declarative index_spec into an IndexBinding
2. caches resolved_index per context
3. produces a legacy SlotIndexModel with byte-identical identity
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.runtime.index_binding import resolve_index_spec_for_ingestor

pytestmark = pytest.mark.integration

_EPOCH = "2024-01-01T00:00:00Z"


class _IntegrationPlugin(DirectZarrIngestor):
    """Minimal plugin for integration testing."""

    PRODUCT_NAME: ClassVar[str] = "integration_test"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="integration_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=600,
                    mode="exact",
                    size=10,
                )
            },
        )

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        _ = (item, ctx)
        return ItemInfo(coordinate=_EPOCH)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(10, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=10,
                        time_indexed=True,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


def _plugin_ctx() -> Any:
    return SimpleNamespace(_ctx=object(), run_id=None, option=lambda key, default=None: default)


def test_resolve_index_spec_for_ingestor_returns_binding() -> None:
    """resolve_index_spec_for_ingestor returns an IndexBinding for a capable plugin."""
    plugin = _IntegrationPlugin.__new__(_IntegrationPlugin)

    binding = resolve_index_spec_for_ingestor(plugin, cast(PluginContext, _plugin_ctx()))

    assert binding is not None
    assert binding.spec.name == "integration_v1"
    assert "data" in binding.resolved.groups
    assert binding.resolved.size("data") == 10
    assert binding.resolved.identity_hash is not None


def test_resolved_index_cached_per_context() -> None:
    """resolved_index returns the same object for the same context."""
    plugin = _IntegrationPlugin.__new__(_IntegrationPlugin)
    ctx = cast(PluginContext, _plugin_ctx())
    plugin._bind_index_at_startup(ctx)

    r1 = plugin.resolved_index(ctx)
    r2 = plugin.resolved_index(ctx)

    assert r1 is r2


def test_resolved_index_different_per_context() -> None:
    """resolved_index returns different objects for different contexts."""
    plugin = _IntegrationPlugin.__new__(_IntegrationPlugin)
    ctx1 = cast(PluginContext, _plugin_ctx())
    ctx2 = cast(PluginContext, _plugin_ctx())

    plugin._bind_index_at_startup(ctx1)
    r1 = plugin.resolved_index(ctx1)
    plugin._bind_index_at_startup(ctx2)
    r2 = plugin.resolved_index(ctx2)

    assert r1 is not r2


def test_as_legacy_slot_index_model_byte_parity() -> None:
    """as_legacy_slot_index_model produces a byte-identical SlotIndexModel."""
    from firecube.core.api import SlotAxis, SlotIndexModel, normalize_epoch_iso

    plugin = _IntegrationPlugin.__new__(_IntegrationPlugin)
    ctx = cast(PluginContext, _plugin_ctx())
    plugin._bind_index_at_startup(ctx)

    resolved = plugin.resolved_index(ctx)
    legacy = resolved.as_legacy_slot_index_model()

    expected = SlotIndexModel(
        name="integration_v1",
        epoch=normalize_epoch_iso(_EPOCH),
        groups={"data": SlotAxis(cadence_s=600, mode="exact")},
        time_unit=None,
    )

    assert legacy is not None
    assert legacy.canonical_bytes() == expected.canonical_bytes()
    assert legacy.identity_hash == expected.identity_hash
