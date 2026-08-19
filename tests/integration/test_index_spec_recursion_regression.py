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

"""Recursion regression test for DirectZarrIngestor with index_spec.

This test verifies that a plugin whose zarr_schema() calls
self.resolved_index(ctx).size(group) does NOT cause a RecursionError.

The original mixin had a recursion: global_expected_time_count ->
_surface_chunk_alignment -> _cached_zarr_schema -> plugin.zarr_schema ->
global_expected_time_count (cache not yet populated).

The new architecture breaks this cycle: resolved_index is cached independently
of zarr_schema, so zarr_schema can safely call resolved_index(ctx).size(group).
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

pytestmark = pytest.mark.integration

_EPOCH = "2024-01-01T00:00:00Z"


class _RecursionProbePlugin(DirectZarrIngestor):
    """Plugin whose zarr_schema calls resolved_index(ctx).size(group)."""

    PRODUCT_NAME: ClassVar[str] = "recursion_probe"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="recursion_probe_v1",
            groups={
                "FWI": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=600,
                    mode="floor",
                    size=12,
                )
            },
        )

    def inspect_item(self, item: object, ctx: PluginContext) -> ItemInfo | None:
        _ = (item, ctx)
        return ItemInfo(coordinate=_EPOCH)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        # This is the pattern that caused RecursionError with the mixin.
        # resolved_index(ctx).size("FWI") must not recurse back into zarr_schema.
        n_times = self.resolved_index(ctx).size("FWI")
        return [
            ZarrGroupSpec(
                group="FWI",
                arrays=[
                    ZarrArraySpec(
                        name="fire_risk",
                        shape=(n_times, 10),
                        dtype="float32",
                        chunks=(1, 10),
                        fill_value=0.0,
                        expected_time_count=n_times,
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


def test_zarr_schema_calling_resolved_index_does_not_recurse() -> None:
    """zarr_schema can call resolved_index(ctx).size(group) without RecursionError."""
    plugin = _RecursionProbePlugin.__new__(_RecursionProbePlugin)
    ctx = cast(PluginContext, _plugin_ctx())
    plugin._bind_index_at_startup(ctx)

    schema = plugin.zarr_schema(ctx)

    assert len(schema) == 1
    assert schema[0].group == "FWI"
    assert schema[0].arrays[0].shape == (12, 10)
