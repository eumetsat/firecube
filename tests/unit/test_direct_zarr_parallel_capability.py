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

import datetime as dt
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    BaseIngestor,
    DirectZarrIngestor,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    ZarrGroupSpec,
)
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability


def _slot_model() -> IndexSpec:
    return IndexSpec(
        name="parallel_capability_test_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2026-01-01T00:00:00Z",
                cadence_s=1,
                mode="exact",
                size=1000,
            )
        },
    )


def _ctx() -> Any:
    return SimpleNamespace(_ctx=object())


class SimpleBaseIngestor(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "test_base"

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        raise NotImplementedError

    def _aggregate_metrics(
        self,
        ctx: RuntimeIngestContext,
        state: PipelineRunState,
    ) -> Mapping[str, Any]:
        return {}


class SimpleNonCapable(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "test_nc"

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        return []


class SimpleCapable(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "test_cap"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return _slot_model()

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        return [ZarrGroupSpec(group="data")]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        return []


def test_non_direct_zarr_ingestor_fails() -> None:
    with pytest.raises(ConfigurationError, match="not a DirectZarrIngestor"):
        validate_parallel_capability(SimpleBaseIngestor(), 0, 100, ctx=_ctx())


def test_direct_zarr_non_capable_fails() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_parallel_capability(SimpleNonCapable(), 0, 100, ctx=_ctx())

    message = str(exc_info.value)
    assert "require index_spec" in message


def test_capable_plugin_passes() -> None:
    result = validate_parallel_capability(SimpleCapable(), 0, 100, ctx=_ctx())

    assert result is not None
    assert result.resolved.size("data") == 1000


def test_no_slot_flags_returns_none() -> None:
    result = validate_parallel_capability(SimpleBaseIngestor(), None, None, ctx=_ctx())

    assert result is None
