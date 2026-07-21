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

from collections.abc import Mapping
from typing import Any, ClassVar

import pytest

from firecube.core.api import SlotAxis, SlotIndexModel
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


def _slot_model() -> SlotIndexModel:
    return SlotIndexModel(
        name="parallel_capability_test_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"data": SlotAxis(cadence_s=1, mode="exact")},
    )


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
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
        return {"data": 1000}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return _slot_model()

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        return [ZarrGroupSpec(group="data")]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        return []


def test_non_direct_zarr_ingestor_fails() -> None:
    with pytest.raises(ConfigurationError, match="not a DirectZarrIngestor"):
        validate_parallel_capability(SimpleBaseIngestor(), 0, 100, ctx=None)  # type: ignore[arg-type]


def test_direct_zarr_non_capable_fails() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_parallel_capability(SimpleNonCapable(), 0, 100, ctx=None)  # type: ignore[arg-type]

    message = str(exc_info.value)
    assert "has not opted into slot-range parallelism" in message


def test_capable_plugin_passes() -> None:
    result = validate_parallel_capability(SimpleCapable(), 0, 100, ctx=None)  # type: ignore[arg-type]

    assert result == {"data": 1000}


def test_empty_global_count_returns_none_fails() -> None:
    class EmptyGlobalCount(SimpleCapable):
        PRODUCT_NAME: ClassVar[str] = "test_empty"

        def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
            return {}

    with pytest.raises(ConfigurationError, match="empty dict"):
        validate_parallel_capability(EmptyGlobalCount(), 0, 100, ctx=None)  # type: ignore[arg-type]


def test_global_count_returning_none_fails() -> None:
    class NoneGlobalCount(SimpleCapable):
        PRODUCT_NAME: ClassVar[str] = "test_none"

        def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
            return None

    with pytest.raises(ConfigurationError, match="None"):
        validate_parallel_capability(NoneGlobalCount(), 0, 100, ctx=None)  # type: ignore[arg-type]


def test_no_slot_flags_returns_none() -> None:
    result = validate_parallel_capability(SimpleBaseIngestor(), None, None, ctx=None)  # type: ignore[arg-type]

    assert result is None


def test_non_positive_global_count_fails() -> None:
    class NonPositiveGlobalCount(SimpleCapable):
        PRODUCT_NAME: ClassVar[str] = "test_non_positive"

        def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
            return {"data": 0}

    with pytest.raises(ConfigurationError, match="non-positive count"):
        validate_parallel_capability(NonPositiveGlobalCount(), 0, 100, ctx=None)  # type: ignore[arg-type]
