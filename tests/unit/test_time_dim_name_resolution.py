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
from dataclasses import fields
from typing import Any, ClassVar

import pytest

from firecube.ingestor.api import ZarrTemplateConfig
from firecube.ingestor.config.engine import EngineConfig, config_keys
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)


class _BaseProbe(BaseIngestor):
    PRODUCT_NAME = "probe"

    def run(self, ctx: IngestContext) -> IngestResult:
        raise NotImplementedError

    def ingest(self, ctx: IngestContext) -> IngestResult:
        raise NotImplementedError

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        _ = batch, ctx
        return PipelineResult(batch=batch)

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> Mapping[str, Any]:
        _ = ctx, state
        return {}


@pytest.mark.unit
def test_default_is_timestamp() -> None:
    assert _BaseProbe()._resolve_time_dim_name() == "timestamp"


@pytest.mark.unit
def test_subclass_override() -> None:
    class _TimeProbe(_BaseProbe):
        time_dim_name: ClassVar[str] = "time"

    assert _TimeProbe()._resolve_time_dim_name() == "time"


@pytest.mark.unit
def test_no_template_config_field() -> None:
    assert "time_dim_name" not in {f.name for f in fields(ZarrTemplateConfig)}


@pytest.mark.unit
def test_not_a_typed_option_key() -> None:
    assert "time_dim_name" not in config_keys(EngineConfig)
    assert "time_dim_name" not in config_keys(ZarrTemplateConfig)
