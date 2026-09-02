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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from firecube.ingestor.api import (
    BaseIngestor,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginConfig,
    PluginContext,
    RuntimeIngestContext,
    register_ingestor,
)

PRODUCT_NAME = "cli_test_product"


@dataclass
class CliTestPluginConfig(PluginConfig):
    test_int: int = 0
    test_str: str = "default"
    test_bool: bool = False


@register_ingestor("cli_test_plugin")
class CliTestIngestor(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME
    plugin_config_class = CliTestPluginConfig

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=Path(ctx.target or "")))

    def _aggregate_metrics(
        self,
        ctx: RuntimeIngestContext,
        state: PipelineRunState,
    ) -> Mapping[str, Any]:
        return self.default_aggregate_metrics(ctx, state)
