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

"""A plugin whose hook still reads ``write_mode`` off ``self.engine_config``, as pre-move plugins did."""

from __future__ import annotations

import os
from typing import Any, ClassVar

from firecube.ingestor.api import (
    BaseIngestor,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    register_ingestor,
)


@register_ingestor("engine_config_read_test_plugin")
class EngineConfigReadIngestor(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "engine_config_read_product"

    def _record_read(self, hook: str) -> None:
        # The tests read this file to learn what the hook saw.
        value = self.engine_config.write_mode  # type: ignore[attr-defined]
        out = os.environ.get("ENGINE_CONFIG_READ_OUT")
        if out:
            with open(out, "a", encoding="utf-8") as handle:
                handle.write(f"{hook}={value}\n")

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        self._record_read("batch")
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=str(ctx.target or "")))

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        self._record_read("discovery")
        return []
