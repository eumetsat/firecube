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

"""Ingestor for {plugin_name}."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar

from firecube.core.api import discover_input_files
from firecube.ingestor.api import (
    BaseIngestor,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginConfig,
    PluginContext,
    RuntimeIngestContext,
    merge_batch_metrics,
    register_ingestor,
)


@dataclass
class {class_name}Config(PluginConfig):
    """Plugin configuration.

    To accept ``--option key=value`` flags, add dataclass fields here, e.g.::

        my_option: str = "default"
        batch_threshold: int = 100

    Fields appear under ``[PLUGIN]`` in ``firecube plugins describe {plugin_name}``.
    See docs/concepts/plugins/create-a-plugin.md for details.
    """

    pass


@register_ingestor("{plugin_name}")
class {class_name}(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    plugin_config_class = {class_name}Config

    def discover_source_files(self, ctx: PluginContext) -> Iterable[str]:
        # TODO: adjust include_suffixes / sniffing as needed for your input format.
        files = discover_input_files(ctx.source, recursive=True)
        return [str(p) for p in files]

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        raise NotImplementedError(
            "{class_name}._process_batch(): implement this hook to process a batch of "
            "source items and return a PipelineResult with outputs=OutputPaths(primary=...). "
            "See docs/concepts/plugins/base-ingestor.md for the contract and examples."
        )

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, object]:
        return merge_batch_metrics(ctx, state)
