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

from dataclasses import dataclass
from typing import ClassVar

from firecube.ingestor.api import (
    BaseIngestor,
    PipelineBatch,
    PipelineResult,
    PluginConfig,
    PluginContext,
    register_ingestor,
)


@dataclass
class {class_name}Config(PluginConfig):
    """Plugin configuration.

    To accept ``--option key=value`` flags, add dataclass fields here, e.g.::

        my_option: str = "default"
        batch_threshold: int = 100

    Fields appear under ``[PLUGIN]`` in ``firecube plugins describe {plugin_name}``.
    See the Firecube plugin development guide for details.
    """

    pass


@register_ingestor("{plugin_name}")
class {class_name}(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    # See BaseIngestor.time_dim_name in the Firecube API reference.
    time_dim_name: ClassVar[str] = "timestamp"
    plugin_config_class = {class_name}Config

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        # Materialize items before passing them to readers that require local paths:
        # local_paths = [ctx.materialize(item) for item in batch.items]
        raise NotImplementedError(
            "{class_name}._process_batch(): implement this hook to process a batch of "
            "source items and return a PipelineResult. "
            "See the Firecube BaseIngestor guide for the contract and examples."
        )
