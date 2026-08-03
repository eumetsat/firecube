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

"""Direct Zarr ingestor for {plugin_name}."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginConfig,
    PluginContext,
    WriteIntent,
    ZarrGroupSpec,
    register_ingestor,
)


@dataclass
class {class_name}Config(PluginConfig):
    """Plugin configuration.

    To accept ``--option key=value`` flags, add dataclass fields here.
    See the Firecube plugin development guide.
    """

    pass


@register_ingestor("{plugin_name}")
class {class_name}(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    plugin_config_class = {class_name}Config

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        raise NotImplementedError(
            "{class_name}.zarr_schema(): implement this hook to declare the Zarr store "
            "layout. Return a list[ZarrGroupSpec] describing groups, arrays, shapes, "
            "dtypes, and chunks. "
            "See the Firecube DirectZarrIngestor guide for examples."
        )

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        # Materialize items before passing them to readers that require local paths:
        # local_paths = [ctx.materialize(item) for item in batch.items]
        raise NotImplementedError(
            "{class_name}.build_write_intents(): implement this hook to convert a batch "
            "into a list[WriteIntent] describing region writes, 1-D writes, or timestamp "
            "writes. Return an empty list to intentionally skip a batch. "
            "See the Firecube DirectZarrIngestor guide for the WriteIntent API."
        )
