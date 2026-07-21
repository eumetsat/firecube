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
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


@dataclass
class {class_name}Config(PluginConfig):
    """Plugin configuration.

    To accept ``--option key=value`` flags, add dataclass fields here.
    See docs/concepts/plugins/create-a-plugin.md.
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
            "dtypes, and chunks. The previous scaffold hard-coded fake shape "
            "(1000, 100, 100) float32 here — you must replace that with your real schema. "
            "See docs/concepts/plugins/direct-zarr.md for examples."
        )

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent]:
        raise NotImplementedError(
            "{class_name}.build_write_intents(): implement this hook to convert a batch "
            "into a list[WriteIntent] describing region writes, 1-D writes, or timestamp "
            "writes. Return an empty list to intentionally skip a batch. "
            "See docs/concepts/plugins/direct-zarr.md for the WriteIntent API."
        )
