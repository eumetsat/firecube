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

"""Generic Zarr ingestor for {plugin_name}."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginConfig,
    PluginContext,
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
class {class_name}(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    plugin_config_class = {class_name}Config

    def build_dataset(
        self, group: str, items: list[Any], ctx: PluginContext
    ) -> xr.Dataset | None:
        raise NotImplementedError(
            "{class_name}.build_dataset(): implement this hook to convert a batch of "
            "source items into an xarray.Dataset for the given group. Return None to "
            "intentionally skip writing this group/batch. "
            "See docs/concepts/plugins/generic-zarr.md for examples."
        )
