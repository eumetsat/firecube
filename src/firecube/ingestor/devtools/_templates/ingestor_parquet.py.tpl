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

"""Generic Parquet ingestor for {plugin_name}."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
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
class {class_name}(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    plugin_config_class = {class_name}Config

    def build_dataset(
        self, group: str, batch: PipelineBatch, ctx: PluginContext
    ) -> Any | None:
        # Return a pyarrow.Table or pandas.DataFrame for this group/batch.
        # Returning None skips writing for this group/batch (intentional skip).
        #
        # If you choose pandas.DataFrame, add `pandas` to your pyproject.toml
        # dependencies.
        raise NotImplementedError(
            "{class_name}.build_dataset(): implement this hook to convert a batch of "
            "source items into a pyarrow.Table or pandas.DataFrame for the given group. "
            "Return None to intentionally skip this group/batch. "
            "See docs/concepts/plugins/generic-parquet.md for examples."
        )
