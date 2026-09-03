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

"""Custom pipeline ingestor for {plugin_name}.

Only ``write_product_item`` knows the source and output formats. Implement it;
``_process_batch`` calls it once per source item and reports the target.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from firecube.core.api import local_path_from_target
from firecube.ingestor.api import (
    BaseIngestor,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    register_ingestor,
)


def write_product_item(source: Path, target_dir: Path) -> Path:
    """Convert one source file, write it below ``target_dir``, and return the written path."""
    raise NotImplementedError(
        f"write_product_item() is not implemented (called for {{source}} -> {{target_dir}}). "
        "Convert the file, write it below the target directory, and return the written path."
    )


@register_ingestor("{plugin_name}")
class {class_name}(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    # To accept ``--option key=value`` flags, attach a PluginConfig subclass;
    # see the Firecube "Add Plugin Configuration Options" guide.

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        target_dir = local_path_from_target(ctx.target or "")
        for item in batch.items:
            write_product_item(ctx.materialize(item), target_dir)
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=target_dir))
