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

"""Generic Parquet ingestor for {plugin_name}.

Only ``read_table`` knows the source format. Implement it; ``build_dataset``
concatenates what it returns into one table per batch.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pyarrow as pa

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
    PluginContext,
    register_ingestor,
)


def read_table(path: Path) -> pa.Table:
    """Read one source file as a ``pyarrow.Table``."""
    raise NotImplementedError(
        f"read_table() is not implemented (called for {{path}}). Read the file and "
        "return a pyarrow.Table."
    )


@register_ingestor("{plugin_name}")
class {class_name}(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    # To accept ``--option key=value`` flags, attach a PluginConfig subclass;
    # see the Firecube "Add Plugin Configuration Options" guide.

    def build_dataset(
        self,
        group: str,  # Called once per output group; most plugins ignore this.
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> pa.Table | None:  # May also return a pandas.DataFrame; None skips the batch.
        _ = group
        if not batch.items:
            return None

        tables = [read_table(ctx.materialize(item)) for item in batch.items]
        return pa.concat_tables(tables)
