# FIRECUBE_TEMPLATE_LICENSE_HEADER_BEGIN
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
# FIRECUBE_TEMPLATE_LICENSE_HEADER_END
{copyright_header}
"""Generic Parquet ingestor for {plugin_name}.

Only ``read_table`` knows the source format. Implement it; ``build_dataset``
concatenates what it returns into one table per batch.

A Parquet target accepts one run. Point every run, including a retry after a
failed run, at a new, empty target.
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
    """Read one source file as a ``pyarrow.Table``.

    Every file must produce the same columns and types. Reader modules are not
    loaded by ``import pyarrow``; import the one you use, for example
    ``import pyarrow.csv``.
    """
    raise NotImplementedError(
        f"read_table() is not implemented (called for {{path}}). Read the file and "
        "return a pyarrow.Table."
    )


@register_ingestor("{plugin_name}")
class {class_name}(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    # To accept your own ``--option key=value`` flags, attach a PluginConfig
    # subclass; see the Firecube "Add Plugin Configuration Options" guide.

    def build_dataset(
        self,
        group: str,  # Called once per output group; most plugins ignore this.
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> pa.Table | None:
        """Return one batch of source files as a single table, or ``None`` to skip it.

        Firecube calls this once per batch and writes each returned table as one
        Parquet part file. ``batch.items`` holds up to ``pipeline_batch_size``
        files (10 by default). A ``pandas.DataFrame`` is also accepted.
        """
        _ = group
        if not batch.items:
            return None

        tables = [read_table(ctx.materialize(item)) for item in batch.items]
        return pa.concat_tables(tables)
