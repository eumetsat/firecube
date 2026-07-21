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

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from firecube.core.controlplane import ChunkManager
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.templates.generic import GenericParquetIngestor
from firecube.ingestor.types.context import PluginContext, RuntimeIngestContext
from tests.helpers.storage import make_test_binding, make_test_context


class _DummyParquetIngestor(GenericParquetIngestor):
    PRODUCT_NAME = "dummy_parquet"

    def build_dataset(self, group, batch, ctx):  # pragma: no cover - not used
        _ = (group, batch, ctx)
        return None


@pytest.mark.unit
def test_remote_parquet_write_requires_storage_config(tmp_path):
    ingestor = _DummyParquetIngestor(
        name="dummy_parquet",
        chunk_manager=ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path),
    )
    ingestor._chunk_manager.repo.storage_config = None

    table = pa.table({"value": [1]})
    ingest_ctx = make_test_context(tmp_path, source=str(tmp_path), product="dummy_parquet")
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        ingest_ctx,
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )
    ctx = PluginContext(runtime_ctx)

    with pytest.raises(
        ConfigurationError,
        match="storage_config is required for remote parquet writes",
    ):
        ingestor.write_parquet(
            table,
            output_path="s3://bucket/path/part-0.parquet",
            storage_options=None,
            ctx=ctx,
        )
