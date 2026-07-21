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

import hashlib
from pathlib import Path

import pytest

from firecube.ingestor.runtime.batching import BatchPlanner
from firecube.ingestor.types.context import IngestContext, PluginContext, RuntimeIngestContext


class _Host:
    batch_id_prefix = "test_"

    def discover_source_files(self, ctx: PluginContext):
        _ = ctx
        return [f"/data/file_{i:03d}.bin" for i in range(120)]

    def filter_item(self, item, ctx: PluginContext) -> bool:
        _ = (item, ctx)
        return True

    def item_size_bytes(self, item) -> int | None:
        _ = item
        return 1

    def get_batch_groups(self, items, ctx: PluginContext) -> list[str]:
        _ = (items, ctx)
        return ["default"]


@pytest.mark.unit
def test_batch_metadata_contains_stable_files_hash():
    planner = BatchPlanner()
    host = _Host()
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source="."),
        run_id="batching-test",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )
    ctx = PluginContext(runtime_ctx)

    batch = next(iter(planner.create_batches(host, ctx, batch_size=200)))
    expected_full_uris = [f"/data/file_{i:03d}.bin" for i in range(120)]
    expected_hash = hashlib.sha256(
        "\n".join(sorted(expected_full_uris)).encode("utf-8")
    ).hexdigest()

    assert batch.metadata["item_uris_total"] == 120
    assert batch.metadata["item_uris_truncated"] is True
    assert batch.metadata["files_hash"] == expected_hash
