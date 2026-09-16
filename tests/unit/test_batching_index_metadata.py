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

"""Planner stamps ``batch_index`` / ``is_last`` without changing ids or hashes."""

from __future__ import annotations

from pathlib import Path

import pytest

from firecube.ingestor.runtime.batching import BatchPlanner
from firecube.ingestor.types.context import IngestContext, PluginContext, RuntimeIngestContext

pytestmark = pytest.mark.unit


class _Host:
    batch_id_prefix = "idx_"

    def __init__(self, count: int) -> None:
        self._count = count

    def discover_source_files(self, ctx: PluginContext):
        _ = ctx
        return [f"/data/file_{i:03d}.bin" for i in range(self._count)]

    def filter_item(self, item, ctx: PluginContext) -> bool:
        _ = (item, ctx)
        return True

    def item_size_bytes(self, item) -> int | None:
        _ = item
        return 1

    def get_batch_groups(self, items, ctx: PluginContext) -> list[str]:
        _ = (items, ctx)
        return ["default"]


def _ctx() -> PluginContext:
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source="."),
        run_id="batching-index-test",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )
    return PluginContext(runtime_ctx)


@pytest.mark.parametrize(
    ("count", "batch_size", "expected_sizes"),
    [
        (0, 10, []),
        (1, 10, [1]),
        (10, 10, [10]),
        (25, 10, [10, 10, 5]),
        (30, 10, [10, 10, 10]),
    ],
)
def test_batch_index_and_is_last_follow_planner_position(
    count: int, batch_size: int, expected_sizes: list[int]
) -> None:
    batches = list(BatchPlanner().create_batches(_Host(count), _ctx(), batch_size=batch_size))

    assert [b.files_count for b in batches] == expected_sizes
    assert [b.metadata["batch_index"] for b in batches] == list(range(len(batches)))
    assert [b.metadata["is_last"] for b in batches] == [
        i == len(batches) - 1 for i in range(len(batches))
    ]
    assert [b.batch_id for b in batches] == [f"idx_batch_{i:04d}" for i in range(len(batches))]


def test_lookahead_preserves_item_assignment_and_hash() -> None:
    batches = list(BatchPlanner().create_batches(_Host(25), _ctx(), batch_size=10))

    assert batches[0].items == [f"/data/file_{i:03d}.bin" for i in range(10)]
    assert batches[2].items == [f"/data/file_{i:03d}.bin" for i in range(20, 25)]
    hashes = {b.metadata["files_hash"] for b in batches}
    assert len(hashes) == 3
