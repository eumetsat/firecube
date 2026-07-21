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

"""Unit tests for ChunkManager read-model dedupe (W2C.3).

During a force-reingest run there is a brief window where prior spans and
the new run's spans are both ``status="active"`` (the prior spans are not
marked ``replaced`` until ``replacement_committed`` fires).  These tests
exercise the post-query dedupe in :meth:`ChunkManager.list_chunks` and
:meth:`ChunkManager.time_coverage_summary` so callers do not see
double coverage during that window.
"""

from __future__ import annotations

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding


def _record_active_span(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    group: str,
    batch_id: str,
    time_min: str = "2024-01-01T00:00:00",
    time_max: str = "2024-01-02T00:00:00",
) -> None:
    output_path = str(manager.workspace / product)
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    manager.record_span(
        product=product,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/FWI"],
            time_index_ranges=[[0, 1]],
            time_min=time_min,
            time_max=time_max,
        ),
        meta={
            "plugin": "test",
            "group": group,
            "time_min": time_min,
            "time_max": time_max,
        },
    )
    manager.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": "test"},
        status="complete",
    )


@pytest.mark.unit
class TestListChunksDedupe:
    def test_list_chunks_dedupes_duplicate_active_spans(self, temp_workspace):
        manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-aaa",
            group="F024",
            batch_id="batch-001",
        )
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-zzz",
            group="F024",
            batch_id="batch-002",
        )

        chunks = manager.list_chunks(product="test_product", chunk_type="span")

        assert len(chunks) == 1
        assert chunks[0].meta is not None
        assert chunks[0].meta["run_id"] == "run-zzz"
        assert chunks[0].status == "active"

    def test_list_chunks_no_dedup_different_groups(self, temp_workspace):
        manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-002",
            group="F048",
            batch_id="batch-002",
        )

        chunks = manager.list_chunks(product="test_product", chunk_type="span")

        assert len(chunks) == 2
        groups = {chunk.meta["group"] for chunk in chunks if chunk.meta is not None}
        assert groups == {"F024", "F048"}

    def test_list_chunks_no_dedup_different_time_ranges(self, temp_workspace):
        manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
            time_min="2024-01-01T00:00:00",
            time_max="2024-01-02T00:00:00",
        )
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-002",
            group="F024",
            batch_id="batch-002",
            time_min="2024-02-01T00:00:00",
            time_max="2024-02-02T00:00:00",
        )

        chunks = manager.list_chunks(product="test_product", chunk_type="span")

        assert len(chunks) == 2

    def test_list_chunks_dedupe_keeps_highest_run_id(self, temp_workspace):
        manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
        for run_id, batch_id in [
            ("run-bbb", "batch-001"),
            ("run-aaa", "batch-002"),
            ("run-ccc", "batch-003"),
        ]:
            _record_active_span(
                manager,
                product="test_product",
                run_id=run_id,
                group="F024",
                batch_id=batch_id,
            )

        chunks = manager.list_chunks(product="test_product", chunk_type="span")

        assert len(chunks) == 1
        assert chunks[0].meta is not None
        assert chunks[0].meta["run_id"] == "run-ccc"


@pytest.mark.unit
class TestTimeCoverageSummaryDedupe:
    def test_time_coverage_summary_dedupes(self, temp_workspace):
        manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-aaa",
            group="F024",
            batch_id="batch-001",
        )
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-zzz",
            group="F024",
            batch_id="batch-002",
        )

        summary = manager.time_coverage_summary(product="test_product")

        assert len(summary) == 1
        entry = summary[0]
        assert entry["group"] == "F024"
        assert entry["span_count"] == 1
        assert entry["time_min"] == "2024-01-01T00:00:00"
        assert entry["time_max"] == "2024-01-02T00:00:00"

    def test_time_coverage_summary_no_dedup_different_groups(self, temp_workspace):
        manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )
        _record_active_span(
            manager,
            product="test_product",
            run_id="run-002",
            group="F048",
            batch_id="batch-002",
        )

        summary = manager.time_coverage_summary(product="test_product")

        assert len(summary) == 2
        assert {entry["group"] for entry in summary} == {"F024", "F048"}
        assert all(entry["span_count"] == 1 for entry in summary)
