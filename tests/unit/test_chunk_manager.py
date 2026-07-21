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

"""Unit tests for ChunkManager functionality."""

from __future__ import annotations

import pytest

from firecube.core.controlplane import ChunkInfo, ChunkManager, SpanCoverage, build_span_entry
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri


def _chunk_manager(workspace, base_uri: str | None = None) -> ChunkManager:
    if base_uri is None:
        product_uri = StorageUri.from_local_path(workspace / "__firecube_controlplane__")
    else:
        parsed_base = (
            StorageUri.parse(base_uri)
            if "://" in base_uri
            else StorageUri.from_local_path(base_uri)
        )
        product_uri = parsed_base.join("__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=workspace)


def _record_completed_span_run(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    group: str,
    batch_id: str,
    status: str = "active",
    plugin: str = "test",
    time_min: str | None = "2024-01-01T00:00:00",
    time_max: str | None = "2024-01-02T00:00:00",
) -> None:
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=str(manager.workspace / product),
        output_format="zarr",
        size=0,
        meta={"plugin": plugin},
    )
    coverage = None
    if status == "active":
        coverage = SpanCoverage(
            group=group,
            arrays=[f"{group}/FWI"],
            time_index_ranges=[[0, 1]],
            time_min=time_min,
            time_max=time_max,
        )
    meta = {"plugin": plugin, "group": group}
    if time_min is not None:
        meta["time_min"] = time_min
    if time_max is not None:
        meta["time_max"] = time_max
    manager.record_span(
        product=product,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status=status,
        reason=None if status == "active" else status,
        coverage=coverage,
        meta=meta,
    )
    manager.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=str(manager.workspace / product),
        output_format="zarr",
        size=1,
        meta={"plugin": plugin},
        status="complete",
    )


def _record_started_run(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
) -> None:
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=str(manager.workspace / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )


def _chunk_group(chunk: ChunkInfo) -> str:
    assert chunk.meta is not None
    return str(chunk.meta["group"])


@pytest.mark.unit
class TestChunkManager:
    def test_manifest_discovery(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-msg",
            group="F024",
            batch_id="batch-001",
        )
        _record_completed_span_run(
            manager,
            product="goes_aod",
            run_id="run-goes",
            group="AOD",
            batch_id="batch-001",
        )

        manifests = manager.discover_manifests()

        assert len(manifests) == 2
        assert (
            StorageUri.from_local_path(temp_workspace / "test_product" / ".firecube").to_str()
            in manifests
        )
        assert (
            StorageUri.from_local_path(temp_workspace / "goes_aod" / ".firecube").to_str()
            in manifests
        )

    def test_list_all_chunks(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="F048",
            batch_id="batch-002",
            status="skipped",
        )

        all_chunks = manager.list_chunks(product="test_product")

        assert len(all_chunks) == 2
        assert {chunk.chunk_type for chunk in all_chunks} == {"span"}

    def test_filter_by_product_does_not_scan_all_manifests(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )

        def _fail_discovery():
            raise AssertionError(
                "discover_manifests() should not be called for product-scoped list"
            )

        manager.repo.discover_manifests = _fail_discovery  # type: ignore[method-assign]

        chunks = manager.list_chunks(product="test_product")

        assert len(chunks) == 1

    def test_filter_by_pattern(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="F120",
            batch_id="batch-002",
        )

        f024_chunks = manager.list_chunks(product="test_product", pattern="span_*_F024")

        assert len(f024_chunks) == 1
        assert f024_chunks[0].meta is not None
        assert f024_chunks[0].meta["group"] == "F024"

    def test_filter_by_chunk_type(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )

        span_entries = manager.list_chunks(product="test_product", chunk_type="span")
        run_entries = manager.list_chunks(product="test_product", chunk_type="run")

        assert len(span_entries) == 1
        assert len(run_entries) == 1
        assert run_entries[0].chunk_type == "run"

    def test_list_chunks_time_range_time_min_after(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="Q1",
            batch_id="batch-001",
            time_min="2024-01-01T00:00:00",
            time_max="2024-03-31T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="Q2",
            batch_id="batch-002",
            time_min="2024-04-01T00:00:00",
            time_max="2024-06-30T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-003",
            group="Q3",
            batch_id="batch-003",
            time_min="2024-07-01T00:00:00",
            time_max="2024-09-30T23:59:59",
        )

        chunks = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_min_after="2024-04-01T00:00:00",
        )

        assert len(chunks) == 2
        assert {_chunk_group(chunk) for chunk in chunks} == {"Q2", "Q3"}

    def test_list_chunks_time_range_time_max_before(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="Q1",
            batch_id="batch-001",
            time_min="2024-01-01T00:00:00",
            time_max="2024-03-31T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="Q2",
            batch_id="batch-002",
            time_min="2024-04-01T00:00:00",
            time_max="2024-06-01T00:00:00",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-003",
            group="Q3",
            batch_id="batch-003",
            time_min="2024-07-01T00:00:00",
            time_max="2024-09-30T23:59:59",
        )

        chunks = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_max_before="2024-06-01T00:00:00",
        )

        assert len(chunks) == 2
        assert {_chunk_group(chunk) for chunk in chunks} == {"Q1", "Q2"}

    def test_list_chunks_time_overlap_returns_partial_matches(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="Q1",
            batch_id="batch-001",
            time_min="2024-01-01T00:00:00",
            time_max="2024-03-31T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="Q2",
            batch_id="batch-002",
            time_min="2024-04-01T00:00:00",
            time_max="2024-06-30T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-003",
            group="Q3",
            batch_id="batch-003",
            time_min="2024-07-01T00:00:00",
            time_max="2024-09-30T23:59:59",
        )

        chunks = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_overlaps=("2024-02-01T00:00:00", "2024-05-01T00:00:00"),
        )

        assert len(chunks) == 2
        assert {_chunk_group(chunk) for chunk in chunks} == {"Q1", "Q2"}

    def test_list_chunks_time_overlap_excludes_non_overlapping_spans(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="Q1",
            batch_id="batch-001",
            time_min="2024-01-01T00:00:00",
            time_max="2024-03-31T23:59:59",
        )

        chunks = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_overlaps=("2024-04-01T00:00:00", "2024-06-01T00:00:00"),
        )

        assert chunks == []

    def test_list_chunks_time_range_excludes_records_without_time_meta(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="WITH-TIME",
            batch_id="batch-001",
            time_min="2024-04-01T00:00:00",
            time_max="2024-06-30T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="WITHOUT-TIME",
            batch_id="batch-002",
            time_min=None,
            time_max=None,
        )

        min_after = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_min_after="2024-04-01T00:00:00",
        )
        max_before = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_max_before="2024-07-01T00:00:00",
        )
        overlaps = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            time_overlaps=("2024-04-01T00:00:00", "2024-05-01T00:00:00"),
        )

        assert {_chunk_group(chunk) for chunk in min_after} == {"WITH-TIME"}
        assert {_chunk_group(chunk) for chunk in max_before} == {"WITH-TIME"}
        assert {_chunk_group(chunk) for chunk in overlaps} == {"WITH-TIME"}

    def test_list_chunks_time_range_combines_with_other_filters(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
            time_min="2024-04-01T00:00:00",
            time_max="2024-04-15T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="F024",
            batch_id="batch-002",
            status="skipped",
            time_min="2024-04-10T00:00:00",
            time_max="2024-04-20T23:59:59",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-003",
            group="F048",
            batch_id="batch-003",
            time_min="2024-04-10T00:00:00",
            time_max="2024-04-20T23:59:59",
        )

        chunks = manager.list_chunks(
            product="test_product",
            chunk_type="span",
            status="active",
            meta={"group": "F024"},
            time_min_after="2024-04-01T00:00:00",
        )

        assert len(chunks) == 1
        assert _chunk_group(chunks[0]) == "F024"
        assert chunks[0].status == "active"

    def test_span_coverage_timestamps_written_counts_ranges(self):
        coverage = SpanCoverage(group="G", arrays=["G/x"], time_index_ranges=[[0, 9], [20, 29]])

        assert coverage.timestamps_written == 20

    def test_span_coverage_timestamps_written_single_timestamp(self):
        coverage = SpanCoverage(group="G", arrays=["G/x"], time_index_ranges=[[0, 0]])

        assert coverage.timestamps_written == 1

    def test_span_coverage_timestamps_written_empty_ranges(self):
        coverage = SpanCoverage(group="G", arrays=["G/x"], time_index_ranges=[])

        assert coverage.timestamps_written == 0

    def test_build_span_entry_includes_timestamps_written(self):
        entry = build_span_entry(
            run_id="run-001",
            batch_id="batch-001",
            group="G",
            meta={"plugin": "test"},
            arrays=["G/x"],
            time_index_ranges=[[0, 9], [20, 29]],
        )

        assert entry["span"]["timestamps_written"] == 20

    def test_chunk_info_exposes_span_timestamps_written(self):
        chunk = ChunkInfo(
            key="span_run-001_batch-001_G",
            product="test_product",
            chunk_type="span",
            size=0,
            timestamp=0.0,
            manifest_path="/tmp/manifest.jsonl",
            record={"span": {"timestamps_written": 20}},
        )

        assert chunk.timestamps_written == 20

    def test_list_runs_filters_by_status_started(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_started_run(manager, product="P", run_id="run-started")
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-complete",
            group="F024",
            batch_id="batch-001",
        )
        manager.record_run_terminal(
            product="P",
            run_id="run-failed",
            output_path=str(manager.workspace / "P"),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="failed",
            error="boom",
        )

        runs = manager.list_runs(product="P", status="started")

        assert [run.status for run in runs] == ["started"]
        assert [run.run_id for run in runs] == ["run-started"]

    def test_list_runs_filters_non_terminal_runs(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_started_run(manager, product="P", run_id="run-started")
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-complete",
            group="F024",
            batch_id="batch-001",
        )
        manager.record_run_terminal(
            product="P",
            run_id="run-failed",
            output_path=str(manager.workspace / "P"),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="failed",
            error="boom",
        )
        _record_started_run(manager, product="P", run_id="run-abandoned")
        manager.abandon_run(product="P", run_id="run-abandoned", reason="stale")

        runs = manager.list_runs(product="P", non_terminal=True)

        assert [run.status for run in runs] == ["started"]
        assert [run.run_id for run in runs] == ["run-started"]

    def test_list_runs_filters_by_status_complete(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_started_run(manager, product="P", run_id="run-started")
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-complete",
            group="F024",
            batch_id="batch-001",
        )
        _record_started_run(manager, product="P", run_id="run-abandoned")
        manager.abandon_run(product="P", run_id="run-abandoned", reason="stale")

        runs = manager.list_runs(product="P", status="complete")

        assert [run.status for run in runs] == ["complete"]
        assert [run.run_id for run in runs] == ["run-complete"]

    def test_list_runs_empty_product_returns_empty_list(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)

        assert manager.list_runs(product="P") == []

    def test_list_runs_non_terminal_excludes_terminal_runs(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_started_run(manager, product="P", run_id="run-started")
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-complete",
            group="F024",
            batch_id="batch-001",
        )
        manager.record_run_terminal(
            product="P",
            run_id="run-failed",
            output_path=str(manager.workspace / "P"),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="failed",
            error="boom",
        )
        _record_started_run(manager, product="P", run_id="run-abandoned")
        manager.abandon_run(product="P", run_id="run-abandoned", reason="stale")

        runs = manager.list_runs(product="P", non_terminal=True)

        assert [run.run_id for run in runs] == ["run-started"]
        assert all(not run.is_terminal for run in runs)

    def test_time_coverage_summary_returns_expected_shape(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-001",
            group="G1",
            batch_id="batch-001",
        )

        time_coverage_summary = manager.time_coverage_summary
        summary = time_coverage_summary(product="P")

        assert isinstance(summary, list)
        assert summary == [
            {
                "group": "G1",
                "time_min": "2024-01-01T00:00:00",
                "time_max": "2024-01-02T00:00:00",
                "span_count": 1,
                "total_timestamps_written": 2,
            }
        ]

    def test_time_coverage_summary_aggregates_per_group(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-001",
            group="G1",
            batch_id="batch-001",
            time_min="2024-01-03T00:00:00",
            time_max="2024-01-05T00:00:00",
        )
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-002",
            group="G1",
            batch_id="batch-002",
            time_min="2024-01-01T00:00:00",
            time_max="2024-01-04T00:00:00",
        )
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-003",
            group="G2",
            batch_id="batch-003",
            time_min="2024-02-01T00:00:00",
            time_max="2024-02-03T00:00:00",
        )

        time_coverage_summary = manager.time_coverage_summary
        summary = time_coverage_summary(product="P")

        assert len(summary) == 2
        assert summary[0] == {
            "group": "G1",
            "time_min": "2024-01-01T00:00:00",
            "time_max": "2024-01-05T00:00:00",
            "span_count": 2,
            "total_timestamps_written": 4,
        }
        assert summary[1] == {
            "group": "G2",
            "time_min": "2024-02-01T00:00:00",
            "time_max": "2024-02-03T00:00:00",
            "span_count": 1,
            "total_timestamps_written": 2,
        }

    def test_time_coverage_summary_filters_by_meta(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-001",
            group="G1",
            batch_id="batch-001",
            plugin="test",
        )
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-002",
            group="G2",
            batch_id="batch-002",
            plugin="other",
        )

        time_coverage_summary = manager.time_coverage_summary
        summary = time_coverage_summary(product="P", meta={"plugin": "test"})

        assert summary == [
            {
                "group": "G1",
                "time_min": "2024-01-01T00:00:00",
                "time_max": "2024-01-02T00:00:00",
                "span_count": 1,
                "total_timestamps_written": 2,
            }
        ]

    def test_time_coverage_summary_empty_product_returns_empty_list(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)

        time_coverage_summary = manager.time_coverage_summary
        assert time_coverage_summary(product="P") == []

    def test_time_coverage_summary_sorted_by_group(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-001",
            group="G2",
            batch_id="batch-001",
        )
        _record_completed_span_run(
            manager,
            product="P",
            run_id="run-002",
            group="G1",
            batch_id="batch-002",
        )

        time_coverage_summary = manager.time_coverage_summary
        summary = time_coverage_summary(product="P")

        assert [entry["group"] for entry in summary] == ["G1", "G2"]

    def test_mark_chunks_replaced(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-001",
            group="F024",
            batch_id="batch-001",
        )
        _record_completed_span_run(
            manager,
            product="test_product",
            run_id="run-002",
            group="F048",
            batch_id="batch-002",
        )

        chunks = manager.list_chunks(product="test_product", chunk_type="span")
        chunk_keys = [c.key for c in chunks[:1]]

        result = manager.mark_chunks_replaced(chunk_keys, "test_product", 123.0)

        assert result["marked_count"] == 1
        remaining = manager.list_chunks(product="test_product", chunk_type="span")
        assert len(remaining) == 1
        assert remaining[0].meta is not None
        assert remaining[0].meta["group"] == "F024" or remaining[0].meta["group"] == "F048"
        assert all(chunk.key not in chunk_keys for chunk in remaining)


@pytest.mark.integration
class TestChunkManagerIntegration:
    def test_base_uri_configuration(self, temp_workspace):
        output_uri = "s3://test-bucket/data"
        manager = _chunk_manager(temp_workspace, output_uri)

        assert manager.base_uri == output_uri

    def test_binding_base_updates_product_and_control_roots(self, temp_workspace):
        manager = _chunk_manager(temp_workspace, "s3://test-bucket/data")

        assert manager.get_product_root("test_product") == "s3://test-bucket/data/test_product"
        assert (
            manager.get_control_root("test_product")
            == "s3://test-bucket/data/test_product/.firecube"
        )
        assert (
            manager.get_latest_pointer("test_product")
            == "s3://test-bucket/data/test_product/.firecube/LATEST.json"
        )

    def test_rebind_setter_is_not_available(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)
        manager.record_run_started(
            product="test_product",
            run_id="run-001",
            output_path=str(temp_workspace / "test_product"),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )

        with pytest.raises(AttributeError):
            attr_name = "output_base_uri"
            setattr(manager, attr_name, "s3://test-bucket/data")
