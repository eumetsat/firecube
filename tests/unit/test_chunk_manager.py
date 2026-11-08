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

import logging
import re
import time
from contextlib import ExitStack, closing
from pathlib import Path
from typing import Any

import pytest

from firecube.cli._errors import render
from firecube.cli.main import cli
from firecube.core.controlplane import ChunkInfo, ChunkManager, SpanCoverage, build_span_entry
from firecube.core.controlplane.manager import maintenance_refusal_message
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ClaimConflictError, ManifestError
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from tests.helpers.cli_hints import run_hint
from tests.helpers.rows import table_row
from tests.helpers.storage import make_test_binding
from tests.unit._helpers.counting_fs import CountingFilesystem, make_counting_local_fs


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

    def test_list_chunks_time_range_filters(self, subtests: Any, temp_workspace: Path) -> None:
        # Q2 ends exactly on the time_max_before bound and starts exactly on the
        # time_min_after bound: both bounds are inclusive.
        quarters = (
            ("Q1", "2024-01-01T00:00:00", "2024-03-31T23:59:59"),
            ("Q2", "2024-04-01T00:00:00", "2024-06-01T00:00:00"),
            ("Q3", "2024-07-01T00:00:00", "2024-09-30T23:59:59"),
        )
        rows: tuple[tuple[str, dict[str, Any], set[str]], ...] = (
            (
                "test_list_chunks_time_range_time_min_after",
                {"time_min_after": "2024-04-01T00:00:00"},
                {"Q2", "Q3"},
            ),
            (
                "test_list_chunks_time_range_time_max_before",
                {"time_max_before": "2024-06-01T00:00:00"},
                {"Q1", "Q2"},
            ),
            (
                "test_list_chunks_time_overlap_returns_partial_matches",
                {"time_overlaps": ("2024-02-01T00:00:00", "2024-05-01T00:00:00")},
                {"Q1", "Q2"},
            ),
        )
        for row_id, query, expected in rows:
            with table_row(subtests, row_id, temp_workspace) as (workdir, _):
                manager = _chunk_manager(workdir)
                for n, (group, time_min, time_max) in enumerate(quarters, start=1):
                    _record_completed_span_run(
                        manager,
                        product="test_product",
                        run_id=f"run-00{n}",
                        group=group,
                        batch_id=f"batch-00{n}",
                        time_min=time_min,
                        time_max=time_max,
                    )

                chunks = manager.list_chunks(product="test_product", chunk_type="span", **query)

                assert len(chunks) == 2
                assert {_chunk_group(chunk) for chunk in chunks} == expected

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

    def test_span_coverage_timestamps_written(self, subtests: Any, temp_workspace: Path) -> None:
        rows = (
            ("test_span_coverage_timestamps_written_counts_ranges", [[0, 9], [20, 29]], 20),
            ("test_span_coverage_timestamps_written_single_timestamp", [[0, 0]], 1),
            ("test_span_coverage_timestamps_written_empty_ranges", [], 0),
        )
        for row_id, ranges, expected in rows:
            with table_row(subtests, row_id, temp_workspace):
                coverage = SpanCoverage(group="G", arrays=["G/x"], time_index_ranges=ranges)

                assert coverage.timestamps_written == expected

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

    def test_list_runs_filters(self, subtests: Any, temp_workspace: Path) -> None:
        # (row id, list_runs filter, expected run id, expected status, expected is_terminal)
        rows: tuple[tuple[str, dict[str, Any], str, str, bool], ...] = (
            (
                "test_list_runs_filters_by_status_started",
                {"status": "started"},
                "run-started",
                "started",
                False,
            ),
            (
                "test_list_runs_filters_non_terminal_runs",
                {"non_terminal": True},
                "run-started",
                "started",
                False,
            ),
            (
                "test_list_runs_filters_by_status_complete",
                {"status": "complete"},
                "run-complete",
                "complete",
                True,
            ),
        )
        for row_id, query, run_id, status, terminal in rows:
            with table_row(subtests, row_id, temp_workspace) as (workdir, _):
                manager = _chunk_manager(workdir)
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

                runs = manager.list_runs(product="P", **query)

                assert [run.status for run in runs] == [status]
                assert [run.run_id for run in runs] == [run_id]
                assert [run.is_terminal for run in runs] == [terminal]

    def test_list_runs_empty_product_returns_empty_list(self, temp_workspace):
        manager = _chunk_manager(temp_workspace)

        assert manager.list_runs(product="P") == []

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
            == "s3://test-bucket/data/test_product/.firecube/compact/LATEST.json"
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


def _claim(manager, stack, name, owner):
    domain = WriteDomain(product="test_product", category="zarr_region", name=name)
    return stack.enter_context(
        manager.acquire_claim(product="test_product", domain=domain, owner_id=owner)
    )


def test_abandon_releases_only_claims_the_run_owns(temp_workspace):
    product, manager = "test_product", _chunk_manager(temp_workspace)
    with closing(manager), ExitStack() as stack:
        for run_id in ("run-r", "run-r__slot=0-4", "run-r:x"):
            _record_started_run(manager, product=product, run_id=run_id)
        for name, owner in (
            ("grp:slot=0", "run-r:grp:slot=0"),
            ("grp:schema", "run-r:grp"),
            ("grp:slot=1", "run-r__slot=0-4:grp:slot=1"),
            ("grp:slot=2", "run-r:x:grp:slot=2"),
        ):
            _claim(manager, stack, name, owner)
        result = manager.abandon_run(product=product, run_id="run-r", reason="killed writer")
        left = {claim.owner_id for claim in manager.list_claims(product=product)}
        assert left == {"run-r__slot=0-4:grp:slot=1", "run-r:x:grp:slot=2"}
        _claim(manager, stack, "grp:slot=0", "other")
        prefix = f"{product}:zarr_region:"
        assert result["released_claims"] == [f"{prefix}grp:schema", f"{prefix}grp:slot=0"]
        assert result["skipped_claims"] == [f"{prefix}grp:slot=2"]


def test_abandon_of_terminal_run_releases_leftover_claims(temp_workspace):
    product, manager = "test_product", _chunk_manager(temp_workspace)
    with closing(manager), ExitStack() as stack:
        _record_started_run(manager, product=product, run_id="run-r")
        manager.record_run_terminal(
            product=product, run_id="run-r", output_path="o", output_format="zarr",
            size=0, meta={}, status="abandoned",
        )  # fmt: skip
        _claim(manager, stack, "grp:slot=0", "run-r:grp:slot=0")
        result = manager.abandon_run(product=product, run_id="run-r", reason="finish")
        assert manager.list_claims(product=product) == []
        assert (result["abandoned"], result["status"]) == (False, "abandoned")
        assert result["released_claims"] == [f"{product}:zarr_region:grp:slot=0"]


def test_claim_conflict_names_owner_run_and_clear_command(temp_workspace):
    product, manager = "test_product", _chunk_manager(temp_workspace)
    domain = f"{product}:zarr_region:grp:slot=0"
    with closing(manager), ExitStack() as stack:
        _record_started_run(manager, product=product, run_id="run-r")
        _claim(manager, stack, "grp:slot=0", "run-r:grp:slot=0")
        with pytest.raises(ClaimConflictError) as excinfo:
            _claim(manager, stack, "grp:slot=0", "other")
        message, root = str(excinfo.value), manager.get_product_root(product)
        printed = render(excinfo.value)
        clear = f"firecube chunks claims clear --product-name {root} --domain {domain}"
        assert f"{clear} --force" in printed
        assert message.startswith(f"Write-domain claim conflict for {domain}")
        assert "run-r" in message and "started" in message
        assert "which releases the claims it alone owns" in message
        assert "ChunkManager.clear_claim" in message and "firecube chunks" not in message
        run_hint(printed, "firecube chunks claims clear", "--yes-i-really-mean-it")
        assert manager.list_claims(product=product) == []


def test_maintenance_refusal_resolves_a_shared_owner_once(temp_workspace):
    product, fs = "test_product", CountingFilesystem(make_counting_local_fs(temp_workspace)[1])
    binding = make_test_binding(temp_workspace, product=product)
    manager = ChunkManager(binding=binding, workspace=temp_workspace, filesystem=fs)
    with closing(manager), ExitStack() as stack:
        _record_started_run(manager, product=product, run_id="run-r")
        for k in range(10):
            _claim(manager, stack, f"grp:slot={k}", "run-r:grp")
        claims, ls_calls, unnamed = manager.list_claims(product=product), [], []
        for batch in (claims, claims[:1]):
            fs.reset()
            message = maintenance_refusal_message(
                repo=manager.repo, product=product, operation="scrub", detail="busy", claims=batch
            )
            ls_calls.append(fs.counts["ls"])
            unnamed += [c.domain for c in batch if f"--domain {c.domain}" not in render(message)]
        assert unnamed == []
        assert ls_calls[0] == ls_calls[1], f"owner resolved once per claim: {ls_calls}"


@pytest.mark.contract
def test_abandon_refuses_when_claims_cannot_be_listed(temp_workspace):
    class _Unlistable(CountingFilesystem):
        broken = False

        def ls(self, uri, detail=False, refresh=False):
            if self.broken and uri.path.endswith("/claims"):
                raise PermissionError("claims directory not listable")
            return super().ls(uri, detail=detail, refresh=refresh)

    product, fs = "test_product", _Unlistable(make_counting_local_fs(temp_workspace)[1])
    binding = make_test_binding(temp_workspace, product=product)
    manager = ChunkManager(binding=binding, workspace=temp_workspace, filesystem=fs)
    with closing(manager), ExitStack() as stack:
        _record_started_run(manager, product=product, run_id="run-r")
        _claim(manager, stack, "grp:slot=0", "run-r:grp:slot=0")
        fs.broken = True  # the first write of an unstamped store lists claims, so break after it
        with pytest.raises(ManifestError, match=r"(?s)run-r.*again") as excinfo:
            manager.abandon_run(product=product, run_id="run-r", reason="killed writer")
        assert excinfo.value.context.product_uri == manager.get_product_root(product)
        assert excinfo.value.context.run_id == "run-r"
        (abandon,) = excinfo.value.recovery
        assert (abandon.name, abandon.arguments["run_id"]) == ("abandon_run", "run-r")
        assert [run.status for run in manager.list_runs(product=product)] == ["abandoned"]
        fs.broken = False
        assert [c.owner_id for c in manager.list_claims(product=product)] == ["run-r:grp:slot=0"]


def test_repro_stale_snapshot_warning_names_only_options_snapshots_rebuild_has(
    temp_workspace, monkeypatch, caplog
):
    with closing(_chunk_manager(temp_workspace)) as manager:
        _record_completed_span_run(manager, product="p", run_id="run-1", group="g", batch_id="b")
        manager.rebuild_compact("p")
    now = time.time()
    monkeypatch.setattr(time, "time", lambda: now + 2 * 86400)

    with closing(_chunk_manager(temp_workspace)) as manager, caplog.at_level(logging.WARNING):
        manager.list_chunks(product="p", chunk_type="span")

    command = cli.commands["chunks"].commands["compact"].commands["rebuild"]  # type: ignore[attr-defined]
    known = {opt for param in command.params for opt in param.opts} | {"--help"}
    printed = set(re.findall(r"--[a-z][\w-]*", caplog.text))
    assert "hours old" in caplog.text and printed <= known, (caplog.text, printed - known)
