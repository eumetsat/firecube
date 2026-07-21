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

from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import SpanCoverage
from tests.helpers.storage import make_test_binding


def _repo(temp_workspace: Path) -> ManifestRepository:
    return ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)


def _record_completed_run(
    repo: ManifestRepository,
    *,
    product: str,
    run_id: str,
    span: bool = False,
) -> None:
    repo.record_run_started(
        product=product,
        run_id=run_id,
        output_path=f"/tmp/{product}",
        output_format="zarr",
        size=0,
        meta={"plugin": "parity"},
    )
    if span:
        repo.record_span_event(
            product=product,
            run_id=run_id,
            batch_id="batch-001",
            group="F024",
            status="active",
            coverage=SpanCoverage(
                group="F024",
                arrays=["F024/FWI"],
                time_index_ranges=[[0, 2]],
                time_min="2024-01-01T00:00:00",
                time_max="2024-01-03T00:00:00",
            ),
            meta={"plugin": "parity", "group": "F024"},
        )
    repo.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=f"/tmp/{product}",
        output_format="zarr",
        size=1,
        meta={"plugin": "parity"},
        status="complete",
    )


def test_record_run_started_to_terminal_is_listed(temp_workspace: Path) -> None:
    repo = _repo(temp_workspace)

    _record_completed_run(repo, product="product", run_id="run-001")

    runs = repo.list_runs(product="product")
    assert [run.run_id for run in runs] == ["run-001"]
    assert runs[0].status == "complete"
    assert runs[0].is_terminal


def test_recorded_spans_are_projected_by_list_chunks(temp_workspace: Path) -> None:
    repo = _repo(temp_workspace)

    _record_completed_run(repo, product="product", run_id="run-001", span=True)

    chunks = repo.list_chunks(product="product")
    assert [chunk.key for chunk in chunks] == ["span_run-001_batch-001_F024"]
    assert chunks[0].chunk_type == "span"
    assert chunks[0].meta == {
        "plugin": "parity",
        "group": "F024",
        "batch_id": "batch-001",
        "run_id": "run-001",
    }
    assert chunks[0].timestamps_written == 3


def test_snapshot_rebuild_writes_latest_pointer_readable_by_repo(temp_workspace: Path) -> None:
    repo = _repo(temp_workspace)
    _record_completed_run(repo, product="product", run_id="run-001", span=True)

    rebuild = repo.rebuild_snapshot("product")
    latest = repo._read_latest_pointer("product")

    assert rebuild["records"] == 2
    assert latest is not None
    assert latest["generation"] == rebuild["generation"]
    assert latest["product"] == "product"
    assert repo._read_snapshot_records(latest)
