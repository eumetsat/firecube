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

from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import SpanCoverage
from firecube.core.filesystem import collect_filesystem_metrics
from firecube.core.filesystem.ops import _open_fsspec_url
from tests.helpers.storage import make_test_binding


def test_collect_filesystem_metrics_counts_basic_io(tmp_path):
    target_dir = tmp_path / "fs-metrics"
    target_dir.mkdir()

    with collect_filesystem_metrics() as metrics:
        fs, root = _open_fsspec_url(str(target_dir))
        file_path = f"{root}/sample.bin"

        with fs.open(file_path, "wb") as handle:
            handle.write(b"abc123")
        assert fs.exists(file_path)
        with fs.open(file_path, "rb") as handle:
            data = handle.read()

    assert data == b"abc123"
    summary = metrics.as_summary()
    assert summary["storage_client_requests"] > 0
    assert summary["storage_client_errors"] == 0
    assert summary["storage_client_latency_s_total"] >= 0.0
    assert summary["storage_client_bytes_written"] >= 6
    assert summary["storage_client_bytes_read"] >= 6


def test_collect_filesystem_metrics_supports_text_iteration(tmp_path):
    target_dir = tmp_path / "fs-metrics-iter"
    target_dir.mkdir()
    lines = ["a\n", "b\n", "c\n"]

    with collect_filesystem_metrics() as metrics:
        fs, root = _open_fsspec_url(str(target_dir))
        file_path = f"{root}/lines.txt"
        with fs.open(file_path, "w") as handle:
            handle.writelines(lines)
        with fs.open(file_path, "r") as handle:
            observed = list(handle)

    assert observed == lines
    summary = metrics.as_summary()
    assert summary["storage_client_errors"] == 0
    assert summary["storage_client_bytes_read"] >= sum(len(line) for line in lines)


def test_manifest_repository_parse_manifest_with_instrumented_file(tmp_path):
    with collect_filesystem_metrics() as metrics:
        repo = ManifestRepository(binding=make_test_binding(tmp_path), workspace=tmp_path)
        repo.record_run_started(
            product="product-a",
            run_id="run-001",
            output_path=str(tmp_path / "product-a"),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        repo.record_span_event(
            product="product-a",
            run_id="run-001",
            batch_id="batch-001",
            group="F024",
            status="active",
            coverage=SpanCoverage(group="F024", arrays=["F024/FWI"], time_index_ranges=[[0, 1]]),
            meta={"plugin": "test"},
        )
        repo.record_run_terminal(
            product="product-a",
            run_id="run-001",
            output_path=str(tmp_path / "product-a"),
            output_format="zarr",
            size=42,
            meta={"plugin": "test"},
            status="complete",
        )
        control_root = tmp_path / "product-a" / ".firecube"
        chunks = list(repo.parse_manifest(control_root.as_uri()))

    assert len(chunks) == 1
    assert chunks[0].meta is not None
    assert chunks[0].meta["group"] == "F024"
    summary = metrics.as_summary()
    assert summary["storage_client_errors"] == 0
