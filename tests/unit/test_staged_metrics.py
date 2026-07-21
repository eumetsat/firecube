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
from typing import cast

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import local_path_from_target
from firecube.ingestor.api import IngestResult, OutputPaths
from firecube.ingestor.contracts.interfaces import PipelineHost
from firecube.ingestor.runtime.engine import PipelineExecutor
from firecube.ingestor.types.context import RuntimeIdentity, RuntimeIngestContext, StorageContext


class _FakeHost:
    name = "dummy"


FAKE_UPLOAD_DURATION = 12.5


def _make_result(staged_output: Path) -> IngestResult:
    return IngestResult(
        outputs=OutputPaths(primary=str(staged_output)),
        output_format="zarr",
        metrics={
            "storage_handled": False,
            "pipeline": {
                "workers": 2,
                "batch_size": 4,
                "batches_total": 1,
                "batches_failed": 0,
                "hook_failures": 0,
                "files_processed": 3,
                "bytes_ingested": 99,
                "rows_processed": 7,
                "duration_total_s": 30.0,
                "duration_pipeline_s": 30.0,
                "duration_processing_s": 20.0,
                "duration_batch_creation_s": 2.0,
                "duration_upload_s": 0.0,
                "duration_cpu_s": 10.0,
                "non_cpu_wait_s": 5.0,
                "cpu_utilization_estimate": 0.5,
                "storage_client_requests": 0,
                "storage_client_errors": 0,
                "storage_client_retryable_errors": 0,
                "storage_client_latency_s_total": 0.0,
                "storage_client_bytes_read": 0,
                "storage_client_bytes_written": 0,
            },
        },
    )


def _write_staged_files(staged_output: Path) -> None:
    for index in range(4):
        (staged_output / f"part-{index}.bin").write_bytes(b"x" * 31)


def _make_ctx(source: Path) -> RuntimeIngestContext:
    target = "s3://bucket/product.zarr"
    product_uri = StorageUri.parse(target)
    session = StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="test_product"),
            driver=StorageDriverConfig(driver="fsspec"),
        )
    )
    return RuntimeIngestContext(
        source=str(source),
        target=target,
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"write_mode": "staged", "upload_workers": 4},
        run_id="test-run",
        identity=RuntimeIdentity(run_id="test-run"),
    )


def _fake_host() -> PipelineHost:
    return cast(PipelineHost, _FakeHost())


def _patch_storage_session_upload(monkeypatch) -> list[tuple[str, str, int]]:
    uploads: list[tuple[str, str, int]] = []

    def _upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs,
    ) -> StorageWriteResult:
        _ = (self, kwargs)
        uploads.append((src.to_str(), dst.to_str(), parallel_workers))
        source_path = local_path_from_target(src.to_str())
        files = [path for path in source_path.rglob("*") if path.is_file()]
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=sum(path.stat().st_size for path in files),
            files_written=len(files),
            duration_s=FAKE_UPLOAD_DURATION,
            storage_type="s3" if dst.is_remote() else "local",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _upload_tree)
    return uploads


def test_staged_s3_manifest_marks_storage_handled_true(tmp_path: Path, monkeypatch) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    _write_staged_files(staged_output)
    source = tmp_path / "source"
    source.mkdir()

    result = _make_result(staged_output)
    ctx = _make_ctx(source)

    uploads = _patch_storage_session_upload(monkeypatch)
    executor = PipelineExecutor()
    updated = executor.complete_output(result, ctx, host=_fake_host())

    assert updated.metrics["storage_handled"] is True
    assert updated.manifest is not None
    assert uploads == [
        (StorageUri.from_local_path(staged_output).to_str(), "s3://bucket/product.zarr", 4)
    ]


def test_staged_s3_pipeline_summary_uses_upload_duration(tmp_path: Path, monkeypatch) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    _write_staged_files(staged_output)
    source = tmp_path / "source"
    source.mkdir()

    result = _make_result(staged_output)
    ctx = _make_ctx(source)

    _patch_storage_session_upload(monkeypatch)
    executor = PipelineExecutor()
    updated = executor.complete_output(result, ctx, host=_fake_host())

    pipeline = updated.metrics["pipeline"]
    assert pipeline["duration_upload_s"] == FAKE_UPLOAD_DURATION
    assert pipeline["duration_pipeline_s"] == 30.0
    assert (
        pipeline["duration_total_s"]
        == pipeline["duration_pipeline_s"] + pipeline["duration_upload_s"]
    )


def test_staged_s3_storage_result_and_pipeline_metrics_include_upload(
    tmp_path: Path, monkeypatch
) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    _write_staged_files(staged_output)
    source = tmp_path / "source"
    source.mkdir()

    result = _make_result(staged_output)
    ctx = _make_ctx(source)

    _patch_storage_session_upload(monkeypatch)
    executor = PipelineExecutor()
    updated = executor.complete_output(result, ctx, host=_fake_host())

    pipeline = updated.metrics["pipeline"]
    assert pipeline["duration_upload_s"] == FAKE_UPLOAD_DURATION
    assert updated.storage_result is not None
    assert updated.storage_result.duration_s == FAKE_UPLOAD_DURATION
    assert updated.storage_result.bytes_written == 124
    assert updated.storage_result.files_written == 4


def test_staged_s3_manifest_points_at_final_remote_target(tmp_path: Path, monkeypatch) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    _write_staged_files(staged_output)
    source = tmp_path / "source"
    source.mkdir()

    result = _make_result(staged_output)
    ctx = _make_ctx(source)

    _patch_storage_session_upload(monkeypatch)
    executor = PipelineExecutor()
    updated = executor.complete_output(result, ctx, host=_fake_host())

    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == "s3://bucket/product.zarr"
