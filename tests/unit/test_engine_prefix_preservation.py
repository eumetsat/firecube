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
from typing import Any, cast

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import IngestResult, OutputPaths, ResultMetrics
from firecube.ingestor.runtime.engine import PipelineExecutor
from firecube.ingestor.types.context import RuntimeIdentity, RuntimeIngestContext, StorageContext


class _FakeHost:
    name = "dummy"


def _make_result(staged_output: Path) -> IngestResult:
    return IngestResult(
        outputs=OutputPaths(primary=str(staged_output)),
        output_format="zarr",
        metrics=ResultMetrics(),
    )


def _session(target: str) -> StorageSession:
    uri = StorageUri.parse(target)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(uri, "zarr", product_name="test_product"),
            driver=StorageDriverConfig(driver="fsspec"),
        )
    )


def _make_ctx(source: Path, *, target: str) -> RuntimeIngestContext:
    return RuntimeIngestContext(
        source=str(source),
        target=target,
        output_format="zarr",
        storage=StorageContext(output=_session(target)),
        options={"write_mode": "staged", "upload_workers": 4},
        run_id="test-run",
        identity=RuntimeIdentity(run_id="test-run"),
    )


def test_staged_upload_preserves_s3_prefix(tmp_path: Path, monkeypatch) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    captured: dict[str, str] = {}

    def _capture_upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = kwargs
        captured["source_uri"] = src.to_str()
        captured["final_target_uri"] = dst.to_str()
        captured["session_product_uri"] = self.product.product_uri.to_str()
        captured["parallel_workers"] = str(parallel_workers)
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _capture_upload_tree)

    executor = PipelineExecutor()
    updated = executor.complete_output(
        _make_result(staged_output),
        _make_ctx(source, target="s3://bucket/data/2026/product.zarr"),
        host=cast(Any, _FakeHost()),
    )

    assert captured["source_uri"] == StorageUri.from_local_path(staged_output).to_str()
    assert captured["final_target_uri"] == "s3://bucket/data/2026/product.zarr"
    assert captured["session_product_uri"] == "s3://bucket/data/2026/product.zarr"
    assert captured["parallel_workers"] == "4"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == "s3://bucket/data/2026/product.zarr"
    assert updated.storage_result is not None
    assert updated.storage_result.path == "s3://bucket/data/2026/product.zarr"


def test_staged_upload_uses_bound_product_uri(tmp_path: Path, monkeypatch) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    captured: dict[str, str] = {}

    def _capture_upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = (self, src, parallel_workers, kwargs)
        captured["final_target_uri"] = dst.to_str()
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _capture_upload_tree)

    executor = PipelineExecutor()
    executor.complete_output(
        _make_result(staged_output),
        _make_ctx(source, target="s3://bucket/product.zarr"),
        host=cast(Any, _FakeHost()),
    )

    assert captured["final_target_uri"] == "s3://bucket/product.zarr"


def test_upload_workers_propagates_from_ctx_to_upload_tree(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    captured: dict[str, int] = {}

    def _capture_upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = (self, src, kwargs)
        captured["parallel_workers"] = parallel_workers
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _capture_upload_tree)

    ctx = _make_ctx(source, target="s3://bucket/data/2026/product.zarr")
    ctx.options["upload_workers"] = 8

    PipelineExecutor().complete_output(
        _make_result(staged_output),
        ctx,
        host=cast(Any, _FakeHost()),
    )

    assert captured["parallel_workers"] == 8


def test_sessionless_completion_preserves_prefix_from_ctx_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    staged_output = tmp_path / "staged-output.zarr"
    staged_output.mkdir()
    target = "s3://bucket/data/2026/product.zarr"
    captured: dict[str, str] = {}

    def _capture_upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = (self, src, parallel_workers, kwargs)
        captured["final_target_uri"] = dst.to_str()
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _capture_upload_tree)

    ctx = RuntimeIngestContext(
        source="source",
        target=target,
        output_format="zarr",
        storage=StorageContext(output=_session(target)),
        options={"write_mode": "staged", "upload_workers": 4},
        run_id="test-run",
        identity=RuntimeIdentity(run_id="test-run"),
    )

    PipelineExecutor().complete_output(
        _make_result(staged_output),
        ctx,
        host=cast(Any, _FakeHost()),
    )

    assert captured["final_target_uri"] == target
