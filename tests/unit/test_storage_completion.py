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
from typing import Any

import pytest

from firecube.core.storage import StorageWriteResult
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import IngestResult, OutputPaths, PipelineMetrics, ResultMetrics
from firecube.ingestor.runtime.engine import PipelineExecutor
from firecube.ingestor.types.context import RuntimeIdentity, RuntimeIngestContext, StorageContext
from firecube.ingestor.types.result_metrics import StorageMetrics
from tests.helpers.storage import make_test_binding


class _Host:
    name = "completion_probe"


def _ctx(
    tmp_path: Path,
    *,
    product: str = "product.zarr",
    protocol: str = "file",
    authority: str | None = None,
    write_mode: str = "staged",
    upload_workers: int = 4,
) -> RuntimeIngestContext:
    session = StorageSession(
        make_test_binding(
            tmp_path,
            product=product,
            protocol=protocol,
            authority=authority,
        )
    )
    return RuntimeIngestContext(
        source="source",
        target=session.product.product_uri.to_str(),
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"write_mode": write_mode, "upload_workers": upload_workers},
        run_id="completion-run",
        identity=RuntimeIdentity(run_id="completion-run"),
    )


def _result(output_path: str, *, write_mode: str | None = None) -> IngestResult:
    return IngestResult(
        output_format="zarr",
        outputs=OutputPaths(primary=output_path, zarr=output_path),
        metrics=ResultMetrics(
            write_mode=write_mode,
            pipeline=PipelineMetrics(duration_pipeline_s=3.0),
        ),
    )


def test_complete_output_local_staged_in_place_does_not_upload(tmp_path: Path) -> None:
    final_target = tmp_path / "product.zarr"
    final_target.mkdir()
    (final_target / "zarr.json").write_text("{}", encoding="utf-8")

    updated = PipelineExecutor().complete_output(
        _result(str(final_target)),
        _ctx(tmp_path),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.storage_result == StorageWriteResult(
        path=str(final_target.resolve()),
        bytes_written=0,
        files_written=0,
        duration_s=0.0,
        storage_type="local",
    )
    assert updated.write_mode_applied == "staged"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == str(final_target.resolve())
    assert updated.manifest["files"] == 0
    assert updated.manifest["bytes"] == 0


def test_complete_output_local_staged_uploads_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "staged" / "product.zarr"
    source.mkdir(parents=True)
    (source / "zarr.json").write_text("{}", encoding="utf-8")
    observed: dict[str, Any] = {}

    def upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        observed["session_target"] = self.product.product_uri.to_str()
        observed["src"] = src.to_str()
        observed["dst"] = dst.to_str()
        observed["parallel_workers"] = parallel_workers
        observed["kwargs"] = kwargs
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=11,
            files_written=2,
            duration_s=1.5,
            storage_type="local",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)

    updated = PipelineExecutor().complete_output(
        _result(str(source)),
        _ctx(tmp_path, upload_workers=7),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    final_target = tmp_path / "product.zarr"
    assert observed == {
        "session_target": StorageUri.from_local_path(final_target).to_str(),
        "src": StorageUri.from_local_path(source.resolve()).to_str(),
        "dst": StorageUri.from_local_path(final_target).to_str(),
        "parallel_workers": 7,
        "kwargs": {},
    }
    assert updated.storage_result is not None
    assert updated.storage_result.path == StorageUri.from_local_path(final_target).to_str()
    assert updated.storage_result.bytes_written == 11
    assert updated.storage_result.files_written == 2


def test_complete_output_s3_direct_uses_storage_summary(tmp_path: Path) -> None:
    result = _result("s3://bucket/product.zarr", write_mode="direct")
    result.metrics.storage = StorageMetrics(
        path="s3://bucket/product.zarr/summary",
        bytes=23,
        files=4,
        duration_s=2.5,
    )

    updated = PipelineExecutor().complete_output(
        result,
        _ctx(tmp_path, protocol="s3", authority="bucket", write_mode="direct"),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.storage_result == StorageWriteResult(
        path="s3://bucket/product.zarr/summary",
        bytes_written=23,
        files_written=4,
        duration_s=2.5,
        storage_type="s3",
    )
    assert updated.write_mode_applied == "direct"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == "s3://bucket/product.zarr/summary"


def test_complete_output_s3_staged_uploads_zarr_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = tmp_path / "primary"
    primary.mkdir()
    zarr_output = tmp_path / "actual.zarr"
    zarr_output.mkdir()
    (zarr_output / "zarr.json").write_text("{}", encoding="utf-8")
    observed: dict[str, Any] = {}

    def upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        observed["session_target"] = self.product.product_uri.to_str()
        observed["src"] = src.to_str()
        observed["dst"] = dst.to_str()
        observed["parallel_workers"] = parallel_workers
        observed["kwargs"] = kwargs
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=31,
            files_written=3,
            duration_s=4.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)
    result = IngestResult(
        output_format="zarr",
        outputs=OutputPaths(primary=str(primary), zarr=str(zarr_output)),
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=6.0)),
    )

    updated = PipelineExecutor().complete_output(
        result,
        _ctx(tmp_path, protocol="s3", authority="bucket", upload_workers=6),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert observed == {
        "session_target": "s3://bucket/product.zarr",
        "src": StorageUri.from_local_path(zarr_output).to_str(),
        "dst": "s3://bucket/product.zarr",
        "parallel_workers": 6,
        "kwargs": {},
    }
    assert updated.storage_result is not None
    assert updated.storage_result.bytes_written == 31
    assert updated.storage_result.files_written == 3
    assert updated.metrics.storage_handled is True
    assert updated.metrics.pipeline is not None
    assert updated.metrics.pipeline.duration_upload_s == 4.0
    assert updated.metrics.pipeline.duration_total_s == 10.0
