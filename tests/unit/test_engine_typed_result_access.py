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

"""T8: PipelineExecutor consumes typed ResultMetrics/OutputPaths attributes.

Asserts that ``PipelineExecutor.complete_output`` operates on
``result.metrics.write_mode``/``result.metrics.storage``/``result.outputs.primary``
attributes (not ``.get(...)``/``[...]`` dict access on the typed payloads) and
that the manifest serializes without leaking the private ``_compat`` mirror.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, cast

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import (
    IngestResult,
    OutputPaths,
    PipelineMetrics,
    ResultMetrics,
    StorageMetrics,
)
from firecube.ingestor.runtime.engine import PipelineExecutor
from firecube.ingestor.types.context import RuntimeIdentity, RuntimeIngestContext, StorageContext


class _FakeHost:
    name = "dummy"


def _session(target: str) -> StorageSession:
    uri = StorageUri.parse(target)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(uri, "zarr", product_name="test_product"),
            driver=StorageDriverConfig(driver="fsspec"),
        )
    )


def _make_ctx(*, target: str, write_mode: str = "staged") -> RuntimeIngestContext:
    return RuntimeIngestContext(
        source="/dev/null",
        target=target,
        output_format="zarr",
        storage=StorageContext(output=_session(target)),
        options={"write_mode": write_mode, "upload_workers": 4},
        run_id="test-run",
        identity=RuntimeIdentity(run_id="test-run"),
    )


def _storage_completer() -> Any:
    module = import_module("firecube.core.storage.completion")
    return cast(Any, module).StorageCompleter()


def _stub_upload_tree(monkeypatch, *, bytes_written: int = 0, files_written: int = 0) -> None:
    def _noop(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = (self, src, parallel_workers, kwargs)
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=bytes_written,
            files_written=files_written,
            duration_s=0.25,
            storage_type="local" if dst.protocol == "file" else "s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _noop)


def test_empty_metrics_does_not_crash_complete_output(tmp_path: Path, monkeypatch) -> None:
    staged = tmp_path / "staged.zarr"
    staged.mkdir()
    target = tmp_path / "product.zarr"

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(),
    )

    _stub_upload_tree(monkeypatch)

    updated = PipelineExecutor().complete_output(
        result,
        _make_ctx(target=StorageUri.from_local_path(target).to_str()),
        host=cast(Any, _FakeHost()),
    )

    assert updated.manifest is not None
    assert "_compat" not in updated.manifest.get("metrics", {})
    assert updated.write_mode_applied == "staged"


def test_storage_metrics_drive_s3_direct_summary(tmp_path: Path) -> None:
    target = "s3://bucket/data/product.zarr"
    storage = StorageMetrics(path=target, bytes=1234, files=5, duration_s=2.5)
    result = IngestResult(
        outputs=OutputPaths(primary="s3://bucket/staged"),
        output_format="zarr",
        metrics=ResultMetrics(write_mode="direct", storage=storage),
    )

    stored = _storage_completer().complete_s3_direct(
        result,
        storage_config=cast(Any, None),
        final_target_uri=target,
    )

    assert stored.path == target
    assert stored.bytes_written == 1234
    assert stored.files_written == 5
    assert stored.duration_s == 2.5


def test_zarr_outputs_attribute_drives_staged_source(tmp_path: Path, monkeypatch) -> None:
    staged = tmp_path / "primary.zarr"
    staged.mkdir()
    zarr_out = tmp_path / "zarr-output.zarr"
    zarr_out.mkdir()

    captured: dict[str, str] = {}

    def _capture(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = (self, parallel_workers, kwargs)
        captured["source_uri"] = src.to_str()
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _capture)

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged), zarr=str(zarr_out)),
        output_format="zarr",
        metrics=ResultMetrics(write_mode="staged"),
    )

    _storage_completer().complete_s3_staged(
        result,
        _make_ctx(target="s3://bucket/product.zarr"),
        final_target_uri="s3://bucket/product.zarr",
    )

    assert captured["source_uri"] == StorageUri.from_local_path(zarr_out).to_str()


def test_storage_handled_set_via_attribute_after_staged_upload(tmp_path: Path, monkeypatch) -> None:
    staged = tmp_path / "staged.zarr"
    staged.mkdir()

    _stub_upload_tree(monkeypatch)

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(
            write_mode="staged",
            pipeline=PipelineMetrics(duration_pipeline_s=1.5),
        ),
    )

    _storage_completer().complete_s3_staged(
        result,
        _make_ctx(target="s3://bucket/product.zarr"),
        final_target_uri="s3://bucket/product.zarr",
    )

    assert result.metrics.storage_handled is True
    assert result.metrics.pipeline is not None
    assert result.metrics.pipeline.duration_pipeline_s == 1.5


def test_manifest_serialization_excludes_private_compat(tmp_path: Path, monkeypatch) -> None:
    staged = tmp_path / "staged.zarr"
    staged.mkdir()
    target = tmp_path / "product.zarr"

    _stub_upload_tree(monkeypatch)

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(
            write_mode="staged",
            pipeline=PipelineMetrics(duration_pipeline_s=0.5),
        ),
    )

    updated = PipelineExecutor().complete_output(
        result,
        _make_ctx(target=StorageUri.from_local_path(target).to_str()),
        host=cast(Any, _FakeHost()),
    )

    assert updated.manifest is not None
    rendered_metrics = updated.manifest.get("metrics")
    assert isinstance(rendered_metrics, dict)
    assert "_compat" not in rendered_metrics
    assert rendered_metrics.get("write_mode") == "staged"


def test_write_mode_attribute_drives_effective_mode(tmp_path: Path, monkeypatch) -> None:
    staged = tmp_path / "staged.zarr"
    staged.mkdir()
    target = tmp_path / "product.zarr"

    _stub_upload_tree(monkeypatch)

    ctx = _make_ctx(target=StorageUri.from_local_path(target).to_str(), write_mode="staged")
    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(write_mode="direct"),
    )

    updated = PipelineExecutor().complete_output(result, ctx, host=cast(Any, _FakeHost()))

    assert updated.write_mode_applied == "direct"
