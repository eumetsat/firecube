# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


class _Host:
    name = "m7_manifest_probe"


def _ctx(
    tmp_path: Path,
    *,
    product: str = "product.zarr",
    protocol: str = "file",
    authority: str | None = None,
    write_mode: str = "direct",
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
        options={"write_mode": write_mode, "upload_workers": 3},
        run_id="m7-run",
        identity=RuntimeIdentity(run_id="m7-run"),
    )


_CONTROL_PLANE = {
    "control_root": "s3://bucket/.firecube/product.zarr",
    "latest_pointer": "s3://bucket/.firecube/product.zarr/latest.json",
}


def _result(
    output_path: str,
    *,
    write_mode: str = "direct",
    seed_control_plane: bool = False,
) -> IngestResult:
    result = IngestResult(
        output_format="zarr",
        outputs=OutputPaths(primary=output_path, zarr=output_path),
        metrics=ResultMetrics(
            write_mode=write_mode,
            pipeline=PipelineMetrics(duration_pipeline_s=1.0),
        ),
    )
    if seed_control_plane:
        # Mirror the engine's finalize seeding: a storage block that carries
        # only control-plane keys and no upload counts.
        result.metrics["storage"] = dict(_CONTROL_PLANE)
    return result


def test_direct_local_manifest_has_explicit_null(tmp_path: Path) -> None:
    """direct+local run manifests keep upload-counter keys with null values."""
    target = tmp_path / "product.zarr"
    target.mkdir()

    updated = PipelineExecutor().complete_output(
        _result(str(target)),
        _ctx(tmp_path),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.manifest is not None
    assert "files" in updated.manifest
    assert "bytes" in updated.manifest
    assert "duration_s" in updated.manifest
    assert updated.manifest["files"] is None
    assert updated.manifest["bytes"] is None
    assert updated.manifest["duration_s"] is None


def test_direct_local_metrics_storage_also_null(tmp_path: Path) -> None:
    """metrics.storage counters also use null for direct+local runs."""
    target = tmp_path / "product.zarr"
    target.mkdir()

    updated = PipelineExecutor().complete_output(
        _result(str(target)),
        _ctx(tmp_path),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.manifest is not None
    storage = updated.manifest["metrics"]["storage"]
    assert storage["files"] is None
    assert storage["bytes"] is None
    assert storage["duration_s"] is None


def test_staged_manifest_still_has_real_counters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verified-correct: staged+S3 completion keeps measured upload counters."""
    staged = tmp_path / "staged" / "product.zarr"
    staged.mkdir(parents=True)
    (staged / "zarr.json").write_text("{}", encoding="utf-8")

    def upload_tree(
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
            bytes_written=17,
            files_written=2,
            duration_s=1.25,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)

    updated = PipelineExecutor().complete_output(
        _result(str(staged), write_mode="staged", seed_control_plane=True),
        _ctx(tmp_path, protocol="s3", authority="bucket", write_mode="staged"),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.manifest is not None
    assert updated.manifest["files"] == 2
    assert updated.manifest["bytes"] == 17
    assert updated.manifest["duration_s"] == 1.25

    # the nested block mirrors the top-level counters exactly and keeps
    # the engine-seeded control-plane keys.
    storage = updated.manifest["metrics"]["storage"]
    assert storage["files"] == updated.manifest["files"]
    assert storage["bytes"] == updated.manifest["bytes"]
    assert storage["duration_s"] == updated.manifest["duration_s"]
    assert storage["path"] == updated.manifest["stored_at"]
    assert storage["control_root"] == _CONTROL_PLANE["control_root"]
    assert storage["latest_pointer"] == _CONTROL_PLANE["latest_pointer"]


def test_direct_s3_counters_null_but_storage_result_uses_path_stats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """direct+S3 runs null the upload counters like direct+local.

    The manifest counters are about a staged upload, which never happens in
    direct mode regardless of locality. The ``storage_result`` still reports
    what is on the target via ``path_stats``, even when the engine-seeded
    ``metrics.storage`` block (control-plane keys only) is present.
    """
    calls: list[str] = []

    def fake_path_stats(uri: str, *, storage_config: Any = None, **kwargs: Any) -> dict[str, int]:
        _ = (storage_config, kwargs)
        calls.append(uri)
        return {"files": 5, "bytes": 99}

    monkeypatch.setattr("firecube.core.storage.completion.path_stats", fake_path_stats)

    def upload_tree(self: StorageSession, *args: Any, **kwargs: Any) -> StorageWriteResult:
        _ = (self, args, kwargs)
        raise AssertionError("direct mode must not stage an upload")

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)

    ctx = _ctx(tmp_path, protocol="s3", authority="bucket", write_mode="direct")
    target = str(ctx.target)
    updated = PipelineExecutor().complete_output(
        _result(target, write_mode="direct", seed_control_plane=True),
        ctx,
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert calls == [target]
    assert updated.storage_result is not None
    assert updated.storage_result.files_written == 5
    assert updated.storage_result.bytes_written == 99
    assert updated.storage_result.storage_type == "s3"

    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == target
    assert updated.manifest["files"] is None
    assert updated.manifest["bytes"] is None
    assert updated.manifest["duration_s"] is None

    storage = updated.manifest["metrics"]["storage"]
    assert storage["files"] is None
    assert storage["bytes"] is None
    assert storage["duration_s"] is None
    assert storage["path"] == target
    assert storage["control_root"] == _CONTROL_PLANE["control_root"]
    assert storage["latest_pointer"] == _CONTROL_PLANE["latest_pointer"]
    assert updated.metrics.storage is not None
    assert updated.metrics.storage.files is None


def test_schema_version_still_v1(tmp_path: Path) -> None:
    """explicit-null counter encoding does not bump manifest schema."""
    target = tmp_path / "product.zarr"
    target.mkdir()

    updated = PipelineExecutor().complete_output(
        _result(str(target)),
        _ctx(tmp_path),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.manifest is not None
    assert updated.manifest["schema_version"] == "v1"
