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

"""Credential redaction boundary tests.

These tests assert that sentinel credentials placed in ``StorageBinding`` never
appear in serialized artifacts emitted by:
  1. ``PipelineExecutor.complete_output`` (the ``IngestManifest`` dict)
  2. WAL events under ``.firecube/runs/<run_id>/events-*.jsonl``
  3. Snapshot files under ``.firecube/snapshots/`` (after ``rebuild_snapshot``)
  4. Telemetry ``emit()`` calls captured during a ``complete_output`` cycle

After flat-uri T17/T19, credentials live on ``StorageBinding.driver.credentials``
and runtime context exposes storage exclusively via ``IngestContext.storage.output``
(a ``StorageSession``). The control-plane ``"key"`` field is structural and must
not be redacted globally (see ``src/firecube/core/controlplane/repo.py``).
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any, Literal

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.credentials import Credentials
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import IngestResult, OutputPaths, PipelineMetrics, ResultMetrics
from firecube.ingestor.runtime.engine import PipelineExecutor
from firecube.ingestor.types.context import (
    RuntimeIdentity,
    RuntimeIngestContext,
    StorageContext,
)

SENTINEL_ACCESS_KEY = "SENTINEL_ACCESS_KEY_DO_NOT_USE"
SENTINEL_SECRET_KEY = "SENTINEL_SECRET_KEY_DO_NOT_USE"


class _FakeHost:
    """Minimal ``PipelineHost`` stand-in used to drive ``complete_output``."""

    name = "redaction_probe"


def _sentinel_binding(product_uri: StorageUri) -> StorageBinding:
    return StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="test_product"),
        driver=StorageDriverConfig(
            credentials=Credentials(
                access_key=SENTINEL_ACCESS_KEY,
                secret_key=SENTINEL_SECRET_KEY,
            ),
        ),
    )


def _build_runtime_ctx(
    *,
    target: str,
    binding: StorageBinding,
    write_mode: str = "staged",
    telemetry: Any | None = None,
) -> RuntimeIngestContext:
    storage = StorageContext(output=StorageSession(binding=binding))
    return RuntimeIngestContext(
        source="source",
        target=target,
        output_format="zarr",
        options={"write_mode": write_mode, "no_progress": True},
        storage=storage,
        run_id="redaction-probe-run",
        identity=RuntimeIdentity(run_id="redaction-probe-run"),
        telemetry=telemetry,
    )


def _stub_upload_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    def _noop(
        self: StorageSession, src: StorageUri, dst: StorageUri, **kwargs: Any
    ) -> StorageWriteResult:
        _ = (self, src, kwargs)
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="local",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _noop)


def test_credentials_do_not_leak_into_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T4b/1: ``IngestManifest`` from ``complete_output`` must not include credentials."""
    target_root = tmp_path / "out"
    target_root.mkdir()
    target = target_root / "product.zarr"

    staged = tmp_path / "staged" / "product.zarr"
    staged.mkdir(parents=True)
    (staged / "zarr.json").write_text("{}", encoding="utf-8")
    (staged / "data.bin").write_bytes(b"payload")

    binding = _sentinel_binding(StorageUri.from_local_path(target))

    ctx = _build_runtime_ctx(target=str(target), binding=binding, write_mode="staged")

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=1.0)),
    )

    _stub_upload_tree(monkeypatch)
    updated = PipelineExecutor().complete_output(result, ctx, host=_FakeHost())  # type: ignore[arg-type]

    assert updated.manifest is not None, "complete_output should populate manifest"
    rendered = json.dumps(updated.manifest, default=str)
    assert SENTINEL_ACCESS_KEY not in rendered, (
        f"manifest serialization leaked access_key: {rendered}"
    )
    assert SENTINEL_SECRET_KEY not in rendered, (
        f"manifest serialization leaked secret_key: {rendered}"
    )


def test_credentials_do_not_leak_into_wal_events(tmp_path: Path) -> None:
    """T4b/2: WAL ``events-*.jsonl`` files must not persist credentials.

    Drives ``ChunkManager.record_run_started`` + ``record_run_terminal`` while
    the manager is bound to a ``StorageBinding`` carrying sentinel credentials.
    The control plane should write authoritative event records to the WAL
    without pulling any field from the storage binding's credentials.
    """
    product = "redaction_product.zarr"
    run_id = "wal-redaction-run"
    product_uri = StorageUri.from_local_path(tmp_path / product)
    binding = _sentinel_binding(product_uri)

    manager = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="complete",
        )
    finally:
        manager.close()

    runs_root = tmp_path / product / ".firecube" / "runs"
    assert runs_root.exists(), f"expected control-plane runs dir at {runs_root}"
    matched = list(runs_root.rglob("events-*.jsonl"))
    assert matched, f"expected at least one events-*.jsonl file under {runs_root}"

    for events_file in matched:
        content = events_file.read_text(encoding="utf-8")
        assert SENTINEL_ACCESS_KEY not in content, (
            f"WAL event file leaked access_key: {events_file}\n{content}"
        )
        assert SENTINEL_SECRET_KEY not in content, (
            f"WAL event file leaked secret_key: {events_file}\n{content}"
        )


def test_credentials_do_not_leak_into_snapshot(tmp_path: Path) -> None:
    """T4b/3: Snapshot files projected from WAL must not persist credentials.

    Builds a complete run lifecycle then explicitly compacts the WAL via
    ``rebuild_snapshot``. Scans every byte under ``.firecube/snapshots/``
    plus the ``LATEST.json`` pointer for sentinel strings.
    """
    product = "snapshot_redaction_product.zarr"
    run_id = "snapshot-redaction-run"
    product_uri = StorageUri.from_local_path(tmp_path / product)
    binding = _sentinel_binding(product_uri)

    manager = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="complete",
        )
        rebuild = manager.rebuild_snapshot(product)
    finally:
        manager.close()

    assert rebuild["records"] >= 1, "rebuild_snapshot should compact at least the run record"

    control_root = tmp_path / product / ".firecube"
    snapshots_root = control_root / "snapshots"
    assert snapshots_root.exists(), f"expected snapshots dir at {snapshots_root}"

    scanned_any = False
    for path in snapshots_root.rglob("*"):
        if not path.is_file():
            continue
        scanned_any = True
        content = path.read_bytes()
        assert SENTINEL_ACCESS_KEY.encode() not in content, (
            f"snapshot file leaked access_key: {path}"
        )
        assert SENTINEL_SECRET_KEY.encode() not in content, (
            f"snapshot file leaked secret_key: {path}"
        )
    assert scanned_any, f"expected at least one snapshot file under {snapshots_root}"

    latest_pointer = control_root / "LATEST.json"
    assert latest_pointer.exists(), "rebuild_snapshot should write LATEST.json pointer"
    pointer_text = latest_pointer.read_text(encoding="utf-8")
    assert SENTINEL_ACCESS_KEY not in pointer_text, "LATEST.json leaked access_key"
    assert SENTINEL_SECRET_KEY not in pointer_text, "LATEST.json leaked secret_key"


def test_credentials_do_not_leak_into_telemetry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T4b/4: Telemetry ``emit()`` calls during ``complete_output`` must not include credentials."""
    captured: list[tuple[str, float, str, dict[str, Any] | None]] = []

    class _CapturingTelemetry:
        def __init__(self, run_id: str = "telemetry-redaction-run") -> None:
            self._run_id = run_id

        @property
        def run_id(self) -> str:
            return self._run_id

        def emit(
            self,
            name: str,
            value: float,
            *,
            kind: Literal["gauge", "counter"] = "gauge",
            meta: dict[str, Any] | None = None,
        ) -> None:
            captured.append((name, value, str(kind), meta))

        def flush(self) -> None:
            return None

        @contextlib.contextmanager
        def span(self, name: str, attributes: dict[str, Any] | None = None):
            _ = (name, attributes)
            yield

        def collect_memory_stats(self) -> None:
            return None

    target_root = tmp_path / "out"
    target_root.mkdir()
    target = target_root / "product.zarr"

    staged = tmp_path / "staged" / "product.zarr"
    staged.mkdir(parents=True)
    (staged / "zarr.json").write_text("{}", encoding="utf-8")
    (staged / "data.bin").write_bytes(b"payload")

    binding = _sentinel_binding(StorageUri.from_local_path(target))

    telemetry_sink = _CapturingTelemetry()

    telemetry_sink.emit("test_baseline_metric", 1.0, meta={"safe": "value"})
    assert len(captured) == 1, "baseline emit should be captured"

    ctx = _build_runtime_ctx(
        target=str(target),
        binding=binding,
        write_mode="staged",
        telemetry=telemetry_sink,
    )

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=1.0)),
    )

    _stub_upload_tree(monkeypatch)
    PipelineExecutor().complete_output(result, ctx, host=_FakeHost())  # type: ignore[arg-type]

    for name, _value, _kind, meta in captured:
        rendered = f"{name}|{meta!r}"
        assert SENTINEL_ACCESS_KEY not in rendered, (
            f"telemetry emit leaked access_key: name={name!r} meta={meta!r}"
        )
        assert SENTINEL_SECRET_KEY not in rendered, (
            f"telemetry emit leaked secret_key: name={name!r} meta={meta!r}"
        )
