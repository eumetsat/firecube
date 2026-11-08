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

"""Maintenance ops produce an audit trail in the control plane.

These tests cover the end-to-end behavior that ``execute_deletion``,
``delete_spans``, and ``archive restore`` each emit ``maintenance_started``
plus ``maintenance_completed``/``maintenance_failed`` WAL events visible
through ``ChunkManager.list_runs`` and the per-run ``run.json`` file.
"""

from __future__ import annotations

import json
import time
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.archive.tensogram.converter import zarr_to_tgm
from firecube.core.controlplane import ChunkInfo, ChunkManager
from firecube.core.controlplane.types import (
    EVENT_MAINTENANCE_COMPLETED,
    EVENT_MAINTENANCE_FAILED,
    EVENT_MAINTENANCE_STARTED,
    DeletionPlan,
    SpanCoverage,
)
from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.product.open import open_product
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime import archive as lib
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


def _open(tmp_path: Path, name: str = "product.zarr"):
    return open_product(
        (tmp_path / name).as_uri(),
        format="zarr",
        driver=StorageDriverConfig(driver="fsspec"),
        product_name=name,
        require_label=False,
    )


def _local_env(tmp_path: Path) -> dict[str, str]:
    return {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}


def _read_events(target: Path, run_id: str) -> list[dict]:
    run_dir = target / ".firecube" / "runs" / run_id
    events: list[dict] = []
    for path in sorted(run_dir.glob("events-*.jsonl")):
        events.extend(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return events


def _read_run_meta(target: Path, run_id: str) -> dict:
    return json.loads(
        (target / ".firecube" / "runs" / run_id / "run.json").read_text(encoding="utf-8")
    )


def test_execute_deletion_creates_audit_run_record(tmp_path):
    product = "product.zarr"
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)
    try:
        plan = DeletionPlan(
            chunks=[],
            total_size=0,
            products_affected={product},
            manifest_files=set(),
        )
        result = manager.execute_deletion(
            plan,
            delete_storage=False,
            delete_manifest=False,
            dry_run=False,
        )
        assert "deleted_chunks" in result

        runs = manager.list_runs(product=product)
        maintenance_runs = [r for r in runs if r.run_id.startswith("maintenance-delete-")]
        assert len(maintenance_runs) == 1
        run = maintenance_runs[0]
        assert run.status == "complete"

        events = _read_events(tmp_path / product, run.run_id)
        types = [e["event_type"] for e in events]
        assert EVENT_MAINTENANCE_STARTED in types
        assert EVENT_MAINTENANCE_COMPLETED in types

        started = next(e for e in events if e["event_type"] == EVENT_MAINTENANCE_STARTED)
        assert started["record"]["meta"]["kind"] == "maintenance"
        assert started["record"]["meta"]["op"] == "delete"
        assert started["record"]["meta"]["delete_storage"] is False
        assert started["record"]["meta"]["delete_manifest"] is False

        meta = _read_run_meta(tmp_path / product, run.run_id)
        assert meta["status"] == "complete"
    finally:
        manager.close()


def test_delete_spans_creates_audit_run_record(tmp_path):
    product = "product.zarr"
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)
    try:
        ingest_run = "ingest-run-001"
        manager.record_run_started(
            product=product,
            run_id=ingest_run,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_span(
            product=product,
            run_id=ingest_run,
            batch_id="batch-1",
            group="F024",
            status="active",
            coverage=SpanCoverage(
                group="F024",
                arrays=["F024/FWI"],
                time_index_ranges=[[0, 0]],
                aligned=True,
            ),
            meta={
                "group": "F024",
                "time_min": "2024-01-01T00:00:00Z",
                "time_max": "2024-01-01T00:00:00Z",
            },
        )
        manager.record_run_terminal(
            product=product,
            run_id=ingest_run,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="complete",
        )

        spans = manager.list_chunks(product=product, chunk_type="span")
        assert len(spans) == 1
        result = manager.delete_spans(spans, dry_run=False)
        assert result["dry_run"] is False

        runs = manager.list_runs(product=product)
        maintenance_runs = [r for r in runs if r.run_id.startswith("maintenance-delete-spans-")]
        assert len(maintenance_runs) == 1
        run = maintenance_runs[0]
        assert run.status == "complete"

        events = _read_events(tmp_path / product, run.run_id)
        types = [e["event_type"] for e in events]
        assert EVENT_MAINTENANCE_STARTED in types
        assert EVENT_MAINTENANCE_COMPLETED in types

        started = next(e for e in events if e["event_type"] == EVENT_MAINTENANCE_STARTED)
        assert started["record"]["meta"]["kind"] == "maintenance"
        assert started["record"]["meta"]["op"] == "delete"
        assert started["record"]["meta"]["spans_count"] == 1
    finally:
        manager.close()


def test_execute_deletion_manifest_errors_complete_with_error_details(tmp_path):
    product = "product.zarr"
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)

    bad_chunk = ChunkInfo(
        key="F024/FWI/c/0/0/0",
        product=product,
        chunk_type="chunk",
        size=1,
        timestamp=time.time(),
        manifest_path="bad://manifest/uri",
    )

    original_remove = manager.repo.remove_from_manifest

    def _boom(manifest_uri: str, chunks_to_remove):
        raise RuntimeError("simulated mutation failure")

    plan = DeletionPlan(
        chunks=[bad_chunk],
        total_size=1,
        products_affected={product},
        manifest_files={"bad://manifest/uri"},
    )

    try:
        manager.repo.remove_from_manifest = _boom
        result = manager.execute_deletion(
            plan,
            delete_storage=False,
            delete_manifest=True,
            dry_run=False,
        )
        assert result["deleted_chunks"] == 0
        assert result["deleted_size_bytes"] == 0
        assert result["storage_errors"] == []
        assert len(result["manifest_errors"]) == 1
        assert "simulated mutation failure" in result["manifest_errors"][0]

        runs = manager.list_runs(product=product)
        maintenance_runs = [r for r in runs if r.run_id.startswith("maintenance-delete-")]
        assert len(maintenance_runs) == 1
        run = maintenance_runs[0]
        assert run.status == "complete"

        events = _read_events(tmp_path / product, run.run_id)
        completed = [e for e in events if e["event_type"] == EVENT_MAINTENANCE_COMPLETED]
        failed = [e for e in events if e["event_type"] == EVENT_MAINTENANCE_FAILED]
        assert completed, (
            "expected maintenance_completed event when remove_from_manifest reports errors"
        )
        assert not failed, (
            "execute_deletion captures errors instead of raising — completed should fire"
        )
    finally:
        manager.repo.remove_from_manifest = original_remove
        manager.close()


def _make_dataset() -> xr.Dataset:
    rng = np.random.default_rng(7)
    return xr.Dataset(
        {
            "FWI": (
                ["timestamp", "lat", "lon"],
                rng.random((4, 3, 5)).astype("float32"),
                {"units": "1"},
            ),
        },
        coords={
            "timestamp": np.arange(4),
            "lat": np.linspace(-10.0, 10.0, 3, dtype="float32"),
            "lon": np.linspace(0.0, 20.0, 5, dtype="float32"),
        },
        attrs={"Conventions": "CF-1.8"},
    )


def test_archive_restore_emits_maintenance_lifecycle_events(tmp_path):
    runner = CliRunner()
    env = _local_env(tmp_path)
    source_zarr = tmp_path / "source.zarr"
    archive_path = tmp_path / "archive.tgm"
    restored = tmp_path / "restored.zarr"
    restored_uri = f"file://{restored}"

    _make_dataset().to_zarr(source_zarr, group="F024")
    zarr_to_tgm(str(source_zarr), str(archive_path), group="F024")

    archive_uri = f"file://{archive_path}"
    result = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            archive_uri,
            "--target",
            restored_uri,
            "--product-name",
            "restored.zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--overwrite",
            "--yes-i-really-mean-it",
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output

    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path / "ws")
    try:
        runs = [
            r
            for r in manager.list_runs(product="restored.zarr")
            if r.run_id.startswith("archive-restore-")
        ]
    finally:
        manager.close()

    assert len(runs) == 1
    run = runs[0]
    assert run.status == "complete"

    events = _read_events(restored, run.run_id)
    types = [e["event_type"] for e in events]
    assert EVENT_MAINTENANCE_STARTED in types
    assert EVENT_MAINTENANCE_COMPLETED in types

    started = next(e for e in events if e["event_type"] == EVENT_MAINTENANCE_STARTED)
    assert started["record"]["meta"]["op"] == "archive_restore"
    assert started["record"]["meta"]["kind"] == "maintenance"
    assert started["record"]["meta"]["source_archive"] == archive_uri
    assert started["record"]["meta"]["target_product"] == restored_uri


def _zip_create_args(tmp_path: Path) -> list[str]:
    product = tmp_path / "product.zarr"
    _make_dataset().to_zarr(product, group="F024")
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)
    try:
        manager.record_run_started(
            product="product.zarr", run_id="seed", output_path=str(product),
            output_format="zarr", size=0, meta={},
        )  # fmt: skip
    finally:
        manager.close()
    out = (tmp_path / "out.zip").as_uri()
    flags = "--archive-format zip --storage-type local --storage-driver fsspec"
    return f"archive create --target {product.as_uri()} --archive {out} {flags}".split()


def test_archive_create_fails_when_record_cannot_be_written(tmp_path, monkeypatch):
    denied = Mock(side_effect=PermissionError("record denied"))
    monkeypatch.setattr(ChunkManager, "record_maintenance_completed", denied)
    res = CliRunner().invoke(cli, _zip_create_args(tmp_path), env=_local_env(tmp_path))
    assert res.exit_code == 1 and "record denied" in res.output and (tmp_path / "out.zip").exists()


def test_archive_create_keeps_archive_error_when_record_setup_fails(tmp_path, monkeypatch):
    from firecube.core.storage.session import StorageSession

    failed = []
    original = StorageSession.control_plane

    def _zarr_to_zip(*a, **k):
        failed.append(True)
        raise ValueError("archive boom")

    def _control_plane(self):
        if failed:
            raise RuntimeError("record boom")
        return original(self)

    monkeypatch.setattr("firecube.core.archive.zip.archive.zarr_to_zip", _zarr_to_zip)
    monkeypatch.setattr(StorageSession, "control_plane", _control_plane)
    result = CliRunner().invoke(cli, _zip_create_args(tmp_path), env=_local_env(tmp_path))
    assert result.exit_code != 0
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert "Error: archive boom" in result.output.splitlines()


def test_archive_restore_failure_emits_maintenance_failed(tmp_path, monkeypatch):
    runner = CliRunner()
    env = _local_env(tmp_path)
    valid_archive = _zip_whose_admitted_restore_fails(tmp_path, monkeypatch)
    restored = tmp_path / "restored.zarr"

    result = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            StorageUri.from_local_path(valid_archive).to_str(),
            "--target",
            StorageUri.from_local_path(restored).to_str(),
            "--product-name",
            "restored.zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
        env=env,
    )
    assert result.exit_code != 0

    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path / "ws")
    try:
        runs = [
            r
            for r in manager.list_runs(product="restored.zarr")
            if r.run_id.startswith("archive-restore-")
        ]
    finally:
        manager.close()

    failed_runs = [r for r in runs if r.status == "failed"]
    assert len(failed_runs) >= 1, [r.status for r in runs]

    events = _read_events(restored, failed_runs[0].run_id)
    types = [e["event_type"] for e in events]
    assert EVENT_MAINTENANCE_STARTED in types
    assert EVENT_MAINTENANCE_FAILED in types

    failed_event = next(e for e in events if e["event_type"] == EVENT_MAINTENANCE_FAILED)
    assert failed_event["record"]["maintenance"]["error"]
    assert failed_event["record"]["meta"]["op"] == "archive_restore"


def _zip_whose_admitted_restore_fails(tmp_path: Path, monkeypatch) -> Path:
    """Write a zip the preflight admits, and make its restore fail while the claim is held."""
    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("zarr.json", '{"zarr_format": 3, "node_type": "group"}')

    def _failing_restore(*a, **k):
        raise ValueError("No Zarr v3 root metadata found")

    monkeypatch.setattr("firecube.core.archive.zip.archive.zip_to_zarr", _failing_restore)
    return archive


@pytest.mark.parametrize("failing", ["record", "release"])
def test_restore_error_survives_failing_bookkeeping(tmp_path, monkeypatch, failing):
    archive = _zip_whose_admitted_restore_fails(tmp_path, monkeypatch)
    if failing == "record":
        boom = Mock(side_effect=OSError("bookkeeping boom"))
        monkeypatch.setattr(ChunkManager, "record_maintenance_failed", boom)
    else:
        real_delete = FsspecFilesystem.compare_delete

        def compare_delete(self, uri, validator):
            if "/claims/" in uri.to_str():
                raise PermissionError("bookkeeping boom")
            return real_delete(self, uri, validator)

        monkeypatch.setattr(FsspecFilesystem, "compare_delete", compare_delete)
    args = ["archive", "restore", "--archive", archive.as_uri(), "--storage-type", "local"]
    args += ["--target", (tmp_path / "r.zarr").as_uri(), "--storage-driver", "fsspec"]
    args += ["--product-name", "r.zarr"]
    result = CliRunner().invoke(cli, args, env=_local_env(tmp_path))
    assert result.exit_code != 0
    assert "No Zarr v3 root metadata found" in result.output
    assert "bookkeeping boom" in result.output


@pytest.mark.contract
def test_restore_archive_raises_its_own_error_when_bookkeeping_fails(tmp_path, monkeypatch):
    archive = _zip_whose_admitted_restore_fails(tmp_path, monkeypatch)
    boom = Mock(side_effect=OSError("bookkeeping boom"))
    monkeypatch.setattr(ChunkManager, "record_maintenance_failed", boom)
    real_delete = FsspecFilesystem.compare_delete

    def compare_delete(self, uri, validator):
        if "/claims/" in uri.to_str():
            raise PermissionError("bookkeeping boom")
        return real_delete(self, uri, validator)

    monkeypatch.setattr(FsspecFilesystem, "compare_delete", compare_delete)
    warnings: list[str] = []
    with (
        pytest.raises(lib.ArchiveConversionError, match="No Zarr v3 root metadata found"),
        _open(tmp_path, "r.zarr") as product,
    ):
        lib.restore_archive(
            product,
            archive=archive.as_uri(),
            archive_path=str(archive),
            archive_format="zip",
            warn=warnings.append,
        )
    assert [w.startswith("Warning: ") and "bookkeeping boom" in w for w in warnings] == [True] * 2


@pytest.mark.contract
@pytest.mark.parametrize(
    ("archive_format", "selection"),
    [("zip", {}), ("tensogram", {"group": "F024", "variables": ["FWI"]})],
)
def test_create_archive_records_the_run_with_its_scope(tmp_path, archive_format, selection):
    _zip_create_args(tmp_path)  # builds the product and its control plane; the args go unused
    out = tmp_path / "out.archive"
    kwargs = {"archive": out.as_uri(), "archive_path": str(out), "archive_format": archive_format}
    with _open(tmp_path) as product:
        result = lib.create_archive(product, **kwargs, **selection)
    assert result.run_id is not None
    events = _read_events(tmp_path / "product.zarr", result.run_id)
    types = [e["event_type"] for e in events]
    assert types == [EVENT_MAINTENANCE_STARTED, EVENT_MAINTENANCE_COMPLETED]
    scope = {"run_kind": "archive_create", "archive_format": archive_format}
    scope |= dict.fromkeys(["group", "variables", "start_date", "end_date"]) | selection
    scope |= {
        "source_product": (tmp_path / "product.zarr").as_uri(),
        "target_archive": kwargs["archive"],
    }
    scope["file_size_bytes"] = out.stat().st_size
    for event in events:
        assert scope.items() <= event["record"]["meta"].items(), event["record"]["meta"]
        datetime.fromisoformat(event["record"]["meta"]["archive_started_at"])


@pytest.mark.contract
def test_create_archive_raises_record_error_with_the_archive_on_disk(tmp_path, monkeypatch):
    _zip_create_args(tmp_path)
    denied = Mock(side_effect=PermissionError("record denied"))
    monkeypatch.setattr(ChunkManager, "record_maintenance_completed", denied)
    out = tmp_path / "out.zip"
    kwargs = {"archive": out.as_uri(), "archive_path": str(out), "archive_format": "zip"}
    with pytest.raises(lib.ArchiveRecordError, match="record denied"), _open(tmp_path) as product:
        lib.create_archive(product, **kwargs)
    assert out.exists()
