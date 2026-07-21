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

"""W3.4: Archive restore opens a synthetic WAL run + audit trail.

These tests assert that ``firecube archive restore`` records a first-class
``archive_restore`` run in the target's ``.firecube/`` control plane so the
audit trail (``firecube chunks runs list``) shows when and how the product
was restored, and whether the restore succeeded or failed.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.storage.uri import StorageUri
from firecube.core.tensogram.converter import zarr_to_tgm
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


def _local_env(tmp_path: Path) -> dict[str, str]:
    return {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}


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


def _read_run_events(target: Path, run_id: str) -> list[dict]:
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
    run_meta_path = target / ".firecube" / "runs" / run_id / "run.json"
    return json.loads(run_meta_path.read_text(encoding="utf-8"))


def _assert_archive_restore_lifecycle_meta(
    events: list[dict],
    *,
    source_archive: str,
    target_product: str | None = None,
) -> None:
    lifecycle = [
        event
        for event in events
        if event["event_type"] in {"run_started", "run_completed", "run_failed"}
    ]
    assert lifecycle, "expected archive_restore lifecycle events"
    for event in lifecycle:
        meta = event["meta"]
        assert meta["run_kind"] == "archive_restore"
        assert meta["source_archive"] == source_archive
        if target_product is not None:
            assert meta["target_product"] == target_product


def _list_archive_restore_runs(workspace: Path, output_base: Path, product: str):
    manager = ChunkManager(binding=make_test_binding(output_base), workspace=workspace)
    try:
        return [
            run for run in manager.list_runs(product=product) if "archive-restore-" in run.run_id
        ]
    finally:
        manager.close()


def test_successful_restore_records_archive_restore_run(tmp_path: Path) -> None:
    runner = CliRunner()
    env = _local_env(tmp_path)
    source_zarr = tmp_path / "source.zarr"
    archive_path = tmp_path / "archive.tgm"
    restored = tmp_path / "restored.zarr"
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    _make_dataset().to_zarr(source_zarr, group="F024")
    zarr_to_tgm(str(source_zarr), str(archive_path), group="F024")

    archive_uri = StorageUri.from_local_path(archive_path).to_str()
    target_uri = StorageUri.from_local_path(restored).to_str()
    result = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            archive_uri,
            "--target",
            target_uri,
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output

    archive_runs = _list_archive_restore_runs(workspace, tmp_path, "restored.zarr")
    assert len(archive_runs) == 1
    run = archive_runs[0]
    assert run.status == "complete"

    events = _read_run_events(restored, run.run_id)
    assert events, "expected WAL events for archive_restore run"
    started_events = [e for e in events if e["event_type"] == "run_started"]
    completed_events = [e for e in events if e["event_type"] == "run_completed"]
    assert started_events, "expected run_started event"
    assert completed_events, "expected run_completed event"

    started_meta = started_events[0]["meta"]
    assert started_meta["run_kind"] == "archive_restore"
    assert started_meta["source_archive"] == archive_uri
    assert started_meta["target_product"] == target_uri
    _assert_archive_restore_lifecycle_meta(
        events,
        source_archive=archive_uri,
        target_product=target_uri,
    )

    run_meta = _read_run_meta(restored, run.run_id)
    assert run_meta["status"] == "complete"
    assert run_meta["product"] == "restored.zarr"


def test_successful_restore_appears_in_chunks_runs_list_cli(tmp_path: Path) -> None:
    runner = CliRunner()
    env = _local_env(tmp_path)
    source_zarr = tmp_path / "source.zarr"
    archive_path = tmp_path / "archive.tgm"
    restored = tmp_path / "restored.zarr"

    _make_dataset().to_zarr(source_zarr, group="F024")
    zarr_to_tgm(str(source_zarr), str(archive_path), group="F024")

    restore_result = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            StorageUri.from_local_path(archive_path).to_str(),
            "--target",
            StorageUri.from_local_path(restored).to_str(),
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
        env=env,
    )
    assert restore_result.exit_code == 0, restore_result.output

    list_result = runner.invoke(
        cli,
        [
            "chunks",
            "--quiet",
            "runs",
            "list",
            "--product-name",
            "restored.zarr",
            "--format",
            "json",
        ],
        env=env,
    )
    assert list_result.exit_code == 0, list_result.output
    runs = json.loads(list_result.output)
    archive_runs = [r for r in runs if "archive-restore-" in r["run_id"]]
    assert len(archive_runs) == 1
    assert archive_runs[0]["status"] == "complete"


def test_failed_restore_records_status_failed(tmp_path: Path) -> None:
    runner = CliRunner()
    env = _local_env(tmp_path)
    source_zarr = tmp_path / "source.zarr"
    archive_path = tmp_path / "archive.tgm"
    restored = tmp_path / "restored.zarr"
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    _make_dataset().to_zarr(source_zarr, group="F024")
    zarr_to_tgm(str(source_zarr), str(archive_path), group="F024")

    archive_uri = StorageUri.from_local_path(archive_path).to_str()
    target_uri = StorageUri.from_local_path(restored).to_str()
    first_restore = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            archive_uri,
            "--target",
            target_uri,
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
        env=env,
    )
    assert first_restore.exit_code == 0, first_restore.output

    second_restore = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            archive_uri,
            "--target",
            target_uri,
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
        env=env,
    )
    assert second_restore.exit_code != 0
    assert "already exists" in second_restore.output.lower()

    archive_runs = _list_archive_restore_runs(workspace, tmp_path, "restored.zarr")
    assert len(archive_runs) >= 2
    failed_runs = [r for r in archive_runs if r.status == "failed"]
    assert len(failed_runs) == 1, (
        f"expected exactly one failed archive_restore run; got {[r.status for r in archive_runs]}"
    )

    failed_meta = _read_run_meta(restored, failed_runs[0].run_id)
    assert failed_meta["status"] == "failed"
    assert failed_meta.get("error")

    events = _read_run_events(restored, failed_runs[0].run_id)
    failed_events = [e for e in events if e["event_type"] == "run_failed"]
    assert failed_events, "expected run_failed event"
    failed_event_meta = failed_events[0]["meta"]
    assert failed_event_meta["run_kind"] == "archive_restore"
    assert failed_event_meta["source_archive"] == archive_uri
    _assert_archive_restore_lifecycle_meta(
        events,
        source_archive=archive_uri,
        target_product=target_uri,
    )
