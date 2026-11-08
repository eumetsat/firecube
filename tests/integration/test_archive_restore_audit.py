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
import zipfile
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.archive.tensogram.converter import zarr_to_tgm
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ConfigurationError, ProductLabelMismatchError
from firecube.core.product.open import open_product
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime import archive as archive_module
from firecube.ingestor.runtime.archive import (
    ArchiveClaimError,
    ArchiveConversionError,
    create_archive,
    restore_archive,
)
from firecube.ingestor.runtime.migrate import adopt_product
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
            "--product-name",
            "restored.zarr",
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
            "--product-name",
            "restored.zarr",
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
            "runs",
            "list",
            "--target",
            StorageUri.from_local_path(restored).to_str(),
            "--format",
            "json",
        ],
        env=env,
    )
    assert list_result.exit_code == 0, list_result.output
    runs_output = list_result.output.split("\n--- Logging error ---", 1)[0]
    runs = json.loads(runs_output)
    archive_runs = [r for r in runs if "archive-restore-" in r["run_id"]]
    assert len(archive_runs) == 1
    assert archive_runs[0]["status"] == "complete"


def test_failed_restore_records_status_failed(tmp_path: Path, monkeypatch) -> None:
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

    def failing_tgm_to_zarr(*args, **kwargs):
        raise ValueError("injected restore failure")

    # The restore is admitted by the preflight and the claim; the converter then fails.
    monkeypatch.setattr("firecube.core.archive.tensogram.restore.tgm_to_zarr", failing_tgm_to_zarr)
    failed_restore = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            archive_uri,
            "--target",
            target_uri,
            "--product-name",
            "restored.zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
        env=env,
    )
    assert failed_restore.exit_code != 0
    assert "injected restore failure" in failed_restore.output

    archive_runs = _list_archive_restore_runs(workspace, tmp_path, "restored.zarr")
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


def test_restore_onto_an_existing_target_is_refused_before_any_run(labelled_zip: Path) -> None:
    target = labelled_zip / "r.zarr"
    args = ["archive", "restore", "--archive", (labelled_zip / "made.zip").as_uri()]
    args += ["--target", target.as_uri()]

    first = CliRunner().invoke(cli, args)
    second = CliRunner().invoke(cli, args)

    assert first.exit_code == 0, first.output
    assert second.exit_code != 0
    assert "already exists" in second.output.lower()
    runs = sorted((target / ".firecube" / "runs").glob("archive-restore-*/run.json"))
    assert [json.loads(p.read_text())["status"] for p in runs] == ["complete"]


@pytest.fixture
def labelled_zip(tmp_path: Path) -> Path:
    """A product store `s.zarr` labelled LBL, its zip archive `made.zip` and a bad `bad.zip`."""
    with zipfile.ZipFile(tmp_path / "bad.zip", "w") as bad:
        bad.writestr("stray.txt", "no root zarr.json")
    store = tmp_path / "s.zarr"
    zarr.open_group(str(store), mode="w").create_array("a", shape=(3,), dtype="f8")
    with _open(store) as product:
        adopt_product(product)
        create_archive(product, **_zip_args(tmp_path / "made.zip"))
    return tmp_path


def _open(path: Path, label: str = "LBL"):
    uri = StorageUri.from_local_path(path)
    return open_product(uri, format="zarr", product_name=label, require_label=False)


def _zip_args(archive: Path) -> dict:
    return {"archive": archive.as_uri(), "archive_path": str(archive), "archive_format": "zip"}


def _hold_write_claim(product) -> None:
    domain = WriteDomain(product="LBL", category="zarr_region", name="probe")
    product.manager.acquire_claim(product="LBL", domain=domain, owner_id="probe-holder")


@pytest.mark.parametrize(
    ("target", "label", "archive", "held", "error"),
    [
        ("r.zarr", "OTHER", "made.zip", False, ProductLabelMismatchError),
        ("r.zarr", "LBL", "gone.zip", False, FileNotFoundError),
        ("s.zarr", "LBL", "made.zip", False, ArchiveConversionError),
        ("r.zarr", "LBL", "made.zip", True, ArchiveClaimError),
        ("r.zarr", "LBL", "bad.zip", False, ArchiveConversionError),
    ],
    ids=[
        "label_differs_from_archive",
        "archive_missing",
        "target_exists_without_overwrite",
        "write_claim_held",
        "archive_without_root_metadata",
    ],
)
def test_restore_dry_run_raises_each_preflight_refusal(
    labelled_zip, target, label, archive, held, error
):
    with _open(labelled_zip / target, label) as product, pytest.raises(error):
        if held:
            _hold_write_claim(product)
        restore_archive(product, **_zip_args(labelled_zip / archive), dry_run=True)


@pytest.mark.parametrize(
    ("store", "options", "archive", "held", "error"),
    [
        ("s.zarr", {"group": "F024"}, "new.zip", False, ConfigurationError),
        ("s.zarr", {}, "made.zip", False, ArchiveConversionError),
        ("s.zarr", {}, "new.zip", True, ArchiveClaimError),
        ("none.zarr", {}, "new.zip", False, ArchiveConversionError),
    ],
    ids=[
        "zip_only_option",
        "archive_exists_without_overwrite",
        "write_claim_held",
        "source_missing",
    ],
)
def test_create_dry_run_raises_each_preflight_refusal(
    labelled_zip, store, options, archive, held, error
):
    with _open(labelled_zip / store) as product, pytest.raises(error):
        if held:
            _hold_write_claim(product)
        create_archive(product, **_zip_args(labelled_zip / archive), **options, dry_run=True)


@pytest.mark.parametrize("op", ["create", "restore"])
def test_clean_dry_run_takes_no_claim_and_records_no_run(labelled_zip, op, monkeypatch):
    store = labelled_zip / "s.zarr"
    target = labelled_zip / "r.zarr"
    acquired: list[str] = []
    real_acquire = ChunkManager.acquire_claim

    def spy(self, **kwargs):
        acquired.append(kwargs["product"])
        return real_acquire(self, **kwargs)

    with _open(store if op == "create" else target) as product:
        monkeypatch.setattr(ChunkManager, "acquire_claim", spy)
        before = [r.run_id for r in product.manager.list_runs(product="LBL")]
        if op == "create":
            create_archive(product, **_zip_args(labelled_zip / "new.zip"), dry_run=True)
        else:
            restore_archive(product, **_zip_args(labelled_zip / "made.zip"), dry_run=True)

        assert acquired == []
        assert product.manager.list_claims(product="LBL") == []
        assert [r.run_id for r in product.manager.list_runs(product="LBL")] == before
    assert not (labelled_zip / "new.zip").exists() and not (target / "zarr.json").exists()


def test_restore_refuses_a_label_stamped_between_the_preflight_and_the_claim(
    labelled_zip, monkeypatch
):
    target = labelled_zip / "r.zarr"
    real_acquire = archive_module._acquire_maintenance_claim

    def acquire_after_drift(**kwargs):
        granted = real_acquire(**kwargs)
        (target / ".firecube" / "product.json").write_text('{"product": "OTHER"}')
        return granted

    monkeypatch.setattr(archive_module, "_acquire_maintenance_claim", acquire_after_drift)
    warnings: list[str] = []
    with _open(target) as product, pytest.raises(ProductLabelMismatchError):
        restore_archive(product, **_zip_args(labelled_zip / "made.zip"), warn=warnings.append)
    # Refused under the claim, before the run: no failure record is attempted, so no warning.
    assert warnings == []
    assert not (target / "zarr.json").exists()
    assert not list((target / ".firecube" / "runs").glob("archive-restore-*"))
