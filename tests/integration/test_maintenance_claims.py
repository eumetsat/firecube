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

"""Maintenance ops acquire write claims before mutating product state."""

from __future__ import annotations

import json
import time
import zipfile
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli._errors import render
from firecube.cli.main import cli
from firecube.core.archive.tensogram import restore as tgm_restore
from firecube.core.archive.tensogram.converter import zarr_to_tgm
from firecube.core.archive.zip import archive as zip_archive
from firecube.core.controlplane import ChunkInfo, ChunkManager
from firecube.core.controlplane.claims import FilesystemClaimService
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import (
    EVENT_MAINTENANCE_COMPLETED,
    EVENT_MAINTENANCE_STARTED,
    EVENT_RECORD_UPSERT,
    SCHEMA_VERSION,
    DeletionPlan,
    SpanCoverage,
    WriteDomain,
    exclusive_maintenance_domain,
)
from firecube.core.errors import (
    ExclusiveMaintenanceRefusedError,
    ManifestError,
    ProductUnderMaintenanceError,
)
from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.index_spec import IndexSpec, IntegerAxis, RegularTimeAxis
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.scrub import run_scrub
from firecube.ingestor.registry import loader as ingestor_loader
from firecube.ingestor.runtime.archive import ArchiveClaimError, create_archive, restore_archive
from firecube.ingestor.runtime.index_record import rebuild_index_record
from tests.helpers.cli_hints import run_hint
from tests.helpers.storage import make_test_binding, make_test_session

pytestmark = pytest.mark.integration


def _local_env(tmp_path: Path) -> dict[str, str]:
    return {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}


def _claim_domain(product: str, group: str = "F024") -> WriteDomain:
    return WriteDomain(product=product, category="zarr_append", name=group)


def _claiming_manager(tmp_path: Path, product: str = "product.zarr", fs=False) -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    binding = make_test_binding(tmp_path, product=product)
    return ChunkManager(binding=binding, workspace=workspace, filesystem=FsspecFilesystem(binding) if fs else None)  # fmt: skip


def _sample_chunk(product: str) -> ChunkInfo:
    return ChunkInfo(
        key="F024/FWI/c/0/0/0",
        product=product,
        chunk_type="chunk",
        size=1,
        timestamp=time.time(),
        manifest_path="",
    )


def _sample_span(product: str) -> ChunkInfo:
    return ChunkInfo(
        key="span-run-1-batch-1-F024",
        product=product,
        chunk_type="span",
        size=0,
        timestamp=time.time(),
        manifest_path="",
        meta={"group": "F024", "run_id": "run-1", "batch_id": "batch-1"},
        record={
            "span": {
                "arrays": ["F024/FWI"],
                "time_index_ranges": [[0, 0]],
                "aligned": True,
            }
        },
    )


def _make_archive(source_zarr: Path, archive_path: Path, group: str | None = "F024") -> None:
    dataset = xr.Dataset(
        {
            "FWI": (
                ["timestamp", "lat", "lon"],
                np.random.default_rng(7).random((2, 2, 2)).astype("float32"),
            )
        },
        coords={
            "timestamp": np.arange(2),
            "lat": np.linspace(-1.0, 1.0, 2, dtype="float32"),
            "lon": np.linspace(0.0, 1.0, 2, dtype="float32"),
        },
    )
    dataset.to_zarr(source_zarr, group=group)
    zarr_to_tgm(str(source_zarr), str(archive_path), group=group)


def _assert_clear_hint_runs_without_abandon(message: str) -> None:
    run_hint(message, "firecube chunks claims clear", "--yes-i-really-mean-it")
    assert "firecube chunks runs abandon" not in message


def _assert_owner_abandon_hint_runs(message: str, root: str) -> None:
    assert f"--product-name {root} --run-id running-ingest" in message
    assert "or abandon run running-ingest (status: started)," in message
    run_hint(message, "firecube chunks claims clear", "--yes-i-really-mean-it")
    run_hint(message, "firecube chunks runs abandon", "--dry-run")


def _assert_claim_facts(
    error: ArchiveClaimError, root: Path, *, owner: str, recovery: list[str], run_id: str = ""
):
    context, domain = error.context, _claim_domain(error.product).identifier
    assert (context.product_uri, context.claim_domain, context.claim_owner) == (
        root.as_uri(), domain, owner,
    )  # fmt: skip
    assert [op.name for op in error.recovery] == recovery
    assert {op.arguments["product_name"] for op in error.recovery} == {root.as_uri()}
    assert error.recovery[0].arguments["domain_id"] == domain
    assert all(op.arguments["run_id"] == run_id for op in error.recovery[1:])


def test_execute_deletion_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    product = "product.zarr"
    manager = _claiming_manager(tmp_path)
    claim = manager.acquire_claim(
        product=product,
        domain=_claim_domain(product),
        owner_id="running-ingest:F024",
    )
    try:
        plan = DeletionPlan(
            chunks=[_sample_chunk(product)],
            total_size=1,
            products_affected={product},
            manifest_files=set(),
        )

        with pytest.raises(ManifestError) as excinfo:
            manager.execute_deletion(
                plan,
                delete_storage=True,
                delete_manifest=False,
                dry_run=False,
            )

        claims = manager.list_claims(product=product)
        assert [info.domain for info in claims] == [_claim_domain(product).identifier]
        assert "ChunkManager.clear_claim" in str(excinfo.value)
        _assert_clear_hint_runs_without_abandon(render(excinfo.value))
    finally:
        claim.release()
        manager.close()


def test_execute_deletion_names_abandon_for_a_recorded_owner(tmp_path: Path) -> None:
    product, manager = "product.zarr", _claiming_manager(tmp_path)
    manager.record_run_started(
        product=product, run_id="running-ingest", output_path="", output_format="zarr",
        size=0, meta={},
    )  # fmt: skip
    claim = manager.acquire_claim(
        product=product, domain=_claim_domain(product), owner_id="running-ingest:F024"
    )
    plan = DeletionPlan([_sample_chunk(product)], 1, {product}, set())
    try:
        with pytest.raises(ManifestError) as excinfo:
            manager.execute_deletion(
                plan, delete_storage=True, delete_manifest=False, dry_run=False
            )
        assert "ChunkManager.abandon_run" in str(excinfo.value)
        _assert_owner_abandon_hint_runs(render(excinfo.value), manager.get_product_root(product))
    finally:
        claim.release()
        manager.close()


def test_delete_spans_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    product = "product.zarr"
    manager = _claiming_manager(tmp_path)
    claim = manager.acquire_claim(
        product=product,
        domain=_claim_domain(product),
        owner_id="running-ingest:F024",
    )
    try:
        with pytest.raises(ManifestError) as excinfo:
            manager.delete_spans([_sample_span(product)], dry_run=False)

        claims = manager.list_claims(product=product)
        assert [info.domain for info in claims] == [_claim_domain(product).identifier]
        _assert_clear_hint_runs_without_abandon(render(excinfo.value))
    finally:
        claim.release()
        manager.close()


def test_scrub_aborts_when_product_has_active_writer_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    product = "product.zarr"
    manager = _claiming_manager(tmp_path, product=product)
    claim = manager.acquire_claim(
        product=product,
        domain=_claim_domain(product),
        owner_id="running-ingest:F024",
    )
    session = make_test_session(tmp_path, product=product)

    # T3.4 migrated scrub to validate_group_with_fs (fs-aware seam) — mock that.
    monkeypatch.setattr(
        "firecube.core.zarr.scrub.validate_group_with_fs",
        lambda *args, **kwargs: SimpleNamespace(extra_chunks=[f"{product}/F024/FWI/c/0/0/0"]),
    )
    monkeypatch.setattr(
        ChunkManager,
        "list_chunks",
        lambda self, *args, **kwargs: [_sample_chunk(product)],
    )

    try:
        with pytest.raises(ManifestError) as excinfo:
            run_scrub(session, "F024")
        _assert_clear_hint_runs_without_abandon(render(excinfo.value))
    finally:
        claim.release()
        manager.close()


def _restore_with_claimed_target(
    tmp_path: Path, check: Callable[[str], None], *, record_owner: bool = False
) -> None:
    """Run a refused restore, then `check` its output while the claim is still held."""
    runner = CliRunner()
    env = _local_env(tmp_path)
    source_zarr = tmp_path / "source.zarr"
    archive_path = tmp_path / "archive.tgm"
    restored = tmp_path / "restored.zarr"
    _make_archive(source_zarr, archive_path)

    manager = _claiming_manager(tmp_path)
    if record_owner:
        manager.record_run_started(
            product="restored.zarr", run_id="running-ingest", output_path="", output_format="zarr",
            size=0, meta={},
        )  # fmt: skip
    claim = manager.acquire_claim(
        product="restored.zarr",
        domain=_claim_domain("restored.zarr"),
        owner_id="running-ingest:F024",
    )
    try:
        result = runner.invoke(
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
        assert result.exit_code != 0
        check(result.output)
    finally:
        claim.release()
        manager.close()


def test_archive_restore_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    _restore_with_claimed_target(tmp_path, _assert_clear_hint_runs_without_abandon)


def test_archive_restore_names_abandon_for_a_recorded_owner(tmp_path: Path) -> None:
    root = StorageUri.from_local_path(tmp_path / "restored.zarr").to_str()
    _restore_with_claimed_target(
        tmp_path, lambda out: _assert_owner_abandon_hint_runs(out, root), record_owner=True
    )


def _zip_create_with_control_plane(tmp_path: Path):
    src = tmp_path / "s.zarr"
    (src / ".firecube").mkdir(parents=True, exist_ok=True)
    (src / ".firecube" / "schema.json").write_text("{}")
    cmd = f"archive create --source {src.as_uri()} --archive {tmp_path.as_uri()}/out.zip"
    cmd += " --archive-format zip"
    return CliRunner().invoke(cli, cmd.split(), env=_local_env(tmp_path))


def test_archive_create_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    manager = _claiming_manager(tmp_path, product="s.zarr")
    claim = manager.acquire_claim(product="s.zarr", domain=_claim_domain("s.zarr"), owner_id="w")
    try:
        result = _zip_create_with_control_plane(tmp_path)
        assert result.exit_code != 0
        _assert_clear_hint_runs_without_abandon(result.output)
    finally:
        claim.release()
    assert not (tmp_path / "out.zip").exists()


def test_archive_refusal_after_a_lost_race_names_clear_once(tmp_path, monkeypatch):
    manager = _claiming_manager(tmp_path, product="s.zarr")
    domain = WriteDomain(product="s.zarr", category="maintenance", name="archive_create")
    claim = manager.acquire_claim(product="s.zarr", domain=domain, owner_id="other-archive")
    held = manager.list_claims(product="s.zarr")
    monkeypatch.setattr(ChunkManager, "list_claims", Mock(side_effect=[[], held]))
    result = _zip_create_with_control_plane(tmp_path)
    claim.release()
    assert result.output.count(f"--domain {domain.identifier}") == 1, result.output


def test_archive_create_keeps_archive_error_and_releases_claim(tmp_path, monkeypatch):
    monkeypatch.setattr(zip_archive, "zarr_to_zip", Mock(side_effect=ValueError("archive boom")))
    monkeypatch.setattr(ChunkManager, "record_maintenance_failed", Mock(side_effect=OSError("rec")))
    result = _zip_create_with_control_plane(tmp_path)
    assert result.exit_code != 0
    assert "archive boom" in result.output
    assert list((tmp_path / "s.zarr" / ".firecube" / "claims").glob("*")) == []


def _restore_state(tmp_path: Path) -> tuple[list[str], list[str]]:
    manager = _claiming_manager(tmp_path, product="restored.zarr")
    try:
        runs = manager.list_runs(product="restored.zarr")
        return (
            [c.domain for c in manager.list_claims(product="restored.zarr")],
            [r.status for r in runs if r.run_id.startswith("archive-restore-")],
        )
    finally:
        manager.close()


def _overwrite_restore(
    tmp_path: Path, kind: str, monkeypatch, on_write: Callable[[], None], *, library: bool = False
):
    """Run an overwrite restore, by the command or (`library`) by `restore_archive`; `on_write`
    runs after each archive write and after the restore function returns."""
    src, target, archive = (
        tmp_path / "source.zarr",
        tmp_path / "restored.zarr",
        tmp_path / f"a.{kind}",
    )
    if kind == "tgm":
        _make_archive(src, archive, group=None)
    else:
        zarr.open_group(str(src), mode="w").create_array("a", data=np.arange(6.0))
        with zipfile.ZipFile(archive, "w") as zf:
            for p in filter(Path.is_file, src.rglob("*")):
                zf.write(p, p.relative_to(src).as_posix())
    zarr.open_group(str(target), mode="w").create_array("old", data=np.arange(3.0))
    writer = (
        (StorageSession, "put_bytes") if kind == "zip" else (tgm_restore, "_restore_data_message")
    )
    entry = (zip_archive, "zip_to_zarr") if kind == "zip" else (tgm_restore, "tgm_to_zarr")

    def spy_on(original):
        def spy(*a, **k):
            result = original(*a, **k)
            on_write()
            return result

        return spy

    for owner, name in (writer, entry):
        monkeypatch.setattr(owner, name, spy_on(getattr(owner, name)))
    if library:
        return restore_archive(
            session=make_test_session(tmp_path, product="restored.zarr"),
            archive=archive.as_uri(),
            archive_path=str(archive),
            archive_format="zip" if kind == "zip" else "tensogram",
            target=target.as_uri(),
            overwrite=True,
        )
    args = ["archive", "restore", "--archive", archive.as_uri(), "--target", target.as_uri()]
    args += ["--overwrite", "--yes-i-really-mean-it", "--storage-type", "local"]
    return CliRunner().invoke(cli, [*args, "--storage-driver", "fsspec"], env=_local_env(tmp_path))


ROUTES = [False, pytest.param(True, marks=pytest.mark.contract)]


@pytest.mark.parametrize("lib", ROUTES, ids=["cli", "library"])
@pytest.mark.parametrize("kind", ["zip", "tgm"])
def test_overwrite_restore_holds_its_claim_and_run_while_writing(tmp_path, monkeypatch, kind, lib):
    states = []
    on_write = lambda: states.append(_restore_state(tmp_path))  # noqa: E731
    _overwrite_restore(tmp_path, kind, monkeypatch, on_write, library=lib)
    domain = WriteDomain(product="restored.zarr", category="maintenance", name="archive_restore")
    assert len(states) >= 2, states
    assert all(state == ([domain.identifier], ["started"]) for state in states), states


@pytest.mark.parametrize("kind", ["zip", "tgm"])
def test_interrupted_overwrite_restore_leaves_a_started_run(tmp_path, monkeypatch, kind):
    def interrupt() -> None:
        raise SystemExit(3)

    _overwrite_restore(tmp_path, kind, monkeypatch, interrupt)
    assert _restore_state(tmp_path)[1] == ["started"]


@pytest.mark.parametrize("lib", ROUTES, ids=["cli", "library"])
def test_restore_stops_its_claim_heartbeat(tmp_path, monkeypatch, lib):
    kwdefaults = FilesystemClaimService.__init__.__kwdefaults__
    monkeypatch.setitem(kwdefaults, "heartbeat_interval_s", 1)
    handles, acquire = [], ChunkManager.acquire_claim

    def spy(self, *a, **k):
        handles.append(acquire(self, *a, **k))
        return handles[-1]

    monkeypatch.setattr(ChunkManager, "acquire_claim", spy)
    _make_archive(tmp_path / "source.zarr", tmp_path / "a.tgm")
    args = ["archive", "restore", "--archive", (tmp_path / "a.tgm").as_uri()]
    args += ["--target", (tmp_path / "r.zarr").as_uri(), "--storage-type", "local"]
    try:
        if lib:
            restore_archive(
                session=make_test_session(tmp_path, product="r.zarr"),
                archive=(tmp_path / "a.tgm").as_uri(),
                archive_path=str(tmp_path / "a.tgm"),
                archive_format="tensogram",
                target=(tmp_path / "r.zarr").as_uri(),
            )
        else:
            result = CliRunner().invoke(
                cli, [*args, "--storage-driver", "fsspec"], env=_local_env(tmp_path)
            )
            assert result.exit_code == 0, result.output
        time.sleep(2.5)
        assert list((tmp_path / "r.zarr" / ".firecube" / "claims").glob("*")) == []
    finally:
        for handle in handles:
            handle.release()


@pytest.mark.contract
@pytest.mark.parametrize("kind", ["zip", "tgm"])
def test_restore_archive_interrupt_keeps_run_started_and_frees_claim(tmp_path, monkeypatch, kind):
    interrupt = Mock(side_effect=SystemExit(3))
    with pytest.raises(SystemExit):
        _overwrite_restore(tmp_path, kind, monkeypatch, interrupt, library=True)
    assert _restore_state(tmp_path) == ([], ["started"])


@pytest.mark.contract
def test_create_archive_refuses_an_active_writer_claim_and_records_no_run(tmp_path):
    (tmp_path / "s.zarr" / ".firecube").mkdir(parents=True)
    (tmp_path / "s.zarr" / ".firecube" / "schema.json").write_text("{}")
    out = tmp_path / "out.zip"
    session = make_test_session(tmp_path, product="s.zarr")
    manager = _claiming_manager(tmp_path, product="s.zarr")
    claim = manager.acquire_claim(product="s.zarr", domain=_claim_domain("s.zarr"), owner_id="w")
    try:
        with pytest.raises(ArchiveClaimError) as refused:
            create_archive(
                session=session, archive=out.as_uri(), archive_path=str(out), archive_format="zip"
            )
        runs = manager.list_runs(product="s.zarr")
    finally:
        claim.release()
        manager.close()
    assert (refused.value.product, refused.value.operation) == ("s.zarr", "archive_create")
    _assert_claim_facts(refused.value, tmp_path / "s.zarr", owner="w", recovery=["clear_claim"])
    assert not out.exists()
    assert [r.run_id for r in runs if r.run_id.startswith("archive-create-")] == []


@pytest.mark.contract
def test_restore_archive_refusal_carries_claim_facts_and_recovery(tmp_path):
    manager = _claiming_manager(tmp_path, product="r.zarr")
    manager.record_run_started(
        product="r.zarr", run_id="running-ingest", output_path="", output_format="zarr",
        size=0, meta={},
    )  # fmt: skip
    owner = "running-ingest:F024"
    claim = manager.acquire_claim(product="r.zarr", domain=_claim_domain("r.zarr"), owner_id=owner)
    try:
        with pytest.raises(ArchiveClaimError) as refused:
            restore_archive(
                session=make_test_session(tmp_path, product="r.zarr"),
                archive=(tmp_path / "a.zip").as_uri(),
                archive_path=str(tmp_path / "a.zip"),
                archive_format="zip",
                target=(tmp_path / "r.zarr").as_uri(),
            )
    finally:
        claim.release()
        manager.close()
    _assert_claim_facts(
        refused.value, tmp_path / "r.zarr", owner=owner, run_id="running-ingest",
        recovery=["clear_claim", "abandon_run"],
    )  # fmt: skip


@pytest.mark.contract
def test_create_archive_of_a_plain_store_takes_no_claim_and_writes_nothing_to_it(tmp_path):
    source = tmp_path / "plain.zarr"
    zarr.open_group(str(source), mode="w").create_array("a", data=np.arange(6.0))
    out = tmp_path / "out.zip"
    session = make_test_session(tmp_path, product="plain.zarr")
    tree = {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    result = create_archive(
        session=session, archive=out.as_uri(), archive_path=str(out), archive_format="zip"
    )
    assert result.run_id is None and out.exists() and not (source / ".firecube").exists()
    assert {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()} == tree


# --- exclusive maintenance claim: acquirer and the checks of recorded writers ---

GATED = "gate.zarr"
MAINT = "maint-run"


def _exclusive() -> str:
    return exclusive_maintenance_domain(GATED).identifier


def _run_events(tmp_path: Path, prefix: str = "") -> dict[str, list[str]]:
    """Map each run directory of the product to its `run_*` event types, in order."""
    runs = tmp_path / GATED / ".firecube" / "runs"
    return {
        d.name: [
            json.loads(line)["event_type"]
            for f in sorted(d.glob("events-*.jsonl"))
            for line in f.read_text().splitlines()
            if line
        ]
        for d in sorted(runs.glob(f"{prefix}*"))
    }


def _start(manager: ChunkManager, run_id: str) -> None:
    manager.record_run_started(
        product=GATED, run_id=run_id, output_path="", output_format="zarr", size=0, meta={}
    )


def _wrap(monkeypatch, cls, name, *, before=None, after=None) -> None:
    """Run `before(self, *a, **k)` and `after(self, *a, **k)` around a method of `cls`."""
    real = getattr(cls, name)

    def wrapper(self, *a, **k):
        if before:
            before(self, *a, **k)
        result = real(self, *a, **k)
        if after:
            after(self, *a, **k)
        return result

    monkeypatch.setattr(cls, name, wrapper)


@pytest.fixture
def pair(tmp_path):
    """Two control-plane managers over one product, as two processes would hold."""
    managers = _claiming_manager(tmp_path, GATED), _claiming_manager(tmp_path, GATED)
    yield managers
    for manager in managers:
        manager.close()


@pytest.mark.contract
@pytest.mark.parametrize("cached_listing", [False, True], ids=["fresh", "after-cached-listing"])
def test_exclusive_acquire_refuses_a_started_run_and_leaves_no_claim(
    pair, tmp_path, cached_listing
):
    acquirer, writer = pair
    with acquirer.repo.run_entries_cache_scope():
        if cached_listing:
            assert acquirer.list_runs(product=GATED) == []
        _start(writer, "live-writer")
        if cached_listing:
            assert acquirer.list_runs(product=GATED) == []  # the warmed listing is stale
        with pytest.raises(ExclusiveMaintenanceRefusedError) as refused:
            acquirer.acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    assert refused.value.runs == ("live-writer",)
    assert list((tmp_path / GATED / ".firecube" / "claims").glob("*")) == []


@pytest.mark.contract
def test_unreadable_exclusive_claim_refuses_a_writer_start_and_fails_its_run(
    pair, tmp_path, monkeypatch
):
    holder, writer = pair
    claim = holder.acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    real_open = FsspecFilesystem.open

    def failing_open(self, uri, mode="rb"):
        if mode == "r" and "/.firecube/claims/" in uri.to_str():
            raise PermissionError("injected claim read failure")
        return real_open(self, uri, mode)

    try:
        with monkeypatch.context() as patched:
            patched.setattr(FsspecFilesystem, "open", failing_open)
            with pytest.raises(ProductUnderMaintenanceError) as refused:
                _start(writer, "writer")
        assert (refused.value.owner_id, refused.value.stale) == (None, None)
        assert [c.domain for c in writer.list_claims(product=GATED)] == [_exclusive()]
    finally:
        claim.release()
    assert _run_events(tmp_path, "writer") == {"writer": ["run_started", "run_failed"]}
    assert writer.list_runs(product=GATED, non_terminal=True) == []


@pytest.mark.contract
def test_exclusive_acquire_fails_closed_on_a_run_listing_error(pair, tmp_path, monkeypatch):
    acquirer, writer = pair
    _start(writer, "live-writer")
    real_ls = FsspecFilesystem.ls

    def failing_ls(self, uri, *a, **k):
        if uri.to_str().rstrip("/").endswith("/runs"):
            raise OSError("injected listing failure")
        return real_ls(self, uri, *a, **k)

    monkeypatch.setattr(FsspecFilesystem, "ls", failing_ls)
    with pytest.raises((ExclusiveMaintenanceRefusedError, OSError)):
        acquirer.acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    assert list((tmp_path / GATED / ".firecube" / "claims").glob("*")) == []


@pytest.mark.contract
def test_exclusive_acquire_refuses_a_run_started_between_acquire_and_verify(
    pair, tmp_path, monkeypatch
):
    acquirer, writer = pair
    late: list[str] = []

    def writer_start_lands(_self, *_a, domain, **_k):
        if domain.identifier == _exclusive() and not late:
            late.append("late-writer")
            writer.repo.record_run_started(
                product=GATED, run_id="late-writer", output_path="", output_format="zarr",
                size=0, meta={},
            )  # fmt: skip

    _wrap(monkeypatch, ChunkManager, "acquire_claim", after=writer_start_lands)
    with pytest.raises(ExclusiveMaintenanceRefusedError) as refused:
        acquirer.acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    assert refused.value.runs == ("late-writer",)
    assert list((tmp_path / GATED / ".firecube" / "claims").glob("*")) == []


@pytest.mark.contract
def test_the_holders_own_run_start_passes_the_gate(pair):
    acquirer, _ = pair
    claim = acquirer.acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    try:
        _start(acquirer, MAINT)
        assert [(r.run_id, r.status) for r in acquirer.list_runs(product=GATED)] == [
            (MAINT, "started")
        ]
    finally:
        claim.release()


def _cli_args(command: str, tmp_path: Path) -> list[str]:
    root = tmp_path / GATED
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    (tmp_path / "input").mkdir(exist_ok=True)
    common = ["--target", root.as_uri(), "--product-name", GATED, "--storage-type", "local"]
    common += ["--storage-driver", "fsspec", "--write-mode", "direct"]
    if command == "ingest":
        return ["--config-file", str(config), "ingest", "cli_test_plugin", *common, "--input-data",
                (tmp_path / "input").as_uri(), "--option", "allow_empty_source=true"]  # fmt: skip
    return ["zarr", "preallocate", "direct_zarr_capable_test_plugin", *common]


@pytest.mark.contract
@pytest.mark.parametrize("command", ["ingest", "preallocate"])
def test_cli_writer_under_exclusive_maintenance_exits_1_with_one_failed_run(
    pair, tmp_path, monkeypatch, command
):
    from cli_test_plugin import CliTestIngestor
    from direct_zarr_capable_test_plugin import DirectZarrCapableTestIngestor

    plugins = {
        "cli_test_plugin": CliTestIngestor,
        "direct_zarr_capable_test_plugin": DirectZarrCapableTestIngestor,
    }
    monkeypatch.setattr(ingestor_loader, "AVAILABLE_INGESTORS", plugins)
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)
    claim = pair[0].acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    try:
        result = CliRunner().invoke(cli, _cli_args(command, tmp_path), env=_local_env(tmp_path))
    finally:
        claim.release()
    assert result.exit_code == 1, result.output
    assert f"under exclusive maintenance by {MAINT}" in result.output
    assert list(_run_events(tmp_path).values()) == [["run_started", "run_failed"]]
    assert pair[1].list_runs(product=GATED, non_terminal=True) == []


@pytest.mark.contract
def test_restore_refused_at_its_run_start_records_one_start_and_one_failure(
    pair, tmp_path, monkeypatch
):
    source, archive = tmp_path / "src.zarr", tmp_path / "a.zip"
    zarr.open_group(str(source), mode="w").create_array("a", data=np.arange(6.0))
    with zipfile.ZipFile(archive, "w") as zf:
        for p in filter(Path.is_file, source.rglob("*")):
            zf.write(p, p.relative_to(source).as_posix())
    held: list[Any] = []

    def exclusive_claim_appears(_self, *_a, run_id="", **_k):
        if run_id.startswith("archive-restore-") and not held:
            held.append(
                pair[1].acquire_claim(
                    product=GATED, domain=exclusive_maintenance_domain(GATED), owner_id=MAINT
                )
            )

    _wrap(monkeypatch, ManifestRepository, "record_run_started", before=exclusive_claim_appears)
    try:
        with pytest.raises((ProductUnderMaintenanceError, ArchiveClaimError)):
            restore_archive(
                session=make_test_session(tmp_path, product=GATED),
                archive=archive.as_uri(),
                archive_path=str(archive),
                archive_format="zip",
                target=(tmp_path / GATED).as_uri(),
            )
        assert list(_run_events(tmp_path, "archive-restore-").values()) == [
            ["run_started", "run_failed"]
        ]
        assert [c.domain for c in pair[1].list_claims(product=GATED)] == [_exclusive()]
    finally:
        if held:
            held[0].release()


@pytest.mark.contract
def test_refused_index_rebuild_leaves_no_run_record_and_releases_the_index_claim(pair, tmp_path):
    class Plugin:
        time_dim_name = "time"

        def index_spec(self, ctx):
            return IndexSpec(name="gate_spec", groups={"data": IntegerAxis(slot_count=3)})

    claim = pair[1].acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    try:
        with pytest.raises(ProductUnderMaintenanceError):
            rebuild_index_record(
                ingestor=Plugin(), plugin_name="gate-plugin", chunk_manager=pair[0],
                target=(tmp_path / GATED).as_uri(), product_name=GATED,
            )  # fmt: skip
        assert [c.domain for c in pair[1].list_claims(product=GATED)] == [_exclusive()]
    finally:
        claim.release()
    assert pair[0].list_runs(product=GATED) == []
    assert not (tmp_path / GATED / ".firecube" / "index" / "current.json").exists()


@pytest.mark.contract
@pytest.mark.parametrize("operation", ["create", "delete"])
def test_archive_create_and_delete_refuse_when_the_claim_appears_after_their_list(
    tmp_path, monkeypatch, operation
):
    holder, manager = _claiming_manager(tmp_path, "s.zarr"), _claiming_manager(tmp_path, "s.zarr")
    (tmp_path / "s.zarr" / ".firecube").mkdir(parents=True)
    (tmp_path / "s.zarr" / ".firecube" / "schema.json").write_text("{}")
    held: list[Any] = []

    def appears(_self, *_a, **_k):
        if not held:
            held.append(None)
            held[0] = holder.acquire_claim(
                product="s.zarr", domain=exclusive_maintenance_domain("s.zarr"), owner_id=MAINT
            )

    _wrap(monkeypatch, ManifestRepository, "list_claims", after=appears)
    out = tmp_path / "out.zip"
    try:
        if operation == "create":
            with pytest.raises(ArchiveClaimError):
                create_archive(
                    session=make_test_session(tmp_path, product="s.zarr"), archive=out.as_uri(),
                    archive_path=str(out), archive_format="zip",
                )  # fmt: skip
        else:
            plan = DeletionPlan([_sample_chunk("s.zarr")], 1, {"s.zarr"}, set())
            with pytest.raises(ManifestError):
                manager.execute_deletion(plan, delete_storage=True, delete_manifest=False)
        assert held and [c.domain for c in holder.list_claims(product="s.zarr")] == [
            exclusive_maintenance_domain("s.zarr").identifier
        ]
        assert not out.exists()
    finally:
        if held:
            held[0].release()
        holder.close()
        manager.close()


@pytest.mark.contract
@pytest.mark.parametrize("start", ["run_started", "run_started_with_replacement"])
def test_stale_exclusive_claim_still_refuses_writers_and_names_clear_claim(pair, tmp_path, start):
    holder, writer = pair
    _start(holder, MAINT)
    if start == "run_started_with_replacement":
        _start(writer, "writer")
    claim = holder.acquire_claim(
        product=GATED, domain=exclusive_maintenance_domain(GATED), owner_id=MAINT
    )
    try:
        (claim_file,) = (tmp_path / GATED / ".firecube" / "claims").glob("*")
        payload = json.loads(claim_file.read_text())
        claim_file.write_text(json.dumps({**payload, "last_heartbeat_at": 1.0}))
        with pytest.raises(ProductUnderMaintenanceError) as refused:
            if start == "run_started":
                _start(writer, "writer")
            else:
                writer.record_run_started_with_replacement(
                    product=GATED, run_id="writer", replaces=[]
                )
        assert (refused.value.owner_id, refused.value.stale) == (MAINT, True)
        text = render(refused.value)
        assert "firecube chunks claims clear" in text and "firecube chunks runs abandon" in text
        assert {r.run_id: r.status for r in writer.list_runs(product=GATED)}["writer"] == "failed"
    finally:
        claim.release()


def _preallocate_integer(monkeypatch, tmp_path: Path, name: str) -> list[str]:
    """Register the integer-axis fixture and a mixed bounded/unbounded subclass; return the args."""
    from index_spec_integer_test_plugin import IntegerAxisIngestor

    class MixedUnbounded(IntegerAxisIngestor):
        PRODUCT_NAME: ClassVar[str] = "mixed_unbounded_test"

        def index_spec(self, ctx):
            unbounded = RegularTimeAxis(
                coordinate="timestamp", epoch="2024-01-01T00:00:00Z", cadence_s=600, mode="exact"
            )
            return IndexSpec(
                name="mixed_unbounded",
                groups={"data": IntegerAxis(slot_count=144), "unbounded": unbounded},
            )

    plugins = {"index_spec_integer_test": IntegerAxisIngestor, "mixed_unbounded": MixedUnbounded}
    monkeypatch.setattr(ingestor_loader, "AVAILABLE_INGESTORS", plugins)
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)
    args = _cli_args("preallocate", tmp_path)
    args[2] = name
    return args


@pytest.mark.contract
@pytest.mark.parametrize("name", ["index_spec_integer_test", "mixed_unbounded"])
def test_preallocate_without_coordinate_arrays_is_refused_before_writing_under_maintenance(
    pair, tmp_path, monkeypatch, name
):
    args = _preallocate_integer(monkeypatch, tmp_path, name)
    claim = pair[0].acquire_exclusive_maintenance(product=GATED, run_id=MAINT)
    try:
        result = CliRunner().invoke(cli, args, env=_local_env(tmp_path))
    finally:
        claim.release()
    assert result.exit_code == 1, result.output
    assert f"under exclusive maintenance by {MAINT}" in result.output
    assert not (tmp_path / GATED / "data" / "values" / "zarr.json").exists()
    assert list(_run_events(tmp_path).values()) == [["run_started", "run_failed"]]


@pytest.mark.contract
def test_exclusive_acquire_is_refused_while_a_coordinate_free_preallocate_runs(
    pair, tmp_path, monkeypatch
):
    args = _preallocate_integer(monkeypatch, tmp_path, "index_spec_integer_test")
    attempts: list[object] = []

    def acquirer_arrives(_self, *_a, run_id="", **_k):
        if run_id.startswith("preallocate") and not attempts:
            try:
                attempts.append(pair[1].acquire_exclusive_maintenance(product=GATED, run_id=MAINT))
            except ExclusiveMaintenanceRefusedError as refused:
                attempts.append(refused)

    _wrap(monkeypatch, ManifestRepository, "record_run_started", after=acquirer_arrives)
    result = CliRunner().invoke(cli, args, env=_local_env(tmp_path))
    assert result.exit_code == 0, result.output
    assert [type(a) for a in attempts] == [ExclusiveMaintenanceRefusedError]


@pytest.mark.contract
@pytest.mark.parametrize("index_claim_held", [True, False], ids=["loser-branch", "no-index-claim"])
def test_cli_index_rebuild_under_exclusive_maintenance_exits_1_and_leaves_the_root_alone(
    pair, tmp_path, monkeypatch, index_claim_held
):
    from index_spec_integer_test_plugin import IntegerAxisIngestor

    monkeypatch.setattr(
        ingestor_loader, "AVAILABLE_INGESTORS", {"index_spec_integer_test": IntegerAxisIngestor}
    )
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)
    root = tmp_path / GATED
    args = ["zarr", "index", "rebuild", "--target", root.as_uri(), "--product-name", GATED]
    args += ["--plugin", "index_spec_integer_test"]
    if index_claim_held:  # the record exists and the root lacks its mirror, so a rewrite shows
        assert CliRunner().invoke(cli, args, env=_local_env(tmp_path)).exit_code == 0
        document = json.loads((root / "zarr.json").read_text())
        document["attributes"] = {}
        (root / "zarr.json").write_text(json.dumps(document))
    before = (root / "zarr.json").read_bytes() if index_claim_held else None
    claims = [pair[1].acquire_exclusive_maintenance(product=GATED, run_id=MAINT)]
    if index_claim_held:
        domain = WriteDomain(product=GATED, category="resolved_index", name="current")
        claims.append(pair[1].acquire_claim(product=GATED, domain=domain, owner_id=MAINT))
    try:
        result = CliRunner().invoke(cli, args, env=_local_env(tmp_path))
    finally:
        for claim in reversed(claims):
            claim.release()
    assert result.exit_code == 1, result.output
    assert isinstance(result.exception, SystemExit), "expected a rendered refusal, not a traceback"
    assert f"under exclusive maintenance by {MAINT}" in result.output
    after = (root / "zarr.json").read_bytes() if (root / "zarr.json").exists() else None
    assert after == before


def _holder_product(tmp_path: Path, op: str):
    root, out = tmp_path / "s.zarr", tmp_path / "out.zip"
    zarr.open_group(str(root), mode="w").create_group("F024").create_array("FWI", data=np.arange(8.0).reshape(2, 2, 2), chunks=(1, 2, 2), dimension_names=("timestamp", "lat", "lon"))  # fmt: skip
    (root / ".firecube").mkdir(exist_ok=True)
    (root / ".firecube" / "schema.json").write_text("{}")
    chunk = root / "F024/FWI/c/0/0/0"
    manager, cover = _claiming_manager(tmp_path, "s.zarr", fs=True), SpanCoverage("F024", ["F024/FWI"], [[0, 0]], True)  # fmt: skip
    manager.record_span(product="s.zarr", run_id="ing", batch_id="b", group="F024", status="active", coverage=cover, meta={})  # fmt: skip
    manager.repo._writer("s.zarr", "ing", resume_existing=True).append(EVENT_RECORD_UPSERT, {"type": "chunk", "key": "F024/FWI/c/0/0/0", "size": 1, "timestamp": 1.0, "schema_version": SCHEMA_VERSION}, meta={}, flush=True)  # fmt: skip  # legacy chunk record: the facade has no producer
    manager.record_run_terminal(product="s.zarr", run_id="ing", output_path="", output_format="zarr", size=0, meta={}, status="complete")  # fmt: skip
    records = lambda: [c.key for c in manager.list_chunks(product="s.zarr", chunk_type="chunk")]  # noqa: E731
    spans = lambda: manager.list_chunks(product="s.zarr", chunk_type="span")  # noqa: E731
    calls = {
        "create": lambda **kw: create_archive(session=make_test_session(tmp_path, product="s.zarr"), archive=out.as_uri(), archive_path=str(out), archive_format="zip", **kw),
        "delete_chunks": lambda **kw: manager.execute_deletion(DeletionPlan([_sample_chunk("s.zarr")], 1, {"s.zarr"}, set()), **kw),
        "delete_spans": lambda **kw: manager.delete_spans(spans(), **kw),
    }  # fmt: skip
    archived = lambda: out.exists() and zipfile.ZipFile(out).read("F024/FWI/c/0/0/0") == chunk.read_bytes()  # fmt: skip  # noqa: E731
    done = {"create": archived, "delete_chunks": lambda: not chunk.exists() and records() == [], "delete_spans": lambda: spans() == [] and not chunk.exists()}  # fmt: skip
    return calls[op], done[op], lambda: chunk.exists() and records() == ["F024/FWI/c/0/0/0"]


@pytest.mark.parametrize("op", ["create", "delete_chunks", "delete_spans"])
@pytest.mark.parametrize("case", ["holder", "other-holder-id", "no-claim", "writer-beside", "writer-owned-by-holder", "lost-after-acquire", "interrupt-after-acquire"])  # fmt: skip
def test_holder_run_id_admits_only_the_owner_of_a_lone_exclusive_claim(tmp_path, monkeypatch, op, case):  # fmt: skip
    call, done, intact = _holder_product(tmp_path, op)
    holder, claims = _claiming_manager(tmp_path, "s.zarr"), []
    if case != "no-claim":
        claims.append(holder.acquire_exclusive_maintenance(product="s.zarr", run_id=MAINT))
    if case in ("writer-beside", "writer-owned-by-holder"):
        claims.append(holder.acquire_claim(product="s.zarr", domain=_claim_domain("s.zarr"), owner_id="w" if case == "writer-beside" else MAINT))  # fmt: skip
    listed = lambda: sorted((c.domain, c.owner_id) for c in holder.list_claims(product="s.zarr"))  # noqa: E731
    root = tmp_path / "s.zarr"
    tree = lambda: {p: p.read_bytes() for p in root.rglob("*") if p.is_file() and "claims" not in p.parts}  # fmt: skip  # noqa: E731
    expected, before = listed(), tree()
    real, released, raised, interrupt = ManifestRepository.acquire_claim, [], None, KeyboardInterrupt("maintenance claim read")  # fmt: skip
    if case.endswith("-after-acquire"):  # once the operation holds its own claim, the exclusive one goes or its read is interrupted  # fmt: skip
        real_release, expected = type(claims[0]).release, [] if case.startswith("lost") else expected  # fmt: skip
        monkeypatch.setattr(type(claims[0]), "release", lambda s: (released.append(s.info.domain), real_release(s))[1])  # fmt: skip

        def acquire(self, **kw):
            handle = real(self, **kw)
            if kw["domain"].name != "exclusive":
                if case.startswith("lost"):
                    claims[0].release()
                else:
                    monkeypatch.setattr(ManifestRepository, "read_claim_by_domain", Mock(side_effect=interrupt))  # fmt: skip
            return handle

        monkeypatch.setattr(ManifestRepository, "acquire_claim", acquire)
    try:
        if case == "holder":
            call(holder_run_id=MAINT)
        else:
            with pytest.raises((ArchiveClaimError, ManifestError, KeyboardInterrupt), match=r"(?i)maintenance|holder|claim") as raised:  # fmt: skip
                call(holder_run_id="other" if case == "other-holder-id" else MAINT)
        held = listed()
    finally:
        for claim in claims:
            claim.release()
    lines = [json.loads(n) for f in sorted((root / ".firecube" / "runs").glob("*/events-*.jsonl")) for n in f.read_text().splitlines() if n]  # fmt: skip
    named = [e["event_type"] for e in lines if e.get("record", {}).get("meta", {}).get("holder_run_id") == MAINT]  # fmt: skip
    ok, recorded = case == "holder", [EVENT_MAINTENANCE_STARTED, EVENT_MAINTENANCE_COMPLETED]
    assert (done(), held, named) == (ok, expected, recorded if ok else [])
    assert ok or (tree() == before and intact()), "a refused call changed the product"
    assert case != "interrupt-after-acquire" or (raised is not None and raised.value is interrupt and len([d for d in released if d != claims[0].info.domain]) == 1)  # fmt: skip


@pytest.mark.contract
@pytest.mark.parametrize("owner", [None, "someone-else"], ids=["no-claim", "foreign-claim"])
def test_holder_run_id_on_a_plain_store_is_refused_and_writes_nothing(tmp_path, owner):
    root, out = tmp_path / "plain.zarr", tmp_path / "out.zip"
    zarr.open_group(str(root), mode="w").create_array("a", data=np.arange(3.0))
    tree = lambda: {p: p.read_bytes() for p in root.rglob("*") if p.is_file() and "claims" not in p.parts}  # fmt: skip  # noqa: E731
    create = lambda **kw: create_archive(session=make_test_session(tmp_path, product="plain.zarr"), archive=out.as_uri(), archive_path=str(out), archive_format="zip", **kw)  # fmt: skip  # noqa: E731
    manager, before = _claiming_manager(tmp_path, "plain.zarr"), tree()
    claim = manager.acquire_exclusive_maintenance(product="plain.zarr", run_id=owner) if owner else None  # fmt: skip
    try:
        with pytest.raises(ArchiveClaimError):
            create(holder_run_id=MAINT)
        assert not out.exists() and tree() == before
    finally:
        claim and claim.release()  # type: ignore[unused-expression]
        manager.close()
    assert create().run_id is None and out.exists()
