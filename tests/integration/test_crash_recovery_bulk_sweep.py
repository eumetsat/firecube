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

"""End-to-end crash recovery: seed crashed state -> sweep -> rebuild.

This test exercises the full operator-facing recovery flow after a crash
leaves stale claims and non-terminal runs behind. Phases 2/5 assert the
compaction guard blocks rebuild; Phase 6 asserts rebuild succeeds once
non-terminal state is resolved. A companion test pins the T5 cache-wiring
bound (1 ls of runs/ per enforce()) end-to-end.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import EVENT_RUN_ABANDONED, WriteDomain
from firecube.core.errors import ManifestError
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from tests.helpers.storage import make_test_binding
from tests.unit._helpers.counting_fs import CountingFilesystem, make_counting_local_fs

pytestmark = pytest.mark.integration

# MUST end in .zarr -- matches make_test_binding default naming convention and
# the URI-form product_name the CLI resolves via resolve_cli_product().
PRODUCT = "crashtest.zarr"


# --- seed helpers (duplicated from unit tests, keep small) --------------------


def _claims_dir(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube" / "claims"


def _runs_dir(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube" / "runs"


def _write_claim(
    tmp_path: Path, *, product: str, domain: WriteDomain, last_heartbeat_at: float
) -> Path:
    d = _claims_dir(tmp_path, product)
    d.mkdir(parents=True, exist_ok=True)
    path = d / domain.claim_name
    payload = {
        "product": product,
        "domain": domain.identifier,
        "owner_id": f"owner:{domain.identifier}",
        "claim_path": StorageUri.from_local_path(path).to_str(),
        "acquired_at": last_heartbeat_at,
        "last_heartbeat_at": last_heartbeat_at,
        "heartbeat_interval_s": 30,
        "stale_threshold_s": 120,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_run(
    tmp_path: Path, *, product: str, run_id: str, status: str, updated_at: float
) -> Path:
    d = _runs_dir(tmp_path, product) / run_id
    d.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "v2",
        "product": product,
        "run_id": run_id,
        "status": status,
        "run_dir": str(d),
        "run_uri": StorageUri.from_local_path(d).to_str(),
        "output_path": str(tmp_path / product),
        "output_format": "zarr",
        "started_at": updated_at,
        "updated_at": updated_at,
        "completed_at": None,
        "events": 1,
        "parts": 1,
        "run_stale_threshold_s": 3600,
    }
    (d / "run.json").write_text(json.dumps(payload), encoding="utf-8")
    return d


def _seed_crashed_state(
    tmp_path: Path,
    *,
    product: str,
    stale_runs: int = 5,
    fresh_runs: int = 5,
    stale_claims: int = 3,
    fresh_claims: int = 2,
) -> dict[str, list[Any]]:
    now = time.time()
    stale_run_ids = [f"stale-run-{i}" for i in range(stale_runs)]
    fresh_run_ids = [f"fresh-run-{i}" for i in range(fresh_runs)]
    stale_claim_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"stale-group-{i}")
        for i in range(stale_claims)
    ]
    fresh_claim_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"fresh-group-{i}")
        for i in range(fresh_claims)
    ]
    for rid in stale_run_ids:
        _write_run(tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200)
    for rid in fresh_run_ids:
        _write_run(tmp_path, product=product, run_id=rid, status="started", updated_at=now - 60)
    for dom in stale_claim_domains:
        _write_claim(tmp_path, product=product, domain=dom, last_heartbeat_at=now - 300)
    for dom in fresh_claim_domains:
        _write_claim(tmp_path, product=product, domain=dom, last_heartbeat_at=now - 30)
    return {
        "stale_run_ids": stale_run_ids,
        "fresh_run_ids": fresh_run_ids,
        "stale_claim_domains": stale_claim_domains,
        "fresh_claim_domains": fresh_claim_domains,
    }


def _make_manager(tmp_path: Path, *, product: str = PRODUCT) -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    return ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=workspace,
    )


def _product_uri(tmp_path: Path, product: str) -> str:
    return StorageUri.from_local_path(tmp_path / product).to_str()


def _read_wal_event_types(tmp_path: Path, product: str, run_id: str) -> list[str]:
    types_seen: list[str] = []
    for segment in sorted((_runs_dir(tmp_path, product) / run_id).glob("events-*.jsonl")):
        for raw in segment.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                evt = json.loads(line).get("event_type")
            except json.JSONDecodeError:
                continue
            if isinstance(evt, str):
                types_seen.append(evt)
    return types_seen


# --- Test 1: full end-to-end recovery flow ------------------------------------


def test_crash_recovery_full_flow(tmp_path: Path) -> None:
    """Simulate a crashed ingestor, then walk operator through full recovery."""
    runner = CliRunner()
    seeded = _seed_crashed_state(tmp_path, product=PRODUCT)
    product_uri = _product_uri(tmp_path, PRODUCT)
    workspace = str(tmp_path / "workspace")

    # --- Phase 2: rebuild initially FAILS (claims block first, then runs) ---
    result = runner.invoke(
        cli,
        [
            "chunks",
            "snapshots",
            "rebuild",
            "--product-name",
            product_uri,
            "--workspace",
            workspace,
        ],
    )
    assert result.exit_code != 0, result.output
    # rebuild_cmd does NOT catch ManifestError -> check the exception, not stdout.
    assert isinstance(result.exception, ManifestError), (
        f"expected ManifestError; got {type(result.exception).__name__}: {result.exception!r}"
    )
    # Claims block BEFORE runs (see repo.py::_assert_compaction_allowed), so the
    # first message we see mentions the claims. Both branches contain the
    # "cannot rebuild snapshot" prefix.
    assert "cannot rebuild snapshot" in str(result.exception).lower()

    # --- Phase 3: claim sweep -- 3 cleared, 2 preserved ---
    result = runner.invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "--product-name",
            product_uri,
            "--workspace",
            workspace,
            "--all-stale",
            "--yes-i-really-mean-it",
        ],
    )
    assert result.exit_code == 0, result.output
    remaining_claim_files = list(_claims_dir(tmp_path, PRODUCT).iterdir())
    assert len(remaining_claim_files) == 2, [p.name for p in remaining_claim_files]
    surviving_names = {p.name for p in remaining_claim_files}
    for dom in seeded["fresh_claim_domains"]:
        assert dom.claim_name in surviving_names, (
            f"fresh claim {dom.claim_name} was incorrectly swept; "
            f"sweep-only-stale invariant violated. Surviving: {surviving_names}"
        )
    for dom in seeded["stale_claim_domains"]:
        assert dom.claim_name not in surviving_names, (
            f"stale claim {dom.claim_name} was not swept; sweep missed a stale claim."
        )

    # --- Phase 4: run sweep -- 5 abandoned, 5 preserved ---
    result = runner.invoke(
        cli,
        [
            "chunks",
            "runs",
            "abandon",
            "--product-name",
            product_uri,
            "--workspace",
            workspace,
            "--all-stale",
            "--reason",
            "test-crash",
            "--yes-i-really-mean-it",
        ],
    )
    assert result.exit_code == 0, result.output
    for rid in seeded["stale_run_ids"]:
        events = _read_wal_event_types(tmp_path, PRODUCT, rid)
        assert EVENT_RUN_ABANDONED in events, (
            f"stale run {rid} missing EVENT_RUN_ABANDONED; events={events}"
        )
        payload = json.loads(((_runs_dir(tmp_path, PRODUCT) / rid) / "run.json").read_text())
        assert payload["status"] == "abandoned", (
            f"stale run {rid} run.json status={payload['status']!r} (expected 'abandoned')"
        )
    for rid in seeded["fresh_run_ids"]:
        events = _read_wal_event_types(tmp_path, PRODUCT, rid)
        assert EVENT_RUN_ABANDONED not in events, (
            f"fresh run {rid} was incorrectly abandoned; sweep-only-stale invariant violated"
        )
        payload = json.loads(((_runs_dir(tmp_path, PRODUCT) / rid) / "run.json").read_text())
        assert payload["status"] == "started", (
            f"fresh run {rid} run.json status={payload['status']!r} (expected 'started')"
        )

    # --- Phase 5: rebuild STILL FAILS (fresh claims OR fresh runs block) ---
    result = runner.invoke(
        cli,
        [
            "chunks",
            "snapshots",
            "rebuild",
            "--product-name",
            product_uri,
            "--workspace",
            workspace,
        ],
    )
    assert result.exit_code != 0, (
        f"rebuild should still fail with fresh non-terminal state; "
        f"output={result.output!r} exc={result.exception!r}"
    )
    assert isinstance(result.exception, ManifestError), (
        f"expected ManifestError; got {type(result.exception).__name__}: {result.exception!r}"
    )

    # --- Phase 6: simulate live pods releasing claims + finishing runs;
    #             final rebuild succeeds. ---
    mgr = _make_manager(tmp_path)
    try:
        # Release fresh claims (simulating pod graceful shutdown).
        for dom in seeded["fresh_claim_domains"]:
            mgr.clear_claim(product=PRODUCT, domain_id=dom.identifier, force=True)
        # Complete fresh runs (simulating late-completion after operator recovery).
        for rid in seeded["fresh_run_ids"]:
            mgr.record_run_terminal(
                product=PRODUCT,
                run_id=rid,
                output_path=str(tmp_path / PRODUCT),
                output_format="zarr",
                size=1,
                meta={"plugin": "crash-recovery-test"},
                status="complete",
            )
    finally:
        mgr.close()

    result = runner.invoke(
        cli,
        [
            "chunks",
            "snapshots",
            "rebuild",
            "--product-name",
            product_uri,
            "--workspace",
            workspace,
        ],
    )
    assert result.exit_code == 0, (
        f"rebuild should succeed after full recovery; exit={result.exit_code} "
        f"output={result.output!r} exc={result.exception!r}"
    )
    assert "Rebuilt snapshot" in result.output


# --- Test 2: T5 op-count assertion (end-to-end enforce()) ---------------------


class _PathCountingFilesystem(CountingFilesystem):
    """CountingFilesystem variant that records which paths were listed."""

    def __init__(self, fs: Any) -> None:
        super().__init__(fs)
        self.ls_paths: list[str] = []

    def ls(self, uri: StorageUri, detail: bool = False) -> list[Any]:
        self.ls_paths.append(uri.path)
        return super().ls(uri, detail=detail)


def _make_ctx(**options: Any) -> MagicMock:
    ctx = MagicMock()
    ctx.force_reingest = bool(options.pop("force_reingest", False))
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def test_enforce_reads_runs_directory_once_after_recovery(tmp_path: Path) -> None:
    """After crash-recovery, a subsequent ingest's ResumeGuard.enforce() must
    read `.firecube/runs/` at most ONCE per invocation (T5 cache scope).

    This is an integration-level regression check that complements the unit
    test in tests/unit/test_resume_guard_enforce_op_counts.py -- it verifies
    the cache-scope wiring holds after the runs/ directory has been
    exercised by the full crash-recovery flow (sweeps, WAL rewrites, etc.).
    """
    product = "opcount.zarr"
    _counting_fs, real_fs = make_counting_local_fs(tmp_path)
    counting_fs = _PathCountingFilesystem(real_fs)
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=workspace,
        filesystem=counting_fs,
    )
    try:
        # Seed 100 terminal runs to make the runs/ listing non-trivial.
        for i in range(100):
            run_id = f"run-{i:03d}"
            meta = {"plugin": "op-count-integration"}
            manager.record_run_started(
                product=product,
                run_id=run_id,
                output_path=f"file:///tmp/{run_id}",
                output_format="zarr",
                size=1,
                meta=meta,
            )
            manager.record_run_terminal(
                product=product,
                run_id=run_id,
                output_path=f"file:///tmp/{run_id}",
                output_format="zarr",
                size=1,
                meta=meta,
                status="complete",
            )

        counting_fs.reset()
        counting_fs.ls_paths.clear()

        guard = ResumeGuard(
            plugin_name="op-count-integration",
            chunk_manager=manager,
            log=logging.getLogger(__name__),
            slice_meta_keys=(),
        )
        guard.enforce(ctx=_make_ctx(), product=product)

        runs_ls_count = sum(1 for path in counting_fs.ls_paths if path.endswith("/.firecube/runs"))
        assert runs_ls_count == 1, (
            f"ResumeGuard.enforce() must list .firecube/runs exactly once "
            f"(cache-scope wiring); observed {runs_ls_count} ls calls. "
            f"All ls paths: {counting_fs.ls_paths}"
        )
    finally:
        manager.close()
