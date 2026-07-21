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

"""Concurrency tests for two ingestor instances targeting the same product.

When two ingestor runs share a product, the control-plane claim service must
serialize them: exactly one writer wins the write-domain claim, and any
contending writer fails with ``ClaimConflictError``. The WAL projection
must then show exactly the winner's run with no duplicate active spans,
regardless of which ``resume_existing``/``force_reingest`` mode each writer
intended to use.

Each thread simulates a single ingestor run by calling ``ChunkManager``
APIs directly (``acquire_claim`` -> ``record_run_started`` ->
``record_span`` -> ``record_run_terminal``), not by running a full ingest
pipeline. Barriers synchronise the threads at the contention point and after
the claim attempt so the winner cannot release before the peer has attempted
to acquire the same claim.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ClaimConflictError
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_PRODUCT = "product.zarr"
_GROUP = "F024"
_TIME_MIN = "2024-01-01T00:00:00"
_TIME_MAX = "2024-01-02T00:00:00"
_JOIN_TIMEOUT_S = 30.0
_BARRIER_TIMEOUT_S = 30.0


@dataclass(slots=True)
class _ThreadOutcome:
    owner_id: str
    mode: str
    claim_acquired: bool
    run_recorded: bool
    error: BaseException | None = None
    unexpected_errors: list[BaseException] = field(default_factory=list)


def _simulate_ingestor(
    *,
    tmp_path: Path,
    owner_id: str,
    mode: str,
    barrier: threading.Barrier,
    claim_attempt_barrier: threading.Barrier,
    outcomes: list[_ThreadOutcome],
    lock: threading.Lock,
) -> None:
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    workspace = tmp_path / f"workspace-{owner_id}"
    workspace.mkdir(exist_ok=True)
    manager = ChunkManager(binding=binding, workspace=workspace)
    domain = WriteDomain(product=_PRODUCT, category="zarr_append", name=_GROUP)
    output_path = f"{manager.base_uri.rstrip('/')}/{_PRODUCT}"
    meta = {"plugin": "test_concurrency", "group": _GROUP, "mode": mode}

    outcome = _ThreadOutcome(owner_id=owner_id, mode=mode, claim_acquired=False, run_recorded=False)

    def wait_for_peer_claim_attempt() -> bool:
        try:
            claim_attempt_barrier.wait(timeout=_BARRIER_TIMEOUT_S)
            return True
        except threading.BrokenBarrierError as exc:
            outcome.unexpected_errors.append(exc)
            outcome.error = exc
            return False

    try:
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)

        try:
            handle = manager.acquire_claim(product=_PRODUCT, domain=domain, owner_id=owner_id)
        except ClaimConflictError as exc:
            outcome.error = exc
            wait_for_peer_claim_attempt()
            return
        except BaseException as exc:
            outcome.unexpected_errors.append(exc)
            outcome.error = exc
            wait_for_peer_claim_attempt()
            return

        outcome.claim_acquired = True
        if not wait_for_peer_claim_attempt():
            return
        try:
            manager.record_run_started(
                product=_PRODUCT,
                run_id=owner_id,
                output_path=output_path,
                output_format="zarr",
                size=0,
                meta=meta,
            )
            manager.record_span(
                product=_PRODUCT,
                run_id=owner_id,
                batch_id=f"batch-{owner_id}",
                group=_GROUP,
                status="active",
                coverage=SpanCoverage(
                    group=_GROUP,
                    arrays=[f"{_GROUP}/FWI"],
                    time_index_ranges=[[0, 1]],
                    time_min=_TIME_MIN,
                    time_max=_TIME_MAX,
                ),
                meta={
                    **meta,
                    "time_min": _TIME_MIN,
                    "time_max": _TIME_MAX,
                },
            )
            manager.record_run_terminal(
                product=_PRODUCT,
                run_id=owner_id,
                output_path=output_path,
                output_format="zarr",
                size=1,
                meta=meta,
                status="complete",
            )
            outcome.run_recorded = True
        finally:
            handle.release()
    finally:
        with lock:
            outcomes.append(outcome)
        manager.close()


def _spawn_contention(
    tmp_path: Path,
    *,
    mode_a: str,
    mode_b: str,
) -> tuple[list[_ThreadOutcome], tuple[threading.Thread, threading.Thread]]:
    barrier = threading.Barrier(2)
    claim_attempt_barrier = threading.Barrier(2)
    outcomes: list[_ThreadOutcome] = []
    lock = threading.Lock()

    threads = (
        threading.Thread(
            target=_simulate_ingestor,
            kwargs={
                "tmp_path": tmp_path,
                "owner_id": "run-a",
                "mode": mode_a,
                "barrier": barrier,
                "claim_attempt_barrier": claim_attempt_barrier,
                "outcomes": outcomes,
                "lock": lock,
            },
            name=f"ingestor-a-{mode_a}",
        ),
        threading.Thread(
            target=_simulate_ingestor,
            kwargs={
                "tmp_path": tmp_path,
                "owner_id": "run-b",
                "mode": mode_b,
                "barrier": barrier,
                "claim_attempt_barrier": claim_attempt_barrier,
                "outcomes": outcomes,
                "lock": lock,
            },
            name=f"ingestor-b-{mode_b}",
        ),
    )
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=_JOIN_TIMEOUT_S)
    return outcomes, threads


def _assert_threads_joined(threads: tuple[threading.Thread, threading.Thread]) -> None:
    for t in threads:
        assert not t.is_alive(), (
            f"Thread {t.name!r} did not finish within {_JOIN_TIMEOUT_S}s; "
            "possible deadlock in claim service or WAL writer."
        )


def _assert_contention_outcome(outcomes: list[_ThreadOutcome]) -> _ThreadOutcome:
    assert len(outcomes) == 2, f"Expected 2 thread outcomes, got {len(outcomes)}: {outcomes!r}"
    unexpected = [exc for outcome in outcomes for exc in outcome.unexpected_errors]
    assert not unexpected, f"Unexpected exception types raised during contention: {unexpected!r}"

    winners = [o for o in outcomes if o.claim_acquired]
    losers = [o for o in outcomes if not o.claim_acquired]
    assert len(winners) == 1, (
        f"Expected exactly 1 claim winner; got {len(winners)} winners "
        f"and {len(losers)} losers. Outcomes: {outcomes!r}"
    )
    assert len(losers) == 1, (
        f"Expected exactly 1 claim loser; got {len(losers)}. Outcomes: {outcomes!r}"
    )

    winner = winners[0]
    loser = losers[0]
    assert winner.run_recorded, (
        f"Claim winner {winner.owner_id!r} did not record the full run lifecycle; "
        "the WAL projection will be incomplete."
    )
    assert not loser.run_recorded, (
        f"Claim loser {loser.owner_id!r} recorded run lifecycle events "
        "despite losing the claim race; this is the bug class this test guards."
    )
    assert isinstance(loser.error, ClaimConflictError), (
        f"Expected loser to raise ClaimConflictError, got "
        f"{type(loser.error).__name__}: {loser.error!r}"
    )
    assert "claim conflict" in str(loser.error).lower(), (
        f"Loser error message must identify itself as a claim conflict "
        f"(per claims contract); got: {loser.error!r}"
    )
    return winner


def _verify_wal_projection_single_run(tmp_path: Path, winner: _ThreadOutcome) -> None:
    verify_workspace = tmp_path / "workspace-verify"
    verify_workspace.mkdir(exist_ok=True)
    verifier = ChunkManager(
        binding=make_test_binding(tmp_path, product=_PRODUCT),
        workspace=verify_workspace,
    )
    try:
        runs = verifier.list_runs(product=_PRODUCT)
        assert len(runs) == 1, (
            f"WAL projection must show exactly 1 run (no duplicate run dirs), "
            f"got {len(runs)}: {[(r.run_id, r.status) for r in runs]}"
        )
        assert runs[0].run_id == winner.owner_id, (
            f"WAL projection's run_id={runs[0].run_id!r} does not match the "
            f"sole claim winner {winner.owner_id!r}"
        )
        assert runs[0].status == "complete", (
            f"Winner's run must be terminal-complete, got {runs[0].status!r}"
        )

        spans = verifier.list_chunks(product=_PRODUCT, chunk_type="span")
        active = [s for s in spans if s.status == "active"]
        assert len(active) == 1, (
            f"Expected exactly 1 active span (no duplicates), got {len(active)}: "
            f"{[(s.key, s.status, (s.meta or {}).get('run_id')) for s in active]}"
        )
        active_meta = active[0].meta or {}
        assert active_meta.get("run_id") == winner.owner_id, (
            f"Active span run_id={active_meta.get('run_id')!r} does not match "
            f"sole winner {winner.owner_id!r}"
        )

        assert verifier.list_claims(product=_PRODUCT) == [], (
            "Expected no leftover claims after winner released; found: "
            f"{verifier.list_claims(product=_PRODUCT)!r}"
        )
    finally:
        verifier.close()


def _verify_single_run_dir_on_disk(tmp_path: Path, winner: _ThreadOutcome) -> None:
    runs_dir = tmp_path / _PRODUCT / ".firecube" / "runs"
    assert runs_dir.is_dir(), f"Expected control-plane runs directory at {runs_dir}, not found"
    run_dirs = sorted(p.name for p in runs_dir.iterdir() if p.is_dir())
    assert run_dirs == [winner.owner_id], (
        f"Expected exactly one run dir on disk for winner {winner.owner_id!r}; "
        f"got {run_dirs!r} (loser must not have written run_started events)"
    )


def _run_contention_scenario(tmp_path: Path, *, mode_a: str, mode_b: str) -> None:
    outcomes, threads = _spawn_contention(tmp_path, mode_a=mode_a, mode_b=mode_b)
    _assert_threads_joined(threads)
    winner = _assert_contention_outcome(outcomes)
    _verify_wal_projection_single_run(tmp_path, winner)
    _verify_single_run_dir_on_disk(tmp_path, winner)


def test_concurrent_both_resume_same_product(tmp_path: Path) -> None:
    """Two resume-mode ingestors contend; exactly one wins the write claim."""
    _run_contention_scenario(tmp_path, mode_a="resume", mode_b="resume")


def test_concurrent_both_force_same_product(tmp_path: Path) -> None:
    """Two force-reingest ingestors contend; exactly one wins the write claim."""
    _run_contention_scenario(tmp_path, mode_a="force", mode_b="force")


def test_concurrent_mixed_resume_and_force_same_product(tmp_path: Path) -> None:
    """A resume + a force ingestor contend; the claim contract is mode-agnostic."""
    _run_contention_scenario(tmp_path, mode_a="resume", mode_b="force")
