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

"""Concurrent ``firecube zarr preallocate`` runs must never materialize concurrently.

``ChunkManager.claim_coord_materialization_window`` performs the
check-and-register atomically under a single write-domain claim that is
held through materialization: a process that overlaps a live peer is
refused (claim conflict or coordinate-chunk overlap), and every successful
materialization records its window as a run with a ``slot_range``.

Exactly-one-winner is a timing outcome, not the invariant: when OS
scheduling fully serializes the two processes, the second run is a
legitimate idempotent rerun and also succeeds. The invariant these tests
pin is (a) every success registered its window, and (b) no two
materialization runs' claim-guarded ``[started_at, completed_at]``
intervals overlap.

Real OS processes only. A ``multiprocessing.Manager().Barrier(2)``
synchronises the two workers so they enter ``preallocate`` at the same
instant. No ``time.sleep`` is used for synchronisation. Each subprocess is
capped at 30s so a hung worker fails the test loudly instead of the whole
run.
"""

from __future__ import annotations

import itertools
import multiprocessing as mp
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

# Hard test dependency: the subprocesses invoke this fixture plugin, so its
# absence must fail collection loudly instead of skipping.
import regular_axis_test_plugin  # noqa: F401

from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.gate, pytest.mark.concurrency]

# ``regular_axis_dense_coord`` declares an exact RegularTimeAxis with
# slot_count=1000 and a coord ``ZarrArraySpec`` with ``chunks=None``.
# ``resolve_coord_chunks`` picks chunk_size=256, so the first coord chunk
# covers slots ``[0, 256)``. Windows A and B both touch chunk 0.
_PLUGIN = "regular_axis_dense_coord"
_PRODUCT = "regular_axis_dense_coord"
_GROUP = "data"
_WINDOW_A: tuple[int, int] = (0, 256)
_WINDOW_B: tuple[int, int] = (128, 384)

_SUBPROCESS_TIMEOUT_S = 30.0
_BARRIER_TIMEOUT_S = 20.0

_worker_barrier: Any = None


def _init_worker(barrier: Any) -> None:
    global _worker_barrier
    _worker_barrier = barrier


def _run_preallocate_subprocess(
    payload: tuple[str, int, int],
) -> tuple[int, str, str, int, int]:
    """Barrier-synchronise then invoke ``firecube zarr preallocate`` once.

    Returns ``(returncode, stdout, stderr, slot_start, slot_end)``.
    ``returncode == -1`` means the subprocess exceeded
    ``_SUBPROCESS_TIMEOUT_S``; ``returncode == -2`` means the barrier
    itself failed (peer never reached it in time).
    """
    target_path, slot_start, slot_end = payload
    if _worker_barrier is not None:
        try:
            _worker_barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        except Exception as exc:
            return (-2, "", f"barrier failed: {exc!r}", slot_start, slot_end)

    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix

    cmd = [
        sys.executable,
        "-m",
        "firecube.cli.main",
        "zarr",
        "preallocate",
        _PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--slot-start",
        str(slot_start),
        "--slot-end",
        str(slot_end),
        "--option",
        "no_progress=true",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_S,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or b""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        return (
            -1,
            stdout,
            f"subprocess timeout after {_SUBPROCESS_TIMEOUT_S}s",
            slot_start,
            slot_end,
        )
    return (proc.returncode, proc.stdout, proc.stderr, slot_start, slot_end)


def _run_preallocate_subprocess_with_slot_group_env(
    payload: tuple[str, int, int, str],
) -> tuple[int, str, str, int, int, tuple[str, ...]]:
    """Barrier-synchronise, invoke preallocate, and observe live claim domains."""
    target_path, slot_start, slot_end, slot_group = payload
    if _worker_barrier is not None:
        try:
            _worker_barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        except Exception as exc:
            return (-2, "", f"barrier failed: {exc!r}", slot_start, slot_end, ())

    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix
    env["FIRECUBE_SLOT_GROUP"] = slot_group

    cmd = [
        sys.executable,
        "-m",
        "firecube.cli.main",
        "zarr",
        "preallocate",
        _PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--slot-start",
        str(slot_start),
        "--slot-end",
        str(slot_end),
        "--option",
        "no_progress=true",
    ]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
    except Exception as exc:
        return (-3, "", f"subprocess spawn failed: {exc!r}", slot_start, slot_end, ())

    observed_domains: set[str] = set()
    binding = make_test_binding(Path(target_path).parent, product=_PRODUCT)
    wal = ChunkManager(binding=binding, workspace=Path(target_path).parent)
    try:
        deadline = time.monotonic() + _SUBPROCESS_TIMEOUT_S
        while proc.poll() is None:
            for claim in wal.list_claims(product=_PRODUCT):
                if ":coord_materialization:" in claim.domain:
                    observed_domains.add(claim.domain)
            if time.monotonic() >= deadline:
                proc.kill()
                stdout, _stderr = proc.communicate()
                return (
                    -1,
                    stdout,
                    f"subprocess timeout after {_SUBPROCESS_TIMEOUT_S}s",
                    slot_start,
                    slot_end,
                    tuple(sorted(observed_domains)),
                )
            time.sleep(0.01)
        stdout, stderr = proc.communicate(timeout=1.0)
    finally:
        wal.close()
    return (
        proc.returncode or 0,
        stdout,
        stderr,
        slot_start,
        slot_end,
        tuple(sorted(observed_domains)),
    )


def _run_default_preallocate_subprocess(
    payload: tuple[str, int, int],
) -> tuple[int, str, str, int, int]:
    """Barrier-synchronise then invoke preallocate WITHOUT slot flags.

    The default full-extent invocation is the production driver's shape;
    it must participate in the materialization claim exactly like a
    windowed one. Payload slots are ignored (kept for result symmetry).
    """
    target_path, slot_start, slot_end = payload
    if _worker_barrier is not None:
        try:
            _worker_barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        except Exception as exc:
            return (-2, "", f"barrier failed: {exc!r}", slot_start, slot_end)

    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix
    cmd = [
        sys.executable,
        "-m",
        "firecube.cli.main",
        "zarr",
        "preallocate",
        _PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_S,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return (-1, "", f"subprocess timeout after {_SUBPROCESS_TIMEOUT_S}s", slot_start, slot_end)
    return (proc.returncode, proc.stdout, proc.stderr, slot_start, slot_end)


def test_default_invocation_concurrent_preallocates_never_both_materialize(
    tmp_path: Path,
) -> None:
    """Two flag-less (full-extent) preallocates must never materialize concurrently.

    Identical full-extent windows overlap on every coordinate chunk. The
    default invocation must participate in the materialization claim like a
    windowed one: every success registers its window as a run, and two
    successes are legal only when their claim-guarded intervals are
    disjoint (a serialized idempotent rerun). An unregistered success or
    overlapping intervals mean the default invocation bypassed the guard —
    the exact gap this test pins.
    """
    target = tmp_path / _PRODUCT
    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    try:
        barrier = manager.Barrier(2)
        payloads = [(str(target), 0, 0), (str(target), 0, 0)]
        with ctx.Pool(processes=2, initializer=_init_worker, initargs=(barrier,)) as pool:
            results = pool.map(_run_default_preallocate_subprocess, payloads, chunksize=1)
    finally:
        manager.shutdown()

    return_codes = [r[0] for r in results]
    stderrs = [r[2] for r in results]
    detail = f"return_codes={return_codes}\nstderr A:\n{stderrs[0]}\nstderr B:\n{stderrs[1]}"

    assert -1 not in return_codes, f"subprocess timed out:\n{detail}"
    assert -2 not in return_codes, f"barrier failed:\n{detail}"
    success_count = sum(1 for rc in return_codes if rc == 0)
    assert success_count >= 1, f"no preallocate succeeded at all:\n{detail}"
    runs = _materialization_runs(tmp_path)
    assert len(runs) == success_count, (
        "the default full-extent invocation must register its materialization "
        f"window under the claim; got {len(runs)} runs for {success_count} "
        f"success(es).\n{detail}"
    )
    _assert_no_concurrent_materialization(runs, detail)
    if success_count == 1:
        loser_stderr = "".join(
            err for rc, err in zip(return_codes, stderrs, strict=True) if rc != 0
        )
        assert "claim" in loser_stderr.lower() or "coordinate chunk" in loser_stderr, (
            "the refused process must name the claim or coordinate-chunk conflict:\n" + detail
        )


def _materialization_runs(tmp_path: Path) -> list[Any]:
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    wal = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        runs = wal.list_runs(product=_PRODUCT)
    finally:
        wal.close()
    return [r for r in runs if r.slot_range is not None]


def _assert_no_concurrent_materialization(runs: list[Any], detail: str) -> None:
    """Two successes are legal only when their claim-guarded intervals are disjoint.

    ``started_at`` is recorded while the materialization claim is held and
    the terminal state is recorded before the claim is released, so any
    overlap between two runs' ``[started_at, completed_at]`` intervals
    proves two processes materialized concurrently — the race the claim
    exists to prevent.
    """
    intervals = []
    for run in runs:
        assert run.started_at is not None, f"run {run.run_id} has no started_at\n{detail}"
        assert run.completed_at is not None, (
            f"run {run.run_id} is non-terminal after preallocate returned\n{detail}"
        )
        intervals.append((float(run.started_at), float(run.completed_at), run.run_id))
    intervals.sort()
    for (_, prev_end, prev_id), (next_start, _, next_id) in itertools.pairwise(intervals):
        assert next_start >= prev_end, (
            f"runs {prev_id} and {next_id} materialized concurrently: intervals overlap\n{detail}"
        )


def _race_two_preallocates(
    target: Path,
) -> list[tuple[int, str, str, int, int]]:
    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    try:
        barrier = manager.Barrier(2)
        payloads = [
            (str(target), _WINDOW_A[0], _WINDOW_A[1]),
            (str(target), _WINDOW_B[0], _WINDOW_B[1]),
        ]
        with ctx.Pool(
            processes=2,
            initializer=_init_worker,
            initargs=(barrier,),
        ) as pool:
            results = pool.map(_run_preallocate_subprocess, payloads, chunksize=1)
    finally:
        manager.shutdown()
    return results


def test_two_concurrent_preallocates_on_overlapping_coord_chunk_must_not_both_succeed(
    tmp_path: Path,
) -> None:
    """Overlapping preallocate windows never materialize chunk 0 concurrently.

    Two subprocesses target the same Zarr store with windows that share
    coordinate chunk 0 (chunk_size=256 for the ``regular_axis_dense_coord``
    fixture):

    * window A: ``[0, 256)``   -> chunk 0 only
    * window B: ``[128, 384)`` -> chunks 0 and 1

    ``ChunkManager.claim_coord_materialization_window`` walks non-terminal
    runs and skips any whose ``slot_range is None``. Because preallocate
    never records a ``slot_range`` today, both workers finish clean and
    both prefill the coord. With the atomic
    ``claim_coord_materialization_window`` the winning worker records its
    ``slot_range`` before touching chunk 0, so the second worker's
    check-and-claim rejects it.
    """
    target = tmp_path / _PRODUCT

    results = _race_two_preallocates(target)

    return_codes = [r[0] for r in results]
    stdouts = [r[1] for r in results]
    stderrs = [r[2] for r in results]

    detail = (
        f"return_codes={return_codes}\n"
        f"--- stdout[A window={_WINDOW_A}] ---\n{stdouts[0]}\n"
        f"--- stderr[A window={_WINDOW_A}] ---\n{stderrs[0]}\n"
        f"--- stdout[B window={_WINDOW_B}] ---\n{stdouts[1]}\n"
        f"--- stderr[B window={_WINDOW_B}] ---\n{stderrs[1]}"
    )

    assert -1 not in return_codes, f"subprocess timed out; hung worker:\n{detail}"
    assert -2 not in return_codes, f"barrier failed to synchronise workers:\n{detail}"

    success_count = sum(1 for rc in return_codes if rc == 0)
    assert success_count >= 1, f"no preallocate succeeded at all:\n{detail}"

    runs = _materialization_runs(tmp_path)
    assert len(runs) >= success_count, (
        f"{len(runs)} materialization runs were recorded for "
        f"{success_count} successful preallocate(s); failed preallocates may "
        "still stamp a run (the shell is marker-managed at creation), but successful materializations must "
        "still register their window under the claim.\n"
        f"{detail}"
    )
    # Exactly-one-winner is a timing outcome, not an invariant: a fully
    # serialized rerun is legitimately idempotent. The invariant is that no
    # two materializations ever ran concurrently.
    _assert_no_concurrent_materialization(runs, detail)
    if success_count == 1:
        loser_stderr = "".join(
            err for rc, err in zip(return_codes, stderrs, strict=True) if rc != 0
        )
        assert "claim" in loser_stderr.lower() or "coordinate chunk" in loser_stderr, (
            f"the refused process must name the claim or chunk conflict:\n{detail}"
        )


@pytest.mark.integration
@pytest.mark.gate
def test_two_concurrent_preallocates_with_slot_group_env_share_a_single_claim_and_never_overlap(
    tmp_path: Path,
) -> None:
    """Concurrent slot_group-scoped preallocates never fragment the claim domain.

    Both workers pass ``FIRECUBE_SLOT_GROUP=data`` — the only real group
    on the ``regular_axis_dense_coord`` fixture — with overlapping windows
    on chunk 0. The invariant is that the claim domain stays global
    (``{product}:coord_materialization:all``) regardless of the
    slot_group value, so a co-scheduled peer racing the same coord chunk
    is refused by the single-writer claim rather than sneaking in under
    a per-group-fragmented claim.

    Historically this test used arbitrary ``"alpha"``/``"beta"`` strings
    to exercise the "any slot_group value" case. Those strings are no
    longer accepted at the CLI boundary (preallocate window scoping rejects group names not
    declared by the plugin's IndexSpec); the claim-collapse invariant is
    preserved by re-pointing the payload at the plugin's real group.
    """
    target = tmp_path / _PRODUCT
    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    try:
        barrier = manager.Barrier(2)
        payloads = [
            (str(target), _WINDOW_A[0], _WINDOW_A[1], _GROUP),
            (str(target), _WINDOW_A[0], _WINDOW_A[1], _GROUP),
        ]
        with ctx.Pool(processes=2, initializer=_init_worker, initargs=(barrier,)) as pool:
            results = pool.map(
                _run_preallocate_subprocess_with_slot_group_env,
                payloads,
                chunksize=1,
            )
    finally:
        manager.shutdown()

    return_codes = [r[0] for r in results]
    stdouts = [r[1] for r in results]
    stderrs = [r[2] for r in results]
    observed_domains = sorted({domain for result in results for domain in result[5]})
    detail = (
        f"return_codes={return_codes}\n"
        f"observed_domains={observed_domains}\n"
        f"--- stdout[A slot_group={_GROUP}] ---\n{stdouts[0]}\n"
        f"--- stderr[A slot_group={_GROUP}] ---\n{stderrs[0]}\n"
        f"--- stdout[B slot_group={_GROUP}] ---\n{stdouts[1]}\n"
        f"--- stderr[B slot_group={_GROUP}] ---\n{stderrs[1]}"
    )

    assert -1 not in return_codes, f"subprocess timed out; hung worker:\n{detail}"
    assert -2 not in return_codes, f"barrier failed to synchronise workers:\n{detail}"
    success_count = sum(1 for rc in return_codes if rc == 0)
    assert success_count >= 1, f"no preallocate succeeded at all:\n{detail}"

    runs = _materialization_runs(tmp_path)
    assert len(runs) >= success_count, (
        f"{len(runs)} materialization runs were recorded for "
        f"{success_count} successful preallocate(s); failed preallocates may "
        "still stamp a run (the shell is marker-managed at creation), but successful materializations must "
        "still register their window under the claim.\n"
        f"{detail}"
    )
    _assert_no_concurrent_materialization(runs, detail)

    for return_code, stderr in zip(return_codes, stderrs, strict=True):
        if return_code == 0:
            continue
        message = stderr.lower()
        assert "coord_materialization" in message or "coordinate chunk" in message, (
            "the refused process must name the materialization claim or "
            f"coordinate-chunk conflict:\n{detail}"
        )

    assert observed_domains in ([], [f"{_PRODUCT}:coord_materialization:all"]), (
        f"slot-group env values must collapse to a single global claim domain:\n{detail}"
    )
