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

"""Concurrency tests for ``WorkspaceManager`` materialization races.

When many ingestor workers race to materialize the *same* source into a shared
workspace, the cache lock inside ``WorkspaceManager.materialize`` must serialize
the copy so that:

* every caller receives the same final cached path,
* the cached file's bytes equal the source's bytes (no torn writes, no zero-byte
  files from a partial copy that the renamer skipped), and
* failures (e.g. a missing source) surface cleanly in *every* racing caller —
  no hangs, no leaked partial files, no thread left holding the lock.

The tests use ``threading.Barrier`` to release all worker threads at the
contention point and use an explicit ``join`` timeout to turn deadlocks into
clear test failures instead of CI stalls.

A worker target is intentionally a ``SourceFile``-shaped object whose
``local_path()`` returns ``None`` — this forces ``materialize`` down the
cache-copy branch (the optimized "already local, return it" branch would
otherwise hide the race entirely).
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import IO

import pytest

from firecube.ingestor.runtime.workspace import WorkspaceManager

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_N_THREADS = 12
_JOIN_TIMEOUT_S = 30.0
_BARRIER_TIMEOUT_S = 30.0


@dataclass(slots=True)
class _Outcome:
    """Per-thread result captured by the worker."""

    worker_id: int
    materialized: Path | None = None
    error: BaseException | None = None
    unexpected_errors: list[BaseException] = field(default_factory=list)


class _RemoteLikeSourceFile:
    """A ``SourceFile`` wrapping a real on-disk file but pretending to be remote.

    ``local_path()`` returns ``None`` so ``WorkspaceManager.materialize`` cannot
    short-circuit by returning the underlying path; it must go through the
    cache-copy code path guarded by the workspace lock.
    """

    def __init__(self, real_path: Path, uri: str) -> None:
        self._real_path = real_path
        self._uri = uri
        self._open_count = 0
        self._open_lock = threading.Lock()

    @property
    def uri(self) -> str:
        return self._uri

    def open(self) -> IO[bytes]:
        with self._open_lock:
            self._open_count += 1
        return self._real_path.open("rb")

    def local_path(self) -> Path | None:
        return None

    @property
    def open_count(self) -> int:
        with self._open_lock:
            return self._open_count


class _MissingRemoteSourceFile:
    """A ``SourceFile`` whose ``open()`` always raises ``FileNotFoundError``.

    Models a remote object that does not exist (e.g. an S3 key that was deleted
    between the planner picking it up and the worker materializing it).
    """

    def __init__(self, uri: str) -> None:
        self._uri = uri

    @property
    def uri(self) -> str:
        return self._uri

    def open(self) -> IO[bytes]:
        raise FileNotFoundError(f"simulated missing source: {self._uri!r}")

    def local_path(self) -> Path | None:
        return None


def _make_workspace_manager(tmp_path: Path, prefix: str) -> WorkspaceManager:
    """Build a ``WorkspaceManager`` rooted at a fresh subdir under ``tmp_path``."""
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(exist_ok=True)
    manager = WorkspaceManager(prefix=prefix)
    ctx = SimpleNamespace(options={"workspace": str(workspace_root)})
    manager.setup(ctx)
    return manager


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _materialize_worker(
    *,
    manager: WorkspaceManager,
    source: object,
    worker_id: int,
    barrier: threading.Barrier,
    outcomes: list[_Outcome],
    outcomes_lock: threading.Lock,
    expected_error_types: tuple[type[BaseException], ...] | None = None,
) -> None:
    """Race target: wait at the barrier, then call ``materialize`` exactly once.

    ``expected_error_types`` is non-None for the missing-source variant; any
    error *not* in that tuple is recorded under ``unexpected_errors`` so the
    test fails with a precise message instead of a generic "thread raised".
    """
    outcome = _Outcome(worker_id=worker_id)
    try:
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        try:
            outcome.materialized = manager.materialize(source)
        except BaseException as exc:
            outcome.error = exc
            if expected_error_types is None or not isinstance(exc, expected_error_types):
                outcome.unexpected_errors.append(exc)
    finally:
        with outcomes_lock:
            outcomes.append(outcome)


def _spawn_race(
    *,
    manager: WorkspaceManager,
    source: object,
    n_threads: int,
    expected_error_types: tuple[type[BaseException], ...] | None = None,
) -> tuple[list[_Outcome], list[threading.Thread]]:
    """Spawn ``n_threads`` workers gated on a single ``Barrier``."""
    barrier = threading.Barrier(n_threads)
    outcomes: list[_Outcome] = []
    outcomes_lock = threading.Lock()
    threads: list[threading.Thread] = [
        threading.Thread(
            target=_materialize_worker,
            kwargs={
                "manager": manager,
                "source": source,
                "worker_id": i,
                "barrier": barrier,
                "outcomes": outcomes,
                "outcomes_lock": outcomes_lock,
                "expected_error_types": expected_error_types,
            },
            name=f"materialize-race-{i}",
        )
        for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=_JOIN_TIMEOUT_S)
    return outcomes, threads


def _assert_all_threads_joined(threads: list[threading.Thread]) -> None:
    stuck = [t.name for t in threads if t.is_alive()]
    assert not stuck, (
        f"Threads still alive after {_JOIN_TIMEOUT_S}s join timeout: {stuck!r}. "
        "This indicates a deadlock in WorkspaceManager.materialize — "
        "likely the workspace lock was not released on a failure path."
    )


def _assert_outcomes_complete(outcomes: list[_Outcome], n_threads: int) -> None:
    assert len(outcomes) == n_threads, (
        f"Expected {n_threads} thread outcomes, got {len(outcomes)}: {outcomes!r}. "
        "A worker thread crashed before appending its outcome — check thread "
        "tracebacks in the worker target."
    )


_PAYLOAD_SIZE = 256 * 1024
_REMOTE_LIKE_URI = "remote://race-test/source.bin"
_MISSING_REMOTE_URI = "remote://race-test/missing.bin"


def test_concurrent_materialize_yields_identical_cached_copies(tmp_path: Path) -> None:
    """All racing materializers return the same cached path with matching bytes."""
    source_bytes = bytes((i * 31 + 7) & 0xFF for i in range(_PAYLOAD_SIZE))
    source_path = tmp_path / "source.bin"
    source_path.write_bytes(source_bytes)
    expected_digest = _sha256_bytes(source_bytes)

    manager = _make_workspace_manager(tmp_path, prefix="race_happy")
    try:
        source = _RemoteLikeSourceFile(source_path, uri=_REMOTE_LIKE_URI)

        outcomes, threads = _spawn_race(manager=manager, source=source, n_threads=_N_THREADS)

        _assert_all_threads_joined(threads)
        _assert_outcomes_complete(outcomes, _N_THREADS)

        unexpected = [exc for outcome in outcomes for exc in outcome.unexpected_errors]
        assert not unexpected, (
            f"Unexpected errors during concurrent materialization: {unexpected!r}"
        )

        materialized_paths = [outcome.materialized for outcome in outcomes]
        assert all(p is not None for p in materialized_paths), (
            f"At least one thread returned no path despite reporting no error: {outcomes!r}"
        )

        distinct_paths = {p for p in materialized_paths if p is not None}
        assert len(distinct_paths) == 1, (
            f"Expected all workers to return the same cached path, got "
            f"{len(distinct_paths)} distinct paths: {distinct_paths!r}"
        )
        cached_path = next(iter(distinct_paths))
        assert cached_path.exists(), (
            f"Cached path {cached_path} does not exist after materialization; "
            "the rename-after-copy step did not run."
        )

        actual_digest = _sha256_path(cached_path)
        assert actual_digest == expected_digest, (
            f"Cached file digest {actual_digest} != source digest {expected_digest}; "
            "indicates a torn copy or partial flush before rename."
        )
        # Re-read per outcome to guard against any post-return mutation between
        # workers; the path is shared so digests must remain stable.
        for outcome in outcomes:
            assert outcome.materialized is not None
            digest = _sha256_path(outcome.materialized)
            assert digest == expected_digest, (
                f"Worker {outcome.worker_id} saw digest {digest}, "
                f"expected {expected_digest}. The cached entry is inconsistent "
                "between concurrent readers."
            )

        assert 1 <= source.open_count <= _N_THREADS, (
            f"source.open() called {source.open_count} times for {_N_THREADS} "
            "workers; expected between 1 and N inclusive."
        )

        cache_dir = cached_path.parent
        stray = sorted(p.name for p in cache_dir.iterdir() if p.name.startswith(".tmp."))
        assert not stray, (
            f"Found leftover partial files in cache dir {cache_dir}: {stray!r}. "
            "Materialize must clean partials on success."
        )
    finally:
        manager.teardown(cleanup_dir=True)


def test_concurrent_materialize_missing_source_raises_in_every_thread(
    tmp_path: Path,
) -> None:
    """Every racing thread surfaces ``FileNotFoundError`` for a missing source."""
    manager = _make_workspace_manager(tmp_path, prefix="race_missing")
    try:
        missing = _MissingRemoteSourceFile(uri=_MISSING_REMOTE_URI)

        outcomes, threads = _spawn_race(
            manager=manager,
            source=missing,
            n_threads=_N_THREADS,
            expected_error_types=(FileNotFoundError,),
        )

        _assert_all_threads_joined(threads)
        _assert_outcomes_complete(outcomes, _N_THREADS)

        unexpected = [exc for outcome in outcomes for exc in outcome.unexpected_errors]
        assert not unexpected, (
            "Missing-source materialization raised unexpected error types in "
            f"some threads: {unexpected!r}. Expected FileNotFoundError only."
        )

        no_error = [o.worker_id for o in outcomes if o.error is None]
        assert not no_error, (
            f"Threads {no_error!r} did not raise despite the source being missing; "
            "materialize returned a path silently."
        )
        for outcome in outcomes:
            assert isinstance(outcome.error, FileNotFoundError), (
                f"Worker {outcome.worker_id} raised "
                f"{type(outcome.error).__name__} instead of FileNotFoundError: "
                f"{outcome.error!r}"
            )

        with_path = [o.worker_id for o in outcomes if o.materialized is not None]
        assert not with_path, (
            f"Threads {with_path!r} returned a path despite the source raising "
            "FileNotFoundError; materialize must not return a partial entry."
        )

        # ``materialize`` may bail before creating ``materialized_cache`` if
        # ``open()`` raises early, so a missing directory is acceptable; if it
        # exists it must be empty (no leftover ``.tmp.*`` partials).
        assert manager.temp_root is not None
        cache_dir = manager.temp_root / "materialized_cache"
        if cache_dir.exists():
            stray = sorted(p.name for p in cache_dir.iterdir())
            assert not stray, (
                f"Found residual entries in cache dir {cache_dir} after failed "
                f"materialization: {stray!r}. Partial files must be cleaned up."
            )
    finally:
        manager.teardown(cleanup_dir=True)
