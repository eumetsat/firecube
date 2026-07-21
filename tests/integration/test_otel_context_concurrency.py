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

"""Concurrency tests for OTel context propagation under error and retry.

Multiple worker threads each process a sequence of tasks. Each task captures
its own ``run`` parent context, attaches it on the worker thread, and runs
through an attempt-1 (which may raise) followed by a retry attempt-2 on the
same captured context. After each task, a *marker* span is recorded with no
context attached.

The three properties verified are exactly the ones called out by §14 point 4
of ``plans/TODO.md`` (concurrency and race-condition coverage):

1. **Sibling isolation** — each task's work spans parent to ITS captured run
   context, never a sibling task's run. Cross-thread context bleed would be
   visible as a wrong ``parent.span_id`` in the exporter output.
2. **Clean detach after error** — after attempt-1 raises and the facade
   detaches the captured context, the marker span recorded between the
   detach and the next task MUST be a root span (no parent). A buggy
   detach would leave the captured context active and make the marker
   inherit it.
3. **Retry trace continuity** — attempt-2 (retry) MUST share both
   ``trace_id`` and ``parent.span_id`` with attempt-1, because both belong
   to the same logical run lineage anchored at the captured ``run`` span.

The test uses ONLY the tracing facade
(:mod:`firecube.core.observability.tracing`). Raw ``opentelemetry`` imports
are limited to the test fixture pattern already established in
``tests/unit/test_tracing_facade.py`` — exporter wiring is not part of the
production facade and is necessarily set up by the test directly.

Synchronisation uses :class:`threading.Barrier` to release all worker
threads simultaneously and the test runs deterministically. The architectural lint in
``tests/unit/test_observability_boundaries.py`` only inspects
``src/firecube/`` and is unaffected by this integration test file.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from firecube.core.observability.tracing import (
    attach_context,
    capture_context,
    detach_context,
    propagated_context,
    span,
)

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_N_WORKERS = 2
_TASKS_PER_WORKER = 2
_TOTAL_TASKS = _N_WORKERS * _TASKS_PER_WORKER
_BARRIER_TIMEOUT_S = 30.0
_JOIN_TIMEOUT_S = 30.0


@pytest.fixture
def exporter() -> Iterator[InMemorySpanExporter]:
    """Per-test ``TracerProvider`` + ``InMemorySpanExporter``.

    Uses ``SimpleSpanProcessor`` so worker-thread spans are flushed
    synchronously as each span ends before reading ``get_finished_spans()``
    on the main thread.

    Writes ``trace._TRACER_PROVIDER`` directly because OTel's
    ``trace.set_tracer_provider`` is set-once and would silently no-op on
    every test after the first one in the process. Restores the previous
    provider on teardown.
    """
    in_memory = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(in_memory))

    previous = trace._TRACER_PROVIDER  # type: ignore[attr-defined]
    trace._TRACER_PROVIDER = provider  # type: ignore[attr-defined]
    try:
        yield in_memory
    finally:
        with contextlib.suppress(Exception):
            provider.shutdown()
        trace._TRACER_PROVIDER = previous  # type: ignore[attr-defined]


class _SimulatedFailure(RuntimeError):
    """Deterministic failure injected into attempt-1 to drive the retry path."""


@dataclass(slots=True)
class _TaskSpec:
    task_id: int
    worker_id: int
    should_fail_first: bool


@dataclass(slots=True)
class _TaskCapture:
    spec: _TaskSpec
    captured_ctx: Any
    run_trace_id: int
    run_span_id: int


@dataclass(slots=True)
class _WorkerOutcome:
    worker_id: int
    tasks_completed: int = 0
    attempts: dict[int, int] = field(default_factory=dict)
    errors: list[BaseException] = field(default_factory=list)


def _parse_task_id(span_name: str) -> int:
    """Extract the trailing integer task id from a span name.

    Span names used by this test all end with ``-<task_id>``, e.g.
    ``work.attempt-1.task-3``, ``run.task-3``, ``marker.task-3``.
    """
    return int(span_name.rsplit("-", 1)[-1])


def _build_task_captures() -> tuple[list[list[_TaskCapture]], dict[int, _TaskCapture]]:
    """Build per-worker task lists with unique captured run contexts.

    Each task opens its own ``run.task-<id>`` span on the main thread and
    captures the resulting context. The span is ended before the captured
    context is handed to a worker thread — workers reattach it to start
    child spans, which is the standard cross-thread propagation pattern.

    The first task on each worker (``slot == 0``) is marked
    ``should_fail_first`` so the worker's loop exercises attempt-1 →
    detach → attempt-2 → detach. The second task uses ``should_fail_first
    = False`` so attempt-1 succeeds; if the previous task leaked its
    context, the leak shows up on the second task's marker span (see
    ``test_context_detaches_cleanly_after_attempt_one_error``).
    """
    per_worker: list[list[_TaskCapture]] = []
    by_id: dict[int, _TaskCapture] = {}

    task_id = 0
    for worker_id in range(_N_WORKERS):
        worker_tasks: list[_TaskCapture] = []
        for slot in range(_TASKS_PER_WORKER):
            spec = _TaskSpec(
                task_id=task_id,
                worker_id=worker_id,
                should_fail_first=(slot == 0),
            )
            with span(f"run.task-{task_id}") as run_span:
                ctx_obj = capture_context()
                sc = run_span.get_span_context()
                cap = _TaskCapture(
                    spec=spec,
                    captured_ctx=ctx_obj,
                    run_trace_id=sc.trace_id,
                    run_span_id=sc.span_id,
                )
            worker_tasks.append(cap)
            by_id[task_id] = cap
            task_id += 1
        per_worker.append(worker_tasks)

    return per_worker, by_id


def _worker_loop(
    *,
    worker_id: int,
    tasks: list[_TaskCapture],
    barrier: threading.Barrier,
    outcomes: list[_WorkerOutcome],
    lock: threading.Lock,
) -> None:
    """Process the assigned tasks sequentially on this worker thread.

    Attempt-1 uses explicit ``attach_context`` / ``detach_context`` so the
    error-path detach is unmistakeable in the source. Attempt-2 (retry)
    uses the ``propagated_context`` context manager. Both paths must produce
    spans that parent to the same captured run.
    """
    outcome = _WorkerOutcome(worker_id=worker_id)
    try:
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        for task in tasks:
            outcome.attempts[task.spec.task_id] = 0

            # Attempt 1: explicit attach + try/finally detach. The finally
            # must run even when the body raises — that is the property
            # the marker span (below) is designed to detect.
            token = attach_context(task.captured_ctx)
            try:
                with span(f"work.attempt-1.task-{task.spec.task_id}"):
                    outcome.attempts[task.spec.task_id] += 1
                    if task.spec.should_fail_first:
                        raise _SimulatedFailure(f"task-{task.spec.task_id} attempt-1")
            except _SimulatedFailure:
                pass
            finally:
                detach_context(token)

            if task.spec.should_fail_first:
                # Attempt 2 (retry) on the same captured context. Uses the
                # ``propagated_context`` context manager so attach/detach
                # are paired by Python's normal context-manager protocol.
                # Trace continuity is asserted in
                # ``test_retry_spans_share_trace_with_original_attempt``.
                with (
                    propagated_context(task.captured_ctx),
                    span(f"work.attempt-2.task-{task.spec.task_id}"),
                ):
                    outcome.attempts[task.spec.task_id] += 1

            # Marker span recorded with NO context attached. If any of the
            # detaches above leaked, the marker inherits the leaked
            # context and ``marker.parent`` becomes the leaker's run.
            with span(f"marker.task-{task.spec.task_id}"):
                pass

            outcome.tasks_completed += 1
    except BaseException as exc:  # pragma: no cover (defensive)
        outcome.errors.append(exc)
    finally:
        with lock:
            outcomes.append(outcome)


def _run_concurrent_workers(
    exporter: InMemorySpanExporter,
) -> tuple[list[_WorkerOutcome], dict[int, _TaskCapture]]:
    """Spawn the worker threads, synchronise them with a barrier, and join.

    Returns ``(outcomes, by_id)`` so tests can assert against both the
    per-worker bookkeeping and the captured-context lookup table.
    """
    per_worker, by_id = _build_task_captures()
    barrier = threading.Barrier(_N_WORKERS)
    outcomes: list[_WorkerOutcome] = []
    lock = threading.Lock()
    threads = [
        threading.Thread(
            target=_worker_loop,
            kwargs={
                "worker_id": worker_id,
                "tasks": per_worker[worker_id],
                "barrier": barrier,
                "outcomes": outcomes,
                "lock": lock,
            },
            name=f"otel-worker-{worker_id}",
        )
        for worker_id in range(_N_WORKERS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=_JOIN_TIMEOUT_S)
    for t in threads:
        assert not t.is_alive(), (
            f"Worker thread {t.name!r} did not finish within {_JOIN_TIMEOUT_S}s; "
            "possible deadlock in the tracing facade or worker loop."
        )
    return outcomes, by_id


def _assert_all_workers_completed(outcomes: list[_WorkerOutcome]) -> None:
    """Sanity gate so failures in the worker loop surface before span asserts."""
    assert len(outcomes) == _N_WORKERS, (
        f"Expected {_N_WORKERS} worker outcomes, got {len(outcomes)}: {outcomes!r}"
    )
    for o in outcomes:
        assert not o.errors, f"worker-{o.worker_id} raised unexpected errors: {o.errors!r}"
        assert o.tasks_completed == _TASKS_PER_WORKER, (
            f"worker-{o.worker_id} completed {o.tasks_completed} tasks, "
            f"expected {_TASKS_PER_WORKER}"
        )


def _expected_work_span_count(by_id: dict[int, _TaskCapture]) -> int:
    """Failing tasks produce 2 work spans (attempt-1, attempt-2);
    succeeding tasks produce 1 (attempt-1 only)."""
    failing = sum(1 for cap in by_id.values() if cap.spec.should_fail_first)
    succeeding = len(by_id) - failing
    return failing * 2 + succeeding


def test_each_task_spans_parent_to_its_captured_run_context(
    exporter: InMemorySpanExporter,
) -> None:
    """Cross-task isolation: every work span must parent to its task's own
    captured ``run`` context. Cross-thread context bleed would surface as a
    wrong ``parent.span_id`` (a sibling task's run).
    """
    outcomes, by_id = _run_concurrent_workers(exporter)
    _assert_all_workers_completed(outcomes)

    finished = exporter.get_finished_spans()
    work_spans = [s for s in finished if s.name.startswith("work.")]
    assert len(work_spans) == _expected_work_span_count(by_id), (
        f"Unexpected work-span count: got {len(work_spans)}, "
        f"expected {_expected_work_span_count(by_id)}; "
        f"names={[s.name for s in work_spans]}"
    )

    for s in work_spans:
        task_id = _parse_task_id(s.name)
        cap = by_id[task_id]
        assert s.context is not None, f"work span {s.name!r} has no SpanContext"
        assert s.parent is not None, (
            f"work span {s.name!r} has no parent — captured context was not "
            f"attached on the worker thread"
        )
        assert s.context.trace_id == cap.run_trace_id, (
            f"work span {s.name!r} trace_id mismatch: "
            f"expected {cap.run_trace_id:x}, got {s.context.trace_id:x} "
            f"(context bled from another task)"
        )
        assert s.parent.span_id == cap.run_span_id, (
            f"work span {s.name!r} parent.span_id mismatch: "
            f"expected {cap.run_span_id:x}, got {s.parent.span_id:x} "
            f"(context bled from another task)"
        )


def test_context_detaches_cleanly_after_attempt_one_error(
    exporter: InMemorySpanExporter,
) -> None:
    """After attempt-1 raises and the facade detaches the captured context,
    the marker span recorded with NO context attached MUST be a root span.

    A buggy detach would leave the captured context active on the worker's
    contextvar, and the marker created right after would inherit it as
    parent — visible here as ``marker.parent is not None``.
    """
    outcomes, by_id = _run_concurrent_workers(exporter)
    _assert_all_workers_completed(outcomes)

    finished = exporter.get_finished_spans()
    markers = [s for s in finished if s.name.startswith("marker.")]
    assert len(markers) == _TOTAL_TASKS, (
        f"Expected one marker per task ({_TOTAL_TASKS}); got {len(markers)}: "
        f"{[s.name for s in markers]}"
    )

    leaked_marker_parents = [
        (m.name, m.parent.span_id if m.parent is not None else None)
        for m in markers
        if m.parent is not None
    ]
    assert not leaked_marker_parents, (
        f"Markers recorded with NO context attached must be root spans, "
        f"but the following inherited a parent — proving a captured context "
        f"leaked past its detach: {leaked_marker_parents!r}"
    )

    # Independently: every marker must carry a fresh trace_id that is NOT
    # equal to any task's captured run trace_id. If a context leaked, the
    # marker would share its leaker's trace_id.
    captured_run_trace_ids = {cap.run_trace_id for cap in by_id.values()}
    for m in markers:
        assert m.context is not None
        assert m.context.trace_id not in captured_run_trace_ids, (
            f"Marker {m.name!r} trace_id {m.context.trace_id:x} matches a "
            f"captured run trace_id — context leaked across detach boundary"
        )


def test_retry_spans_share_trace_with_original_attempt(
    exporter: InMemorySpanExporter,
) -> None:
    """Attempt-2 (retry) MUST share its ``trace_id`` and ``parent.span_id``
    with attempt-1 because both belong to the same logical run lineage,
    anchored at the captured ``run`` span.
    """
    outcomes, by_id = _run_concurrent_workers(exporter)
    _assert_all_workers_completed(outcomes)

    finished = exporter.get_finished_spans()
    failing_tasks = [cap for cap in by_id.values() if cap.spec.should_fail_first]
    assert failing_tasks, (
        "Test setup invariant: at least one task must be configured to fail "
        "so the retry path is exercised."
    )

    for cap in failing_tasks:
        attempts = [
            s
            for s in finished
            if s.name.startswith("work.") and _parse_task_id(s.name) == cap.spec.task_id
        ]
        assert len(attempts) == 2, (
            f"task-{cap.spec.task_id} (failing) should have 2 attempt spans "
            f"(attempt-1 + retry), got {len(attempts)}: "
            f"{[s.name for s in attempts]}"
        )
        a1 = next(s for s in attempts if "attempt-1" in s.name)
        a2 = next(s for s in attempts if "attempt-2" in s.name)
        assert a1.context is not None and a2.context is not None
        assert a1.parent is not None and a2.parent is not None

        assert a1.context.trace_id == a2.context.trace_id == cap.run_trace_id, (
            f"task-{cap.spec.task_id} retry trace_id mismatch: "
            f"attempt-1.trace_id={a1.context.trace_id:x}, "
            f"attempt-2.trace_id={a2.context.trace_id:x}, "
            f"expected captured-run trace_id={cap.run_trace_id:x}"
        )
        assert a1.parent.span_id == a2.parent.span_id == cap.run_span_id, (
            f"task-{cap.spec.task_id} retry parent.span_id mismatch: "
            f"attempt-1.parent={a1.parent.span_id:x}, "
            f"attempt-2.parent={a2.parent.span_id:x}, "
            f"expected captured-run span_id={cap.run_span_id:x}"
        )
