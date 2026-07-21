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

"""TDD lock for the tracing facade in ``firecube.core.observability.tracing``.

Pins the contract of six helpers introduced by the observability-boundary
refactor: ``span``, ``set_current_span_attribute``, ``capture_context``,
``attach_context``, ``detach_context``, ``propagated_context``.

The helpers do NOT exist yet. Each test imports them at call time so test
collection succeeds (4 items visible) while the tests themselves fail with
``ImportError`` until T5 ships the facade. Do not "fix" the deferred imports
or add fallbacks — the failing imports ARE the test.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)


@pytest.fixture
def exporter() -> Iterator[InMemorySpanExporter]:
    """Fresh per-test TracerProvider + InMemorySpanExporter; restores prior global on teardown.

    Bypasses ``trace.set_tracer_provider`` because OTel's set-once guard would
    silently no-op on every test after the first one in this module, causing
    subsequent tests to capture spans into the wrong (already-shutdown)
    exporter. Writing ``trace._TRACER_PROVIDER`` directly is the standard test
    pattern used by the OpenTelemetry SDK's own unit suite.
    """
    in_memory = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(in_memory))

    previous = trace._TRACER_PROVIDER  # type: ignore[attr-defined]
    trace._TRACER_PROVIDER = provider  # type: ignore[attr-defined]
    try:
        yield in_memory
    finally:
        # Shutdown failure must not mask the test's own assertion outcome.
        with contextlib.suppress(Exception):
            provider.shutdown()
        trace._TRACER_PROVIDER = previous  # type: ignore[attr-defined]


def test_span_creates_named_span_with_attributes(
    exporter: InMemorySpanExporter,
) -> None:
    from firecube.core.observability.tracing import span

    with span("test.op", attributes={"x": 1, "y": "z"}):
        pass

    finished = exporter.get_finished_spans()
    assert len(finished) == 1, f"expected 1 span, got {len(finished)}"

    only = finished[0]
    assert only.name == "test.op"
    assert only.attributes is not None
    assert only.attributes.get("x") == 1
    assert only.attributes.get("y") == "z"


def test_set_current_span_attribute_mutates_active_span(
    exporter: InMemorySpanExporter,
) -> None:
    from firecube.core.observability.tracing import (
        set_current_span_attribute,
        span,
    )

    with span("op"):
        set_current_span_attribute("k", "v")

    finished = exporter.get_finished_spans()
    assert len(finished) == 1, f"expected 1 span, got {len(finished)}"

    only = finished[0]
    assert only.name == "op"
    assert only.attributes is not None
    assert only.attributes.get("k") == "v"


def test_context_propagation_across_thread_preserves_parent(
    exporter: InMemorySpanExporter,
) -> None:
    from firecube.core.observability.tracing import (
        attach_context,
        capture_context,
        detach_context,
        span,
    )

    with span("parent"):
        ctx = capture_context()

        def worker() -> None:
            token = attach_context(ctx)
            try:
                with span("child"):
                    pass
            finally:
                detach_context(token)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            future.result()

    finished = exporter.get_finished_spans()
    names = {s.name for s in finished}
    assert names == {"parent", "child"}, f"unexpected span names: {names}"

    parent_span = next(s for s in finished if s.name == "parent")
    child_span = next(s for s in finished if s.name == "child")

    child_parent_ctx = child_span.parent
    parent_own_ctx = parent_span.context
    assert child_parent_ctx is not None, "child span must have a parent"
    assert parent_own_ctx is not None, "parent span must have a span context"
    assert child_parent_ctx.span_id == parent_own_ctx.span_id, (
        "child.parent.span_id must equal parent.context.span_id "
        "(context did not propagate across the thread boundary)"
    )


def test_propagated_context_manager_form(
    exporter: InMemorySpanExporter,
) -> None:
    from firecube.core.observability.tracing import (
        capture_context,
        propagated_context,
        span,
    )

    with span("parent"):
        ctx = capture_context()

        def worker() -> None:
            with propagated_context(ctx), span("child"):
                pass

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            future.result()

    finished = exporter.get_finished_spans()
    names = {s.name for s in finished}
    assert names == {"parent", "child"}, f"unexpected span names: {names}"

    parent_span = next(s for s in finished if s.name == "parent")
    child_span = next(s for s in finished if s.name == "child")

    child_parent_ctx = child_span.parent
    parent_own_ctx = parent_span.context
    assert child_parent_ctx is not None, "child span must have a parent"
    assert parent_own_ctx is not None, "parent span must have a span context"
    assert child_parent_ctx.span_id == parent_own_ctx.span_id, (
        "propagated_context must reattach the captured context so the child "
        "span inherits the parent created on the main thread"
    )
