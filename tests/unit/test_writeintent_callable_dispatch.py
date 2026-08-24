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

"""Unit tests for callable ``WriteIntent.data`` dispatch semantics.

Contracts protected:

- A callable payload is resolved exactly once at dispatch time, never at
  construction.
- Resolution follows dispatch order.
- Callable exceptions propagate unwrapped; the writer is never touched.
- Eager ndarray payloads are forwarded without copying (zero-copy path).
- Callable data on unsupported kinds raises ``TypeError`` at construction.

Every test drives the real ``IndexedRegionStrategy._dispatch_intent`` static
method against a stub writer so the dispatch code path — not a
re-implementation of it — is what is being exercised.

Stub writers are used instead of real ``RegionZarrWriter`` objects because
``_dispatch_intent`` is a private static method that cannot be exercised
through the public ingest path without a full Zarr store. The stubs implement
only the write methods that ``_dispatch_intent`` routes to; they record the
exact payload object received so tests can assert byte-identity and invocation
count without I/O. The static resume path (which does need a real store) is
covered separately in
``tests/fixtures/callable_payload_test_plugin/tests/test_static_resume_documented_behavior.py``.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pytest

from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent

pytestmark = pytest.mark.unit


def _dispatch(writer: object, intent: WriteIntent) -> None:
    """Bridge duck-typed stub writers to ``_dispatch_intent``'s concrete signature.

    ``IndexedRegionStrategy._dispatch_intent`` is typed against
    ``RegionZarrWriter`` — the concrete class, not a Protocol. The stubs here
    duck-type the four write methods the dispatch layer touches; ``cast``
    silences the pyright ``reportArgumentType`` diagnostic without adopting an
    architectural change (introducing a Protocol just for tests) that the
    ``RegionZarrWriter`` interface has not otherwise required.
    """

    IndexedRegionStrategy._dispatch_intent(cast(RegionZarrWriter, writer), intent)


class _RecordingWriter:
    """Duck-typed ``RegionZarrWriter`` stub that records dispatched payloads.

    Only the four write methods that ``IndexedRegionStrategy._dispatch_intent``
    routes to are implemented. The dispatch layer forwards the resolved payload
    to the writer unchanged; recording the exact object it receives is what
    lets these unit tests assert byte-identity and invocation count without
    touching a Zarr store.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def write_region(
        self,
        *,
        group: str,
        array_name: str,
        ts_index: int,
        y_slice: slice,
        data: np.ndarray,
        channel_index: int | None = None,
    ) -> None:
        self.calls.append(("region", {"group": group, "array": array_name, "data": data}))

    def write_1d(
        self,
        *,
        group: str,
        array_name: str,
        ts_index: int,
        data: np.ndarray,
    ) -> None:
        self.calls.append(("1d", {"group": group, "array": array_name, "data": data}))

    def write_timestamp(
        self,
        *,
        group: str,
        ts_index: int,
        timestamp_val: Any,
    ) -> None:
        self.calls.append(
            ("timestamp", {"group": group, "ts_index": ts_index, "timestamp_val": timestamp_val})
        )

    def write_static(
        self,
        *,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None:
        self.calls.append(("static", {"group": group, "array": array_name, "data": data}))


def _region_intent(data: Any) -> WriteIntent:
    return WriteIntent(
        group="g",
        array="a",
        ts_index=0,
        data=data,
        kind="region",
        y_slice=slice(0, 4),
    )


def _static_intent(data: Any) -> WriteIntent:
    return WriteIntent(
        group="g",
        array="s",
        ts_index=0,
        data=data,
        kind="static",
    )


def test_eager_ndarray_dispatched_byte_identical() -> None:
    """Eager path: when ``data`` is an ndarray, dispatch forwards it verbatim.

    Byte-identity is asserted via ``is`` (same object, no defensive copy) and
    ``assert_array_equal`` (contents unchanged). Callable detection must return
    ``False`` for a plain ndarray — the ``callable(intent.data)`` check is the
    gate that keeps eager writes on the zero-copy path.
    """

    payload = np.ones((4, 4), dtype=np.float32)
    intent = _region_intent(payload)

    assert not callable(intent.data), (
        "np.ndarray must not be callable; callable(intent.data) is the dispatch gate"
    )

    writer = _RecordingWriter()
    _dispatch(writer, intent)

    assert len(writer.calls) == 1
    kind, captured = writer.calls[0]
    assert kind == "region"
    assert captured["data"] is payload, "eager payload must be forwarded without copying"
    np.testing.assert_array_equal(captured["data"], payload)


def test_callable_resolved_exactly_once_per_dispatch() -> None:
    """A callable payload is invoked once, and only once, per dispatch call.

    Two calls would double-resolve every plugin's lazy payload — a silent
    correctness/cost regression the ``call_count`` counter exists to catch.
    """

    call_count = [0]

    def payload() -> np.ndarray:
        call_count[0] += 1
        return np.ones((4, 4), dtype=np.float32)

    intent = _region_intent(payload)
    writer = _RecordingWriter()

    _dispatch(writer, intent)

    assert call_count[0] == 1, (
        f"callable must fire exactly once at dispatch; observed {call_count[0]}"
    )
    assert len(writer.calls) == 1
    np.testing.assert_array_equal(writer.calls[0][1]["data"], np.ones((4, 4), dtype=np.float32))


def test_callable_not_invoked_at_construction_time() -> None:
    """WriteIntent construction must not invoke the callable.

    A ``__post_init__`` that touched ``data`` would fire every plugin's lazy
    thunk at ``build_write_intents`` time, defeating the whole point of lazy
    payloads (all payloads resident before any writer call). The counter
    assertion pre- and post-dispatch is the regression guard for that.
    """

    call_count = [0]

    def payload() -> np.ndarray:
        call_count[0] += 1
        return np.ones((4, 4), dtype=np.float32)

    intent = _region_intent(payload)
    assert call_count[0] == 0, "WriteIntent(...) construction must not fire the callable"

    writer = _RecordingWriter()
    _dispatch(writer, intent)
    assert call_count[0] == 1, "callable must fire exactly once at dispatch"


def test_callables_are_resolved_in_dispatch_order() -> None:
    """Callables fire in the order the caller dispatches their intents.

    ``IndexedRegionStrategy`` dispatches intents sequentially per group, so the
    order the strategy sees is the order the writer sees. This locks in the
    contract that plugins can reason about ordering when their callables have
    observable side effects (e.g. logging, tracing spans, workspace reads).
    """

    invocation_log: list[int] = []

    def make_payload(tag: int) -> Any:
        def _load() -> np.ndarray:
            invocation_log.append(tag)
            return np.full((4, 4), float(tag), dtype=np.float32)

        return _load

    intents = [_region_intent(make_payload(i)) for i in (1, 2, 3)]
    writer = _RecordingWriter()

    for intent in intents:
        _dispatch(writer, intent)

    assert invocation_log == [1, 2, 3], f"expected dispatch order [1, 2, 3]; got {invocation_log}"
    assert [captured["data"][0, 0] for _kind, captured in writer.calls] == [1.0, 2.0, 3.0]


def test_callable_exception_propagates_unwrapped() -> None:
    """A callable exception surfaces with its original type and message.

    Wrapping the exception (e.g. in a generic ``DispatchError``) would hide the
    plugin's real failure — a missing file, a closed handle, a decode error —
    behind an infrastructure exception. The dispatch layer must be transparent
    to the caller. Also asserts the writer was never touched, so a failing
    callable does not leave a partial write on the store.
    """

    def payload() -> np.ndarray:
        raise RuntimeError("test payload error")

    intent = _region_intent(payload)
    writer = _RecordingWriter()

    with pytest.raises(RuntimeError, match="test payload error") as exc_info:
        _dispatch(writer, intent)

    assert type(exc_info.value) is RuntimeError, (
        f"exception must be raised unwrapped; got {type(exc_info.value).__name__}"
    )
    assert writer.calls == [], "writer must not be invoked when the callable raises"


class _ShapeValidatingWriter(_RecordingWriter):
    """Stub that mirrors ``RegionZarrWriter.write_static`` shape validation.

    The real ``write_static`` raises ``ValueError`` with a ``shape mismatch``
    message when ``data.shape`` disagrees with the stored array. This stub
    replicates that boundary so the unit test can prove the dispatch layer
    forwards a wrong-shape callable output verbatim — the writer, not the
    dispatch, rejects it.
    """

    def __init__(self, expected_shape: tuple[int, ...]) -> None:
        super().__init__()
        self._expected_shape = expected_shape

    def write_static(
        self,
        *,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None:
        if data.shape != self._expected_shape:
            raise ValueError(
                f"Array {group!r}/{array_name!r} shape mismatch: "
                f"stored={self._expected_shape}, data={data.shape}"
            )
        super().write_static(group=group, array_name=array_name, data=data)


def test_wrong_shape_callable_output_is_rejected_at_writer_boundary() -> None:
    """A wrong-shape callable output surfaces as a WRITER ``ValueError``.

    Dispatch resolves the callable and forwards the array unchanged; validating
    ``data.shape`` against the target array is the writer's boundary. This
    documents the failure surface so callers know where to look — a shape
    mismatch is never a dispatch-layer bug.
    """

    def payload() -> np.ndarray:
        return np.ones((2, 2), dtype=np.float64)

    intent = _static_intent(payload)
    writer = _ShapeValidatingWriter(expected_shape=(4,))

    with pytest.raises(ValueError, match="shape mismatch"):
        _dispatch(writer, intent)

    assert writer.calls == [], "recorded call list should stay empty on rejection"


class _NdimAccessingWriter(_RecordingWriter):
    """Stub that touches ``data.ndim`` first, matching ``RegionZarrWriter.write_static``.

    The real ``RegionZarrWriter.write_static`` accesses ``data.ndim`` before
    any other attribute (verified against the current implementation in
    ``src/firecube/core/zarr/region_writer.py``). On a ``None`` payload this
    raises ``AttributeError: 'NoneType' object has no attribute 'ndim'``.
    The stub replicates that first access so the test proves dispatch forwards
    ``None`` verbatim — the writer's attribute access is what fails, not the
    dispatch layer. If the real writer's first access ever changes, this stub
    must be updated to match.
    """

    def write_static(
        self,
        *,
        group: str,
        array_name: str,
        data: np.ndarray,
    ) -> None:
        _ = data.ndim  # AttributeError on None; matches RegionZarrWriter.write_static
        super().write_static(group=group, array_name=array_name, data=data)


def test_callable_returning_none_surfaces_as_attribute_error_at_writer() -> None:
    """A ``None``-returning callable is forwarded verbatim; the writer fails.

    Dispatch uses ``typing.cast`` (a no-op at runtime), so ``None`` passes
    through. The writer's first attribute access — ``data.ndim`` in
    ``write_static`` — raises ``AttributeError``. The message contains
    ``ndim`` so a plugin author can locate the broken callable quickly.
    ``writer.calls`` staying empty proves no partial write reached the store.
    """

    def payload() -> np.ndarray:
        return None  # type: ignore[return-value]

    intent = _static_intent(payload)
    writer = _NdimAccessingWriter()

    with pytest.raises(AttributeError, match="ndim"):
        _dispatch(writer, intent)

    assert writer.calls == []


def test_callable_dispatch_covers_region_and_static_kinds() -> None:
    """Callable resolution applies to ``region`` and ``static`` kinds only.

    Regression guard: if a future refactor forgets one branch (e.g. lifts the
    resolution into ``_dispatch_intent`` for ``region`` but leaves ``static``
    on the raw ``intent.data``), only that branch would break. This test
    exercises both payload-carrying callable kinds together so a missing
    branch fails loudly here rather than during a plugin release.

    ``kind="1d"`` and ``kind="timestamp"`` are out of scope for callable
    payloads in this milestone — ``1d`` passes ``intent.data`` directly to
    the writer, and ``timestamp`` carries ``timestamp_val`` instead of a
    payload.
    """

    region_counter = [0]
    static_counter = [0]

    def region_payload() -> np.ndarray:
        region_counter[0] += 1
        return np.full((4, 4), 1.0, dtype=np.float32)

    def static_payload() -> np.ndarray:
        static_counter[0] += 1
        return np.asarray([3.0, 3.0, 3.0, 3.0], dtype=np.float64)

    intents = [
        _region_intent(region_payload),
        _static_intent(static_payload),
    ]
    writer = _RecordingWriter()

    for intent in intents:
        _dispatch(writer, intent)

    assert (region_counter[0], static_counter[0]) == (1, 1), (
        "each of region/static callables must fire exactly once"
    )
    kinds = [kind for kind, _captured in writer.calls]
    assert kinds == ["region", "static"], (
        f"writer must receive one call per intent kind in order; got {kinds}"
    )


def test_callable_data_rejected_at_construction_for_unsupported_kind() -> None:
    """Callable data raises ``TypeError`` at construction for unsupported kinds."""

    def payload() -> np.ndarray:
        return np.ones((4,), dtype=np.float64)

    with pytest.raises(TypeError, match="kind='1d'"):
        WriteIntent(group="g", array="a", ts_index=0, data=payload, kind="1d")

    with pytest.raises(TypeError, match="kind='timestamp'"):
        WriteIntent(
            group="g",
            array="a",
            ts_index=0,
            data=payload,
            kind="timestamp",
            timestamp_val=0,
        )

    WriteIntent(group="g", array="a", ts_index=0, data=np.ones((4,)), kind="1d")
