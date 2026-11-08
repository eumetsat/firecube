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

from __future__ import annotations

import json
import threading
import time
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import numpy as np
import pytest
import zarr
from zarr.buffer import default_buffer_prototype
from zarr.storage import MemoryStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.claims import ClaimLossLatch
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ClaimLostError
from firecube.core.zarr._zarr_compat import sync
from firecube.core.zarr.guard.guarded_write import WriteResult
from firecube.core.zarr.guard.metadata_authority import AuthorityBasis, MetadataAuthority
from firecube.core.zarr.guard.unit_ownership import StagedSlotWrite, WriteFootprint
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.runtime.zarr.write_session import (
    SessionState,
    WriteSessionStateError,
    ZarrWriteSession,
)
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.types.context import ExecutionParams
from tests.helpers.storage import begin_write_session, make_test_binding

pytestmark = pytest.mark.unit


class _DummyDirectIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "dummy_direct"

    def __init__(self, *, intents: list[WriteIntent], chunk_manager: Any) -> None:
        super().__init__(name="dummy_direct", chunk_manager=chunk_manager)
        self._intents = intents

    def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx):
        return [
            ZarrGroupSpec(
                group="F024", arrays=[ZarrArraySpec(name="data", shape=(8, 1), dtype="float32")]
            )
        ]

    def build_write_intents(self, batch, ctx):
        return self._intents


class _Writer:
    def ensure_group(self, group: str, **kwargs) -> None:
        _ = (group, kwargs)


def _intent(ts_index: int, *, group: str = "F024") -> WriteIntent:
    return WriteIntent(
        group=group,
        array="data",
        ts_index=ts_index,
        data=None,
        y_slice=slice(0, 1),
    )


def _patch_strategy_io(monkeypatch: pytest.MonkeyPatch, calls: list[object] | None = None) -> None:
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        lambda *args, **kwargs: _Writer(),
    )

    def _dispatch(writer, intent) -> None:
        if calls is not None:
            calls.append(("dispatch", intent.ts_index))

    monkeypatch.setattr(IndexedRegionStrategy, "_dispatch_intent", staticmethod(_dispatch))


def _run_direct_process_batch(monkeypatch: pytest.MonkeyPatch, intents: list[WriteIntent]):
    import firecube.ingestor.api as api

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        def write_groups(self, **kwargs):
            captured.update(kwargs)
            group_to_intents = kwargs["group_to_intents"]
            kwargs["claim_for_group"]("F024")
            for ts_index in sorted({intent.ts_index for intent in group_to_intents["F024"]}):
                kwargs["claim_for_slot"]("F024", ts_index)
            return {"coverage": []}

    captured: dict[str, object] = {}
    chunk_manager = SimpleNamespace(
        storage_config=SimpleNamespace(),
        acquire_claim=MagicMock(return_value=nullcontext()),
    )
    ingestor = _DummyDirectIngestor(intents=intents, chunk_manager=chunk_manager)
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: "out.zarr")
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)

    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        execution=ExecutionParams(write_mode="direct"),
        option=lambda key, default=None: default,
    )
    batch = SimpleNamespace(items=["source"], metadata={})

    begin_write_session(ingestor, ctx)
    result = ingestor._process_batch(cast(Any, batch), cast(Any, ctx))
    return result, captured, chunk_manager.acquire_claim


def test_schema_claim_acquired_once_per_group(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_strategy_io(monkeypatch)
    calls: list[str] = []

    def claim_for_group(group_name: str):
        calls.append(group_name)
        return nullcontext()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(index) for index in range(5)]},
        claim_for_group=claim_for_group,
        claim_for_slot=lambda group_name, ts_index: nullcontext(),
    )

    assert calls == ["F024"]


def test_same_ts_index_shares_slot_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_strategy_io(monkeypatch)
    calls: list[tuple[str, int]] = []

    def claim_for_slot(group_name: str, ts_index: int):
        calls.append((group_name, ts_index))
        return nullcontext()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(5), _intent(5), _intent(5)]},
        claim_for_slot=claim_for_slot,
    )

    assert calls == [("F024", 5)]


def test_schema_claim_uses_correct_domain_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, acquire_claim = _run_direct_process_batch(monkeypatch, [_intent(5)])

    schema_call = acquire_claim.call_args_list[0]
    assert schema_call.kwargs["domain"] == WriteDomain(
        product="dummy_direct", category="zarr_region", name="F024:schema"
    )
    assert schema_call.kwargs["owner_id"] == "run-1:F024"


def test_slot_claim_uses_correct_domain_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, acquire_claim = _run_direct_process_batch(monkeypatch, [_intent(5)])

    slot_call = acquire_claim.call_args_list[1]
    assert slot_call.kwargs["domain"] == WriteDomain(
        product="dummy_direct", category="zarr_region", name="F024:slot=5"
    )
    assert slot_call.kwargs["owner_id"] == "run-1:F024:slot=5"


def test_empty_intents_acquires_no_slot_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.api as api

    slot_claim = MagicMock(return_value=nullcontext())

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            pass

        def write_groups(self, **kwargs):
            return {"coverage": []}

    chunk_manager = SimpleNamespace(storage_config=SimpleNamespace(), acquire_claim=slot_claim)
    ingestor = _DummyDirectIngestor(intents=[], chunk_manager=chunk_manager)
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: "out.zarr")
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        execution=ExecutionParams(write_mode="direct"),
        option=lambda key, default=None: default,
    )

    begin_write_session(ingestor, ctx)
    result = ingestor._process_batch(
        cast(Any, SimpleNamespace(items=[], metadata={})), cast(Any, ctx)
    )

    assert result.success is True
    slot_claim.assert_not_called()


def test_fallback_chain_claim_for_slot_none(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    _patch_strategy_io(monkeypatch, calls)

    def claim_for_group(group_name: str):
        calls.append(("group_claim", group_name))
        return nullcontext()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(0), _intent(1)]},
        claim_for_group=claim_for_group,
        claim_for_slot=None,
    )

    assert calls == [
        ("group_claim", "F024"),
        ("group_claim", "F024"),
        ("dispatch", 0),
        ("group_claim", "F024"),
        ("dispatch", 1),
    ]


def test_fallback_chain_both_none(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    _patch_strategy_io(monkeypatch, calls)

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(0)]},
        claim_for_group=None,
        claim_for_slot=None,
    )

    assert calls == [("dispatch", 0)]


def test_high_slot_count_no_leftover_claims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_strategy_io(monkeypatch)
    product = "product.zarr"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product), workspace=workspace
    )

    def claim_for_slot(group_name: str, ts_index: int):
        domain = WriteDomain(
            product=product, category="zarr_region", name=f"{group_name}:slot={ts_index}"
        )
        return manager.acquire_claim(
            product=product,
            domain=domain,
            owner_id=f"run-1:{group_name}:slot={ts_index}",
        )

    try:
        IndexedRegionStrategy(store_uri=str(tmp_path / product)).write_groups(
            group_to_intents={"F024": [_intent(index) for index in range(100)]},
            claim_for_slot=claim_for_slot,
        )
        claim_files = list((tmp_path / product / ".firecube" / "claims").glob("*.json"))
        assert claim_files == []
    finally:
        manager.close()


def _session(calls: list[dict[str, Any]], **overrides: Any) -> ZarrWriteSession:
    def acquire_claim(**kwargs: Any):
        calls.append(kwargs)
        return nullcontext()

    kwargs: dict[str, Any] = {
        "product": "product.zarr",
        "run_id": "run-1",
        "write_mode": "direct",
        "window": None,
        "slot_group": None,
        "acquire_claim": acquire_claim,
        "open_root": lambda _uri: None,
        "open_final_root": lambda _ctx: None,
    }
    kwargs.update(overrides)
    return ZarrWriteSession(**kwargs)


def test_session_shares_schema_claim_between_concurrent_batches() -> None:
    calls: list[dict[str, Any]] = []
    session = _session(calls)
    first = session.schema_claim("F024")
    second = session.schema_claim("F024")
    assert len(calls) == 1
    with first:
        pass
    with second:
        pass


def test_session_transitions_prepared_writing_publishing_terminal() -> None:
    session = _session([])
    assert session.state is SessionState.PREPARED
    with session.admit_write():
        assert session.state is SessionState.WRITING
    with session.admit_write():
        assert session.state is SessionState.WRITING
    session.verify_promotion()
    assert session.state is SessionState.PUBLISHING
    with pytest.raises(WriteSessionStateError):
        session.admit_write().__enter__()
    with pytest.raises(WriteSessionStateError):
        session.verify_promotion()
    session.close()
    session.close()
    assert session.state is SessionState.TERMINAL
    with pytest.raises(WriteSessionStateError):
        session.admit_write().__enter__()


def test_session_verify_promotion_allowed_from_prepared() -> None:
    session = _session([])
    session.verify_promotion()
    assert session.state is SessionState.PUBLISHING


def test_session_refuses_every_operation_after_terminal() -> None:
    calls: list[dict[str, Any]] = []
    session = _session(calls)
    session.close()
    write = StagedSlotWrite("g/a", 0)
    result = WriteResult(footprint=WriteFootprint("g/a", ((0, 4),)), units=((0,),))
    operations = [
        session.verify_promotion,
        lambda: session.schema_claim("F024"),
        lambda: session.slot_claim("F024", 1),
        lambda: session.record_touched("ws", [write]),
        lambda: session.record_written("ws", [write]),
        lambda: session.record_write_result(result, store_uri="ws"),
        lambda: session.written_units("ws"),
        lambda: session.absent_units("ws"),
        lambda: session.final_root(None),
        session.engine_owned_checked,
        session.mark_engine_owned_checked,
    ]
    for operation in operations:
        with pytest.raises(WriteSessionStateError):
            operation()
    assert calls == []
    assert session._ledger.touched == []
    assert session._ledger.written == []
    assert session._written_units == {}


class _Hook:
    """Pauses the first caller until released; later callers pass straight through."""

    def __init__(self) -> None:
        self.paused = threading.Event()
        self.release = threading.Event()

    def __call__(self, *_args: Any, **_kwargs: Any) -> None:
        if not self.paused.is_set():
            self.paused.set()
            assert self.release.wait(10)


class _HookLock:
    def __init__(self, hook: _Hook) -> None:
        self._hook = hook
        self._lock = threading.Lock()

    def __enter__(self) -> None:
        self._hook()
        self._lock.acquire()

    def __exit__(self, *_exc: object) -> None:
        self._lock.release()


_RESULT = WriteResult(
    footprint=WriteFootprint("g/a", ((0, 4),)), units=((0,),), absent=((0,),), tracked=True
)
_OPERATIONS = [
    "schema_claim",
    "slot_claim",
    "final_root",
    "record_touched",
    "record_written",
    "record_write_result",
    "written_units",
    "absent_units",
    "engine_owned_checked",
    "mark_engine_owned_checked",
]


@pytest.mark.parametrize("name", _OPERATIONS)
def test_session_close_cannot_complete_between_an_operation_check_and_its_effect(
    name: str,
) -> None:
    hook = _Hook()
    session = _session(
        [],
        acquire_claim=lambda **_kw: (hook(), nullcontext())[1],
        open_final_root=lambda _ctx: hook(),
        drain_timeout=10.0,
    )
    session._ledger.record = hook  # type: ignore[method-assign]
    session._results_lock = _HookLock(hook)  # type: ignore[assignment]
    write = StagedSlotWrite("g/a", 0)
    operations = {
        "schema_claim": lambda: session.schema_claim("F024"),
        "slot_claim": lambda: session.slot_claim("F024", 1),
        "final_root": lambda: session.final_root(None),
        "record_touched": lambda: session.record_touched("ws", [write]),
        "record_written": lambda: session.record_written("ws", [write]),
        "record_write_result": lambda: session.record_write_result(_RESULT, store_uri="ws"),
        "written_units": lambda: session.written_units("ws"),
        "absent_units": lambda: session.absent_units("ws"),
        "engine_owned_checked": session.engine_owned_checked,
        "mark_engine_owned_checked": session.mark_engine_owned_checked,
    }
    errors: list[BaseException] = []

    def run(fn: Any) -> None:
        try:
            fn()
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=run, args=(operations[name],))
    worker.start()
    assert hook.paused.wait(5), "the operation never reached its side effect"
    closer = threading.Thread(target=run, args=(session.close,))
    closer.start()
    closer.join(0.3)
    completed_early = not closer.is_alive()
    hook.release.set()
    worker.join(10)
    closer.join(10)
    assert not completed_early, "close() completed while the operation was in flight"
    assert errors == []
    assert session.state is SessionState.TERMINAL
    with pytest.raises(WriteSessionStateError):
        operations[name]()


def test_session_operation_inside_an_admitted_batch_does_not_deadlock_close() -> None:
    session = _session([], drain_timeout=10.0)
    entered = threading.Event()
    done = threading.Event()
    errors: list[BaseException] = []

    def batch() -> None:
        try:
            with session.admit_write():
                session.record_write_result(_RESULT, store_uri="ws")
                entered.set()
                assert done.wait(10)
                with pytest.raises(WriteSessionStateError):
                    session.record_write_result(_RESULT, store_uri="ws")
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=batch)
    worker.start()
    assert entered.wait(5)
    closer = threading.Thread(target=session.close)
    closer.start()
    deadline = time.monotonic() + 5
    while session.state is not SessionState.TERMINAL and time.monotonic() < deadline:
        time.sleep(0.01)
    done.set()
    worker.join(10)
    closer.join(10)
    assert errors == []
    assert session.state is SessionState.TERMINAL


def test_session_publication_waits_for_in_flight_writes() -> None:
    session = _session([], drain_timeout=0.2)
    with session.admit_write():
        with pytest.raises(WriteSessionStateError, match="1 write"):
            session.verify_promotion()
        # new admissions are already refused while publication drains
        with pytest.raises(WriteSessionStateError):
            session.admit_write().__enter__()
    assert session.state is SessionState.PUBLISHING


def test_session_close_waits_for_in_flight_writes() -> None:
    session = _session([], drain_timeout=0.2)
    with session.admit_write():
        with pytest.raises(WriteSessionStateError, match="1 write"):
            session.close()
        with pytest.raises(WriteSessionStateError):
            session.admit_write().__enter__()
    assert session.state is SessionState.TERMINAL


class _SignallingCondition(threading.Condition):
    """A condition that sets ``waiting[thread name]`` as that thread starts to wait."""

    def __init__(self, waiting: dict[str, threading.Event]) -> None:
        super().__init__()
        self.waiting = waiting

    def wait_for(self, predicate: Any, timeout: float | None = None) -> Any:
        event = self.waiting.get(threading.current_thread().name)
        if event is not None:
            event.set()  # still holding the lock: nothing can notify before the wait
        return super().wait_for(predicate, timeout)


def test_session_second_close_returns_only_after_the_first_close_finished(tmp_path: Path) -> None:
    """A second closer waits for the first closer's drain and authority release.

    When that drain timed out with the batch still in flight, the second
    closer raises the same failure instead of returning; once the batch has
    left, a later close completes.
    """
    session = _session(
        [],
        write_mode="staged",
        workspace_uri=str(tmp_path / "ws.zarr"),
        drain_timeout=0.5,
    )
    first_draining, second_settled = threading.Event(), threading.Event()
    session._cond = _SignallingCondition(
        {"first-close": first_draining, "second-close": second_settled}
    )
    seeding = session.staged_seeding_authority()
    assert seeding is not None and seeding.active
    entered = threading.Event()
    release = threading.Event()
    first_errors: list[BaseException] = []
    second_errors: list[BaseException] = []
    at_second_return: list[tuple[int, bool]] = []

    def batch() -> None:
        with session.admit_write():
            entered.set()
            assert release.wait(10)

    def first_close() -> None:
        try:
            session.close()
        except BaseException as exc:
            first_errors.append(exc)

    def second_close() -> None:
        try:
            session.close()
        except BaseException as exc:
            second_errors.append(exc)
        at_second_return.append((session._in_flight, seeding.active))
        second_settled.set()  # returned without waiting (or after it)

    worker = threading.Thread(target=batch)
    first = threading.Thread(target=first_close, name="first-close")
    second = threading.Thread(target=second_close, name="second-close")
    worker.start()
    try:
        assert entered.wait(5)
        first.start()
        assert first_draining.wait(5)  # TERMINAL, the first closer waits for the batch
        second.start()
        assert second_settled.wait(5)  # the second closer waits, or has returned
        first.join(10)
        second.join(10)
    finally:
        release.set()
        worker.join(10)

    assert len(first_errors) == 1 and "still in flight" in str(first_errors[0])
    assert [(type(e), str(e)) for e in second_errors] == [
        (WriteSessionStateError, str(first_errors[0]))
    ]
    assert at_second_return == [(1, False)]
    assert session._in_flight == 0
    session.close()
    assert session.state is SessionState.TERMINAL


def test_session_publication_proceeds_once_in_flight_write_leaves() -> None:
    session = _session([], drain_timeout=5.0)
    entered = threading.Event()
    release = threading.Event()

    def worker() -> None:
        with session.admit_write():
            entered.set()
            release.wait(5)

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(5)
    threading.Timer(0.1, release.set).start()
    session.verify_promotion()  # blocks until the worker leaves
    thread.join(5)
    assert session.state is SessionState.PUBLISHING
    with pytest.raises(WriteSessionStateError):
        session.admit_write().__enter__()


def test_session_records_write_results_per_store() -> None:
    session = _session([])
    result = WriteResult(
        footprint=WriteFootprint("g/a", ((0, 4),)),
        units=((0,), (1,), (2,)),
        absent=((1,),),
        tracked=True,
    )
    session.record_write_result(result, store_uri="ws")
    assert session.written_units("ws") == {"g/a": frozenset({(0,), (1,), (2,)})}
    assert session.absent_units("ws") == {"g/a": frozenset({(1,)})}
    assert session.written_units("other") == {}


def test_session_recreated_unit_leaves_the_absent_set() -> None:
    session = _session([])
    footprint = WriteFootprint("g/a", ((0, 4),))
    session.record_write_result(
        WriteResult(footprint=footprint, units=((0,), (1,)), absent=((0,), (1,)), tracked=True),
        store_uri="ws",
    )
    session.record_write_result(
        WriteResult(footprint=footprint, units=((0,),), absent=(), tracked=True),
        store_uri="ws",
    )
    assert session.absent_units("ws") == {"g/a": frozenset({(1,)})}
    assert session.written_units("ws") == {"g/a": frozenset({(0,), (1,)})}


def _spy_writer_authorities(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any, bool]]:
    """Record the authority each writer metadata call receives, and whether it was active."""
    seen: list[tuple[str, Any, bool]] = []
    for name in ("ensure_group", "set_group_attrs", "update_array_attrs"):
        original = getattr(RegionZarrWriter, name)

        def spy(self: Any, *args: Any, _original: Any = original, _name: str = name, **kw: Any):
            authority = kw.get("authority")
            active = isinstance(authority, MetadataAuthority) and authority.active
            seen.append((_name, authority, active))
            return _original(self, *args, **kw)

        monkeypatch.setattr(RegionZarrWriter, name, spy)
    return seen


def test_serial_schema_step_and_static_marker_run_under_the_run_schema_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_uri = str(tmp_path / "fresh.zarr")  # no root yet: created before the claim's scope
    session = _session([])
    seen = _spy_writer_authorities(monkeypatch)
    schema = [
        ZarrGroupSpec(
            group="g",
            arrays=[
                ZarrArraySpec(
                    name="lat", shape=(4,), dtype="int32", fill_value=0, time_indexed=False
                )
            ],
            attrs={"title": "serial"},
        )
    ]
    IndexedRegionStrategy(store_uri=store_uri, schema=schema).write_groups(
        group_to_intents={
            "g": [WriteIntent.static(group="g", array="lat", data=np.arange(4, dtype="int32"))]
        },
        schema=schema,
        claim_for_group=session.schema_claim,
        claim_for_slot=session.slot_claim,
    )

    assert [name for name, *_ in seen] == ["ensure_group", "set_group_attrs", "update_array_attrs"]
    for name, authority, active in seen:
        assert isinstance(authority, MetadataAuthority), f"{name} ran without an authority"
        assert authority.basis is AuthorityBasis.CLAIM
        assert authority.claim_domain == "product.zarr:zarr_region:g:schema"
        assert authority.owner_id == "run-1:g"
        assert authority.scope.prefix == "g"
        assert active and not authority.active  # released when the claim's scope ended
    lat = cast(Any, zarr.open_group(store=store_uri, mode="r", zarr_format=3)["g/lat"])
    assert lat.attrs["firecube_static_written"] is True
    assert lat[...].tolist() == [0, 1, 2, 3]


class _StagedUnitsIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "staged_units"

    def __init__(self, *, chunk_manager: Any) -> None:
        super().__init__(name="staged_units", chunk_manager=chunk_manager)

    def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx):
        return [
            ZarrGroupSpec(
                group="F024",
                arrays=[
                    ZarrArraySpec(
                        name="data", shape=(4, 2), dtype="float32", chunks=(1, 2), fill_value=0.0
                    )
                ],
            )
        ]

    def build_write_intents(self, batch, ctx):
        return [
            WriteIntent.slot(group="F024", array="data", index=0, data=np.zeros(2, "float32")),
            WriteIntent.slot(group="F024", array="data", index=1, data=np.ones(2, "float32")),
        ]


def test_staged_direct_zarr_batch_records_written_and_absent_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_uri = str(tmp_path / "workspace.zarr")
    chunk_manager = SimpleNamespace(storage_config=None, acquire_claim=lambda **_kw: nullcontext())
    ingestor = _StagedUnitsIngestor(chunk_manager=chunk_manager)
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: store_uri)
    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        execution=ExecutionParams(write_mode="staged"),
        option=lambda key, default=None: default,
    )

    session = begin_write_session(ingestor, ctx)
    batch = SimpleNamespace(items=["s"], metadata={})
    result = ingestor._process_batch(cast(Any, batch), cast(Any, ctx))

    assert result.success is True, result.error
    written = session.written_units(store_uri)
    absent = session.absent_units(store_uri)
    # Slot 0 is all fill: its chunk holds no object, yet it was written.
    assert written == {"F024/data": frozenset({(0, 0), (1, 0)})}
    assert absent == {"F024/data": frozenset({(0, 0)})}
    assert absent["F024/data"] <= written["F024/data"]


def test_direct_batch_keeps_no_written_or_absent_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_uri = str(tmp_path / "target.zarr")
    chunk_manager = SimpleNamespace(storage_config=None, acquire_claim=lambda **_kw: nullcontext())
    ingestor = _StagedUnitsIngestor(chunk_manager=chunk_manager)
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: store_uri)
    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        execution=ExecutionParams(write_mode="direct"),
        option=lambda key, default=None: default,
    )

    session = begin_write_session(ingestor, ctx)
    batch = SimpleNamespace(items=["s"], metadata={})
    ingestor._process_batch(cast(Any, batch), cast(Any, ctx))

    data = cast(Any, zarr.open_group(store=store_uri, mode="r", zarr_format=3)["F024/data"])
    assert data[1].tolist() == [1.0, 1.0]
    assert session.written_units(store_uri) == {}
    assert session.absent_units(store_uri) == {}


def test_verify_promotion_raises_when_draining_batch_loses_claim() -> None:
    store = MemoryStore()
    zarr.group(store=store, attributes={"title": "successor"})
    latch = ClaimLossLatch()
    entered, draining = threading.Event(), threading.Event()
    session = _session(
        [],
        write_mode="staged",
        claim_latch=latch,
        workspace_uri="file:///unused",
        open_final_store=lambda: ("file:///final", store),
    )
    real_drain = session._drain

    def drain_signalling(what: str) -> None:
        draining.set()
        real_drain(what)

    session._drain = drain_signalling  # type: ignore[method-assign]  # only seam for "drain started"

    def batch() -> None:
        with session.admit_write():
            entered.set()
            assert draining.wait(5)
            latch.set("p:zarr_region:g:slot=0")

    worker = threading.Thread(target=batch)
    worker.start()
    try:
        assert entered.wait(5)
        with pytest.raises(ClaimLostError):
            session.verify_promotion()
    finally:
        worker.join(5)
    assert not worker.is_alive()
    assert latch.is_set
    document = json.loads(
        sync(store.get("zarr.json", prototype=default_buffer_prototype())).to_bytes()  # type: ignore[union-attr]
    )
    assert document["attributes"] == {"title": "successor"}
    session.close()
