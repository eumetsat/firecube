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

import contextlib
import datetime as dt
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import numpy as np
import pytest

from firecube.core.controlplane.types import (
    INDEX_ENSURED_OUTCOME_CREATED,
    INDEX_ENSURED_OUTCOME_MATCHED_EXISTING,
    IndexEnsuredEvent,
    ResolvedIndexRecord,
)
from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.observability.metrics import TelemetryService
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)

pytestmark = pytest.mark.unit


class _FakeChunkManager:
    storage_config = None

    def __init__(
        self,
        *,
        outcome: str = INDEX_ENSURED_OUTCOME_CREATED,
    ) -> None:
        self._outcome = outcome
        self.ensure_calls: list[dict[str, Any]] = []
        self.wal_events: list[IndexEnsuredEvent] = []

    def acquire_claim(self, *, product: str, domain: Any, owner_id: str):
        _ = (product, domain, owner_id)
        return nullcontext()

    def ensure_resolved_index(
        self, *, product: str, record: ResolvedIndexRecord, run_id: str | None = None
    ) -> tuple[ResolvedIndexRecord, str]:
        self.ensure_calls.append({"product": product, "record": record, "run_id": run_id})
        return record, self._outcome

    def get_slot_index_model(self, *, product: str) -> None:
        _ = product
        return None

    def get_resolved_index(self, *, product: str) -> ResolvedIndexRecord | None:
        _ = product
        return None

    def get_control_root(self, product: str) -> str:
        return f"/tmp/{product}/.firecube"

    def get_product_root(self, product: str) -> str:
        return f"/tmp/{product}"

    def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
        self.wal_events.append(event)


class _FakeTelemetry:
    def __init__(self) -> None:
        self.emitted: list[tuple[str, float, str, dict[str, Any] | None]] = []

    @property
    def run_id(self) -> str:
        return "test-run"

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: str = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.emitted.append((name, value, kind, dict(meta) if meta else None))

    def flush(self) -> None:
        return None

    def span(self, name: str, attributes: dict[str, Any] | None = None):
        _ = (name, attributes)
        return contextlib.nullcontext()

    def collect_memory_stats(self) -> None:
        return None


class _CapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "index_ensured_test"

    def __init__(self, *, chunk_manager: _FakeChunkManager) -> None:
        super().__init__(name="index_ensured_test", chunk_manager=cast(Any, chunk_manager))

    def index_spec(self, ctx: Any) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="index_ensured_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=8,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: Any) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def ingest(self, ctx: Any):  # pragma: no cover
        raise NotImplementedError

    def zarr_schema(self, ctx: Any) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(8, 4),
                        dtype=np.float32,
                        chunks=(4, 4),
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: Any, ctx: Any) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


def _ctx(*, telemetry: _FakeTelemetry | None = None) -> Any:
    return SimpleNamespace(
        _ctx=SimpleNamespace(telemetry=telemetry),
        run_id="startup-run",
        storage=None,
        telemetry=telemetry,
        option=lambda key, default=None: default,
    )


def _sample_event() -> IndexEnsuredEvent:
    return IndexEnsuredEvent(
        run_id="run-abc",
        product="prod-1",
        identity_hash="a" * 64,
        axis_kinds=("regular_time",),
        groups=("data",),
        outcome=INDEX_ENSURED_OUTCOME_CREATED,
        timestamp="2026-08-20T12:00:00Z",
    )


def test_telemetry_service_emit_index_ensured_emits_counter() -> None:
    telemetry = _FakeTelemetry()
    service = TelemetryService(cast(Any, telemetry), "test-plugin")

    service.emit_index_ensured(
        product="prod-1",
        identity_hash="a" * 64,
        axis_kinds=("regular_time",),
        groups=("data",),
        outcome=INDEX_ENSURED_OUTCOME_CREATED,
    )

    assert len(telemetry.emitted) == 1
    name, value, kind, meta = telemetry.emitted[0]
    assert "index_ensured" in name
    assert value == 1.0
    assert kind == "counter"
    assert meta is not None
    assert meta["outcome"] == INDEX_ENSURED_OUTCOME_CREATED
    assert meta["identity_hash"] == "a" * 64
    assert meta["axis_kinds"] == "regular_time"
    assert meta["groups"] == "data"


def test_telemetry_service_emit_index_ensured_no_sink_is_noop() -> None:
    service = TelemetryService(None, "test-plugin")

    service.emit_index_ensured(
        product="prod-1",
        identity_hash="a" * 64,
        axis_kinds=("regular_time",),
        groups=("data",),
        outcome=INDEX_ENSURED_OUTCOME_MATCHED_EXISTING,
    )


def test_telemetry_service_emit_index_ensured_joins_multi_axis_and_groups() -> None:
    telemetry = _FakeTelemetry()
    service = TelemetryService(cast(Any, telemetry), "test-plugin")

    service.emit_index_ensured(
        product="prod-1",
        identity_hash="b" * 64,
        axis_kinds=("integer", "regular_time"),
        groups=("g_a", "g_b"),
        outcome=INDEX_ENSURED_OUTCOME_CREATED,
    )

    _, _, _, meta = telemetry.emitted[0]
    assert meta is not None
    assert meta["axis_kinds"] == "integer,regular_time"
    assert meta["groups"] == "g_a,g_b"


def test_wal_writer_record_index_ensured_event_appends(chunk_manager: Any) -> None:
    appended: list[dict[str, Any]] = []

    def _fake_writer(product: str, run_id: str, resume_existing: bool) -> Any:
        _ = (product, run_id, resume_existing)

        class _W:
            def append(
                self,
                event_type: str,
                record: dict[str, Any],
                *,
                meta: dict[str, Any],
                flush: bool,
            ) -> None:
                appended.append(
                    {
                        "event_type": event_type,
                        "record": record,
                        "meta": meta,
                        "flush": flush,
                    }
                )

        return _W()

    chunk_manager.repo._wal_writer._writer = _fake_writer  # type: ignore[method-assign]

    event = _sample_event()
    chunk_manager.repo._wal_writer.record_index_ensured_event(event)

    assert len(appended) == 1
    entry = appended[0]
    assert entry["event_type"] == "index_ensured"
    assert entry["record"]["outcome"] == INDEX_ENSURED_OUTCOME_CREATED
    assert entry["record"]["identity_hash"] == "a" * 64
    assert entry["record"]["axis_kinds"] == ["regular_time"]
    assert entry["record"]["groups"] == ["data"]
    assert entry["record"]["product"] == "prod-1"
    assert entry["record"]["run_id"] == "run-abc"


def test_repo_facade_record_index_ensured_event_delegates(chunk_manager: Any) -> None:
    received: list[IndexEnsuredEvent] = []
    chunk_manager.repo._wal_writer.record_index_ensured_event = (  # type: ignore[method-assign]
        lambda event: received.append(event)
    )

    event = _sample_event()
    chunk_manager.repo.record_index_ensured_event(event)

    assert received == [event]


def _bypass_legacy_check(ingestor: DirectZarrIngestor) -> None:
    ingestor._check_legacy_index_record_at_startup = lambda **_kwargs: None  # type: ignore[method-assign]


def test_startup_emits_telemetry_and_wal_on_fresh_create() -> None:
    manager = _FakeChunkManager(outcome=INDEX_ENSURED_OUTCOME_CREATED)
    ingestor = _CapableIngestor(chunk_manager=manager)
    _bypass_legacy_check(ingestor)
    telemetry = _FakeTelemetry()
    ctx = cast(Any, _ctx(telemetry=telemetry))

    ingestor._ensure_index_identity_at_startup(ctx)

    assert len(manager.wal_events) == 1
    event = manager.wal_events[0]
    assert event.outcome == INDEX_ENSURED_OUTCOME_CREATED
    assert event.product == "index_ensured_test"
    assert event.run_id == "startup-run"
    assert event.axis_kinds == ("regular_time",)
    assert event.groups == ("data",)
    assert len(event.identity_hash) == 64

    assert len(telemetry.emitted) == 1
    name, value, kind, meta = telemetry.emitted[0]
    assert "index_ensured" in name
    assert kind == "counter"
    assert value == 1.0
    assert meta is not None
    assert meta["outcome"] == INDEX_ENSURED_OUTCOME_CREATED
    assert meta["identity_hash"] == event.identity_hash


def test_startup_emits_matched_existing_when_record_matches() -> None:
    manager = _FakeChunkManager(outcome=INDEX_ENSURED_OUTCOME_MATCHED_EXISTING)
    ingestor = _CapableIngestor(chunk_manager=manager)
    _bypass_legacy_check(ingestor)
    telemetry = _FakeTelemetry()
    ctx = cast(Any, _ctx(telemetry=telemetry))

    ingestor._ensure_index_identity_at_startup(ctx)

    assert len(manager.wal_events) == 1
    assert manager.wal_events[0].outcome == INDEX_ENSURED_OUTCOME_MATCHED_EXISTING

    assert len(telemetry.emitted) == 1
    _, _, _, meta = telemetry.emitted[0]
    assert meta is not None
    assert meta["outcome"] == INDEX_ENSURED_OUTCOME_MATCHED_EXISTING


def test_startup_no_double_emission_when_already_stamped() -> None:
    manager = _FakeChunkManager()
    ingestor = _CapableIngestor(chunk_manager=manager)
    _bypass_legacy_check(ingestor)
    telemetry = _FakeTelemetry()
    ctx = cast(Any, _ctx(telemetry=telemetry))

    ingestor._ensure_index_identity_at_startup(ctx)
    assert len(manager.wal_events) == 1
    assert len(telemetry.emitted) == 1

    ingestor._ensure_index_identity_at_startup(ctx)
    assert len(manager.wal_events) == 1
    assert len(telemetry.emitted) == 1


def test_startup_survives_without_telemetry_sink() -> None:
    manager = _FakeChunkManager(outcome=INDEX_ENSURED_OUTCOME_CREATED)
    ingestor = _CapableIngestor(chunk_manager=manager)
    _bypass_legacy_check(ingestor)
    ctx = cast(Any, _ctx(telemetry=None))

    ingestor._ensure_index_identity_at_startup(ctx)

    assert len(manager.wal_events) == 1
    assert manager.wal_events[0].outcome == INDEX_ENSURED_OUTCOME_CREATED


def test_startup_telemetry_failure_does_not_block_wal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FailingTelemetry(_FakeTelemetry):
        def emit(self, name: str, value: float, *, kind: str = "gauge", meta=None) -> None:
            raise RuntimeError("telemetry sink is down")

    manager = _FakeChunkManager(outcome=INDEX_ENSURED_OUTCOME_CREATED)
    ingestor = _CapableIngestor(chunk_manager=manager)
    _bypass_legacy_check(ingestor)
    ctx = cast(Any, _ctx(telemetry=_FailingTelemetry()))

    with caplog.at_level("WARNING"):
        ingestor._ensure_index_identity_at_startup(ctx)

    assert len(manager.wal_events) == 1
    assert any("telemetry" in rec.message.lower() for rec in caplog.records)
