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

import hashlib
import logging
from typing import Any, cast

import pytest

from firecube.core.controlplane.types import (
    INDEX_ENSURED_OUTCOME_CREATED,
    IndexEnsuredEvent,
    ResolvedIndexRecord,
    canonical_index_bytes,
)
from firecube.core.observability.metrics import (
    METRIC_INDEX_ENSURED,
    TelemetryService,
    emit_index_ensured_full,
)

pytestmark = pytest.mark.unit


class _FakeChunkManager:
    def __init__(self) -> None:
        self.wal_events: list[IndexEnsuredEvent] = []

    def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
        self.wal_events.append(event)


class _FakeTelemetry:
    def __init__(self) -> None:
        self.emitted: list[tuple[str, float, str, dict[str, Any] | None]] = []

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: str = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.emitted.append((name, value, kind, dict(meta) if meta else None))


def _record() -> ResolvedIndexRecord:
    index = {
        "name": "test_index_v1",
        "groups": {
            "b": {"kind": "integer"},
            "a": {"kind": "regular_time"},
        },
    }
    return ResolvedIndexRecord(
        schema_version="v1",
        recorded_at="2026-08-21T00:00:00Z",
        recorded_by_run_id="ensure-run",
        identity_hash=hashlib.sha256(canonical_index_bytes(index)).hexdigest(),
        index=index,
    )


def test_emit_index_ensured_full_calls_wal_and_telemetry_together() -> None:
    manager = _FakeChunkManager()
    telemetry_sink = _FakeTelemetry()
    telemetry = TelemetryService(cast(Any, telemetry_sink), "test-plugin")
    record = _record()

    emit_index_ensured_full(
        cast(Any, manager),
        telemetry,
        product="product-1",
        run_id="run-1",
        record=record,
        outcome=INDEX_ENSURED_OUTCOME_CREATED,
        logger=logging.getLogger("test_emit_index_ensured_full"),
    )

    assert len(manager.wal_events) == 1
    event = manager.wal_events[0]
    assert event.product == "product-1"
    assert event.run_id == "run-1"
    assert event.identity_hash == record.identity_hash
    assert event.axis_kinds == ("integer", "regular_time")
    assert event.groups == ("a", "b")
    assert event.outcome == INDEX_ENSURED_OUTCOME_CREATED

    assert telemetry_sink.emitted == [
        (
            METRIC_INDEX_ENSURED,
            1.0,
            "counter",
            {
                "plugin": "test-plugin",
                "product": "product-1",
                "outcome": INDEX_ENSURED_OUTCOME_CREATED,
                "identity_hash": record.identity_hash,
                "axis_kinds": "integer,regular_time",
                "groups": "a,b",
            },
        )
    ]


def test_emit_index_ensured_full_logs_wal_failure_at_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FailingManager(_FakeChunkManager):
        def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
            _ = event
            raise RuntimeError("wal down")

    logger = logging.getLogger("test_emit_index_ensured_full.wal")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        emit_index_ensured_full(
            cast(Any, _FailingManager()),
            None,
            product="product-1",
            run_id="run-1",
            record=_record(),
            outcome=INDEX_ENSURED_OUTCOME_CREATED,
            logger=logger,
        )

    assert any(
        rec.levelno == logging.ERROR and "WAL audit event" in rec.message for rec in caplog.records
    )
