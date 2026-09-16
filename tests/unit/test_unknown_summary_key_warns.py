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

"""Unknown pipeline-summary keys must surface as WARNING, not DEBUG.

`TelemetryService.emit_run_metrics` filters out envelope keys
(`_PIPELINE_ENVELOPE_KEYS`) and then compares the remainder to
`RUN_SUMMARY_SCHEMA`. Any leftover key is real schema drift the operator
must see: it is emitted at WARNING and the message names
`RUN_SUMMARY_SCHEMA` so the operator knows where to update the schema.

An earlier drift downgraded the message to DEBUG, which combined with the
envelope filter to hide real drift behind two silent layers. This test
locks the WARNING contract so a future downgrade regresses loudly.
"""

from __future__ import annotations

import contextlib
import logging

import pytest

from firecube.core.observability.metrics import (
    _PIPELINE_ENVELOPE_KEYS,
    RUN_SUMMARY_SCHEMA,
    TelemetryService,
)
from firecube.ingestor.contracts.interfaces import IngestionTelemetry


class _MockTelemetry(IngestionTelemetry):
    def __init__(self):
        self.emitted: list[tuple[str, float, str, dict | None]] = []

    def emit(self, name, value, *, kind="gauge", meta=None):
        self.emitted.append((name, value, kind, meta))

    @property
    def run_id(self):
        return "test-run"

    def flush(self):
        return None

    def span(self, name, attributes=None):
        _ = (name, attributes)
        return contextlib.nullcontext()

    def collect_memory_stats(self):
        return None


@pytest.mark.unit
def test_unknown_summary_key_emits_warning_naming_schema(caplog):
    telemetry = _MockTelemetry()
    service = TelemetryService(telemetry, "test-plugin")
    summary = dict.fromkeys(RUN_SUMMARY_SCHEMA, 0)
    summary["bogus_key"] = 1

    with caplog.at_level(logging.WARNING, logger="firecube.ingestor.telemetry"):
        service.emit_run_metrics(summary)

    warning_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "unknown pipeline summary" in r.message
    ]
    assert len(warning_records) == 1, (
        f"Expected exactly 1 WARNING for unknown key, got {len(warning_records)}: "
        f"{[r.message for r in warning_records]}"
    )
    message = warning_records[0].message
    assert "bogus_key" in message, (
        f"Expected WARNING to name the offending key 'bogus_key', got: {message!r}"
    )
    assert "RUN_SUMMARY_SCHEMA" in message, (
        f"Expected WARNING to name RUN_SUMMARY_SCHEMA as the schema source, got: {message!r}"
    )


@pytest.mark.unit
def test_envelope_key_does_not_emit_warning(caplog):
    telemetry = _MockTelemetry()
    service = TelemetryService(telemetry, "test-plugin")
    envelope_key = next(iter(_PIPELINE_ENVELOPE_KEYS))
    summary: dict[str, object] = dict.fromkeys(RUN_SUMMARY_SCHEMA, 0)
    summary[envelope_key] = {"nested": "payload"}

    with caplog.at_level(logging.WARNING, logger="firecube.ingestor.telemetry"):
        service.emit_run_metrics(summary)

    warning_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "unknown pipeline summary" in r.message
    ]
    assert not warning_records, (
        f"Expected no WARNING for envelope key {envelope_key!r}, got: "
        f"{[r.message for r in warning_records]}"
    )
