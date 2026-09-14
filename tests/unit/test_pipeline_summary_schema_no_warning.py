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
import logging

import pytest

from firecube.core.observability.metrics import (
    _PIPELINE_ENVELOPE_KEYS,
    RUN_SUMMARY_SCHEMA,
    TelemetryService,
)
from firecube.ingestor.contracts.interfaces import IngestionTelemetry
from firecube.ingestor.types.result_metrics import PipelineMetrics


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
def test_pipeline_metrics_dict_emits_no_warning(caplog):
    telemetry = _MockTelemetry()
    service = TelemetryService(telemetry, "test-plugin")
    summary = PipelineMetrics().to_dict()

    with caplog.at_level(logging.WARNING, logger="firecube.ingestor.telemetry"):
        service.emit_run_metrics(summary)

    warning_records = [
        r
        for r in caplog.records
        if r.levelno >= logging.WARNING and "unknown pipeline summary" in r.message
    ]
    assert not warning_records, (
        f"Expected no WARNING about unknown pipeline summary keys, got: {[r.message for r in warning_records]}"
    )


@pytest.mark.unit
def test_envelope_keys_are_not_in_schema():
    for key in _PIPELINE_ENVELOPE_KEYS:
        assert key not in RUN_SUMMARY_SCHEMA, (
            f"Envelope key {key!r} must not appear in RUN_SUMMARY_SCHEMA"
        )
