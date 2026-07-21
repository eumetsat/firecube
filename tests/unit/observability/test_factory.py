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

from firecube.core.observability.telemetry import (
    CompositeIngestionTelemetry,
    TracingIngestionTelemetry,
    create_ingestion_telemetry,
)


def test_factory_returns_tracing_only_when_metrics_disabled(monkeypatch):
    calls: list[str] = []

    def fake_push_to_gateway(*args, **kwargs):
        _ = (args, kwargs)
        calls.append("push")

    monkeypatch.setattr(
        "firecube.core.observability.telemetry.pushgateway.push_to_gateway",
        fake_push_to_gateway,
    )
    monkeypatch.setenv("FIRECUBE_METRICS_DISABLED", "true")
    monkeypatch.setenv("FIRECUBE_PUSHGATEWAY_URL", "http://pushgateway:9091")
    telemetry = create_ingestion_telemetry(
        plugin="p",
        product="x",
        run_id="r1",
    )
    assert isinstance(telemetry, TracingIngestionTelemetry)
    telemetry.emit("probe_metric", 1.0, kind="gauge")
    telemetry.flush()
    assert calls == []


def test_factory_returns_composite_when_pushgateway_available(monkeypatch):
    calls: list[str] = []

    def fake_push_to_gateway(*args, **kwargs):
        _ = (args, kwargs)
        calls.append("push")

    monkeypatch.setattr(
        "firecube.core.observability.telemetry.pushgateway.push_to_gateway",
        fake_push_to_gateway,
    )
    monkeypatch.delenv("FIRECUBE_METRICS_DISABLED", raising=False)
    monkeypatch.setenv("FIRECUBE_PUSHGATEWAY_URL", "http://pushgateway:9091")
    telemetry = create_ingestion_telemetry(
        plugin="p",
        product="x",
        run_id="r2",
    )
    assert isinstance(telemetry, CompositeIngestionTelemetry)
    telemetry.emit("probe_metric", 1.0, kind="gauge")
    telemetry.flush()
    assert calls == ["push"]
