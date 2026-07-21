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

from firecube.core.observability.telemetry import PushGatewayIngestionTelemetry


def test_pushgateway_flush_pushes_once(monkeypatch):
    calls: list[str] = []

    def fake_push_to_gateway(*args, **kwargs):
        calls.append("push")

    monkeypatch.setattr(
        "firecube.core.observability.telemetry.pushgateway.push_to_gateway",
        fake_push_to_gateway,
    )

    telemetry = PushGatewayIngestionTelemetry(
        plugin="plugin",
        product="product",
        run_id="run-1",
        pushgateway_url="http://pushgateway:9091",
    )
    telemetry.emit("x", 1.0, kind="gauge")
    telemetry.flush()

    assert calls == ["push"]


def test_pushgateway_flush_deletes_when_enabled(monkeypatch):
    calls: list[str] = []

    def fake_push_to_gateway(*args, **kwargs):
        calls.append("push")

    def fake_delete_from_gateway(*args, **kwargs):
        calls.append("delete")

    monkeypatch.setenv("FIRECUBE_PUSHGATEWAY_DELETE_AFTER_PUSH", "true")
    monkeypatch.setattr(
        "firecube.core.observability.telemetry.pushgateway.push_to_gateway",
        fake_push_to_gateway,
    )
    monkeypatch.setattr(
        "firecube.core.observability.telemetry.pushgateway.delete_from_gateway",
        fake_delete_from_gateway,
    )

    telemetry = PushGatewayIngestionTelemetry(
        plugin="plugin",
        product="product",
        run_id="run-2",
        pushgateway_url="http://pushgateway:9091",
    )
    telemetry.emit("x", 1.0, kind="counter")
    telemetry.flush()

    assert calls == ["push", "delete"]
