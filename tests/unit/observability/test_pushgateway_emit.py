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


def test_pushgateway_emit_creates_counter_and_gauge():
    telemetry = PushGatewayIngestionTelemetry(
        plugin="plugin",
        product="product",
        run_id="run-1",
        pushgateway_url="http://pushgateway:9091",
    )
    telemetry.emit("custom_counter", 2.0, kind="counter")
    telemetry.emit("custom_gauge", 3.0, kind="gauge")

    assert "firecube_custom_counter_total" in telemetry._counters
    assert "firecube_custom_gauge" in telemetry._gauges
