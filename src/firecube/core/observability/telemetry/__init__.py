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

import os

from firecube.core.observability.telemetry.pushgateway import (
    PushGatewayIngestionTelemetry,
    load_pushgateway_url,
    normalize_name_for_kind,
    sanitize_label_key,
    sanitize_label_value,
    sanitize_metric_name,
)
from firecube.core.observability.telemetry.sinks import (
    CompositeIngestionTelemetry,
    IngestionTelemetry,
    NoopIngestionTelemetry,
    TracingIngestionTelemetry,
)


def create_ingestion_telemetry(
    *,
    plugin: str,
    product: str,
    output_format: str | None = None,
    write_mode: str | None = None,
    run_id: str,
    base_meta: dict[str, object] | None = None,
) -> IngestionTelemetry:
    """Create telemetry sinks for a batch ingestion run."""
    sinks: list[IngestionTelemetry] = []
    sinks.append(TracingIngestionTelemetry(plugin=plugin, run_id=run_id))

    metrics_disabled = os.getenv("FIRECUBE_METRICS_DISABLED", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not metrics_disabled:
        gateway = load_pushgateway_url()
        if gateway:
            sinks.append(
                PushGatewayIngestionTelemetry(
                    plugin=plugin,
                    product=product,
                    output_format=output_format,
                    write_mode=write_mode,
                    run_id=run_id,
                    pushgateway_url=gateway,
                    base_meta=base_meta,
                )
            )

    if not sinks:
        return NoopIngestionTelemetry(run_id=run_id)
    if len(sinks) == 1:
        return sinks[0]
    return CompositeIngestionTelemetry(sinks)


__all__ = [
    "CompositeIngestionTelemetry",
    "IngestionTelemetry",
    "NoopIngestionTelemetry",
    "PushGatewayIngestionTelemetry",
    "TracingIngestionTelemetry",
    "create_ingestion_telemetry",
    "normalize_name_for_kind",
    "sanitize_label_key",
    "sanitize_label_value",
    "sanitize_metric_name",
]
