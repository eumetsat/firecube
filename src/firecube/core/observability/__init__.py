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

"""Firecube observability helpers (logging, tracing, telemetry).

This package is the single public surface area used by CLI/API entrypoints.
Plugins should only interact with observability via the injected
`IngestContext.telemetry` sink (see `telemetry/`).
"""

from firecube.core.observability._state import is_initialized, set_initialized
from firecube.core.observability.logging import configure_logging
from firecube.core.observability.telemetry import (
    IngestionTelemetry,
    NoopIngestionTelemetry,
    create_ingestion_telemetry,
)
from firecube.core.observability.tracing import (
    attach_context,
    capture_context,
    configure_tracing,
    detach_context,
    propagated_context,
    set_current_span_attribute,
    shutdown_tracing,
    span,
)


def init_observability(service_name: str = "firecube-service"):
    """Unified entrypoint for logs and tracing (idempotent per process)."""
    if is_initialized():
        return
    configure_logging()
    configure_tracing(service_name)
    set_initialized(True)


def shutdown_observability(timeout_millis: int = 5000) -> None:
    """Shut down all observability providers and reset the initialized flag."""
    shutdown_tracing(timeout_millis=timeout_millis)
    set_initialized(False)


__all__ = [
    "IngestionTelemetry",
    "NoopIngestionTelemetry",
    "attach_context",
    "capture_context",
    "configure_logging",
    "configure_tracing",
    "create_ingestion_telemetry",
    "detach_context",
    "init_observability",
    "is_initialized",
    "propagated_context",
    "set_current_span_attribute",
    "set_initialized",
    "shutdown_observability",
    "span",
]
