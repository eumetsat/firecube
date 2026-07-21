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

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry import context as _otel_context
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

_TRACER_PROVIDER: TracerProvider | None = None


def configure_tracing(service_name: str = "firecube-service"):
    """Configure OpenTelemetry tracing based on environment variables."""
    global _TRACER_PROVIDER
    if _TRACER_PROVIDER is not None:
        return _TRACER_PROVIDER

    debug = os.getenv("OTEL_DEBUG", "false").lower() in ("1", "true", "yes")
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    headers = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")
    existing_provider = getattr(trace, "_TRACER_PROVIDER", None)

    if isinstance(existing_provider, TracerProvider):
        provider = existing_provider
    else:
        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(provider)

    if endpoint:
        exporter = OTLPSpanExporter(endpoint=endpoint, headers=headers)  # type: ignore[arg-type]
        provider.add_span_processor(BatchSpanProcessor(exporter))
        logging.info("Tracing configured (OTLP endpoint=%s)", endpoint)
    elif debug:
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
        logging.info("Tracing in DEBUG console mode (no OTLP endpoint)")
    else:
        logging.debug("Tracing disabled (no OTLP endpoint, debug off)")

    _TRACER_PROVIDER = provider

    return provider


def shutdown_tracing(timeout_millis: int = 5000) -> bool:
    """Flush pending spans and shut down the active TracerProvider.

    Returns True on clean shutdown, False if no provider is active or shutdown fails.
    Uses half the given timeout for force_flush and the full timeout for provider shutdown.
    """
    global _TRACER_PROVIDER
    provider = _TRACER_PROVIDER
    if provider is None:
        return False
    try:
        provider.force_flush(timeout_millis=timeout_millis // 2)
    except Exception as exc:
        logging.warning("TracerProvider force_flush failed: %s", exc)
    try:
        provider.shutdown()
        _TRACER_PROVIDER = None
        return True
    except Exception as exc:
        logging.warning("TracerProvider shutdown failed: %s", exc)
        _TRACER_PROVIDER = None
        return False


def span(name: str, attributes: dict[str, Any] | None = None):
    """Open a named span as a context manager."""
    tracer = trace.get_tracer("firecube")
    return tracer.start_as_current_span(name, attributes=attributes)


def set_current_span_attribute(key: str, value: Any) -> None:
    """Set an attribute on the currently active span. No-op if no active span."""
    trace.get_current_span().set_attribute(key, value)


def capture_context() -> Context:
    """Capture the current OTel context for cross-thread propagation."""
    return _otel_context.get_current()


def attach_context(ctx: Context) -> object:
    """Attach a captured context; returns a token for detach()."""
    return _otel_context.attach(ctx)


def detach_context(token: object) -> None:
    """Detach a previously attached context."""
    _otel_context.detach(token)  # type: ignore[arg-type]


@contextmanager
def propagated_context(ctx: Context) -> Iterator[None]:
    """Context manager that attaches ctx on enter and detaches on exit."""
    token = attach_context(ctx)
    try:
        yield
    finally:
        detach_context(token)
