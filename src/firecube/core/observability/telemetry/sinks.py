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
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import Any, Literal, Protocol

from opentelemetry import trace


class IngestionTelemetry(Protocol):
    """Backend-agnostic ingestion telemetry interface (metrics + tracing)."""

    @property
    def run_id(self) -> str: ...

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: Literal["gauge", "counter"] = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None: ...

    def flush(self) -> None: ...

    def span(
        self, name: str, attributes: dict[str, Any] | None = None
    ) -> AbstractContextManager[None]: ...

    def collect_memory_stats(self) -> None: ...


class NoopIngestionTelemetry:
    """No-op telemetry sink used when telemetry is not configured."""

    def __init__(self, run_id: str = "unknown"):
        self._run_id = str(run_id)

    @property
    def run_id(self) -> str:
        return self._run_id

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: Literal["gauge", "counter"] = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        _ = (name, value, kind, meta)
        return None

    def flush(self) -> None:
        return None

    def span(self, name: str, attributes: dict[str, Any] | None = None):
        _ = (name, attributes)
        return nullcontext()

    def collect_memory_stats(self) -> None:
        return None


class TracingIngestionTelemetry:
    """Tracing-only telemetry sink that keeps span context active."""

    def __init__(self, *, plugin: str, run_id: str):
        self._plugin = str(plugin)
        self._run_id = str(run_id)
        self._tracer = trace.get_tracer(f"firecube.ingestor.{self._plugin}")

    @property
    def run_id(self) -> str:
        return self._run_id

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: Literal["gauge", "counter"] = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        _ = (name, value, kind, meta)
        return None

    def flush(self) -> None:
        return None

    @contextmanager
    def span(self, name: str, attributes: dict[str, Any] | None = None):
        attrs = dict(attributes or {})
        kfp_run_id = os.getenv("KFP_RUN_ID")
        if kfp_run_id:
            attrs["kfp_run_id"] = kfp_run_id

        with self._tracer.start_as_current_span(str(name), attributes=attrs):
            yield

    def collect_memory_stats(self) -> None:
        return None


class CompositeIngestionTelemetry:
    """Dispatches telemetry operations to multiple sinks."""

    def __init__(self, sinks: list[IngestionTelemetry]):
        self._sinks = sinks

    @property
    def run_id(self) -> str:
        return self._sinks[0].run_id if self._sinks else "unknown"

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: Literal["gauge", "counter"] = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        for sink in self._sinks:
            sink.emit(name, value, kind=kind, meta=meta)

    def flush(self) -> None:
        for sink in self._sinks:
            sink.flush()

    def collect_memory_stats(self) -> None:
        for sink in self._sinks:
            sink.collect_memory_stats()

    @contextmanager
    def span(self, name: str, attributes: dict[str, Any] | None = None):
        if not self._sinks:
            yield
            return
        with self._sinks[0].span(name, attributes):
            yield
