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

from contextlib import contextmanager
from typing import Any

from click.testing import CliRunner
from opentelemetry.sdk.trace import TracerProvider

import firecube.cli.main as cli_main
from firecube.core import observability as observability_module
from firecube.core.observability import (
    init_observability,
    is_initialized,
    set_initialized,
    shutdown_observability,
    tracing,
)
from firecube.core.observability.tracing import configure_tracing, shutdown_tracing


def setup_function() -> None:
    tracing._TRACER_PROVIDER = None
    tracing.trace.set_tracer_provider = lambda provider: None
    set_initialized(False)


def teardown_function() -> None:
    tracing._TRACER_PROVIDER = None
    set_initialized(False)


def test_provider_stored(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_DEBUG", raising=False)

    provider = configure_tracing("test")

    assert tracing._TRACER_PROVIDER is provider
    assert isinstance(tracing._TRACER_PROVIDER, TracerProvider)


def test_shutdown_clears_provider(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_DEBUG", raising=False)

    configure_tracing("test")

    assert shutdown_tracing() is True
    assert tracing._TRACER_PROVIDER is None


def test_shutdown_no_provider():
    tracing._TRACER_PROVIDER = None

    assert shutdown_tracing() is False
    assert tracing._TRACER_PROVIDER is None


def test_shutdown_handles_exception():
    class FailingProvider:
        def force_flush(self, timeout_millis: int):
            return True

        def shutdown(self):
            raise RuntimeError("boom")

    tracing._TRACER_PROVIDER = FailingProvider()

    assert shutdown_tracing() is False
    assert tracing._TRACER_PROVIDER is None


def test_shutdown_after_init(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_DEBUG", raising=False)

    init_observability("test")

    assert is_initialized() is True

    shutdown_observability()

    assert is_initialized() is False
    assert tracing._TRACER_PROVIDER is None


def test_shutdown_idempotent(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_DEBUG", raising=False)

    init_observability("test")
    assert is_initialized() is True

    shutdown_observability()
    assert is_initialized() is False
    assert tracing._TRACER_PROVIDER is None

    shutdown_observability()
    assert is_initialized() is False
    assert tracing._TRACER_PROVIDER is None


def test_ingest_cli_opens_operator_span(monkeypatch, tmp_path):
    spans: list[tuple[str, dict[str, Any] | None]] = []

    @contextmanager
    def record_span(name: str, attributes: dict[str, Any] | None = None):
        spans.append((name, attributes))
        yield

    class ProbeIngestor:
        PRODUCT_NAME = "probe_product"

    def fail_after_span(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("stop after span")

    source = tmp_path / "source"
    source.mkdir()

    monkeypatch.setattr(cli_main, "span", record_span)
    monkeypatch.setattr(observability_module, "init_observability", lambda service_name: None)
    monkeypatch.setattr(cli_main, "discover_ingestors", lambda: {"probe": ProbeIngestor})
    monkeypatch.setattr(cli_main.ProductResolver, "resolve", staticmethod(fail_after_span))

    result = CliRunner().invoke(
        cli_main.cli,
        [
            "ingest",
            "probe",
            "--input-data",
            str(source),
            "--target",
            (tmp_path / "out.zarr").as_uri(),
            "--product-name",
            "probe_product",
            "--write-mode",
            "staged",
        ],
    )

    assert result.exit_code != 0
    assert spans == [
        (
            "firecube.cli.ingest",
            {"firecube.plugin": "probe", "firecube.write_mode": "staged"},
        )
    ]
