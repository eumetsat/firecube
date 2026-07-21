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

import logging
import os
import re
import sys
from contextlib import contextmanager
from typing import Any, Literal

import psutil
from opentelemetry import trace
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    delete_from_gateway,
    push_to_gateway,
)

from firecube.core.config import load_config_file

log = logging.getLogger("firecube.core.observability.telemetry")

_LABEL_KEY_RE = re.compile(r"[^a-zA-Z0-9_]")
_METRIC_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_label_key(key: str) -> str:
    """Normalize a label key to Prometheus-compatible format."""
    safe = _LABEL_KEY_RE.sub("_", str(key).strip())
    safe = safe.strip("_") or "label"
    if not safe[0].isalpha() and safe[0] != "_":
        safe = "_" + safe
    return safe[:64]


def sanitize_label_value(value: Any) -> str:
    """Normalize label values and cap potentially high-cardinality inputs."""
    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        if len(items) > 5:
            return ""
        parts: list[str] = []
        for item in items:
            string_value = str(item)
            if len(string_value) > 64:
                return ""
            parts.append(string_value)
        return ",".join(parts)
    if isinstance(value, dict):
        return ""
    return str(value)[:128]


def sanitize_metric_name(name: str) -> str:
    """Normalize metric names and enforce the `firecube_` prefix."""
    base = str(name).strip()
    if not base:
        base = "metric"
    if not base.startswith("firecube_"):
        base = "firecube_" + base
    base = _METRIC_NAME_RE.sub("_", base)
    if base[0].isdigit():
        base = "firecube_" + base
    return base


def normalize_name_for_kind(name: str, kind: Literal["gauge", "counter"]) -> str:
    """Normalize metric suffixes according to metric kind."""
    base = str(name).strip()
    if kind == "counter" and not base.endswith("_total"):
        base = base + "_total"
    return base


def default_allowed_meta_keys() -> list[str]:
    """Return default allowlist for telemetry metadata labels."""
    return [
        "group",
        "forecast_horizon_hours",
        "msg_region",
        "satellite",
        "status",
        "error_type",
        "resolution",
    ]


def load_pushgateway_url() -> str | None:
    """Resolve pushgateway URL from env vars or config file."""
    env_url = os.getenv("FIRECUBE_PUSHGATEWAY_URL")
    if env_url:
        return str(env_url).strip() or None

    cfg = load_config_file()
    metrics_cfg = cfg.get("metrics", {}) if isinstance(cfg, dict) else {}
    if isinstance(metrics_cfg, dict):
        url = metrics_cfg.get("pushgateway_url")
        if url:
            return str(url).strip() or None
    return None


def _iter_allowed_meta_keys(value: Any) -> list[str]:
    """Normalize config or env label lists into a flat string list.

    The sink stays generic by accepting either a TOML list or a comma-separated
    string. That keeps new product labels opt-in through configuration rather
    than requiring code changes in the telemetry sink.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def load_allowed_meta_keys() -> list[str]:
    """Load label allowlist from defaults plus config/env overrides."""
    keys = set(default_allowed_meta_keys())

    cfg = load_config_file()
    metrics_cfg = cfg.get("metrics", {}) if isinstance(cfg, dict) else {}
    if isinstance(metrics_cfg, dict):
        for raw in _iter_allowed_meta_keys(metrics_cfg.get("label_allowlist")):
            keys.add(raw)

    for raw in _iter_allowed_meta_keys(os.getenv("FIRECUBE_METRICS_LABEL_ALLOWLIST", "")):
        keys.add(raw)

    return sorted({sanitize_label_key(key) for key in keys})


class PushGatewayIngestionTelemetry:
    """Telemetry sink that buffers metrics and pushes once to Pushgateway."""

    @property
    def run_id(self) -> str:
        return self._run_id

    def __init__(
        self,
        *,
        plugin: str,
        product: str,
        output_format: str | None = None,
        write_mode: str | None = None,
        run_id: str,
        pushgateway_url: str,
        base_meta: dict[str, Any] | None = None,
    ):
        self._plugin = str(plugin)
        self._product = str(product)
        self._output_format = str(output_format or "")
        self._write_mode = str(write_mode or "")
        self._run_id = str(run_id)
        self._gateway = str(pushgateway_url).strip()
        self._tracer = trace.get_tracer(f"firecube.ingestor.{self._plugin}")

        self._instance = str(os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "host")
        self._job = str(os.getenv("FIRECUBE_PUSHGATEWAY_JOB") or "firecube").strip() or "firecube"
        self._delete_after_push = str(
            os.getenv("FIRECUBE_PUSHGATEWAY_DELETE_AFTER_PUSH") or "false"
        ).lower() in {"1", "true", "yes", "on"}

        self._allowed_meta_keys = load_allowed_meta_keys()
        include_run_id_label = str(
            os.getenv("FIRECUBE_METRICS_INCLUDE_RUN_ID_LABEL") or "false"
        ).lower() in {"1", "true", "yes", "on"}
        self._include_run_id_label = bool(include_run_id_label)
        base_labels = ["plugin", "product", "output_format", "write_mode"]
        if self._include_run_id_label:
            base_labels.append("run_id")
        self._labelnames = [*base_labels, *self._allowed_meta_keys]
        self._registry = CollectorRegistry()
        self._gauges: dict[str, Gauge] = {}
        self._counters: dict[str, Counter] = {}
        self._base_meta = dict(base_meta or {})

        raw_keys = str(os.getenv("FIRECUBE_PUSHGATEWAY_GROUPING_KEYS") or "instance,plugin,product")
        grouping_keys = [sanitize_label_key(key) for key in raw_keys.split(",") if key.strip()]
        grouping: dict[str, str] = {}
        for key in grouping_keys:
            if key == "plugin":
                grouping[key] = sanitize_label_value(self._plugin)
            elif key == "product":
                grouping[key] = sanitize_label_value(self._product)
            elif key == "run_id":
                grouping[key] = sanitize_label_value(self._run_id)
            elif key == "instance":
                grouping[key] = sanitize_label_value(self._instance)
            else:
                env_key = f"FIRECUBE_PUSHGATEWAY_GROUP_{key.upper()}"
                grouping[key] = sanitize_label_value(os.getenv(env_key))
        self._grouping_key = {k: v for k, v in grouping.items() if isinstance(v, str) and v != ""}

    def _labels(self, meta: dict[str, Any] | None) -> dict[str, str]:
        merged = dict(self._base_meta)
        if isinstance(meta, dict):
            merged.update(meta)

        labels: dict[str, str] = {
            "plugin": self._plugin,
            "product": self._product,
            "output_format": self._output_format,
            "write_mode": self._write_mode,
        }
        if self._include_run_id_label:
            labels["run_id"] = self._run_id
        for key in self._allowed_meta_keys:
            labels[key] = sanitize_label_value(merged.get(key))
        return labels

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: Literal["gauge", "counter"] = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        metric_name = sanitize_metric_name(normalize_name_for_kind(name, kind))
        labels = self._labels(meta)

        if kind == "counter":
            counter = self._counters.get(metric_name)
            if counter is None:
                counter = Counter(
                    metric_name,
                    "Firecube emitted counter",
                    self._labelnames,
                    registry=self._registry,
                )
                self._counters[metric_name] = counter
            try:
                counter.labels(**labels).inc(float(value))
            except Exception:
                return None
            return None

        gauge = self._gauges.get(metric_name)
        if gauge is None:
            gauge = Gauge(
                metric_name, "Firecube emitted gauge", self._labelnames, registry=self._registry
            )
            self._gauges[metric_name] = gauge
        try:
            gauge.labels(**labels).set(float(value))
        except Exception:
            return None
        return None

    def flush(self) -> None:
        if not self._gauges and not self._counters:
            return None
        try:
            push_to_gateway(
                self._gateway,
                job=self._job,
                grouping_key=self._grouping_key or None,
                registry=self._registry,
            )
        except Exception as exc:
            log.warning(
                "Failed to push metrics to Pushgateway",
                extra={
                    "pushgateway_url": self._gateway,
                    "job": self._job,
                    "grouping_key": dict(self._grouping_key),
                    "error": str(exc),
                },
            )
            return None
        if not self._delete_after_push:
            return None
        try:
            delete_from_gateway(
                self._gateway,
                job=self._job,
                grouping_key=self._grouping_key or None,
            )
        except Exception as exc:
            log.warning(
                "Failed to delete metrics from Pushgateway after push",
                extra={
                    "pushgateway_url": self._gateway,
                    "job": self._job,
                    "grouping_key": dict(self._grouping_key),
                    "error": str(exc),
                },
            )
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
        try:
            try:
                import resource

                usage = resource.getrusage(resource.RUSAGE_SELF)
                rss = usage.ru_maxrss
                if sys.platform != "darwin":
                    rss *= 1024
                self.emit("process_memory_peak_rss_bytes", float(rss), kind="gauge")
            except ImportError:
                process = psutil.Process()
                mem_info = process.memory_info()
                self.emit("process_memory_rss_bytes", float(mem_info.rss), kind="gauge")
        except Exception:
            pass
