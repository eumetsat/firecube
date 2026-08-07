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

"""Build-time macros for mkdocs-macros-plugin.

Register macros with ``@env.macro`` inside ``define_env``.
These macros are called as ``{{ macro_name() }}`` in Markdown pages.
"""

from __future__ import annotations

import importlib.metadata
import os
import sys
import tomllib

# Allow importing from src/ without installing the package in editable mode
_src = os.path.join(os.path.dirname(__file__), "..", "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from firecube.core.observability.metrics import RUN_SUMMARY_SCHEMA  # noqa: E402

# Human-readable descriptions for each summary key.
# Kept here so docs/_macros.py is the single source for doc prose;
# the metric names and kinds come from RUN_SUMMARY_SCHEMA.
_METRIC_DESCRIPTIONS: dict[str, str] = {
    "workers": "Pipeline worker count",
    "batch_size": "Configured batch size",
    "batches_total": "Completed batch count",
    "batches_failed": "Failed batch count",
    "hook_failures": "Non-fatal lifecycle hook failures",
    "files_processed": "Source files processed",
    "bytes_ingested": "Source bytes ingested",
    "rows_processed": "Rows processed when available",
    "duration_total_s": "End-to-end run duration",
    "duration_pipeline_s": "Pipeline duration before staged upload",
    "duration_processing_s": "Batch processing duration",
    "duration_batch_creation_s": "Batch creation duration",
    "duration_upload_s": "Staged upload duration",
    "duration_cpu_s": "Estimated CPU time",
    "non_cpu_wait_s": "Estimated non-CPU wait time",
    "cpu_utilization_estimate": "Estimated CPU utilization, bounded from 0 to 1",
    "storage_client_requests": "Storage client request count",
    "storage_client_errors": "Storage client errors",
    "storage_client_retryable_errors": "Retryable storage client errors",
    "storage_client_latency_s_total": "Total observed storage client latency",
    "storage_client_bytes_read": "Bytes read by storage client",
    "storage_client_bytes_written": "Bytes written by storage client",
    "wal_corruption_count": "Control-plane WAL corruption events",
    "wal_torn_tail_recovery_count": "Torn WAL tails recovered automatically",
    "wal_snapshot_rebuild_duration_s": "Snapshot rebuild duration",
    "wal_snapshot_rebuild_count": "Snapshot rebuild count",
}

# Observability environment variables grouped by subsystem.
# Source of truth: src/firecube/core/observability/{logging,tracing,telemetry/}.
_ENV_VARS: list[dict[str, str]] = [
    # Logging
    {
        "name": "FIRECUBE_LOG_FORMAT",
        "default": "`json`",
        "description": "`json` or `plain`",
    },
    {
        "name": "FIRECUBE_LOG_LEVEL",
        "default": "`INFO`",
        "description": "Root logging level",
    },
    {
        "name": "FIRECUBE_LOG_STRUCTURED_FIELDS",
        "default": "default JSON field list",
        "description": "Comma-separated JSON log fields",
    },
    {
        "name": "FIRECUBE_DEBUG",
        "default": "unset",
        "description": "Enables DEBUG logs for Firecube namespaces",
    },
    # Tracing
    {
        "name": "OTEL_EXPORTER_OTLP_ENDPOINT",
        "default": "unset",
        "description": "OTLP HTTP trace export endpoint",
    },
    {
        "name": "OTEL_EXPORTER_OTLP_HEADERS",
        "default": "unset",
        "description": "Headers passed to the OTLP exporter",
    },
    {
        "name": "OTEL_DEBUG",
        "default": "`false`",
        "description": "Emits spans to the console when no OTLP endpoint is set",
    },
    {
        "name": "KFP_RUN_ID",
        "default": "unset",
        "description": "Adds `kfp_run_id` to telemetry spans",
    },
    # Metrics / Pushgateway
    {
        "name": "FIRECUBE_PUSHGATEWAY_URL",
        "default": "unset",
        "description": "Pushgateway URL",
    },
    {
        "name": "FIRECUBE_METRICS_DISABLED",
        "default": "`false`",
        "description": "Disable metric pushing",
    },
    {
        "name": "FIRECUBE_METRICS_LABEL_ALLOWLIST",
        "default": "unset",
        "description": "Additional allowed metadata label keys",
    },
    {
        "name": "FIRECUBE_METRICS_INCLUDE_RUN_ID_LABEL",
        "default": "`false`",
        "description": "Add `run_id` as a metric label; not recommended",
    },
    {
        "name": "FIRECUBE_PUSHGATEWAY_JOB",
        "default": "`firecube`",
        "description": "Pushgateway job name",
    },
    {
        "name": "FIRECUBE_PUSHGATEWAY_GROUPING_KEYS",
        "default": "`instance,plugin,product`",
        "description": "Comma-separated grouping keys",
    },
    {
        "name": "FIRECUBE_PUSHGATEWAY_GROUP_<KEY>",
        "default": "unset",
        "description": "Value for custom grouping key `<key>`",
    },
    {
        "name": "FIRECUBE_PUSHGATEWAY_DELETE_AFTER_PUSH",
        "default": "`false`",
        "description": "Delete pushed metrics after a successful push",
    },
]


def define_env(env):
    """Register macros available in all Markdown pages."""

    @env.macro
    def render_metrics_table() -> str:
        """Render a Markdown table of pipeline metrics from RUN_SUMMARY_SCHEMA."""
        header = "| Summary key | Prometheus metric | Kind | Description |"
        separator = "|---|---|---|---|"
        rows = [header, separator]
        for key, spec in RUN_SUMMARY_SCHEMA.items():
            description = _METRIC_DESCRIPTIONS.get(key, "")
            rows.append(f"| `{key}` | `{spec.metric_name}` | {spec.kind} | {description} |")
        return "\n".join(rows)

    @env.macro
    def render_env_vars() -> str:
        """Render a Markdown table of observability env vars."""
        header = "| Variable | Default | Purpose |"
        separator = "|---|---|---|"
        rows = [header, separator]
        rows.extend(
            f"| `{var['name']}` | {var['default']} | {var['description']} |" for var in _ENV_VARS
        )
        return "\n".join(rows)

    @env.macro
    def render_event_types() -> str:
        """Render a Markdown table of control-plane event types from EVENT_* constants."""
        from firecube.core.controlplane import types

        _descriptions: dict[str, str] = {
            "run_started": "A new ingestion run started",
            "run_completed": "Run finished successfully",
            "run_failed": "Run terminated with error",
            "run_abandoned": "Run explicitly abandoned (stale or interrupted)",
            "span_committed": "A batch span was successfully committed to the WAL",
            "span_failed": "A batch span failed during write",
            "span_noop": "A batch span was a no-op (already covered)",
            "record_replaced": "An existing WAL record was replaced",
            "record_upsert": "A new WAL record was created or updated",
            "schema_verification": "Schema verification check recorded",
            "run_started_with_replacement": "Run started, replacing a prior abandoned run",
            "replacement_committed": "Replacement run commit finalized",
            "maintenance_started": "Maintenance operation started (delete, scrub, or archive restore)",
            "maintenance_completed": "Maintenance operation completed successfully",
            "maintenance_failed": "Maintenance operation failed",
        }

        header = "| Constant | Event Type String | Description |"
        separator = "|---|---|---|"
        rows = [header, separator]
        for name in sorted(n for n in dir(types) if n.startswith("EVENT_")):
            value = getattr(types, name)
            description = _descriptions.get(value, "")
            rows.append(f"| `{name}` | `{value}` | {description} |")
        return "\n".join(rows)

    @env.macro
    def firecube_version() -> str:
        """Return the Firecube package version for rendered docs."""
        try:
            return importlib.metadata.version("firecube")
        except importlib.metadata.PackageNotFoundError:
            version = None

        try:
            from firecube._version import get_version
        except ImportError:
            version = None
        else:
            version = get_version()
            if version != "0.0.0+unknown":
                return version

        pyproject = os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
        try:
            with open(pyproject, "rb") as file:
                version = tomllib.load(file).get("project", {}).get("version")
        except (OSError, tomllib.TOMLDecodeError):
            version = None
        return version if isinstance(version, str) and version else "0.0.0+unknown"
