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
import re
import sys
import tomllib

# Allow importing from src/ without installing the package in editable mode
_src = os.path.join(os.path.dirname(__file__), "..", "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from firecube.core.observability.metrics import RUN_SUMMARY_SCHEMA  # noqa: E402

# Sphinx-style roles occasionally appear in first docstring lines; render the
# target as inline code in summary tables.
_RST_ROLE = re.compile(r":(?:class|func|meth|mod|attr|obj|exc|data):`(~?[^`]+)`")

_GRIFFE_MODEL = None


def _strip_rst_roles(text: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        target = match.group(1)
        if target.startswith("~"):
            target = target[1:].rsplit(".", 1)[-1]
        return f"`{target}`"

    return _RST_ROLE.sub(_replace, text)


def _load_firecube_model():
    """Load the firecube package as a static griffe model (no code is imported)."""
    global _GRIFFE_MODEL
    if _GRIFFE_MODEL is None:
        import griffe

        _GRIFFE_MODEL = griffe.load("firecube", search_paths=[_src], submodules=True)
    return _GRIFFE_MODEL


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
    def render_api_summary(module: str, names: list[str]) -> str:
        """Render a summary table of public API names with one-line descriptions.

        ``module`` is a public facade path such as ``firecube.ingestor.api``;
        ``names`` are exported names (dotted member paths are allowed). Each row
        links to the full reference entry through mkdocstrings-autorefs, and the
        description is the first docstring line read statically with griffe, so
        the table cannot drift from the source. Unknown names raise at build
        time and fail ``mkdocs build --strict``.
        """
        model = _load_firecube_model()
        rows = ["| Name | Description |", "|---|---|"]
        for name in names:
            obj = model[f"{module.removeprefix('firecube.')}.{name}"]
            target = obj.final_target if obj.is_alias else obj
            doc = target.docstring.value if target.docstring else ""
            first = _strip_rst_roles(doc.strip().split("\n", 1)[0]).replace("|", "\\|")
            rows.append(f"| [`{name}`][{module}.{name}] | {first} |")
        return "\n".join(rows)

    @env.macro
    def render_storage_driver_capabilities() -> str:
        """Render optional capabilities declared by each filesystem backend."""
        from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
        from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
        from firecube.core.filesystem.protocol import Multipart, RangedRead, Signer

        drivers = {
            "fsspec": FsspecFilesystem,
            "obstore": ObstoreFilesystem,
        }
        capability_types = (Multipart, RangedRead, Signer)
        declared: dict[str, set[type]] = {}
        for name, driver_type in drivers.items():
            instance = object.__new__(driver_type)
            declared[name] = driver_type.capabilities(instance)

        header = "| Capability | `fsspec` | `obstore` |"
        separator = "|---|:---:|:---:|"
        rows = [header, separator]
        for capability in capability_types:
            cells = ["yes" if capability in declared[name] else "no" for name in drivers]
            rows.append(f"| `{capability.__name__}` | {cells[0]} | {cells[1]} |")
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
