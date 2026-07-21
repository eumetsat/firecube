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

"""Shared datatypes for ingestion plugins and runners."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from .result_metrics import (  # pyright: ignore[reportMissingImports]
    OutputPaths,
    ResultMetrics,
    _coerce_output_paths,
    _coerce_result_metrics,
)

if TYPE_CHECKING:
    from firecube.core.storage import StorageWriteResult
    from firecube.core.storage.session import StorageSession

    # Avoid circular import at runtime
    from firecube.ingestor.contracts.interfaces import IngestionTelemetry


# Internal constants
BATCH_META_TIMESTAMPS = "timestamps"


@dataclass(slots=True)
class StorageContext:
    """Named storage role bindings for an ingestion run."""

    output: StorageSession | None = None


@dataclass(slots=True)
class IngestContext:
    """Caller-provided input context for one ingestion run."""

    source: str
    target: str | None = None
    in_memory: bool = True
    output_format: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    storage: StorageContext | None = None

    # Engine-injected runtime configuration (READ-ONLY for plugins)
    run_id: str | None = None

    # Core-injected telemetry sink (metrics + tracing). Plugins should only call
    # this object; they must not import Prometheus/OTel directly.
    telemetry: IngestionTelemetry | None = None

    def option(self, key: str, default: Any = None) -> Any:
        """Convenience accessor for optional plugin parameters."""
        return self.options.get(key, default)


@dataclass(slots=True)
class RuntimeIdentity:
    """Run identity and workspace location for one execution."""

    run_id: str
    temp_root: Path | None = None


@dataclass(slots=True)
class RuntimeFlags:
    """Engine-owned execution flags for one run."""

    force_reingest: bool = False
    incremental: bool = False
    dry_run: bool = False


@dataclass(slots=True)
class RuntimeServices:
    """Internal service handles used by runtime-only context behavior."""

    materializer: Any | None = field(default=None, repr=False, compare=False)


@dataclass(slots=True)
class RuntimeIngestContext(IngestContext):
    """Engine-owned runtime context copied from ``IngestContext``.

    This carries internal execution-only state and must never be reused across runs.
    """

    identity: RuntimeIdentity = field(default_factory=lambda: RuntimeIdentity(run_id=""))
    flags: RuntimeFlags = field(default_factory=RuntimeFlags)
    services: RuntimeServices = field(default_factory=RuntimeServices)

    @classmethod
    def from_ingest_context(
        cls,
        ctx: IngestContext,
        *,
        run_id: str,
        temp_root: Path | None,
        materializer: Any,
    ) -> RuntimeIngestContext:
        """Build an isolated runtime copy without mutating the caller context."""
        options = dict(ctx.options or {})
        options.setdefault("run_id", run_id)
        return cls(
            source=ctx.source,
            target=ctx.target,
            in_memory=ctx.in_memory,
            output_format=ctx.output_format,
            options=options,
            storage=ctx.storage,
            run_id=run_id,
            telemetry=ctx.telemetry,
            identity=RuntimeIdentity(run_id=run_id, temp_root=temp_root),
            services=RuntimeServices(materializer=materializer),
        )

    @property
    def temp_root(self) -> Path | None:
        """Per-run workspace root used for materialization and temporary files."""
        return self.identity.temp_root

    @temp_root.setter
    def temp_root(self, value: Path | None) -> None:
        """Update the per-run workspace root for this runtime context."""
        self.identity.temp_root = value

    @property
    def force_reingest(self) -> bool:
        """Whether this run is allowed to overwrite existing slice data."""
        return self.flags.force_reingest

    @force_reingest.setter
    def force_reingest(self, value: bool) -> None:
        """Set overwrite behavior for this runtime execution."""
        self.flags.force_reingest = bool(value)

    @property
    def incremental(self) -> bool:
        """Whether plugin logic should prefer incremental update behavior."""
        return self.flags.incremental

    @incremental.setter
    def incremental(self, value: bool) -> None:
        """Set incremental processing preference for this runtime execution."""
        self.flags.incremental = bool(value)

    @property
    def dry_run(self) -> bool:
        """Whether side-effecting writes should be suppressed when supported."""
        return self.flags.dry_run

    @dry_run.setter
    def dry_run(self, value: bool) -> None:
        """Set dry-run behavior for this runtime execution."""
        self.flags.dry_run = bool(value)

    @property
    def _materializer(self) -> Any | None:
        return self.services.materializer

    @_materializer.setter
    def _materializer(self, value: Any | None) -> None:
        self.services.materializer = value

    def materialize(self, source: Any) -> Path:
        """Ensure the source file is available locally and return its path.

        If the file is remote (e.g. S3), it will be downloaded to the per-run cache.
        If it's already local, the path is returned directly.
        """
        if self.services.materializer:
            return self.services.materializer(source)

        # Fallback for local files if no materializer configured
        if isinstance(source, (str, Path)):
            p = Path(source).resolve()  # firecube: STORAGE-URI
            if p.exists():
                return p

        # Fallback for SourceFile protocol
        local_path = getattr(source, "local_path", None)
        if callable(local_path):
            lp = local_path()
            if isinstance(lp, Path):
                return lp
            if isinstance(lp, str):
                return Path(lp).resolve()

        raise RuntimeError("MaterializationContext not configured: cannot materialize source.")


class PluginContext:
    """Read-only proxy for IngestContext for all plugin-facing hooks."""

    def __init__(self, ctx: RuntimeIngestContext):
        self._ctx = ctx
        raw_options = getattr(ctx, "options", {}) or {}
        if not isinstance(raw_options, dict):
            try:
                raw_options = dict(raw_options)
            except Exception:
                raw_options = {}
        self._options = MappingProxyType(dict(raw_options))

    @property
    def source(self) -> str:
        return self._ctx.source

    @property
    def target(self) -> str | None:
        return self._ctx.target

    @property
    def in_memory(self) -> bool:
        return self._ctx.in_memory

    @property
    def output_format(self) -> str | None:
        return self._ctx.output_format

    @property
    def storage(self) -> StorageContext | None:
        """Storage sessions bound to this plugin run, keyed by role."""
        return self._ctx.storage

    @property
    def temp_root(self) -> Path | None:
        return self._ctx.temp_root

    @property
    def force_reingest(self) -> bool:
        return self._ctx.force_reingest

    @property
    def incremental(self) -> bool:
        return self._ctx.incremental

    @property
    def dry_run(self) -> bool:
        return self._ctx.dry_run

    @property
    def telemetry(self) -> IngestionTelemetry | None:
        return self._ctx.telemetry

    @property
    def options(self) -> dict[str, Any]:
        return self._options  # type: ignore[return-value]

    @property
    def run_id(self) -> str | None:
        """Stable run identifier assigned by the engine for this execution."""
        return self._ctx.run_id

    def option(self, key: str, default: Any = None) -> Any:
        return self._options.get(key, default)

    def materialize(self, source: Any) -> Path:
        return self._ctx.materialize(source)

    def __getattr__(self, name: str) -> Any:
        # PluginContext is intentionally narrow: hooks must only rely on the
        # explicitly exposed read-only API above.
        if name in ("_chunk_manager", "_materializer"):
            raise AttributeError(f"Access to {name} is forbidden in PluginContext")
        raise AttributeError(f"{name} is not available on PluginContext")


@dataclass(slots=True, init=False)
class IngestResult:
    """Structured dataset returned by plugins."""

    output_format: str
    outputs: OutputPaths = field(default_factory=OutputPaths)
    metrics: ResultMetrics = field(default_factory=ResultMetrics)
    registered: bool = False
    spans_recorded: bool = False
    storage_result: StorageWriteResult | None = None
    write_mode_applied: str | None = None
    manifest: dict | None = None

    def __init__(
        self,
        *,
        output_format: str,
        outputs: OutputPaths | None = None,
        metrics: ResultMetrics | dict[str, Any] | None = None,
        registered: bool = False,
        spans_recorded: bool = False,
        storage_result: StorageWriteResult | None = None,
        write_mode_applied: str | None = None,
        manifest: dict | None = None,
    ) -> None:
        self.output_format = output_format
        self.outputs = _coerce_output_paths(outputs, output_format=output_format)
        self.metrics = _coerce_result_metrics(metrics)
        self.registered = registered
        self.spans_recorded = spans_recorded
        self.storage_result = storage_result
        self.write_mode_applied = write_mode_applied
        self.manifest = manifest

    @property
    def output_path(self) -> Path | str | None:
        """Compatibility view of the primary output path."""
        return self.outputs.primary

    def all_outputs(self) -> OutputPaths:
        """Return the typed outputs container for compatibility callers."""
        return self.outputs


@dataclass(slots=True)
class PipelineBatch:
    """A batch of data ready for processing in the pipeline."""

    batch_id: str
    data_path: Path
    items: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    size_bytes: int = 0
    files_count: int = 0
    groups: list[str] = field(
        default_factory=list
    )  # Logical groups (e.g. horizons) covered by this batch


@dataclass(slots=True, init=False)
class PipelineResult:
    """Result from a pipeline batch processing."""

    batch: PipelineBatch
    output_format: str = "zarr"
    outputs: OutputPaths = field(default_factory=OutputPaths)
    duration_s: float = 0.0
    cpu_time_s: float = 0.0
    io_time_s: float = 0.0
    metrics: ResultMetrics = field(default_factory=ResultMetrics)
    success: bool = True
    error: str | None = None

    def __init__(
        self,
        *,
        batch: PipelineBatch,
        outputs: OutputPaths | None = None,
        duration_s: float = 0.0,
        cpu_time_s: float = 0.0,
        io_time_s: float = 0.0,
        metrics: ResultMetrics | dict[str, Any] | None = None,
        success: bool = True,
        error: str | None = None,
        output_format: str = "zarr",
    ) -> None:
        self.batch = batch
        self.output_format = output_format
        self.outputs = _coerce_output_paths(outputs, output_format=output_format)
        self.duration_s = duration_s
        self.cpu_time_s = cpu_time_s
        self.io_time_s = io_time_s
        self.metrics = _coerce_result_metrics(metrics)
        self.success = success
        self.error = error

    @property
    def output_path(self) -> Path | str | None:
        """Compatibility view of the primary output path."""
        return self.outputs.primary


@dataclass(slots=True, frozen=True)
class PipelineRunState:
    """Immutable run snapshot for pipeline orchestration and hooks."""

    product: str
    pipeline_workers: int
    batch_size: int
    batches: tuple[PipelineBatch, ...]
    ingestion_start_time: float
    batch_creation_duration: float
    processing_start_time: float
    processing_duration: float = 0.0
    total_ingestion_duration: float = 0.0
    results: tuple[PipelineResult, ...] = field(default_factory=tuple)
    total_metrics: dict[str, Any] = field(default_factory=dict)
    total_rows_processed: int = 0
    hook_failures: int = 0
    cpu_time_total: float = 0.0
    io_time_total: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "batches", tuple(self.batches))
        object.__setattr__(self, "results", tuple(self.results))
