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

"""Base implementation for Firecube ingestors.

This module provides the `BaseIngestor` abstract base class which orchestrates
ingestion using composition of runtime services.
"""

from __future__ import annotations

import contextlib
import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, cast, get_type_hints

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.metrics import collect_wal_metrics
from firecube.core.filesystem import collect_filesystem_metrics
from firecube.core.formats import discover_input_files
from firecube.core.intake import CatalogGroupInfo
from firecube.core.observability import create_ingestion_telemetry
from firecube.core.observability.metrics import TelemetryService
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.config.engine import SYSTEM_KEYS, EngineConfig, config_keys
from firecube.ingestor.contracts.interfaces import Ingestor, is_dataset_producer
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.aggregation import merge_batch_metrics
from firecube.ingestor.runtime.base_hooks import BaseIngestorHookMixin
from firecube.ingestor.runtime.batching import BatchPlanHost, BatchPlanner
from firecube.ingestor.runtime.configure import (
    ExecutionMode,
    TierConfigurator,
    determine_execution_mode,
    ensure_chunk_manager_config,
    ensure_run_id,
)
from firecube.ingestor.runtime.engine import run_sequential
from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState
from firecube.ingestor.runtime.parallel_run_id import derive_pod_run_id
from firecube.ingestor.runtime.preflight import preflight
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from firecube.ingestor.runtime.workspace import LocalSourceFile, WorkspaceManager
from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility
from firecube.ingestor.templates.config import TemplateConfig
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeFlags,
    RuntimeIngestContext,
)
from firecube.ingestor.types.result_metrics import OutputPaths
from firecube.ingestor.validation import validate_config_collisions, validate_product_name_contract

__all__ = [
    "BaseIngestor",
    "LocalSourceFile",
]


def _output_storage_session(
    ctx: IngestContext | RuntimeIngestContext | PluginContext,
) -> Any | None:
    return storage.output if (storage := getattr(ctx, "storage", None)) is not None else None


def _output_product_name(
    ctx: IngestContext | RuntimeIngestContext | PluginContext, default: str
) -> str:
    if (session := _output_storage_session(ctx)) is not None:
        return str(session.product.product_name)
    return default


def _maybe_run_storage_preflight(
    runtime_ctx: RuntimeIngestContext, engine_config: EngineConfig
) -> None:
    if engine_config.skip_preflight:
        return

    storage = runtime_ctx.storage
    if storage is not None and storage.output is not None:
        preflight(storage.output)


def _select_output_format_strategy(ingestor: Any, ctx: IngestContext) -> None:
    """Wire output-format-specific template config and batch strategy."""
    if str(ctx.output_format or "").lower() != "tensogram":
        return
    if not is_dataset_producer(type(ingestor)):
        raise ConfigurationError(
            f"{type(ingestor).__name__} does not implement DatasetProducer; "
            "cannot use output_format='tensogram'."
        )

    from firecube.ingestor.runtime.configure import TierConfigurator
    from firecube.ingestor.templates.generic_tensogram import bind_tensogram_strategy

    bind_tensogram_strategy(ingestor)
    routed_ingestor = cast(Any, ingestor)
    routed_ingestor._configurator = TierConfigurator(
        routed_ingestor.template_config_class,
        routed_ingestor.plugin_config_class,
        plugin_name=routed_ingestor.name,
    )


def _storage_uri_from_raw(raw: str) -> StorageUri:
    if "://" in raw:
        return StorageUri.parse(raw)
    return StorageUri.from_local_path(Path(raw).expanduser().resolve())


def _minimal_binding(product: str, fmt: str = "zarr") -> StorageBinding:
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="firecube_controlplane_"))
    uri = StorageUri.from_local_path((root / product).resolve())
    return StorageBinding(
        identity=ProductIdentity.from_uri(uri, fmt, product_name=product),
        driver=StorageDriverConfig.from_storage_config_or_default(None),
    )


def _binding_from_context(ctx: IngestContext, product: str) -> StorageBinding:
    session = _output_storage_session(ctx)
    if session is not None:
        return StorageBinding(identity=session.product, driver=session.driver)

    fmt = ctx.output_format or "zarr"
    if ctx.target:
        return StorageBinding(
            identity=ProductIdentity.from_uri(
                _storage_uri_from_raw(str(ctx.target)),
                fmt,
                product_name=product,
            ),
            driver=StorageDriverConfig.from_storage_config_or_default(None),
        )

    return _minimal_binding(product, fmt)


class BaseIngestor(BaseIngestorHookMixin, Ingestor, ABC):
    """Orchestrating base class for all Firecube ingestors.

    ``BaseIngestor`` is a composition facade.  It wires together the runtime
    services (batching, telemetry, recording, workspace, resume-guard) and
    exposes a hook surface so that plugin authors only need to implement their
    domain logic.

    ``run()`` delegates to:
    - ``BatchPlanner``      — creates ``PipelineBatch`` objects from items.
    - ``TierConfigurator``  — splits flat options into EngineConfig /
                              TemplateConfig / PluginConfig options tiers
                              (not storage layers; storage I/O wiring is
                              handled by StorageBinding / StorageSession).
    - ``WorkspaceManager``  — manages per-run temp directories.
    - ``ResumeGuard``       — enforces resume / overwrite safety.
    - ``TelemetryService``  — wraps the telemetry sink with metric limits.
    - ``SpanRecorder``      — writes manifest entries to ChunkManager.
    - ``PipelineExecutor``  — parallel or sequential batch dispatch.

    Hooks fall into three categories.  The class-variable ``name`` is also
    required.

    MUST override (abstract):
        ``_process_batch(batch, ctx) -> PipelineResult``
            Core per-batch logic.  Called in worker threads for parallel mode.
            Templates (``GenericZarrIngestor``, ``GenericParquetIngestor``)
            implement this; plain plugins may override it directly.

    SHOULD override (return empty defaults; rarely correct without change):
        ``discover_source_files(ctx) -> Iterable``
            Discovers source files to be batched (paths/URIs/objects) using
            configured include patterns.

    CAN override (optional lifecycle / metadata hooks):
        ``filter_item(item, ctx) -> bool``
            Per-item filter applied before batching.  Default: keep all.
        ``item_size_bytes(item) -> int | None``
            Used for batch size estimation.  Default: ``None``.
        ``get_batch_groups(items, ctx) -> list[str]``
            Derives logical output groups for a batch.  Default: ``['default']``.
        ``slice_meta_keys() -> list[str]``
            Option keys that identify a logical "slice" (used for resume
            conflict detection).  Default: empty list.
        ``validation_group(ctx) -> str | None``
            Zarr group path used by the resume-guard's ``validate_zarr`` check.
        ``on_pipeline_start(ctx, state)``
            Called once before any batch runs.  Useful for shared resource
            setup (e.g. persistent DuckDB schema).
        ``on_batch_success(ctx, state, batch, result)``
            Called on the main thread after each successful batch.
        ``on_batch_failure(ctx, state, batch, result)``
            Called on the main thread after each failed batch.
    DO NOT override (framework internals):
        ``run(ctx)``          — top-level orchestration entry point.
        ``_create_batches``   — delegates to ``BatchPlanner``.
        ``finalize_pipeline`` — delegates to ``PipelineExecutor``.
    """

    PRODUCT_NAME: ClassVar[str]
    # Firecube's append/index dimension name as written into the Zarr store.
    # Default 'timestamp' preserves back-compat.
    # Plugins override on their subclass for CF-1.8 conventional naming (e.g.
    # `time_dim_name: ClassVar[str] = 'time'`). NOT a config-tier field,
    # NOT exposed via --option.
    time_dim_name: ClassVar[str] = "timestamp"
    name: str

    # Configuration Tiers (Declarative)
    template_config_class: type[TemplateConfig] | None = None
    plugin_config_class: type[PluginConfig] | None = None

    # Instance Configs (Populated in run())
    engine_config: EngineConfig
    template_config: TemplateConfig | None = None
    plugin_config: PluginConfig | None = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Build-time validation for ingestor class contracts."""
        super().__init_subclass__(**kwargs)

        validate_product_name_contract(cls)

        e_keys = config_keys(EngineConfig)
        t_keys = config_keys(cls.template_config_class) if cls.template_config_class else set()
        p_keys = config_keys(cls.plugin_config_class) if cls.plugin_config_class else set()

        if t_keys and p_keys:
            validate_config_collisions(
                dict.fromkeys(t_keys),
                dict.fromkeys(p_keys),
                scope=f"Template/Plugin Config in {cls.__name__}",
            )

        if t_keys:
            validate_config_collisions(
                dict.fromkeys(e_keys),
                dict.fromkeys(t_keys),
                scope=f"Engine/Template Config in {cls.__name__}",
            )

        if p_keys:
            validate_config_collisions(
                dict.fromkeys(e_keys),
                dict.fromkeys(p_keys),
                scope=f"Engine/Plugin Config in {cls.__name__}",
            )

    def _resolve_time_dim_name(self) -> str:
        return type(self).time_dim_name

    @classmethod
    def describe_options(cls) -> dict[str, list[str]]:
        """Return structured options documentation for CLI introspection."""
        resolved_product_name = getattr(cls, "PRODUCT_NAME", None)
        if isinstance(resolved_product_name, str) and resolved_product_name:
            product_name_entry: list[str] = [resolved_product_name]
        else:
            product_name_entry = []
        info = {
            "Product Name": product_name_entry,
            "Engine Options": sorted(config_keys(EngineConfig)),
            "System Keys": sorted(SYSTEM_KEYS),
        }
        if cls.template_config_class:
            info["Template Options"] = sorted(config_keys(cls.template_config_class))
        if cls.plugin_config_class:
            info["Plugin Options"] = sorted(config_keys(cls.plugin_config_class))
        return info

    def __init__(self, *, name: str | None = None, chunk_manager: ChunkManager | None = None):
        self.name = name or getattr(self, "name", self.__class__.__name__.lower())
        self._log = logging.getLogger(f"firecube.ingestor.{self.name}")

        if chunk_manager is not None:
            self._chunk_manager = chunk_manager
            self._owns_chunk_manager = False
        else:
            self._chunk_manager = ChunkManager(binding=_minimal_binding(self.name))
            self._owns_chunk_manager = True

        # Composition
        self._workspace = WorkspaceManager(self.name)
        self._configurator = TierConfigurator(
            self.template_config_class,
            self.plugin_config_class,
            plugin_name=self.name,
        )
        self._batch_planner = BatchPlanner()
        self._span_recorder = SpanRecorder(self._chunk_manager)

        self._firecube_engine = None  # Lazy init for PipelineExecutor
        self._parallel_execution_state: _ParallelExecutionState | None = None
        # True once ensure_slot_index_model has succeeded in this pod process;
        # fast-path in _ensure_slot_index_model_at_startup.
        self._slot_index_model_stamped: bool = False

    # --- BatchPlanHost Protocol Implementation ---

    @property
    def batch_id_prefix(self) -> str:
        return f"{self.name}_"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        """Discover source files matching include patterns."""
        if not ctx.source:
            raise ConfigurationError(
                "--input-data is required when using default source discovery. "
                "Override discover_source_files() in your plugin if your "
                "ingestor does not consume an input path."
            )
        includes = None
        if getattr(self, "engine_config", None) is not None:
            includes = getattr(self.engine_config, "include_patterns", None)

        files = discover_input_files(
            ctx.source,
            storage_config=self._chunk_manager.storage_config,
            preferred_globs=includes,
            recursive=True,
        )

        if not files:
            self._log.warning("No input files found in %s", ctx.source)
            return iter(())

        self._log.info("Found %d files", len(files))
        return files

    def filter_item(self, item: Any, ctx: PluginContext) -> bool:
        """Filter items before batching (Hook). Default: True."""
        return True

    def item_size_bytes(self, item: Any) -> int | None:
        """Estimate size of an item in bytes."""
        try:
            return Path(item).stat().st_size
        except (TypeError, OSError):
            return None

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return logical write groups for a batch of items (Hook). Default: ['default']."""
        return ["default"]

    def batch_setup(self, ctx: PluginContext) -> None:
        """Hook for per-batch setup (e.g. DB connections). Cooperatively calls super."""
        # Intentional cooperative super call:
        # this base can be used with or without mixins defining batch_setup().
        # Missing parent hook is treated as a no-op by design.
        super_setup = getattr(super(), "batch_setup", None)
        if callable(super_setup):
            super_setup(ctx)

    def batch_teardown(self, ctx: PluginContext) -> None:
        """Hook for per-batch cleanup. Cooperatively calls super."""
        # Same cooperative behavior as batch_setup(); keep teardown chain optional.
        super_teardown = getattr(super(), "batch_teardown", None)
        if callable(super_teardown):
            super_teardown(ctx)

    def prepare_batch_data(self, batch: PipelineBatch, ctx: PluginContext) -> dict[str, Any] | None:
        """Optional hook to prepare data (e.g. load files to DB) before group iteration."""
        return None

    def cleanup_batch_data(self, batch: PipelineBatch, ctx: PluginContext) -> None:
        """Optional hook to clean up batch data (e.g. drop table rows)."""

    def _materialize(self, source: Any) -> Path:
        return self._workspace.materialize(source)

    # --- PipelineHost Implementation (Facade) ---

    def _create_batches(
        self, ctx: RuntimeIngestContext, batch_size: int
    ) -> Iterable[PipelineBatch]:
        """Delegate batch creation to BatchPlanner."""
        host = cast(BatchPlanHost, self)
        for batch in self._batch_planner.create_batches(host, PluginContext(ctx), batch_size):
            self._verify_existing_cube_batch_groups(ctx, batch.groups)
            yield batch

    def _verify_existing_cube_batch_groups(
        self, ctx: RuntimeIngestContext, group_paths: Sequence[str]
    ) -> None:
        if str(ctx.output_format or "").lower() != "zarr":
            return
        verify_dim_compatibility(
            target_uri=str(ctx.target or ""),
            declared_dim=self._resolve_time_dim_name(),
            group_paths=group_paths,
            storage_config=self._chunk_manager.storage_config,
        )

    def finalize_pipeline(self, ctx: RuntimeIngestContext, state: PipelineRunState) -> IngestResult:
        """Delegate finalization to PipelineExecutor."""
        if self._firecube_engine:
            return self._firecube_engine.finalize(ctx, state, self)

        # Fallback if engine not permanently attached (e.g. strict sequential usage or tests)
        from firecube.ingestor.runtime.engine import PipelineExecutor

        return PipelineExecutor().finalize(ctx, state, self)

    # --- Execution Logic ---

    @abstractmethod
    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        """Process a single batch. Called in worker threads for parallel execution.

        Subclasses must implement this.  Templates (``GenericZarrIngestor``,
        ``GenericParquetIngestor``) provide a default implementation via their
        own ``build_dataset`` abstract hook.
        """

    @staticmethod
    def default_aggregate_metrics(
        ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, Any]:
        """Default aggregate helper for plugins that do not need custom policy."""
        return merge_batch_metrics(ctx, state)

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> Mapping[str, Any]:
        """Use standard batch-metric merge policy unless plugin overrides it."""
        return merge_batch_metrics(ctx, state)

    def _verify_schema_at_pod_startup(self, ctx: PluginContext) -> None:
        """Optional lifecycle hook for per-pod schema verification before planning."""
        _ = ctx
        return

    def _ensure_slot_index_model_at_startup(self, ctx: PluginContext) -> None:
        """Optional lifecycle hook for per-pod slot-index model negotiation."""
        _ = ctx
        return

    def _validate_context_hook_signatures(self) -> None:
        """Enforce PluginContext vs RuntimeIngestContext boundary on subclass hooks.

        Plugin-facing hooks must annotate ``ctx: PluginContext``.
        Runtime-finalization hooks must annotate ``ctx: RuntimeIngestContext``.
        Mismatches raise ConfigurationError with the expected type.
        """

        checks: dict[str, type[Any]] = {
            "discover_source_files": PluginContext,
            "filter_item": PluginContext,
            "batch_setup": PluginContext,
            "batch_teardown": PluginContext,
            "prepare_batch_data": PluginContext,
            "cleanup_batch_data": PluginContext,
            "build_dataset": PluginContext,
            "get_batch_groups": PluginContext,
            "get_zarr_config": PluginContext,
            "output_relpath": PluginContext,
            "write_parquet": PluginContext,
            "prepare_duckdb_schema": PluginContext,
            "slice_meta": PluginContext,
            "validation_group": PluginContext,
            "on_pipeline_start": PluginContext,
            "on_batch_success": PluginContext,
            "on_batch_failure": PluginContext,
            "_process_batch": PluginContext,
            "_aggregate_metrics": RuntimeIngestContext,
            "finalize_pipeline": RuntimeIngestContext,
        }

        cls = type(self)
        for method_name, expected_ctx_type in checks.items():
            method = getattr(cls, method_name, None)
            if method is None or not callable(method):
                continue

            try:
                sig = inspect.signature(method)
            except (TypeError, ValueError):
                continue
            if "ctx" not in sig.parameters:
                continue

            try:
                hints = get_type_hints(method)
            except Exception:
                hints = {}
            ctx_hint = hints.get("ctx")
            if ctx_hint is None:
                continue

            if expected_ctx_type is PluginContext and ctx_hint is RuntimeIngestContext:
                raise ConfigurationError(
                    f"{cls.__name__}.{method_name} must use PluginContext, not RuntimeIngestContext."
                )
            if expected_ctx_type is RuntimeIngestContext and ctx_hint is PluginContext:
                raise ConfigurationError(
                    f"{cls.__name__}.{method_name} must use RuntimeIngestContext, not PluginContext."
                )

    # --- Orchestration ---

    def run(self, ctx: IngestContext) -> IngestResult:
        """Orchestrate the ingestion.

        The caller-provided ``IngestContext`` is treated as immutable input.
        Runtime-only fields are carried in an internal ``RuntimeIngestContext``
        clone for the duration of this run.

        ``ctx.storage.output`` must already be populated by the caller
        (CLI/SDK boundary) before invoking ``run()``; this method does not
        auto-construct sessions.
        """
        if ctx.storage is None or ctx.storage.output is None:
            raise ValueError(
                "IngestContext.storage.output must be set before calling run(). "
                "Use ProductResolver + StorageBinding + StorageSession to build "
                "the context at the CLI/SDK boundary."
            )

        self._validate_context_hook_signatures()

        _select_output_format_strategy(self, ctx)

        product = _output_product_name(ctx, self.name)
        if self._owns_chunk_manager:
            self._chunk_manager = ChunkManager(binding=_binding_from_context(ctx, product))
            self._span_recorder = SpanRecorder(self._chunk_manager)
        ensure_chunk_manager_config(self._chunk_manager, ctx)
        self._workspace._storage_config = self._chunk_manager.storage_config

        # 1. Workspace Cleanup/Setup
        workspace_root = self._workspace.setup(ctx)

        # Resolve run_id without mutating the caller context.
        _base_run_id = ensure_run_id(ctx=ctx, plugin_name=self.name)

        # Create a runtime-enriched copy for use within this run.
        runtime_ctx = RuntimeIngestContext.from_ingest_context(
            ctx,
            run_id=_base_run_id,
            temp_root=workspace_root,
            materializer=self._materialize,
        )
        runtime_plugin_ctx = PluginContext(runtime_ctx)

        # 2. Configuration
        self.engine_config, self.template_config, self.plugin_config = self._configurator.configure(
            runtime_ctx
        )
        runtime_ctx.flags = RuntimeFlags(
            force_reingest=bool(self.engine_config.force_reingest),
            incremental=bool(self.engine_config.incremental),
            dry_run=bool(self.engine_config.dry_run),
        )

        # 2a. Derive slot-aware run_id for parallel mode (single-pod returns base unchanged).
        run_id = derive_pod_run_id(
            _base_run_id,
            self.engine_config.slot_start,
            self.engine_config.slot_end,
            slot_group=self.engine_config.slot_group,
        )
        if run_id != _base_run_id:
            runtime_ctx.run_id = run_id
            runtime_ctx.identity.run_id = run_id
            runtime_ctx.options["run_id"] = run_id
            runtime_plugin_ctx = PluginContext(runtime_ctx)

        _maybe_run_storage_preflight(runtime_ctx, self.engine_config)

        # 3. Telemetry Setup
        # BaseIngestor always uses TelemetryService for unified limits
        if getattr(runtime_ctx, "telemetry", None) is None:
            runtime_ctx.telemetry = create_ingestion_telemetry(
                plugin=self.name,
                product=product,
                output_format=str(runtime_ctx.output_format or ""),
                write_mode=str(getattr(self.engine_config, "write_mode", "")),
                run_id=run_id,
                base_meta=self.slice_meta(runtime_plugin_ctx),
            )
        assert runtime_ctx.telemetry is not None

        telemetry_service = TelemetryService(runtime_ctx.telemetry, self.name)
        telemetry_service.start(run_id)
        fs_metrics = wal_metrics = None
        resume_guard: ResumeGuard | None = None
        execution_result = IngestResult(
            outputs=OutputPaths(primary=str(runtime_ctx.target or "")),
            output_format=str(runtime_ctx.output_format or ""),
        )
        run_started_recorded = False

        try:
            ingest_span = (
                runtime_ctx.telemetry.span(
                    "firecube.ingest",
                    attributes={
                        "firecube.run_id": run_id,
                        "firecube.plugin": self.name,
                        "firecube.product": product,
                    },
                )
                or contextlib.nullcontext()
            )
            with (
                collect_filesystem_metrics() as fs_metrics,
                collect_wal_metrics() as wal_metrics,
                ingest_span,
            ):
                # 3.5 Capability gate for slot-range parallel ingestion
                from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability

                parallel_global_schema = validate_parallel_capability(
                    ingestor=self,
                    slot_start=self.engine_config.slot_start,
                    slot_end=self.engine_config.slot_end,
                    ctx=runtime_plugin_ctx,
                    slot_group=self.engine_config.slot_group,
                )
                self._parallel_execution_state = (
                    _ParallelExecutionState(global_expected=parallel_global_schema)
                    if parallel_global_schema is not None
                    else None
                )

                # 4. Resume Guard
                slot_range_for_record: tuple[int, int] | None = (
                    (self.engine_config.slot_start, self.engine_config.slot_end)
                    if self.engine_config.slot_start is not None
                    and self.engine_config.slot_end is not None
                    else None
                )
                guard = ResumeGuard(
                    plugin_name=self.name,
                    chunk_manager=self._chunk_manager,
                    log=self._log,
                    slice_meta_keys=self.slice_meta_keys(),
                )
                resume_guard = guard
                guard.enforce(
                    ctx=runtime_ctx,
                    product=product,
                    slice_meta=self.slice_meta(runtime_plugin_ctx),
                    slot_range=slot_range_for_record,
                    slot_group=self.engine_config.slot_group,
                    validation_group=self.validation_group(runtime_plugin_ctx),
                )

                # 5. Runtime control-plane lifecycle
                slice_meta = self.slice_meta(runtime_plugin_ctx)
                slice_meta.setdefault("plugin", self.name)
                self._span_recorder.register_run_started(
                    run_id=run_id,
                    product=product,
                    output_path=str(runtime_ctx.target or ""),
                    output_format=str(runtime_ctx.output_format or ""),
                    slice_meta=slice_meta,
                    slot_range=slot_range_for_record,
                    slot_group=self.engine_config.slot_group,
                )
                run_started_recorded = True

                self._ensure_slot_index_model_at_startup(runtime_plugin_ctx)
                self._verify_schema_at_pod_startup(runtime_plugin_ctx)

                # 6. Planning
                mode = determine_execution_mode(self.engine_config)
                # Sequential falls back to pipeline with 0/1 workers pattern or distinct loop?
                # Using BatchPlanner for both ensuring consistency.

                if self._firecube_engine is None:
                    from firecube.ingestor.runtime.engine import PipelineExecutor

                    self._firecube_engine = PipelineExecutor()

                execution_result: IngestResult

                if mode == ExecutionMode.PIPELINE:
                    execution_result = self._firecube_engine.run_pipeline(runtime_ctx, host=self)
                else:
                    state = run_sequential(
                        ctx=runtime_ctx,
                        host=self,
                        product=product,
                        batch_size=int(self.engine_config.pipeline_batch_size),
                        engine_config=self.engine_config,
                        log=self._log,
                    )
                    if not state.batches:
                        execution_result = IngestResult(
                            outputs=OutputPaths(primary=str(runtime_ctx.target)),
                            output_format=str(runtime_ctx.output_format or ""),
                            metrics={},
                        )
                    else:
                        execution_result = self._firecube_engine.finalize(runtime_ctx, state, self)

                # 7. Output completion (storage write + manifest)
                #
                # Original ordering (buggy for staged uploads):
                #     if not execution_result.registered:
                #         self._span_recorder.register_run(...)
                #     execution_result = self._firecube_engine.complete_output(...)
                #
                # That let the WAL record terminal status="complete" before the
                # staged upload actually finished. If complete_output() then raised,
                # the exception path skipped register_run_failure() because
                # execution_result.registered was already True.
                execution_result = self._firecube_engine.complete_output(
                    execution_result, runtime_ctx, host=self
                )

                # 8. Recording
                if not execution_result.registered:
                    self._span_recorder.register_run(
                        ctx=runtime_ctx,
                        result=execution_result,
                        run_id=run_id,
                        product=product,
                        slice_meta=slice_meta,
                        record_spans=not execution_result.spans_recorded,
                    )

                return execution_result
        except Exception as exc:
            # Record a terminal failure only for runs that reached "started" but
            # never reached a terminal WAL event. After the output-completion
            # reorder above, execution_result.registered becomes True only after
            # both complete_output() and register_run() succeed.
            if run_started_recorded and not execution_result.registered:
                with contextlib.suppress(Exception):
                    slice_meta = self.slice_meta(runtime_plugin_ctx)
                    slice_meta.setdefault("plugin", self.name)
                    self._span_recorder.register_run_failure(
                        run_id=run_id,
                        product=product,
                        output_path=str(runtime_ctx.target or ""),
                        output_format=str(runtime_ctx.output_format or ""),
                        slice_meta=slice_meta,
                        error=str(exc),
                    )
                    execution_result.registered = True
            raise

        finally:
            # Teardown
            should_cleanup = (
                self.engine_config.cleanup_workspace if hasattr(self, "engine_config") else False
            )
            with contextlib.suppress(Exception):
                # Engine finalization populates metrics["pipeline"] via
                # compute_run_summary(); emit that canonical run snapshot once.
                pipeline_summary = getattr(execution_result, "metrics", {}).get("pipeline")
                if isinstance(pipeline_summary, dict):
                    if fs_metrics is not None:
                        pipeline_summary.update(fs_metrics.as_summary())
                    if wal_metrics is not None:
                        pipeline_summary.update(wal_metrics.as_summary())
                    if resume_guard is not None and resume_guard.last_metrics is not None:
                        pipeline_summary.update(resume_guard.last_metrics.as_summary())
                    telemetry_service.emit_run_metrics(pipeline_summary)
            with contextlib.suppress(Exception):
                telemetry_service.flush()
            self._workspace.teardown(cleanup_dir=bool(should_cleanup))

    def ingest(self, ctx: IngestContext) -> IngestResult:
        """Compatibility entry point matching the public ``Ingestor`` protocol."""
        return self.run(ctx)

    def catalog_group_info(
        self, group: str, store_uri: str, storage_config: Any | None = None
    ) -> CatalogGroupInfo | None:
        """Optionally annotate or hide one discovered catalog group."""
        _ = (group, store_uri, storage_config)
        return None

    def resolve_output_uri(
        self, ctx: RuntimeIngestContext | PluginContext, write_mode: str = "staged"
    ) -> str:
        """Resolve the canonical output URI (Dataset Directory) for this run."""
        from firecube.core.product import resolve_dataset_target, write_mode_policy
        from firecube.core.uris import is_remote_target

        product = _output_product_name(ctx, self.name)
        policy = write_mode_policy(write_mode)
        if policy.resolves_to_workspace:
            return resolve_dataset_target(product, write_mode=policy, temp_root=ctx.temp_root)

        session = _output_storage_session(ctx)
        if session is None:
            raise ConfigurationError(
                "Direct write mode requires ctx.storage.output to resolve the product root."
            )
        product_uri = session.product.product_uri
        direct_base_uri = product_uri.parent().to_str()
        expected_output_uri = product_uri.to_str()
        control_plane_output_uri = self._chunk_manager.get_product_root(product)
        if str(control_plane_output_uri).rstrip("/") != str(expected_output_uri).rstrip("/"):
            raise ConfigurationError(
                "ChunkManager control-plane is bound to a different output root than direct write "
                f"resolution (expected={expected_output_uri}, bound={control_plane_output_uri})."
            )
        if is_remote_target(str(direct_base_uri)) and not is_remote_target(
            str(expected_output_uri)
        ):
            raise ConfigurationError(
                "Direct write mode resolved a remote output base into a local workspace path; "
                f"expected remote output under {direct_base_uri}, got {expected_output_uri}."
            )
        return expected_output_uri
