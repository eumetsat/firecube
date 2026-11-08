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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, cast, get_type_hints

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.claims import ClaimLossLatch, reporting_claim_loss
from firecube.core.controlplane.metrics import collect_wal_metrics
from firecube.core.controlplane.types import IndexEnsuredOutcome, WriteDomain
from firecube.core.discovery import DiscoveryRequest, SourceItem, discover
from firecube.core.errors import (
    ClaimLostError,
    LegacyIndexRecordError,
    Message,
    Param,
    PluginOption,
    ProductUnderMaintenanceError,
    SchemaDriftError,
)
from firecube.core.filesystem import collect_filesystem_metrics
from firecube.core.index_resolve import ExtentUnknownError
from firecube.core.intake import CatalogGroupInfo
from firecube.core.observability import create_ingestion_telemetry
from firecube.core.observability.metrics import TelemetryService, emit_index_ensured_full
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import infer_target_protocol
from firecube.ingestor.config.engine import SYSTEM_KEYS, EngineConfig, config_keys
from firecube.ingestor.contracts.interfaces import Ingestor
from firecube.ingestor.errors import ConfigurationError, ResumeConflictError
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
from firecube.ingestor.runtime.engine import _create_batches_with_parallel_filter, run_sequential
from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState
from firecube.ingestor.runtime.parallel_run_id import derive_pod_run_id
from firecube.ingestor.runtime.preflight import preflight
from firecube.ingestor.runtime.producer import capture_producer
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from firecube.ingestor.runtime.workspace import LocalSourceFile, WorkspaceManager
from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility
from firecube.ingestor.templates.config import TemplateConfig
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import (
    ExecutionParams,
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

# A dry-run plan lists at most this many batches and sets batches_truncated beyond it.
_DRY_RUN_BATCH_LIMIT = 100
# A dry-run finding with one of these statuses blocks the run.
_BLOCKING_STATUSES = frozenset({"blocked", "conflict"})


def _claim_loss_behind(exc: BaseException) -> ClaimLostError | None:
    """The `ClaimLostError` that a run failure, or one of its failed batches, came from."""
    failures = [exc, *(res.failure for res in getattr(exc, "failed", ()))]
    for failure in failures:
        seen: set[int] = set()
        current: BaseException | None = failure
        while current is not None and id(current) not in seen:
            if isinstance(current, ClaimLostError):
                return current
            seen.add(id(current))
            current = current.__cause__ or current.__context__
    return None


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


def _refuse_other_output_format(ingestor_cls: type[BaseIngestor], ctx: IngestContext) -> None:
    """Refuse a context that names an output format the ingestor class does not declare.

    Raises:
        ConfigurationError: ``ctx.output_format`` is set and differs from the class value.
    """
    declared = ingestor_cls.output_format
    if ctx.output_format is None or ctx.output_format == declared:
        return
    raise ConfigurationError(
        Message(
            "{plugin} writes {declared!r}; the context names output format {given!r}. "
            "The ingestor class declares its output format: leave "
            "IngestContext.output_format unset or name {declared!r}.",
            plugin=ingestor_cls.__name__,
            declared=declared,
            given=ctx.output_format,
        )
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


_UNSET: Any = object()
"""Marks an ingestor attribute that is built on first read."""


def _dry_run_workspace() -> Path:
    """The existing temporary directory, named so a dry run's control plane creates none.

    A control plane bound to a product reads its workspace only when it has no base URI.
    """
    import tempfile

    return Path(tempfile.gettempdir())


def _batch_plan(batch: PipelineBatch) -> dict[str, Any]:
    """What a dry-run plan reports of one planned batch."""
    return {
        "batch_id": batch.batch_id,
        "files_count": batch.files_count,
        "size_bytes": batch.size_bytes,
        "groups": list(batch.groups),
    }


def _slot_range(execution: ExecutionParams) -> tuple[int, int] | None:
    """The slot window of a run, or ``None`` for a run without one."""
    if execution.slot_start is None or execution.slot_end is None:
        return None
    return (execution.slot_start, execution.slot_end)


def _binding_from_context(ctx: IngestContext, product: str, fmt: str) -> StorageBinding:
    session = _output_storage_session(ctx)
    if session is not None:
        return StorageBinding(identity=session.product, driver=session.driver)

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


@dataclass(frozen=True)
class _SourceSnapshot:
    """What one command's discovery returned, held for every consumer of that command."""

    context: object
    """The run context the discovery ran for; another context discovers again."""

    files: tuple[Any, ...]
    """The items ``discover_source_files`` returned."""

    items: tuple[SourceItem, ...]
    """The items the default discovery resolved; empty when the plugin overrides it."""


def _context_key(ctx: object) -> object:
    """The run context behind a plugin context, which a new command replaces."""
    return getattr(ctx, "_ctx", ctx)


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

    Attributes:
        time_dim_name: The firecube append/index dimension name written into
            the Zarr store. Defaults to ``"timestamp"`` for backward
            compatibility; mirrors the ``PRODUCT_NAME`` pattern. Not a
            config-tier field, not exposed via ``--option`` — a property of
            the plugin/product, not a per-run knob. Override on a plugin
            subclass to match the product's own naming convention (e.g.
            ``time_dim_name: ClassVar[str] = "time"``), matching whatever
            dimension the plugin's own hook implementation actually
            produces. The runtime engine also consumes this name to seed
            coordinate-array chunks in staged mode. Plugin declaration is
            authoritative: changing it against an existing store fails
            loudly with migration guidance rather than silently
            reinterpreting the store.
        SOURCE_ITEMS: What the default ``discover_source_files`` hands to
            ``filter_item``, ``inspect_item`` and the build hooks. ``False``
            (the default): the ``href`` string of each item, a local path or
            URI. ``True``: the `SourceItem` itself, which also carries the
            STAC collection, item id, asset key, datetime, bbox, media type
            and roles of a ``stac://`` source.
        output_format: The format this ingestor writes. ``"zarr"`` by default;
            the Parquet and Tensogram templates declare ``"parquet"`` and ``"tensogram"``.
    """

    PRODUCT_NAME: ClassVar[str]
    time_dim_name: ClassVar[str] = "timestamp"
    SOURCE_ITEMS: ClassVar[bool] = False
    output_format: ClassVar[str] = "zarr"
    # Whether the runner stops at the first failed batch. Templates whose
    # batches build on each other (append-style Zarr) set this to True so
    # nothing lands past a gap; independent-batch templates keep False.
    stop_on_batch_failure: ClassVar[bool] = False
    requires_fresh_target: ClassVar[bool] = False
    fresh_target_format_label: ClassVar[str] = "Output"
    name: str

    # Configuration Tiers (Declarative)
    template_config_class: type[TemplateConfig] | None = None
    plugin_config_class: type[PluginConfig] | None = None

    # Instance Configs (Populated in run())
    engine_config: EngineConfig
    template_config: TemplateConfig | None = None
    plugin_config: PluginConfig | None = None

    # The current command's discovery (discovered_source_files). A class default,
    # so an instance built without __init__ still discovers.
    _source_snapshot: _SourceSnapshot | None = None

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

    def _emit_index_ensured_event(
        self,
        *,
        ctx: PluginContext,
        product: str,
        run_id: str,
        record: Any,
        outcome: IndexEnsuredOutcome,
    ) -> None:
        telemetry_handle = getattr(ctx, "telemetry", None)
        telemetry = (
            TelemetryService(telemetry_handle, self.name) if telemetry_handle is not None else None
        )
        emit_index_ensured_full(
            self._chunk_manager,
            telemetry,
            product=product,
            run_id=run_id,
            record=record,
            outcome=outcome,
            logger=self._log,
        )

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

        # Without a given manager, the placeholder and its temporary directories are
        # made on the first read of `_chunk_manager`; `run` replaces it unread.
        self._chunk_manager_value: Any = _UNSET if chunk_manager is None else chunk_manager
        self._owns_chunk_manager = chunk_manager is None

        # Composition
        self._workspace = WorkspaceManager(self.name)
        self._configurator = TierConfigurator(
            self.template_config_class,
            self.plugin_config_class,
            plugin_name=self.name,
        )
        self._batch_planner = BatchPlanner()
        self._span_recorder_value: Any = _UNSET

        self._firecube_engine = None  # Lazy init for PipelineExecutor
        self._parallel_execution_state: _ParallelExecutionState | None = None
        self._discovered_source_files: tuple[Any, ...] | None = None
        # What the default discovery resolved in its last call, whatever SOURCE_ITEMS
        # hands on; discovered_source_files keeps it in the command's snapshot.
        self._discovered_source_items: tuple[SourceItem, ...] = ()
        # True once the engine-owned resolved-index record has been ensured in
        # this pod process; fast-path in _ensure_index_record_at_startup.
        self._resolved_index_stamped: bool = False
        # Records the loss of any claim of the current run; `run` replaces it per run.
        self._run_claim_latch = ClaimLossLatch()

    @property
    def _chunk_manager(self) -> ChunkManager:
        """The control plane; a placeholder under a new temporary root until `run` binds one."""
        manager = self.__dict__.get("_chunk_manager_value", _UNSET)
        if manager is _UNSET:
            manager = self._chunk_manager_value = ChunkManager(binding=_minimal_binding(self.name))
        return manager

    @_chunk_manager.setter
    def _chunk_manager(self, manager: ChunkManager) -> None:
        self._chunk_manager_value = manager

    @property
    def _span_recorder(self) -> SpanRecorder:
        """The span recorder, built on the control plane at its first read."""
        recorder = self.__dict__.get("_span_recorder_value", _UNSET)
        if recorder is _UNSET:
            recorder = self._span_recorder_value = SpanRecorder(self._chunk_manager)
        return recorder

    @_span_recorder.setter
    def _span_recorder(self, recorder: SpanRecorder) -> None:
        self._span_recorder_value = recorder

    # --- BatchPlanHost Protocol Implementation ---

    @property
    def batch_id_prefix(self) -> str:
        """Prefix applied to every batch ID this ingestor produces.

        Derived from the ingestor's ``name`` so batch IDs stay attributable to
        their plugin in logs, metrics, and control-plane records.
        """
        return f"{self.name}_"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        """Discover the source items to ingest (Hook).

        The default implementation requires ``--input-data`` (``ctx.sources``)
        and raises ``ConfigurationError`` without it. It discovers every
        source of the run, in order, deduplicates the items and sorts them
        once:

        - A file tree, given as a ``file://`` or ``s3://`` URI, is searched
          recursively for ``.zip``, ``.h5``, ``.nc``, ``.nc4``, ``.hdf``, and ``.he5``
          files plus local files with unselected or absent suffixes whose content looks like HDF5.
          Positive ``input_filters`` entries add files to that set; entries
          starting with ``!`` exclude matches, overriding suffix and content
          selection regardless of list order.
        - A ``stac://<host>/<api-root>/collections/<id>`` source is searched
          through the STAC API with the run's ``stac_search`` filter, and
          every asset whose roles contain ``data`` is collected. The suffix
          list does not apply; ``input_filters`` narrow the assets on the
          basename of their href.

        Each entry of ``ctx.sources`` from the CLI is a ``file://``, ``s3://``
        or ``stac://`` URI, as given. A library caller may pass a local path.

        Pass ``--input-filters`` as one JSON list. Filters match
        case-sensitively, including for single-file sources. Items are
        returned as href strings, or as `SourceItem` objects when the class
        declares ``SOURCE_ITEMS = True``. Custom discovery overrides must
        explicitly apply filters, for example by forwarding them to
        ``discover_input_files`` or `discover`.

        Override this hook when source layout rules cannot be expressed as
        patterns, or when sources are not file trees at all (catalogs,
        APIs); overriding removes the ``--input-data`` requirement. Returned
        items flow through ``filter_item`` and batching into the build
        hooks; return path or URI strings unless the plugin's own hooks
        handle richer item objects end to end.

        The engine calls this hook at most once per command, through
        `discovered_source_files`, and only when the command needs the items.
        A fixed-grid preallocate without engine-owned arrays never calls it. It
        materialises the returned iterable in full before the first batch is
        planned; index binding, batching and preallocation then reuse the held
        items and never re-discover them. Returning an
        iterator or generator is fine for large listings and avoids building
        a second container in the plugin, but it is consumed completely up
        front, so it cannot defer or stream discovery across batches.
        """
        if not ctx.sources:
            raise ConfigurationError(
                Message(
                    "{input_data} is required when using default source discovery. "
                    "Override discover_source_files() in your plugin if your "
                    "ingestor does not consume an input path.",
                    input_data=Param("input_data"),
                    plugin=self.name,
                    product_uri=ctx.target,
                    run_id=ctx.run_id,
                )
            )
        engine_config: EngineConfig | None = getattr(self, "engine_config", None)
        items = discover(
            DiscoveryRequest(
                sources=ctx.sources,
                storage_config=self._chunk_manager.storage_config,
                input_filters=engine_config.input_filters if engine_config else None,
                stac_search=engine_config.stac_search if engine_config else None,
            )
        )
        self._discovered_source_items = tuple(items)
        files: list[Any] = list(items) if self.SOURCE_ITEMS else [item.href for item in items]

        if not files:
            engine_cfg: EngineConfig | None = getattr(self, "engine_config", None)
            allow_empty = engine_cfg is not None and engine_cfg.allow_empty_source
            in_slot_range = ctx.execution.slot_start is not None
            if not allow_empty and not in_slot_range:
                kinds = {infer_target_protocol(source) == "stac" for source in ctx.sources}
                context = {"plugin": self.name, "product_uri": ctx.target, "run_id": ctx.run_id}
                if kinds == {True}:
                    message = Message(
                        "No source files found after applying input filters. "
                        "The STAC search found no asset with the data role that passes "
                        "the input filters. Adjust {stac_search} or {input_filters}, or "
                        "set {allow_empty_source} to proceed with an empty source.",
                        stac_search=Param("stac_search"),
                        input_filters=Param("input_filters"),
                        allow_empty_source=PluginOption("allow_empty_source", "true"),
                        **context,
                    )
                elif True in kinds:
                    message = Message(
                        "No source files found after applying input filters. "
                        "The default discovery looks for files ending in: "
                        ".zip, .h5, .nc, .nc4, .hdf, .he5 below a path or prefix, "
                        "and the STAC search found no asset with the data role that "
                        "passes the input filters. To match different files, pass "
                        "{input_filters} or adjust {stac_search}, or set "
                        "{allow_empty_source} to proceed with an empty source.",
                        stac_search=Param("stac_search"),
                        input_filters=Param("input_filters", '["*.csv"]'),
                        allow_empty_source=PluginOption("allow_empty_source", "true"),
                        **context,
                    )
                else:
                    message = Message(
                        "No source files found after applying input filters. The default discovery looks for files ending in: "
                        ".zip, .h5, .nc, .nc4, .hdf, .he5. To match different files, pass "
                        "{input_filters} or set "
                        "{allow_empty_source} to proceed with an empty source.",
                        input_filters=Param("input_filters", '["*.csv"]'),
                        allow_empty_source=PluginOption("allow_empty_source", "true"),
                        **context,
                    )
                raise ConfigurationError(message)
            self._log.warning("No input files found in %s", ", ".join(ctx.sources))
            return iter(())

        self._log.info("Found %d files", len(files))
        return files

    def discovered_source_files(self, ctx: PluginContext) -> tuple[Any, ...]:
        """Return what ``discover_source_files`` returned for this command.

        The first call for a context runs the hook; later calls for the same
        context return the same items, so every consumer of one command reads
        one discovery. A call for another context discovers again. Not a hook:
        override ``discover_source_files`` instead.

        Args:
            ctx: The plugin context of the command.

        Returns:
            The discovered items, unfiltered.
        """
        key = _context_key(ctx)
        snapshot = self._source_snapshot
        if snapshot is None or snapshot.context is not key:
            self._discovered_source_items = ()
            files = tuple(self.discover_source_files(ctx))
            snapshot = _SourceSnapshot(
                context=key, files=files, items=self._discovered_source_items
            )
            self._source_snapshot = snapshot
        return snapshot.files

    def _forget_discovery(self) -> None:
        """Drop the held discovery, so the next command discovers its own items."""
        self._source_snapshot = None
        self._discovered_source_items = ()

    def stac_discovery_record(self, ctx: PluginContext) -> dict[str, Any] | None:
        """Return what a run record keeps of STAC discovery, or ``None`` without a stac:// source.

        The record holds the ``stac://`` sources, the search filter as given,
        and the ``collection/item_id`` of every STAC item the default discovery
        resolved for this command. A plugin that overrides
        ``discover_source_files``, or a command that has not discovered, records
        no item ids.

        Args:
            ctx: The plugin context of the command.

        Returns:
            The ``sources``, ``search`` and ``item_ids`` of the record, or ``None``.
        """
        stac_sources = [source for source in ctx.sources if infer_target_protocol(source) == "stac"]
        if not stac_sources:
            return None
        engine_config: EngineConfig | None = getattr(self, "engine_config", None)
        snapshot = self._source_snapshot
        discovered = (
            snapshot.items if snapshot is not None and snapshot.context is _context_key(ctx) else ()
        )
        item_ids = {
            f"{item.collection}/{item.item_id}" for item in discovered if item.scheme == "stac"
        }
        return {
            "sources": stac_sources,
            "search": engine_config.stac_search if engine_config else None,
            "item_ids": sorted(item_ids),
        }

    def _run_record_meta(self, ctx: PluginContext) -> dict[str, Any]:
        """The meta of the run's started and failed records: slice meta, plugin, STAC provenance."""
        meta = self.slice_meta(ctx)
        meta.setdefault("plugin", self.name)
        stac_record = self.stac_discovery_record(ctx)
        if stac_record is not None:
            meta["stac_discovery"] = stac_record
        return meta

    def filter_item(self, item: Any, ctx: PluginContext) -> bool:
        """Decide whether one discovered item proceeds to batching (Hook).

        Called once per item, after discovery and before batching.
        Returning ``False`` drops the item silently; it never reaches a
        batch or a plugin hook. Unlike ``input_filters``/
        ``discover_source_files``, which control which files discovery
        finds at all, this hook filters items discovery has already found.

        Args:
            item: One discovered item, in whatever form discovery produced
                it.
            ctx: Read-only plugin context.

        Returns:
            ``True`` to keep the item, ``False`` to drop it. Default:
            ``True`` (keep all).

        Examples:
            Drop zero-byte files before they reach batching:

                def filter_item(self, item, ctx):
                    return Path(item).stat().st_size > 0
        """
        return True

    def item_size_bytes(self, item: Any) -> int | None:
        """Estimate size of an item in bytes."""
        try:
            return Path(item).stat().st_size
        except (TypeError, OSError):
            return None

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return the logical write groups covered by a batch of items (Hook).

        A group names a logical write destination: for Zarr templates it
        becomes the group path inside the store, for Parquet it becomes the
        output partition (subdirectory). Called once per planned batch;
        ``build_dataset`` is then invoked once per group with the full batch
        item list, and the plugin routes items to the group it is building.

        A GenericZarr plugin returns ``["/"]`` to write its dataset at the store
        root instead of in a named group.

        Implementations MUST be deterministic and return a stable, sorted
        list so grouping stays consistent across runs.

        Args:
            items: Items assigned to this batch.
            ctx: Read-only plugin context.

        Returns:
            Group names for the batch. Default: ``["default"]``.

        Examples:
            Write two Zarr groups from the same source items. Each group
            name becomes the group path in the store, and ``build_dataset``
            selects what belongs in the group it is called for:

                def get_batch_groups(self, items, ctx):
                    return ["quality", "sst"]

                def build_dataset(self, group, items, ctx):
                    ds = self._open(items, ctx)
                    if group == "sst":
                        return ds[["sea_surface_temperature"]]
                    return ds[["quality_level"]]
        """
        return ["default"]

    def batch_setup(self, ctx: PluginContext) -> None:
        """Acquire per-batch resources through cooperative setup hooks.

        Overrides must call ``super().batch_setup(ctx)``.
        ``GenericZarrIngestor``, ``GenericParquetIngestor``, and
        ``DirectZarrIngestor`` wrap batch processing in setup and teardown,
        including dataset construction or write-intent generation. Teardown
        runs only after successful setup; if setup raises, the acquiring
        hook must release resources it has already acquired. Direct
        ``BaseIngestor`` subclasses own this boundary themselves.

        Args:
            ctx: Read-only context for the current run.
        """
        # Intentional cooperative super call:
        # this base can be used with or without mixins defining batch_setup().
        # Missing parent hook is treated as a no-op by design.
        super_setup = getattr(super(), "batch_setup", None)
        if callable(super_setup):
            super_setup(ctx)

    def batch_teardown(self, ctx: PluginContext) -> None:
        """Release resources acquired by successful cooperative batch setup.

        Overrides must call ``super().batch_teardown(ctx)``. Callers that
        create a ``BatchResourceRegistry`` must explicitly drain it; this
        hook does not automatically discover or manage registry instances.

        Args:
            ctx: Read-only context for the current run.
        """
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
        cached_source_files = self._discovered_source_files
        for batch in self._batch_planner.create_batches(
            host,
            PluginContext(ctx),
            batch_size,
            items=cached_source_files,
        ):
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

    def _refuse_lost_claim(self) -> None:
        """Lifecycle hook: refuse to complete a run whose claim loss latch is set.

        Called once the workers and claim scopes have finished, and again before
        the successful terminal record (main thread).

        Raises:
            ClaimLostError: The run lost one of its claims.
        """
        self._run_claim_latch.raise_if_set()

    def _close_run_state(self) -> None:
        """Optional lifecycle hook: end of every run, whatever its outcome (main thread).

        Raise when the run's writes are still in flight: ``run`` then logs it
        and leaves the workspace in place instead of deleting it.
        """
        return

    def _resolve_index_binding_at_startup(self, ctx: PluginContext) -> Any:
        """Internal helper: resolve index binding at pod startup.

        Owns the lazy import from firecube.ingestor.runtime.index_binding so that
        DirectZarrIngestor (a template) can call this WITHOUT importing runtime
        internals from direct_zarr.py — which would violate the architecture boundary
        enforced by tests/architecture/test_import_boundaries.py.

        Returns None if the plugin's index_spec() returns None.
        """
        from firecube.ingestor.runtime.index_binding import resolve_index_spec_for_ingestor

        return resolve_index_spec_for_ingestor(self, ctx)

    def bind_index_at_startup(self, ctx: PluginContext) -> None:
        """Bind the plugin's index spec to a resolved index for `ctx`.

        Call it first in a command. It drops the source items an earlier
        command discovered, so `discovered_source_files` discovers again.

        Args:
            ctx: The plugin context of the run.
        """
        self._forget_discovery()
        self._bind_index_at_startup(ctx)

    def _bind_index_at_startup(self, ctx: PluginContext) -> None:
        """Hook: bind IndexSpec to a resolved IndexBinding at pod startup.

        Called on the main thread BEFORE validate_parallel_capability and
        BEFORE _ensure_index_record_at_startup. Base default sets
        self._index_binding = None to clear stale state from any prior run.

        Templates that need pre-bound index state (currently only
        DirectZarrIngestor) override this to resolve and store their binding.
        """
        _ = ctx
        self._index_binding = None

    def _ensure_index_record_at_startup(self, ctx: PluginContext) -> None:
        """Optional lifecycle hook for per-pod resolved-index record negotiation."""
        _ = ctx
        return

    def _check_legacy_index_record_at_startup(self, *, product: str, plugin_name: str) -> None:
        """Fail when a product only has the legacy slot-index record."""

        from firecube.core.controlplane.manager import check_legacy_index_record

        check_legacy_index_record(
            self._chunk_manager,
            product=product,
            plugin_name=plugin_name,
        )

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

    def _runtime_context(
        self, ctx: IngestContext, *, temp_root: Path | None, output_format: str
    ) -> RuntimeIngestContext:
        """Configure the tiers and return the run's runtime copy of ``ctx``, with its pod run id."""
        # Resolve run_id without mutating the caller context.
        base_run_id = ensure_run_id(ctx=ctx, plugin_name=self.name)
        runtime_ctx = RuntimeIngestContext.from_ingest_context(
            ctx,
            run_id=base_run_id,
            temp_root=temp_root,
            materializer=self._materialize,
        )
        runtime_ctx.output_format = output_format
        execution = runtime_ctx.execution
        # The run reads only the sources it discovers itself.
        self._forget_discovery()

        self.engine_config, self.template_config, self.plugin_config = self._configurator.configure(
            runtime_ctx
        )
        runtime_ctx.flags = RuntimeFlags(
            force_reingest=bool(self.engine_config.force_reingest),
            incremental=bool(self.engine_config.incremental),
            dry_run=execution.dry_run,
        )

        # Derive slot-aware run_id for parallel mode (single-pod returns base unchanged).
        run_id = derive_pod_run_id(
            base_run_id,
            execution.slot_start,
            execution.slot_end,
            slot_group=execution.slot_group,
        )
        if run_id != base_run_id:
            runtime_ctx.run_id = run_id
            runtime_ctx.identity.run_id = run_id
            runtime_ctx.options["run_id"] = run_id
        return runtime_ctx

    def _check_at_startup(self, ctx: PluginContext) -> None:
        """Bind the index, refuse a bad schema and gate slot-range parallelism, before any write."""
        execution = ctx.execution
        self._bind_index_at_startup(ctx)

        # Schema refusals of the root group run after this context's index
        # is bound and before anything touches the target.
        from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

        schema_host: Any = self
        if isinstance(schema_host, DirectZarrIngestor):
            schema_host._cached_zarr_schema(ctx)

        binding = getattr(self, "_index_binding", None)
        if binding is not None:
            from firecube.ingestor.errors import UnboundedAxisError
            from firecube.ingestor.runtime.parallel_gate import (
                validate_global_expected_subset_of_schema,
                warn_on_chunk_alignment,
            )

            ingestor_self: Any = self
            if isinstance(ingestor_self, DirectZarrIngestor):
                serial_run = execution.slot_start is None and execution.slot_end is None
                global_expected: dict[str, int] = {}
                for group in binding.resolved.groups:
                    try:
                        global_expected[group] = binding.resolved.size(group)
                    except ExtentUnknownError as exc:
                        if not serial_run:
                            raise UnboundedAxisError(group) from exc
                        # A serial run may legitimately declare an axis
                        # without a horizon (RegularTimeAxis with neither
                        # slot_count nor end_date); arrays grow via
                        # allow_grow. Skip extent validation for the
                        # group. Parallel runs still fail loudly above.
                        continue
                schema = ingestor_self.zarr_schema(ctx)
                if schema:
                    validate_global_expected_subset_of_schema(global_expected, schema)
                    warn_on_chunk_alignment(global_expected, schema)

        # Capability gate for slot-range parallel ingestion
        from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability

        parallel_binding = validate_parallel_capability(
            ingestor=self,
            slot_start=execution.slot_start,
            slot_end=execution.slot_end,
            ctx=ctx,
            slot_group=execution.slot_group,
        )
        self._parallel_execution_state = (
            _ParallelExecutionState(
                global_expected={
                    group: parallel_binding.resolved.size(group)
                    for group in parallel_binding.resolved.groups
                }
            )
            if parallel_binding is not None
            else None
        )

    def _new_resume_guard(self) -> ResumeGuard:
        return ResumeGuard(
            plugin_name=self.name,
            chunk_manager=self._chunk_manager,
            log=self._log,
            slice_meta_keys=self.slice_meta_keys(),
            time_dim_name=self._resolve_time_dim_name(),
        )

    def _enforce_resume_guard(
        self,
        guard: ResumeGuard,
        runtime_ctx: RuntimeIngestContext,
        ctx: PluginContext,
        product: str,
    ) -> None:
        guard.enforce(
            ctx=runtime_ctx,
            product=product,
            slice_meta=self.slice_meta(ctx),
            slot_range=_slot_range(runtime_ctx.execution),
            slot_group=runtime_ctx.execution.slot_group,
            validation_group=self.validation_group(ctx),
        )

    def _plan_dry_run(
        self, ctx: IngestContext, *, product: str, output_format: str
    ) -> IngestResult:
        """Plan the run and report what would block it, writing nothing.

        It makes no workspace, claim, run record, label stamp or store byte, and emits no
        run telemetry. Discovery, index binding, the schema checks and batching run as in a
        real run; their refusals raise as they would there.

        Returns:
            The result whose manifest carries ``dry_run`` and the ``plan``.
        """
        from firecube.ingestor.runtime.output_guard import fresh_output_findings
        from firecube.ingestor.types.manifest import IngestManifest

        runtime_ctx = self._runtime_context(ctx, temp_root=None, output_format=output_format)
        plugin_ctx = PluginContext(runtime_ctx)
        run_id = str(runtime_ctx.run_id)
        _maybe_run_storage_preflight(runtime_ctx, self.engine_config)
        try:
            self._check_at_startup(plugin_ctx)
            findings: dict[str, dict[str, Any]] = {
                "resume_guard": self._resume_guard_finding(runtime_ctx, plugin_ctx, product)
            }
            self._discovered_source_files = self.discovered_source_files(plugin_ctx)
            findings["fresh_output"] = fresh_output_findings(
                runtime_ctx,
                self._chunk_manager,
                required=self.requires_fresh_target,
                format_label=self.fresh_target_format_label,
            )
            findings["maintenance"] = self._maintenance_finding(product, run_id)
            findings["index"] = self._index_finding(plugin_ctx, product, run_id)
            # The real runners' planner: the slot window filters the items first.
            batches = _create_batches_with_parallel_filter(
                host=self,
                ctx=runtime_ctx,
                batch_size=int(self.engine_config.pipeline_batch_size),
                log=self._log,
                refuse_sparse_window=False,
            )
        finally:
            self._discovered_source_files = None
            self._forget_discovery()
            self._parallel_execution_state = None

        slot_range = _slot_range(runtime_ctx.execution)
        plan: dict[str, Any] = {
            "batches": [_batch_plan(batch) for batch in batches[:_DRY_RUN_BATCH_LIMIT]],
            "batches_total": len(batches),
            "batches_truncated": len(batches) > _DRY_RUN_BATCH_LIMIT,
            "groups": sorted({group for batch in batches for group in batch.groups}),
            "slot_range": list(slot_range) if slot_range is not None else None,
            **findings,
            "blocked": [
                name
                for name, finding in findings.items()
                if finding["status"] in _BLOCKING_STATUSES
            ],
        }
        target = str(runtime_ctx.target or "")
        manifest = IngestManifest(
            plugin=self.name,
            output_format=output_format,
            stored_at=target,
            files=None,
            bytes=None,
            duration_s=None,
            run_id=run_id,
            product=product,
        ).to_dict()
        manifest.update(dry_run=True, plan=plan)
        return IngestResult(
            outputs=OutputPaths(primary=target), output_format=output_format, manifest=manifest
        )

    def _resume_guard_finding(
        self, runtime_ctx: RuntimeIngestContext, ctx: PluginContext, product: str
    ) -> dict[str, Any]:
        """Run the resume guard and report its refusal instead of raising it."""
        try:
            self._enforce_resume_guard(self._new_resume_guard(), runtime_ctx, ctx, product)
        except ResumeConflictError as exc:
            return {"status": "blocked", "reason": str(exc)}
        return {"status": "pass", "reason": None}

    def _maintenance_finding(self, product: str, run_id: str) -> dict[str, Any]:
        """Report another owner's exclusive maintenance claim; reads the claim, never takes it."""
        try:
            self._chunk_manager.check_exclusive_maintenance(product=product, owner_id=run_id)
        except ProductUnderMaintenanceError as exc:
            return {"status": "blocked", "reason": str(exc)}
        return {"status": "pass", "reason": None}

    def _index_finding(self, ctx: PluginContext, product: str, run_id: str) -> dict[str, Any]:
        """Report what the startup index negotiation would do, reading only.

        The status is ``none`` (no index), ``unbounded`` (no full record),
        ``would_create``, ``unchanged`` or ``conflict``.
        """
        from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

        binding = getattr(self, "_index_binding", None)
        host: Any = self
        if binding is None or not isinstance(host, DirectZarrIngestor):
            return {"status": "none", "reason": None}
        # A calendar group without a preallocated coordinate raises, as in a real run.
        host._verify_calendar_axes_preallocated_at_startup(ctx, binding.resolved)
        manager = self._chunk_manager
        try:
            self._check_legacy_index_record_at_startup(product=product, plugin_name=self.name)
            record = binding.resolved.as_resolved_index_record(run_id=run_id)
        except LegacyIndexRecordError as exc:
            return {"status": "conflict", "reason": str(exc)}
        except ExtentUnknownError:
            try:
                host._verify_per_group_identity_at_startup(ctx, binding.resolved)
            except SchemaDriftError as exc:
                return {"status": "conflict", "reason": str(exc)}
            return {"status": "unbounded", "reason": None}
        stored = manager.get_resolved_index(product=product)
        attrs_hash = manager.read_resolved_index_attrs_hash(product=product)
        declared = record.identity_hash
        if stored is None and attrs_hash is None:
            # A held claim refuses the run's acquire, even a stale one, and the run then
            # waits for a record that its holder never wrote.
            domain = WriteDomain(product=product, category="resolved_index", name="current")
            holder = manager.repo.read_claim_by_domain(product=product, domain=domain.identifier)
            if holder is None:
                return {"status": "would_create", "reason": None}
            state = "stale" if holder.stale else "live"
            reason = (
                f"claim {holder.domain} is held by {holder.owner_id} ({state}) and the store "
                "has no resolved-index record, so the run would not converge on one"
            )
        elif stored is None:
            reason = "zarr root attrs have a resolved-index identity hash but no record"
        elif stored.identity_hash != declared:
            reason = f"stored={stored.identity_hash[:16]} declared={declared[:16]}"
        elif attrs_hash is None:
            # The run mirrors the stored record onto the root attributes.
            return {"status": "would_create", "reason": None}
        elif attrs_hash != declared:
            reason = f"zarr root attrs have drifted: attrs_hash={attrs_hash[:16]!r}"
        else:
            return {"status": "unchanged", "reason": None}
        return {"status": "conflict", "reason": reason}

    # --- Orchestration ---

    def run(self, ctx: IngestContext) -> IngestResult:
        """Orchestrate the ingestion.

        The caller-provided ``IngestContext`` is treated as immutable input.
        Runtime-only fields are carried in an internal ``RuntimeIngestContext``
        clone for the duration of this run.

        ``ctx.storage.output`` must already be populated by the caller
        (CLI/SDK boundary) before invoking ``run()``; this method does not
        auto-construct sessions.

        With ``ctx.execution.dry_run`` it returns a plan and writes nothing.

        Raises:
            ConfigurationError: ``ctx.output_format`` names a format other than the class's.
        """
        if ctx.storage is None or ctx.storage.output is None:
            raise ValueError(
                "IngestContext.storage.output must be set before calling run(). "
                "Use ProductResolver + StorageBinding + StorageSession to build "
                "the context at the CLI/SDK boundary."
            )

        self._validate_context_hook_signatures()

        output_format = type(self).output_format
        _refuse_other_output_format(type(self), ctx)

        product = _output_product_name(ctx, self.name)
        if self._owns_chunk_manager:
            self._chunk_manager = ChunkManager(
                binding=_binding_from_context(ctx, product, output_format),
                workspace=_dry_run_workspace() if ctx.execution.dry_run else None,
            )
            self._span_recorder = SpanRecorder(self._chunk_manager)
        ensure_chunk_manager_config(self._chunk_manager, ctx)
        self._workspace._storage_config = self._chunk_manager.storage_config

        if ctx.execution.dry_run:
            return self._plan_dry_run(ctx, product=product, output_format=output_format)

        # 1. Workspace Cleanup/Setup
        workspace_root = self._workspace.setup(ctx)

        # 2. Configuration
        runtime_ctx = self._runtime_context(
            ctx, temp_root=workspace_root, output_format=output_format
        )
        execution = runtime_ctx.execution
        run_id = str(runtime_ctx.run_id)
        runtime_plugin_ctx = PluginContext(runtime_ctx)

        _maybe_run_storage_preflight(runtime_ctx, self.engine_config)

        # 3. Telemetry Setup
        # BaseIngestor always uses TelemetryService for unified limits
        if getattr(runtime_ctx, "telemetry", None) is None:
            runtime_ctx.telemetry = create_ingestion_telemetry(
                plugin=self.name,
                product=product,
                output_format=output_format,
                write_mode=execution.write_mode,
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
        producer: dict[str, Any] | None = None
        # Every claim of this run reports its loss here: claims taken on this thread
        # through `reporting_claim_loss`, worker claims by explicit attachment.
        claim_latch = self._run_claim_latch = ClaimLossLatch()

        from firecube.ingestor.runtime.output_guard import (
            fresh_output_run,
            require_empty_data_target,
        )

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
                reporting_claim_loss(claim_latch),
                contextlib.ExitStack() as output_ownership,
            ):
                self._check_at_startup(runtime_plugin_ctx)

                # 4. Resume Guard
                slot_range_for_record = _slot_range(execution)
                guard = resume_guard = self._new_resume_guard()
                self._enforce_resume_guard(guard, runtime_ctx, runtime_plugin_ctx, product)

                self._discovered_source_files = self.discovered_source_files(runtime_plugin_ctx)
                output_ownership.enter_context(
                    fresh_output_run(
                        runtime_ctx,
                        self._chunk_manager,
                        required=self.requires_fresh_target,
                        format_label=self.fresh_target_format_label,
                    )
                )

                # 5. Runtime control-plane lifecycle. The producer is captured once;
                # the failure record below carries the same record. It stays out of
                # slice_meta: register_run copies that into span records and into the
                # force-reingest query, which matches prior spans on every meta key.
                producer = capture_producer(
                    plugin_name=self.name,
                    plugin_cls=type(self),
                    configs={
                        "engine": self.engine_config,
                        "execution": execution,
                        "template": self.template_config,
                        "plugin": self.plugin_config,
                    },
                )
                slice_meta = self._run_record_meta(runtime_plugin_ctx)
                self._span_recorder.register_run_started(
                    run_id=run_id,
                    product=product,
                    output_path=str(runtime_ctx.target or ""),
                    output_format=output_format,
                    slice_meta={**slice_meta, "producer": producer},
                    slot_range=slot_range_for_record,
                    slot_group=execution.slot_group,
                )
                run_started_recorded = True

                self._ensure_index_record_at_startup(runtime_plugin_ctx)
                self._verify_schema_at_pod_startup(runtime_plugin_ctx)

                # 6. Planning
                mode = determine_execution_mode(self.engine_config)
                # pipeline_workers >= 2 selects the threaded pipeline runner;
                # otherwise the sequential runner. Both plan via BatchPlanner.

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

                # A claim lost while the last batch ran fails the run, recorded below.
                self._refuse_lost_claim()

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
                if self.requires_fresh_target and execution.write_mode == "staged":
                    require_empty_data_target(
                        ctx.storage.output, format_label=self.fresh_target_format_label
                    )
                execution_result = self._firecube_engine.complete_output(
                    execution_result, runtime_ctx, host=self
                )
                # A claim lost during output completion, or one whose loss only its later
                # release would find: one read per claim still held, such as the product claim.
                confirmed = claim_latch.confirm_held_claims()
                self._log.debug("confirmed %d held claim(s) before the terminal record", confirmed)
                self._refuse_lost_claim()

                # 8. Recording
                if not execution_result.registered:
                    self._span_recorder.register_run(
                        result=execution_result,
                        run_id=run_id,
                        product=product,
                        slice_meta=slice_meta,
                        force_reingest=runtime_ctx.force_reingest,
                        record_spans=not execution_result.spans_recorded,
                    )

                return execution_result
        except Exception as exc:
            # Record a terminal failure only for runs that reached "started" but
            # never reached a terminal WAL event. After the output-completion
            # reorder above, execution_result.registered becomes True only after
            # both complete_output() and register_run() succeed.
            # A batch that failed for another reason after a claim was lost still names
            # the lost claim, from the run's latch.
            lost = _claim_loss_behind(exc) or claim_latch.error()
            if run_started_recorded and not execution_result.registered:
                with contextlib.suppress(Exception):
                    slice_meta = self._run_record_meta(runtime_plugin_ctx)
                    failure_meta = {**slice_meta, "producer": producer}
                    if lost is not None:
                        failure_meta["lost_claim"] = lost.domain
                    self._span_recorder.register_run_failure(
                        run_id=run_id,
                        product=product,
                        output_path=str(runtime_ctx.target or ""),
                        output_format=str(runtime_ctx.output_format or ""),
                        slice_meta=failure_meta,
                        error=str(exc),
                    )
                    execution_result.registered = True
            if exc.__cause__ is None and lost is not None and lost is not exc:
                # The run is recorded failed; name the lost claim as the root cause.
                exc.__cause__ = lost
            raise

        finally:
            self._discovered_source_files = None
            self._forget_discovery()
            close_failed = False
            try:
                self._close_run_state()
            except Exception as exc:
                # Never masks the run's own outcome.
                close_failed = True
                self._log.warning("Closing the run state failed: %s", exc)
            # Teardown
            should_cleanup = (
                self.engine_config.cleanup_workspace if hasattr(self, "engine_config") else False
            )
            if should_cleanup and close_failed:
                # The run's writes may still be landing in the staged store
                # inside the workspace; deleting it would race them.
                should_cleanup = False
                self._log.error(
                    "Workspace %s left in place: the run's writes were still in flight "
                    "when its run state was closed",
                    self._workspace.temp_root,
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
        self, ctx: RuntimeIngestContext | PluginContext, write_mode: str = "direct"
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
