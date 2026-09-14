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

"""Generic ingestor template facades for Zarr and Parquet batch pipelines."""

from __future__ import annotations

import contextlib
from abc import abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any, ClassVar, cast

import xarray as xr

from firecube.core.api import (
    create_filesystem_for_uri,  # type: ignore
    is_remote_target,
    local_path_from_target,
)
from firecube.core.filesystem import fs_kwargs_for_uri
from firecube.ingestor.api import (
    BaseIngestor,
    ConfigurationError,
    OutputPaths,
    ParquetTemplateConfig,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    WriteDomain,
    ZarrTemplateConfig,
)
from firecube.ingestor.extensions.duck import DuckDbMixin
from firecube.ingestor.runtime.zarr import batch_runner
from firecube.ingestor.runtime.zarr.alignment import AlignmentMonitor
from firecube.ingestor.runtime.zarr.append_failure import AppendBatchFailed
from firecube.ingestor.runtime.zarr.append_order import AppendOrder
from firecube.ingestor.runtime.zarr.ordered_gate import ExclusiveSection, OrderedWriteGate
from firecube.ingestor.templates._parquet import relative_data_path
from firecube.ingestor.templates.config import validate_zarr_writer_dict
from firecube.ingestor.types import write_mode_policy


def _ctx_output_session(ctx: PluginContext) -> Any | None:
    storage = ctx.storage
    return storage.output if storage is not None else None


def _ctx_product_name(ctx: PluginContext, default: str) -> str:
    session = _ctx_output_session(ctx)
    if session is not None:
        return str(session.product.product_name)
    return default


def _telemetry_span(telemetry: Any | None, name: str, attrs: dict[str, str] | None = None):
    if telemetry is None:
        return contextlib.nullcontext()
    return (
        cast(Any, telemetry.span(name, attrs))
        if attrs is not None
        else cast(Any, telemetry.span(name))
    )


def _runtime_reingest_options(ctx: PluginContext) -> tuple[bool, bool]:
    resume_existing = bool(ctx.option("resume_existing", False))
    force_reingest = bool(ctx.option("force_reingest", False)) or bool(
        getattr(ctx, "force_reingest", False)
    )
    return resume_existing, force_reingest


def _batch_write_index(batch: PipelineBatch) -> int | None:
    """Return the planner position stamped by ``BatchPlanner``, or ``None``.

    Batches built outside the planner (direct ``_process_batch`` calls) carry
    no index and are admitted to the write gate in arrival order.
    """
    value = batch.metadata.get("batch_index")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _resolve_zarr_batch_targets(
    ingestor: Any, ctx: PluginContext, write_mode: str
) -> tuple[str, str | None]:
    store_uri = ingestor.resolve_output_uri(ctx, write_mode=write_mode)
    final_target_uri: str | None = None
    try:
        final_target_uri = ingestor.resolve_output_uri(ctx, write_mode="direct")
    except Exception as uri_exc:
        ingestor._log.debug("Could not resolve direct URI for output target: %s", uri_exc)
    return store_uri, final_target_uri


def _build_zarr_batch_runtime(
    ingestor: Any,
    ctx: PluginContext,
    *,
    store_uri: str,
    final_target_uri: str | None,
    groups: list[str],
    zarr_config: dict[str, Any],
    resume_existing: bool,
    force_reingest: bool,
    write_mode: str,
) -> tuple[Any, Any, Any]:
    append_read_target_uri = final_target_uri
    if (
        write_mode_policy(write_mode).seeds_staged_metadata
        and final_target_uri is not None
        and store_uri != final_target_uri
    ):
        # Staged mode seeds final-target metadata into the workspace before
        # every batch. Append reads must use that workspace so batches append
        # to prior staged writes instead of repeatedly re-reading the stale
        # final target.
        append_read_target_uri = None

    claim_for_group = batch_runner.build_claim_closure_for_append(
        chunk_manager=ingestor._chunk_manager,
        product=_ctx_product_name(ctx, ingestor.name),
        run_id=str(ctx.run_id or ctx.option("run_id", "unknown")),
    )
    write_ctx_mgr = batch_runner.build_zarr_write_context(zarr_config=zarr_config)
    strategy = batch_runner.build_append_strategy(
        store_uri=store_uri,
        final_target_uri=append_read_target_uri,
        zarr_config=zarr_config,
        resume_existing=resume_existing,
        force_reingest=force_reingest,
        append_dim=ingestor._resolve_time_dim_name(),
        chunk_manager=ingestor._chunk_manager,
        session=_ctx_output_session(ctx),
        logger=ingestor._log,
        alignment=ingestor._alignment,
        order=ingestor._append_order,
        pipeline_write_mode=write_mode,
        staged_final_target_uri=final_target_uri,
        preflight_compare_target_uri=final_target_uri,
    )
    return write_ctx_mgr, claim_for_group, strategy


class GenericZarrIngestor(BaseIngestor):
    """Thin facade over `AppendStrategy` for Zarr-based batch ingestion.

    Resolves URIs/storage, acquires write claims, then delegates all append
    logic to ``AppendStrategy.write_groups()``.

    Subclasses implement ``build_dataset(group, items, ctx) -> xr.Dataset | None``.

    Appends commit in planner batch order through an ``OrderedWriteGate``:
    with ``pipeline_workers > 1`` the workers parallelise ``prepare_batch_data``
    only, and the time axis stays monotonic. The host stops at the first
    failed batch (``stop_on_batch_failure``) so no batch appends past a gap;
    later batches are reported as not attempted.
    """

    template_config_class = ZarrTemplateConfig
    stop_on_batch_failure: ClassVar[bool] = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._write_gate = OrderedWriteGate()
        self._alignment = AlignmentMonitor()
        self._append_order = AppendOrder()

    @property
    def write_lock(self) -> ExclusiveSection:
        """Serialise plugin store access with the engine's batch writes.

        Use it in hooks that open the target store from the main thread
        (typically ``on_batch_success`` bookkeeping) so the access never
        overlaps a worker's append. It waits for the current batch write to
        finish, holds the writer slot for the block, and does not change the
        batch order.

        Returns:
            A context manager holding exclusive access during its block.

        Examples:
            with self.write_lock:
                root = zarr.open_group(target, mode="a", use_consolidated=False)
                root.attrs["receipts"] = receipts
        """
        return self._write_gate.exclusive()

    def _validate_duckdb_persistence_contract(self) -> None:
        """Fail fast when persistent DuckDB mode is requested without required hooks."""
        if isinstance(self, DuckDbMixin):
            return
        raise ConfigurationError(
            "duckdb_persist_batches=true requires DuckDbMixin-compatible hooks. "
        )

    def on_pipeline_start(self, ctx: PluginContext, state: PipelineRunState) -> None:
        """Initialize pipeline resources (including persistent DuckDB)."""
        super().on_pipeline_start(ctx, state)
        self._write_gate.reset()
        self._alignment = AlignmentMonitor()
        self._append_order = AppendOrder()
        if state.pipeline_workers > 1:
            self._log.warning(
                "Pipeline configured with workers=%d: appends commit in batch order through "
                "the ordered write gate; workers parallelise prepare_batch_data only.",
                state.pipeline_workers,
            )

        # Handle persistent DuckDB initialization to prevent write-write conflicts
        # Only needed if we are actually USING persistence (duckdb_persist_batches=True).
        # Otherwise, workers use transient memory DBs, so this main-thread DB is just litter.
        should_persist = bool(getattr(self.engine_config, "duckdb_persist_batches", False))
        if should_persist:
            self._validate_duckdb_persistence_contract()

        if should_persist and not ctx.in_memory and ctx.temp_root and isinstance(self, DuckDbMixin):
            # We explicitly setup/teardown in the main thread
            self.setup_duckdb(
                workspace=ctx.temp_root,
                options=ctx.options,
                in_memory=False,
            )
            try:
                self.prepare_duckdb_schema(self.con, ctx)
            except Exception as e:
                self._log.warning("Failed to prepare DuckDB schema: %s", e)
                raise
            finally:
                self.teardown_duckdb()

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, Any]:
        """Merge batch metrics and close the run's alignment monitor.

        Runs once per run, before the engine raises on failed batches, so the
        alignment summary is logged and ``metrics["zarr"]["unaligned_batches"]``
        is set whether or not the run succeeded.
        """
        merged = dict(super()._aggregate_metrics(ctx, state))
        self._alignment.emit_summary(self._log)
        zarr_metrics = merged.get("zarr")
        if isinstance(zarr_metrics, dict):
            zarr_metrics["unaligned_batches"] = self._alignment.unaligned_total
        return merged

    @abstractmethod
    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        """Convert a sub-batch of items into an Xarray Dataset for the given group.

        ``items`` is the time-grouped slice for this batch iteration.
        Returns None to skip writing for this group/batch.

        The returned dataset must carry the ingestor's ``time_dim_name``
        dimension, ordered on that dimension, with values that do not
        overlap another batch; it is appended along that dimension.
        Variables, dimensions, coordinates, and data types must remain
        compatible across batches. Every time-aligned coordinate must be
        supplied on append and overwrite. Timestamps must be unique within
        each batch, and append-only batches must follow the stored maximum.
        Declare datetime encoding precise enough for all batches when the
        first batch alone cannot establish the required resolution.

        Examples:
            Build one dataset per batch from the discovered items:

                def build_dataset(self, group, items, ctx):
                    paths = [ctx.materialize(item) for item in items]
                    ds = xr.open_mfdataset(paths, combine="by_coords")
                    return ds[["temperature"]].sortby(self.time_dim_name)

            Route variables per group when ``get_batch_groups`` declares
            more than one; every group receives the same ``items``:

                def build_dataset(self, group, items, ctx):
                    ds = self._open(items, ctx)
                    if group == "quality":
                        return ds[["quality_level"]]
                    return ds[["temperature"]]
        """

    def get_zarr_config(self, ctx: PluginContext) -> dict[str, Any]:
        """Return writer options, optionally deriving a layout at runtime.

        The default maps validated ``ZarrTemplateConfig`` fields to writer
        keys. Override this hook for dynamic layouts; start with
        ``super().get_zarr_config(ctx)`` to retain configured defaults.

        Args:
            ctx: Read-only context for the current run.

        Returns:
            Writer options with keys ``chunk_shape``, ``compression``,
            ``zarr_codecs``, ``consolidate``, ``time_encoding``,
            ``async_concurrency``, ``write_empty_chunks``, ``dask_scheduler``,
            ``write_threads``, ``shard_shape``, and ``sharding``. These are
            writer keys, not the ``zarr_*`` CLI option names. The default
            returns an empty mapping if no Zarr template config is bound.
            ``time_encoding`` must be ``None`` or empty; non-empty values
            are rejected as not implemented. Set encoding on the dataset.
        """
        cfg = self.template_config  # Validated ZarrTemplateConfig
        if not isinstance(cfg, ZarrTemplateConfig):
            return {}

        # Map strict config to internal zarr_utils expectations
        return {
            "chunk_shape": cfg.zarr_chunk_shape,
            "compression": cfg.zarr_compression,
            "zarr_codecs": cfg.zarr_codecs,
            "consolidate": cfg.zarr_consolidate,
            "time_encoding": cfg.zarr_time_encoding,
            "async_concurrency": cfg.zarr_async_concurrency,
            "write_empty_chunks": cfg.zarr_write_empty_chunks,
            "dask_scheduler": cfg.dask_scheduler,
            "write_threads": cfg.dask_write_threads,
            "shard_shape": cfg.zarr_shard_shape,
            "sharding": cfg.zarr_sharding,
        }

    def _bind_index_at_startup(self, ctx: PluginContext) -> None:
        super()._bind_index_at_startup(ctx)
        validate_zarr_writer_dict(self.get_zarr_config(ctx))

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        self.batch_setup(ctx)
        write_turn = self._write_gate.turn(_batch_write_index(batch))
        store_uri: str | None = None

        try:
            telemetry = getattr(ctx, "telemetry", None)
            with _telemetry_span(telemetry, "firecube.batch.prepare"):
                prep_metrics = self.prepare_batch_data(batch, ctx) or {}

            files = batch.items if batch.items else batch.metadata.get("files", [])
            groups = self.get_batch_groups(files, ctx)
            if not groups:
                groups = ["default"]
            zarr_config = self.get_zarr_config(ctx)
            validate_zarr_writer_dict(zarr_config)
            write_mode = self.engine_config.write_mode
            resume_existing, force_reingest = _runtime_reingest_options(ctx)
            store_uri, final_target_uri = _resolve_zarr_batch_targets(self, ctx, write_mode)
            time_dim_name = self._resolve_time_dim_name()
            batch_runner.prepare_staged_append_metadata(
                ctx=ctx,
                store_uri=store_uri,
                final_target_uri=final_target_uri,
                groups=groups,
                resume_existing=resume_existing,
                force_reingest=force_reingest,
                write_mode=write_mode,
                logger=self._log,
                time_dim_name=time_dim_name,
            )
            write_ctx_mgr, claim_for_group, strategy = _build_zarr_batch_runtime(
                self,
                ctx,
                store_uri=store_uri,
                final_target_uri=final_target_uri,
                groups=groups,
                zarr_config=zarr_config,
                resume_existing=resume_existing,
                force_reingest=force_reingest,
                write_mode=write_mode,
            )

            zarr_metrics: dict[str, Any] = {}
            with write_turn as admitted:
                if not admitted:
                    return PipelineResult(
                        batch=batch,
                        outputs=OutputPaths(primary=Path("")),
                        success=False,
                        attempted=False,
                        error="not attempted: run halted after an earlier batch failed",
                    )
                with (
                    write_ctx_mgr,
                    _telemetry_span(
                        telemetry,
                        "firecube.batch.zarr_write",
                        {"firecube.store_uri": str(store_uri)},
                    ),
                ):
                    zarr_metrics = strategy.write_groups(
                        group_to_timestamps=dict.fromkeys(groups, files),
                        dataset_for_batch=lambda g, items: self.build_dataset(g, list(items), ctx),
                        batch_size=len(files),
                        claim_for_group=claim_for_group,
                        is_final_batch=bool(batch.metadata.get("is_last", False)),
                    )

            final_metrics = batch_runner.assemble_batch_metrics(
                prep_metrics=prep_metrics,
                zarr_metrics=zarr_metrics,
                file_count=len(files),
                write_mode=write_mode,
            )

            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=str(store_uri), zarr=str(store_uri)),
                metrics=final_metrics,
                success=True,
            )

        except AppendBatchFailed as exc:
            # The append path repaired the failed group and reports what the
            # store now holds: earlier groups stay committed, the failed
            # group's region slots carry state 3, later groups were not
            # attempted. The result carries all of it so the failure hook
            # records one truthful span per group. Raised inside the write
            # turn, so the gate already released this index as failed.
            self._log.exception("Batch processing failed")
            outcome = exc.outcome
            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=str(store_uri or ""), zarr=str(store_uri or "")),
                metrics={
                    "zarr": dict(outcome.counters),
                    "coverage": list(outcome.committed),
                    "failed_coverage": (
                        [outcome.failed_entry] if outcome.failed_entry is not None else []
                    ),
                    "failed_group": outcome.failed_group,
                    "not_attempted_groups": list(outcome.not_attempted_groups),
                    "repair": outcome.repair.to_dict(),
                },
                success=False,
                error=str(exc),
            )

        except Exception as exc:
            self._log.exception("Batch processing failed")
            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=Path("")),
                success=False,
                error=str(exc),
            )

        finally:
            # A batch that failed before its write turn still owns a slot in
            # the ordered sequence; releasing it as failed halts later batches
            # instead of stalling them.
            write_turn.forfeit()
            try:
                self.cleanup_batch_data(batch, ctx)
            except Exception as exc:
                self._log.warning("Batch cleanup failed: %s", exc)
            self.batch_teardown(ctx)


class GenericParquetIngestor(BaseIngestor):
    """Write independent Parquet parts into a fresh product target.

    Existing product data or previous runs are refused, including when
    ``resume_existing`` or ``force_reingest`` is requested. Use a new target
    for each run until the template supports stable slice identity and replay.
    A run owns the target through completion; batches can write distinct
    relative paths concurrently inside that run.
    """

    template_config_class = ParquetTemplateConfig
    requires_fresh_target: ClassVar[bool] = True
    fresh_target_format_label: ClassVar[str] = "Parquet"

    @abstractmethod
    def build_dataset(self, group: str, batch: PipelineBatch, ctx: PluginContext) -> Any | None:  # pyright: ignore[reportIncompatibleMethodOverride]
        """Convert a batch of items into an in-memory tabular dataset for the given group.

        Supported return types for the default Parquet writer:
          - ``pyarrow.Table``
          - ``pandas.DataFrame`` (if pandas is installed)

        Returns None to skip writing for this group/batch.

        Unlike the Zarr template, this hook receives the ``PipelineBatch``
        itself rather than a list of items.

        Examples:
            Return one table per batch:

                def build_dataset(self, group, batch, ctx):
                    rows = []
                    for item in batch.items:
                        rows.extend(read_detections(ctx.materialize(item)))
                    if not rows:
                        return None
                    return pyarrow.Table.from_pylist(rows)
        """

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return the logical write groups for a batch (Hook).

        Each group produces one ``build_dataset`` call (receiving the full
        batch) and one Parquet file; non-default group names become
        subdirectories of the dataset root via ``output_relpath``. Must be
        deterministic and stable-sorted. Default: ``["default"]``.
        """
        _ = items
        _ = ctx
        return ["default"]

    def output_relpath(self, group: str, batch: PipelineBatch, ctx: PluginContext) -> str:
        """Return a unique relative data path for this group and batch.

        Paths must stay inside the dataset root and outside ``.firecube``.
        Reusing a path within the run is refused before overwriting its file.
        """
        _ = ctx
        chunk_name = f"part-{batch.batch_id}.parquet"
        if group and group != "default":
            return f"{group}/{chunk_name}"
        return chunk_name

    def write_parquet(
        self,
        dataset: Any,
        *,
        output_path: str,
        storage_options: dict[str, Any] | None,
        ctx: PluginContext,
    ) -> int:
        """Write a supported dataset to Parquet and return number of rows written."""
        try:
            import pyarrow as pa  # type: ignore
            import pyarrow.parquet as pq  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pyarrow is required for GenericParquetIngestor. Install `pyarrow` "
                "or override `write_parquet()`."
            ) from exc

        table: pa.Table
        if isinstance(dataset, pa.Table):
            table = dataset
        else:
            try:
                import pandas as pd  # type: ignore
            except ImportError as exc:  # pragma: no cover
                raise TypeError(
                    "Unsupported dataset type for Parquet writer. Return `pyarrow.Table`, "
                    "install pandas and return `pandas.DataFrame`, or override `write_parquet()`."
                ) from exc

            if not isinstance(dataset, pd.DataFrame):
                raise TypeError(
                    "Unsupported dataset type for Parquet writer. Return `pyarrow.Table`, "
                    "return `pandas.DataFrame`, or override `write_parquet()`."
                )
            table = pa.Table.from_pandas(dataset, preserve_index=False)

        if is_remote_target(output_path):
            storage_config = self._chunk_manager.storage_config
            if storage_config is not None:
                fs, fs_path = create_filesystem_for_uri(
                    output_path, storage_config, format="parquet"
                )
            else:
                raise ConfigurationError(
                    "storage_config is required for remote parquet writes; pass --storage-type and --storage-driver"
                )
            with fs.open(fs_path, "wb") as f:  # pyright: ignore[reportArgumentType]
                pq.write_table(table, f)
        else:
            pq.write_table(table, output_path)

        return int(table.num_rows)

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        self.batch_setup(ctx)

        try:
            prep_metrics = self.prepare_batch_data(batch, ctx) or {}

            write_mode = self.engine_config.write_mode
            base_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
            self._log.debug(
                "Parquet Output Resolution: target=%s base_uri=%s", ctx.target, base_uri
            )

            files = batch.items if batch.items else batch.metadata.get("files", [])
            groups = self.get_batch_groups(files, ctx)
            if not groups:
                groups = ["default"]

            outputs: list[str] = []
            coverage: list[dict[str, Any]] = []
            rows_written_total = 0

            for group in groups:
                dataset = self.build_dataset(group, batch, ctx)
                if dataset is None:
                    continue

                rel = relative_data_path(self.output_relpath(group, batch, ctx))
                if is_remote_target(base_uri):
                    output_path = f"{base_uri.rstrip('/')}/{rel.lstrip('/')}"
                else:
                    output_path = str(local_path_from_target(base_uri) / rel)

                self._log.debug("Parquet Output Path: %s", output_path)

                if not is_remote_target(output_path):
                    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

                storage_options = fs_kwargs_for_uri(output_path, self._chunk_manager.storage_config)
                product = _ctx_product_name(ctx, self.name)
                run_id = str(ctx.run_id or ctx.option("run_id", "unknown"))
                domain = WriteDomain(
                    product=product,
                    category="parquet_path",
                    name=str(rel),
                )
                with self._chunk_manager.acquire_claim(
                    product=product,
                    domain=domain,
                    owner_id=f"{run_id}:{rel}",
                ):
                    storage_config = self._chunk_manager.storage_config
                    if storage_config is None:
                        raise ConfigurationError("Parquet writing requires output storage.")
                    fs, path = create_filesystem_for_uri(
                        output_path, storage_config, format="parquet"
                    )
                    if fs.exists(path):
                        raise ConfigurationError(f"Parquet output already exists: {output_path}")
                    rows_written_total += self.write_parquet(
                        dataset, output_path=output_path, storage_options=storage_options, ctx=ctx
                    )
                outputs.append(output_path)
                coverage.append(
                    {
                        "group": group or "default",
                        "arrays": ["parquet"],
                        "time_index_ranges": [],
                        "outputs": [output_path],
                    }
                )

            metrics = dict(prep_metrics)
            metrics.setdefault("rows", rows_written_total)
            metrics.setdefault("outputs", outputs)
            metrics.setdefault("coverage", coverage)

            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(
                    primary=local_path_from_target(base_uri)
                    if not is_remote_target(base_uri)
                    else base_uri
                ),
                metrics=metrics,
                success=True,
            )
        except Exception as exc:
            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=Path("")),
                success=False,
                error=str(exc),
            )
        finally:
            try:
                self.cleanup_batch_data(batch, ctx)
            except Exception as exc:
                self._log.warning("Batch cleanup failed: %s", exc)
            self.batch_teardown(ctx)


__all__ = [
    "GenericParquetIngestor",
    "GenericZarrIngestor",
]
