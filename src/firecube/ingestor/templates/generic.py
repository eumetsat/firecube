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
import threading
from abc import abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

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
    WriteDomain,
    ZarrTemplateConfig,
)
from firecube.ingestor.extensions.duck import DuckDbMixin
from firecube.ingestor.runtime.zarr import batch_runner


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
    claim_for_group = batch_runner.build_claim_closure_for_append(
        chunk_manager=ingestor._chunk_manager,
        product=_ctx_product_name(ctx, ingestor.name),
        run_id=str(ctx.run_id or ctx.option("run_id", "unknown")),
    )
    write_ctx_mgr = batch_runner.build_zarr_write_context(
        zarr_config=zarr_config,
        write_lock=ingestor._write_lock,
    )
    strategy = batch_runner.build_append_strategy(
        store_uri=store_uri,
        final_target_uri=final_target_uri,
        zarr_config=zarr_config,
        resume_existing=resume_existing,
        force_reingest=force_reingest,
        append_dim=ingestor._resolve_time_dim_name(),
        chunk_manager=ingestor._chunk_manager,
        session=_ctx_output_session(ctx),
        logger=ingestor._log,
    )
    return write_ctx_mgr, claim_for_group, strategy


class GenericZarrIngestor(BaseIngestor):
    """Thin facade over :class:`AppendStrategy` for Zarr-based batch ingestion.

    Resolves URIs/storage, acquires write claims, then delegates all append
    logic to ``AppendStrategy.write_groups()``.

    Subclasses implement ``build_dataset(group, items, ctx) -> xr.Dataset | None``.
    """

    template_config_class = ZarrTemplateConfig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._write_lock = threading.Lock()

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
        if state.pipeline_workers > 1:
            self._log.warning(
                "Pipeline configured with workers=%d, but Zarr writes are serialized by a global lock. "
                "Parallelism accelerates preprocessing only.",
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

    @abstractmethod
    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        """Convert a sub-batch of items into an Xarray Dataset for the given group.

        ``items`` is the time-grouped slice for this batch iteration.
        Returns None to skip writing for this group/batch.

        The returned dataset must carry the ingestor's ``time_dim_name``
        dimension, ordered on that dimension, with values that do not
        overlap another batch; it is appended along that dimension.
        Variables, dimensions, coordinates, and data types must remain
        compatible across batches.

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
        """Return Zarr storage options from validated template config."""
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

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        self.batch_setup(ctx)

        try:
            telemetry = getattr(ctx, "telemetry", None)
            with _telemetry_span(telemetry, "firecube.batch.prepare"):
                prep_metrics = self.prepare_batch_data(batch, ctx) or {}

            files = batch.items if batch.items else batch.metadata.get("files", [])
            groups = self.get_batch_groups(files, ctx)
            if not groups:
                groups = ["default"]
            zarr_config = self.get_zarr_config(ctx)
            write_mode = self.engine_config.write_mode
            resume_existing, force_reingest = _runtime_reingest_options(ctx)
            store_uri, final_target_uri = _resolve_zarr_batch_targets(self, ctx, write_mode)
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
            with (
                write_ctx_mgr,
                _telemetry_span(
                    telemetry, "firecube.batch.zarr_write", {"firecube.store_uri": str(store_uri)}
                ),
            ):
                zarr_metrics = strategy.write_groups(
                    group_to_timestamps=dict.fromkeys(groups, files),
                    dataset_for_batch=lambda g, items: self.build_dataset(g, list(items), ctx),
                    batch_size=len(files),
                    claim_for_group=claim_for_group,
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

        except Exception as exc:
            self._log.exception("Batch processing failed")
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


class GenericParquetIngestor(BaseIngestor):
    """Generic Pipelined Ingestor for Parquet outputs."""

    template_config_class = ParquetTemplateConfig

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
        """Return a relative output path (within the dataset root) for a group/batch."""
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

                rel = self.output_relpath(group, batch, ctx)
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
