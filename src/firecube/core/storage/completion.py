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

import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from firecube.core.config import StorageConfig
from firecube.core.errors import ConfigurationError, Message, Param, StorageError
from firecube.core.product import CompletionRoute, write_mode_policy
from firecube.core.product.identity import ProductIdentity
from firecube.core.product.target import ProductTarget
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.results import StorageWriteResult
from firecube.core.storage.session import StorageSession, storage_config_from_binding
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import is_remote_target, local_path_from_target

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession as _StorageSession


def _output_session(ctx: Any) -> _StorageSession | None:
    storage = ctx.storage
    return storage.output if storage is not None else None


def _output_product_name(ctx: Any, default: str = "") -> str:
    session = _output_session(ctx)
    if session is not None:
        return str(session.product.product_name)
    return default


def _storage_config_from_ctx(ctx: Any) -> StorageConfig | None:
    session = _output_session(ctx)
    if session is None:
        return None
    return storage_config_from_binding(session._binding)


@dataclass(frozen=True, slots=True)
class StagedPromotionUnits:
    """What a staged run proved it wrote: promotion publishes it as an object delta.

    Keys are store keys relative to the product root (``group/array/c/0/1``),
    the same in the workspace and in the target.

    Attributes:
        workspace: URI of the workspace product root the keys are relative to.
        target: URI of the target product root the delta is published onto.
        written: Object key of every stored unit the run wrote in every cell.
        absent: The written units whose object the write left absent (every
            cell equals the fill value); promotion deletes them from the target.
        publish_metadata: Puts one promoted ``zarr.json`` document (key
            relative to ``target``) under the run's authority over the target;
            returns ``False`` when it wrote nothing, which promotion then
            does not count as written.
        ledger_arrays: ``group/array`` paths whose writes the run records.
            Promotion refuses a changed data object of these arrays that
            ``written`` does not hold.
        managed_coordinates: ``group/array`` paths of the time coordinates
            the target marks as preallocated or coordinate-managed.
            Promotion neither puts nor compares their data objects; it
            publishes their ``zarr.json`` as any other.
    """

    workspace: str
    target: str
    written: frozenset[str]
    absent: frozenset[str]
    publish_metadata: Callable[[str, bytes], bool | None]
    ledger_arrays: frozenset[str] = frozenset()
    managed_coordinates: frozenset[str] = frozenset()


def _same_root(left: str, right: StorageUri) -> bool:
    """Whether the URI *left* names the product root *right*."""
    if not is_remote_target(left) and not right.is_remote():
        return local_path_from_target(left).resolve() == Path(right.path).resolve()
    return StorageUri.parse(left).to_str().rstrip("/") == right.to_str().rstrip("/")


def _promote(
    session: Any,
    src: StorageUri,
    dst: StorageUri,
    *,
    staged_units: StagedPromotionUnits | None,
    parallel_workers: int,
) -> StorageWriteResult:
    """Publish a staged tree: as a delta when the run reported its units, else the whole tree.

    A delta is refused, before anything is written to ``dst``, when the
    units name other roots than ``src`` and ``dst`` (their keys would land on
    other objects), when the plan cannot be proved (a written unit neither
    in the workspace nor reported absent), or when it would publish a changed
    data object of a ledger array that no recorded write covers.

    Raises:
        StorageError: The roots differ or the plan cannot be proved.
        ConfigurationError: The plan holds such an uncovered data object.
    """
    from firecube.core.zarr.guard.promotion_plan import unclaimed_data_objects

    if staged_units is None:
        return session.upload_tree(src, dst, parallel_workers=parallel_workers)
    if not _same_root(staged_units.workspace, src) or not _same_root(staged_units.target, dst):
        raise StorageError(
            f"staged promotion refused, target untouched: the run's units are relative to "
            f"{staged_units.workspace} -> {staged_units.target}, promotion is "
            f"{src.to_str()} -> {dst.to_str()}"
        )
    try:
        plan = session.plan_promotion(
            src,
            dst,
            written_units=staged_units.written,
            absent_units=staged_units.absent,
            managed_coordinates=staged_units.managed_coordinates,
        )
    except ValueError as exc:
        raise StorageError(f"staged promotion refused, target untouched: {exc}") from exc
    uncovered = unclaimed_data_objects(plan, staged_units.ledger_arrays)
    if uncovered:
        raise ConfigurationError(
            f"staged promotion refused, target untouched: {len(uncovered)} workspace data "
            "object(s) differ from the target, but no write of this run covers them: "
            f"{list(uncovered[:20])}. Publishing them would overwrite target data this run "
            "never wrote. They come from outside this run's batches, for example a reused "
            "workspace; rerun with a fresh workspace."
        )
    return session.publish_plan(
        plan,
        src,
        dst,
        parallel_workers=parallel_workers,
        publish_metadata=staged_units.publish_metadata,
    )


class StorageCompleter:
    def complete_output(
        self, result: Any, ctx: Any, *, staged_units: StagedPromotionUnits | None = None
    ) -> StorageWriteResult:
        """Publish the run's output to the product target.

        Args:
            result: The run's result; ``outputs`` names the staged output.
            ctx: The run context; ``ctx.storage.output`` is the target session.
                ``ctx.execution.write_mode`` applies when the result records no write mode.
            staged_units: Given by a staged run that proved what it wrote:
                promotion then publishes the object delta (puts, deletes of
                units the run left all fill, ``zarr.json`` documents through
                ``staged_units.publish_metadata``) instead of uploading the
                whole workspace tree. ``None``: the whole tree, as always.

        Raises:
            StorageError: No target session, the staged output is missing,
                the delta is refused (nothing written), or an upload fails.
            ConfigurationError: The delta would publish a changed data object
                of ``staged_units.ledger_arrays`` that no recorded write
                covers; nothing is written.
        """
        session = _output_session(ctx)
        if session is None:
            raise StorageError("Storage completion requires ctx.storage.output.")
        storage_config = storage_config_from_binding(session._binding)

        effective_write_mode = str(result.metrics.write_mode or ctx.execution.write_mode)
        product_uri = session.product.product_uri
        if staged_units is None:
            if not product_uri.is_remote():
                return self.complete_local_storage(result, ctx)
            return self.complete_s3_storage(result, ctx, storage_config, effective_write_mode)
        if not product_uri.is_remote():
            return self.complete_local_storage(result, ctx, staged_units=staged_units)
        return self.complete_s3_storage(
            result, ctx, storage_config, effective_write_mode, staged_units=staged_units
        )

    def complete_local_storage(
        self, result: Any, ctx: Any, *, staged_units: StagedPromotionUnits | None = None
    ) -> StorageWriteResult:
        session = _output_session(ctx)
        if session is None:
            raise StorageError("Local completion requires ctx.storage.output.")

        product_uri = session.product.product_uri
        final_target = Path(product_uri.path)
        output_path = str(result.outputs.primary or "")
        if "://" in output_path:
            source_uri = StorageUri.parse(output_path)
            if source_uri.protocol != "file":
                raise StorageError(f"Local staged output must be file://, got {output_path}")
            source_path = Path(source_uri.path).resolve()
        else:
            source_path = Path(output_path).resolve()

        is_already_in_place = False
        try:
            source_path.relative_to(final_target.resolve())
            is_already_in_place = True
        except ValueError:
            pass

        if is_already_in_place:
            return StorageWriteResult(
                path=str(source_path),
                bytes_written=0,
                files_written=0,
                duration_s=0.0,
                storage_type="local",
            )

        if not source_path.exists():
            raise StorageError(f"Local staged output not found at {source_path}")

        upload_workers = int(ctx.option("upload_workers", 4))
        return _promote(
            session,
            StorageUri.from_local_path(source_path),
            StorageUri.from_local_path(final_target),
            staged_units=staged_units,
            parallel_workers=upload_workers,
        )

    def complete_s3_storage(
        self,
        result: Any,
        ctx: Any,
        storage_config: StorageConfig,
        effective_write_mode: str,
        *,
        staged_units: StagedPromotionUnits | None = None,
    ) -> StorageWriteResult:
        session = _output_session(ctx)
        if session is None:
            raise StorageError("Remote completion requires ctx.storage.output.")
        final_target_uri = session.product.product_uri.to_str()

        route = write_mode_policy(effective_write_mode).completion_route
        if route is CompletionRoute.DIRECT:
            return self.complete_s3_direct(result, storage_config, final_target_uri)

        if staged_units is None:
            return self.complete_s3_staged(result, ctx, final_target_uri)
        return self.complete_s3_staged(result, ctx, final_target_uri, staged_units=staged_units)

    def complete_s3_direct(
        self,
        result: Any,
        storage_config: StorageConfig,
        final_target_uri: str,
    ) -> StorageWriteResult:
        """Describe a direct S3 write without scanning the target store.

        A plugin-reported ``result.metrics.storage`` summary is trusted only
        when it carries counts (``files`` or ``bytes`` not ``None``); the
        engine-seeded block that holds just ``control_root``/``latest_pointer``
        has none and must not be mistaken for a zero-byte write. Otherwise the
        returned ``storage_result`` carries zero counters to match direct-local
        behaviour: no staged upload happened, so the manifest and metrics stay
        ``null`` per :meth:`PipelineExecutor.complete_output`. The
        ``storage_config`` parameter is retained for signature compatibility
        with the sibling completion routes but is not consulted in this branch.
        """
        storage_summary = result.metrics.storage
        if storage_summary is not None and (
            storage_summary.files is not None or storage_summary.bytes is not None
        ):
            return StorageWriteResult(
                path=str(storage_summary.path or final_target_uri),
                bytes_written=int(storage_summary.bytes or 0),
                files_written=int(storage_summary.files or 0),
                duration_s=float(storage_summary.duration_s or 0.0),
                storage_type="s3",
            )

        return StorageWriteResult(
            path=str(final_target_uri),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    def complete_s3_staged(
        self,
        result: Any,
        ctx: Any,
        final_target_uri: str,
        *,
        staged_units: StagedPromotionUnits | None = None,
    ) -> StorageWriteResult:
        source_path = Path(str(result.outputs.primary or ""))
        if result.output_format == "zarr":
            zarr_out = result.outputs.zarr
            zarr_out_str = str(zarr_out) if zarr_out is not None else ""
            if zarr_out_str and not is_remote_target(zarr_out_str):
                candidate = Path(zarr_out_str)
                if candidate.exists():
                    source_path = candidate
            elif zarr_out_str and zarr_out_str.startswith("file://"):
                try:
                    candidate = local_path_from_target(zarr_out_str)
                    if candidate.exists():
                        source_path = candidate
                except Exception:
                    pass

        resolved_source = source_path.resolve()
        if resolved_source == Path.home().resolve() or resolved_source == Path("/").resolve():
            raise StorageError(f"Critical Safety: Attempted to upload {resolved_source}. Aborting.")

        if not source_path.exists():
            raise StorageError(
                Message(
                    "Ingestion failed: write_mode is 'staged' but no local output "
                    "was found at {source_path}. "
                    "If the plugin writes directly to the target, use {write_mode}.",
                    source_path=source_path,
                    product_uri=final_target_uri,
                    write_mode=Param("write_mode", "direct"),
                )
            )

        storage_config = _storage_config_from_ctx(ctx)
        if storage_config is None:
            raise StorageError("Staged remote upload requires ctx.storage.output.")

        driver_config = StorageDriverConfig.from_storage_config(storage_config)
        storage_session = _output_session(ctx)
        session = storage_session
        if session is None:
            resolved_product = ProductTarget.resolve(
                final_target_uri,
                driver_config,
                product_name=_output_product_name(ctx, final_target_uri),
                plugin_default_format=result.output_format or "zarr",
            )
            session = StorageSession(
                StorageBinding(
                    identity=ProductIdentity(
                        product_name=resolved_product.product_name,
                        product_uri=resolved_product.product_uri,
                        control_root_uri=resolved_product.control_root_uri,
                        format=resolved_product.format,
                    ),
                    driver=driver_config,
                )
            )

        effective_target_uri = (
            session.product.product_uri.to_str()
            if storage_session is not None
            else final_target_uri
        )
        telemetry = getattr(ctx, "telemetry", None)
        upload_span = (
            telemetry.span(
                "firecube.upload_s3",
                attributes={"firecube.target": str(final_target_uri)},
            )
            if telemetry is not None
            else contextlib.nullcontext()
        )
        upload_workers = int(ctx.option("upload_workers", 4))
        source_uri = StorageUri.from_local_path(source_path)
        with upload_span:
            stored = _promote(
                session,
                source_uri,
                StorageUri.parse(effective_target_uri),
                staged_units=staged_units,
                parallel_workers=upload_workers,
            )

        result.metrics.storage_handled = True
        pipeline_metrics = result.metrics.pipeline
        if pipeline_metrics is not None:
            pipeline_metrics.duration_upload_s = stored.duration_s
            pipeline_metrics.duration_total_s = (
                pipeline_metrics.duration_pipeline_s + stored.duration_s
            )

        return stored
