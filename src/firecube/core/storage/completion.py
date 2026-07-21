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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from firecube.core.config import StorageConfig
from firecube.core.errors import StorageError
from firecube.core.filesystem import path_stats
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


class StorageCompleter:
    def complete_output(self, result: Any, ctx: Any) -> StorageWriteResult:
        session = _output_session(ctx)
        if session is None:
            raise StorageError("Storage completion requires ctx.storage.output.")
        storage_config = storage_config_from_binding(session._binding)

        effective_write_mode = str(result.metrics.write_mode or ctx.option("write_mode") or "")
        product_uri = session.product.product_uri
        if not product_uri.is_remote():
            return self.complete_local_storage(result, ctx)
        return self.complete_s3_storage(result, ctx, storage_config, effective_write_mode)

    def complete_local_storage(self, result: Any, ctx: Any) -> StorageWriteResult:
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
        return session.upload_tree(
            StorageUri.from_local_path(source_path),
            StorageUri.from_local_path(final_target),
            parallel_workers=upload_workers,
        )

    def complete_s3_storage(
        self,
        result: Any,
        ctx: Any,
        storage_config: StorageConfig,
        effective_write_mode: str,
    ) -> StorageWriteResult:
        session = _output_session(ctx)
        if session is None:
            raise StorageError("Remote completion requires ctx.storage.output.")
        final_target_uri = session.product.product_uri.to_str()

        route = write_mode_policy(effective_write_mode).completion_route
        if route is CompletionRoute.DIRECT:
            return self.complete_s3_direct(result, storage_config, final_target_uri)

        return self.complete_s3_staged(result, ctx, final_target_uri)

    def complete_s3_direct(
        self,
        result: Any,
        storage_config: StorageConfig,
        final_target_uri: str,
    ) -> StorageWriteResult:
        storage_summary = result.metrics.storage
        if storage_summary is not None:
            return StorageWriteResult(
                path=str(storage_summary.path or final_target_uri),
                bytes_written=int(storage_summary.bytes or 0),
                files_written=int(storage_summary.files or 0),
                duration_s=float(storage_summary.duration_s or 0.0),
                storage_type="s3",
            )

        stats = path_stats(final_target_uri, storage_config=storage_config)
        return StorageWriteResult(
            path=str(final_target_uri),
            bytes_written=int(stats.get("bytes", 0) or 0),
            files_written=int(stats.get("files", 0) or 0),
            duration_s=0.0,
            storage_type="s3",
        )

    def complete_s3_staged(
        self,
        result: Any,
        ctx: Any,
        final_target_uri: str,
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
                f"Ingestion failed: write_mode is 'staged' but no local output "
                f"was found at {source_path}. "
                "If the plugin writes directly to the target, use --write-mode direct."
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
            stored = session.upload_tree(
                source_uri,
                StorageUri.parse(effective_target_uri),
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
