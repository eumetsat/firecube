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

"""Batch-execution helpers for GenericZarrIngestor._process_batch (§22 facade thinning).

Each helper extracts one inline responsibility from _process_batch, matching
the §4a service decomposition precedent (append_services.py + append.py).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from firecube.core.controlplane.types import WriteDomain
from firecube.core.product import write_mode_policy
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
from firecube.ingestor.runtime.zarr.write_context import ZarrWriteContext


def seed_staged_metadata_for_batch(
    *,
    ctx: Any,
    store_uri: str,
    final_target_uri: str | None,
    groups: list[str] | None = None,
    resume_existing: bool,
    force_reingest: bool,
    write_mode: str,
    logger: logging.Logger,
    coordinate_arrays: list[str] | None = None,
) -> None:
    """Seed staged temp-store metadata from the final target when applicable."""
    if not write_mode_policy(write_mode).seeds_staged_metadata or final_target_uri is None:
        return

    from firecube.ingestor.runtime.zarr.staged_metadata import (
        StagedMetadataError,
        seed_staged_store_metadata,
    )

    strict = bool(resume_existing and not force_reingest)
    storage = getattr(ctx, "storage", None)
    output_session = storage.output if storage is not None else None
    if output_session is None:
        logger.debug("Staged metadata seeding skipped: no output session")
        return

    try:
        seed_staged_store_metadata(
            temp_store_uri=store_uri,
            final_target_uri=final_target_uri,
            groups=groups,
            session=output_session,
            strict=strict,
            coordinate_arrays=coordinate_arrays,
        )
    except StagedMetadataError:
        raise
    except Exception as seed_exc:
        logger.debug("Staged metadata seeding skipped: %s", seed_exc)


def seed_staged_metadata_pre_batch(
    *,
    host: Any,
    ctx: Any,
    logger: logging.Logger,
    coordinate_arrays: list[str] | None = None,
) -> None:
    """Runtime-level staged metadata seeding hook for Zarr batch execution."""
    engine_config = getattr(host, "engine_config", None)
    write_mode = str(getattr(engine_config, "write_mode", ""))
    if not write_mode_policy(write_mode).seeds_staged_metadata:
        return

    store_uri = host.resolve_output_uri(ctx, write_mode=write_mode)
    try:
        final_target_uri = host.resolve_output_uri(ctx, write_mode="direct")
    except Exception as uri_exc:
        logger.debug("Could not resolve direct URI for staged metadata seeding: %s", uri_exc)
        return

    seed_staged_metadata_for_batch(
        ctx=ctx,
        store_uri=store_uri,
        final_target_uri=final_target_uri,
        groups=None,
        resume_existing=bool(ctx.option("resume_existing", False)),
        force_reingest=bool(ctx.option("force_reingest", False))
        or bool(getattr(ctx, "force_reingest", False)),
        write_mode=write_mode,
        logger=logger,
        coordinate_arrays=coordinate_arrays,
    )


def build_zarr_write_context(
    *,
    zarr_config: dict[str, Any],
    write_lock: Any,
) -> ZarrWriteContext:
    """Build the Zarr write context from template config and the ingestor lock."""
    return ZarrWriteContext(
        write_lock=write_lock,
        configured_scheduler=zarr_config.get("dask_scheduler"),
        write_threads=int(zarr_config.get("write_threads", 0)),
        async_concurrency=int(zarr_config.get("async_concurrency", 10)),
    )


def build_claim_closure_for_append(
    *,
    chunk_manager: Any,
    product: str,
    run_id: str,
) -> Callable[[str], Any]:
    """Build the per-group append claim closure used by AppendStrategy."""

    def _claim_for_group(group_name: str) -> Any:
        domain = WriteDomain(product=product, category="zarr_append", name=str(group_name))
        return chunk_manager.acquire_claim(
            product=product,
            domain=domain,
            owner_id=f"{run_id}:{group_name}",
        )

    return _claim_for_group


def build_append_strategy(
    *,
    store_uri: str,
    final_target_uri: str | None,
    zarr_config: dict[str, Any],
    resume_existing: bool,
    force_reingest: bool,
    append_dim: str = "timestamp",
    chunk_manager: Any,
    session: Any,
    logger: logging.Logger,
) -> AppendStrategy:
    """Build the append strategy with the same static arguments as _process_batch."""
    return AppendStrategy(
        store=store_uri,
        store_uri=store_uri,
        resume_target_uri=final_target_uri,
        chunk_shape=zarr_config.get("chunk_shape"),
        shard_shape=zarr_config.get("shard_shape"),
        sharding=bool(zarr_config.get("sharding", False)),
        compression=zarr_config.get("compression") or False,
        consolidate=bool(zarr_config.get("consolidate")),
        resume_existing=bool(resume_existing and not force_reingest),
        append_dim=append_dim,
        logger=logger,
        storage_config=chunk_manager.storage_config,
        session=session,
    )


def assemble_batch_metrics(
    *,
    prep_metrics: dict[str, Any],
    zarr_metrics: dict[str, Any],
    file_count: int,
    write_mode: str,
) -> dict[str, Any]:
    """Assemble final batch metrics using GenericZarrIngestor's merge shape."""
    final_metrics = dict(prep_metrics)
    final_metrics.update(
        {
            "zarr": zarr_metrics,
            "coverage": zarr_metrics.get("coverage", []),
            "count": file_count,
            "storage_handled": write_mode_policy(write_mode).storage_handled_by_engine,
        }
    )
    return final_metrics
