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

"""Deletion Engine: Handles chunk deletion, vacuuming, and storage cleanup."""

from __future__ import annotations

import itertools
import logging
import math
import uuid
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from firecube.core.config import StorageConfig
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.time_dim import resolve_span_time_dims, resolve_time_dim_index
from firecube.core.controlplane.types import (
    MAINTENANCE_OP_DELETE,
    STATE_DELETED_BY_FIRECUBE,
    ChunkInfo,
    DeletionPlan,
    WriteDomain,
)
from firecube.core.errors import ClaimConflictError, ManifestError
from firecube.core.runtime import identity_from_storage_config
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.validation import _STATE_ARRAY_NAME

log = logging.getLogger(__name__)


def _span_time_indices(time_ranges: Sequence[Any]) -> list[int]:
    indices: set[int] = set()
    for pair in time_ranges:
        if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
            continue
        try:
            start_i, end_i = int(pair[0]), int(pair[1])
        except (ValueError, TypeError, IndexError):
            continue
        if end_i < start_i:
            continue
        indices.update(range(start_i, end_i + 1))
    return sorted(indices)


def _contiguous_ranges(indices: Sequence[int]) -> list[tuple[int, int]]:
    ordered = sorted({int(i) for i in indices if int(i) >= 0})
    if not ordered:
        return []

    ranges: list[tuple[int, int]] = []
    start = previous = ordered[0]
    for idx in ordered[1:]:
        if idx == previous + 1:
            previous = idx
            continue
        ranges.append((start, previous))
        start = previous = idx
    ranges.append((start, previous))
    return ranges


def _span_group_name(span: ChunkInfo) -> str:
    meta = span.meta if isinstance(span.meta, dict) else {}
    return str(meta["group"]).strip("/")


def _span_state_array_name(
    *, spec: dict[str, Any], group: str, default: str = _STATE_ARRAY_NAME
) -> str:
    state_path = spec.get("state_array")
    if not isinstance(state_path, str):
        return default

    clean = state_path.strip("/")
    if not clean:
        return default

    group_prefix = f"{group.strip('/')}/" if group.strip("/") else ""
    if group_prefix and clean.startswith(group_prefix):
        return clean.removeprefix(group_prefix).split("/", 1)[0]
    return clean.rsplit("/", 1)[-1]


def _is_completed_region_fill(span: ChunkInfo) -> bool:
    """True when ``span`` was already replaced by an in-place NaN/fill.

    Such spans reach ``delete_spans`` only through ``include_replaced``; their
    slots are already filled and their state already marks them deleted, so a
    second fill is a no-op the operator must ask for explicitly.
    """
    if span.status != "replaced":
        return False
    meta = span.meta if isinstance(span.meta, dict) else {}
    return meta.get("write_strategy") == "region_nan_fill"


def _array_path_relative_to_group(array_path: str, group: str) -> str:
    clean = str(array_path).strip("/")
    group = group.strip("/")
    if group and clean.startswith(f"{group}/"):
        return clean[len(group) + 1 :]
    return clean


def _array_time_dim_index(array: Any, expected_time_dim_name: str) -> int:
    metadata = getattr(array, "metadata", None)
    raw_dim_names = getattr(metadata, "dimension_names", None)
    if raw_dim_names is None:
        raw_dim_names = getattr(array, "attrs", {}).get("_ARRAY_DIMENSIONS")
    dim_names = [str(dim) for dim in raw_dim_names] if raw_dim_names else []
    return resolve_time_dim_index(dim_names, expected_time_dim_name)


def _fill_value_for_array_write(array: Any) -> Any:
    import numpy as np

    fill_value = array.fill_value
    dtype = np.dtype(array.dtype)
    if fill_value is not None:
        return fill_value
    if dtype.kind in ("f", "c"):
        return np.nan
    if dtype.kind == "M":
        return np.datetime64("NaT")
    if dtype.kind == "m":
        return np.timedelta64("NaT")
    return dtype.type(0)


def _time_selection(ndim: int, time_dim: int, start: int, end: int) -> tuple[Any, ...]:
    selection: list[Any] = [slice(None)] * ndim
    selection[time_dim] = slice(start, end + 1)
    return tuple(selection)


def _zarr_group_has_array(
    *,
    store_uri: str,
    group: str,
    array_name: str,
    storage_config: StorageConfig,
) -> bool:
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store

    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
    try:
        root = cast(
            Any,
            zarr.open_group(
                **handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
            ),
        )
    except FileNotFoundError:
        return False
    try:
        zarr_group = cast(Any, root if not group.strip("/") else root[group.strip("/")])
    except KeyError:
        return False
    try:
        zarr_group[array_name]
    except KeyError:
        return False
    return True


def delete_span_via_region_nan_fill(
    store_uri: str,
    group: str,
    time_indices: list[int],
    storage_config: StorageConfig,
    state_array_name: str = _STATE_ARRAY_NAME,
    *,
    array_paths: Sequence[str] | None = None,
    time_dim_name: str = "timestamp",
    update_state: bool = True,
    state_deleted_value: int = STATE_DELETED_BY_FIRECUBE,
) -> None:
    """NaN/fill the specified time indices in-place without deleting chunk keys."""
    import numpy as np
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.zarr.region_writer import _array_is_all_fill

    ranges = _contiguous_ranges(time_indices)
    if not ranges:
        return

    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r+")
    root = cast(
        Any,
        zarr.open_group(**handle.zarr_kwargs(), mode="r+", zarr_format=3, use_consolidated=False),
    )
    zarr_group = cast(Any, root if not group.strip("/") else root[group.strip("/")])

    if array_paths is None:
        relative_array_paths = [
            str(name) for name in zarr_group.array_keys() if str(name) != state_array_name
        ]
    else:
        relative_array_paths = []
        for array_path in array_paths:
            relative = _array_path_relative_to_group(str(array_path), group)
            if relative and relative != state_array_name and relative not in relative_array_paths:
                relative_array_paths.append(relative)

    for relative_array_path in relative_array_paths:
        array = cast(Any, zarr_group[relative_array_path])
        time_dim = _array_time_dim_index(array, time_dim_name)
        fill_value = _fill_value_for_array_write(array)
        for start, end in ranges:
            selection = _time_selection(array.ndim, time_dim, start, end)
            array[selection] = fill_value
            filled = np.asarray(array[selection])
            if not _array_is_all_fill(filled, fill_value):
                raise RuntimeError(
                    f"Failed to fill {group}/{relative_array_path} with its declared fill value"
                )

    if not update_state:
        return

    state_array = cast(Any, zarr_group[state_array_name])
    state_value = np.uint8(int(state_deleted_value))
    for start, end in ranges:
        state_array[start : end + 1] = state_value


def _local_base_from_storage_config(storage_config: StorageConfig | None) -> Path | None:
    """Derive the local base directory used to materialise chunk paths.

    Mirrors the legacy dict-based contract: when the CLI passes a typed
    StorageConfig, the local base is the ``target_path`` exposed on the
    bridge-extended config (matches ``identity_from_storage_config(...).product_uri.path``).
    """
    if storage_config is None:
        return None
    identity = identity_from_storage_config(storage_config)
    if identity is None:
        return None
    product_uri = identity.product_uri
    if product_uri.protocol != "file":
        return None
    return Path(product_uri.path)


class DeletionEngine:
    """Handles deletion of chunks from storage and manifests."""

    def __init__(
        self,
        repo: ManifestRepository,
        filesystem: Any = None,
        *,
        time_dim_name: str = "timestamp",
    ):
        self.repo = repo
        self.filesystem = filesystem
        self.time_dim_name = time_dim_name
        self.log = logging.getLogger(f"{__name__}.DeletionEngine")

    def create_deletion_plan(
        self,
        pattern: str | None = None,
        product: str | None = None,
        before: datetime | None = None,
        after: datetime | None = None,
        chunk_type: str | None = None,
        status: str | None = None,
        include_metadata: bool = False,
        meta: dict[str, Any] | None = None,
        filter_fn: Callable[[ChunkInfo], bool] | None = None,
        *,
        time_overlaps: tuple[str, str] | None = None,
    ) -> DeletionPlan:
        """Create a plan for deleting chunks based on filters."""
        # Query repo
        chunks = self.repo.list_chunks(
            pattern=pattern,
            product=product,
            before=before,
            after=after,
            chunk_type=chunk_type,
            status=status,
            meta=meta,
            filter_fn=filter_fn,
            time_overlaps=time_overlaps,
        )

        if not include_metadata and chunk_type != "meta":
            chunks = [c for c in chunks if c.chunk_type != "meta"]

        total_size = sum(c.size for c in chunks)
        products = {c.product for c in chunks}
        manifests = {c.manifest_path for c in chunks}

        return DeletionPlan(
            chunks=chunks,
            total_size=total_size,
            products_affected=products,
            manifest_files=manifests,
        )

    def _maintenance_claim_message(self, *, product: str, operation: str, detail: str) -> str:
        return (
            f"Cannot run {operation} for product {product}: {detail}. "
            "If a prior writer is stuck, resolve it with `firecube chunks runs abandon`."
        )

    def _acquire_maintenance_claims(
        self, *, products: Iterable[str], operation: str
    ) -> list[tuple[str, WriteDomain]]:
        product_list = sorted({product for product in products if product})
        if not product_list:
            return []

        owner_id = f"maintenance:{operation}:{uuid.uuid4()}"
        acquired: list[tuple[str, WriteDomain]] = []
        try:
            for product in product_list:
                active_claims = self.repo.list_claims(product=product)
                if active_claims:
                    details = ", ".join(
                        f"{claim.domain} (owner={claim.owner_id})" for claim in active_claims
                    )
                    raise ManifestError(
                        self._maintenance_claim_message(
                            product=product,
                            operation=operation,
                            detail=f"active write claim(s) exist: {details}",
                        )
                    )

            for product in product_list:
                domain = WriteDomain(product=product, category="maintenance", name=operation)
                self.repo.acquire_claim(product=product, domain=domain, owner_id=owner_id)
                acquired.append((product, domain))
        except ClaimConflictError as exc:
            detail = str(exc) or "write claim acquisition failed"
            raise ManifestError(
                self._maintenance_claim_message(
                    product=product_list[0],
                    operation=operation,
                    detail=detail,
                )
            ) from exc
        except Exception:
            self._clear_maintenance_claims(acquired)
            raise
        return acquired

    def _clear_maintenance_claims(self, claims: Iterable[tuple[str, WriteDomain]]) -> None:
        for product, domain in reversed(list(claims)):
            with suppress(Exception):
                self.repo.clear_claim(product=product, domain_id=domain.identifier, force=True)

    def _record_maintenance_started(
        self,
        *,
        products: list[str],
        run_id: str,
        op: str,
        scope_for: Callable[[str], dict[str, Any]],
    ) -> list[str]:
        started: list[str] = []
        for product in products:
            try:
                self.repo.record_maintenance_started(
                    product=product,
                    run_id=run_id,
                    op=op,
                    scope_meta=scope_for(product),
                )
            except Exception:
                self.log.exception(
                    "Failed to record maintenance_started for product=%s run=%s",
                    product,
                    run_id,
                )
                continue
            started.append(product)
        return started

    def _record_maintenance_completed(
        self,
        *,
        products: list[str],
        run_id: str,
        op: str,
        scope_for: Callable[[str], dict[str, Any]],
    ) -> None:
        for product in products:
            try:
                self.repo.record_maintenance_completed(
                    product=product,
                    run_id=run_id,
                    op=op,
                    scope_meta=scope_for(product),
                )
            except Exception:
                self.log.exception(
                    "Failed to record maintenance_completed for product=%s run=%s",
                    product,
                    run_id,
                )

    def _record_maintenance_failed(
        self,
        *,
        products: list[str],
        run_id: str,
        op: str,
        scope_for: Callable[[str], dict[str, Any]],
        error: str,
    ) -> None:
        for product in products:
            try:
                self.repo.record_maintenance_failed(
                    product=product,
                    run_id=run_id,
                    op=op,
                    scope_meta=scope_for(product),
                    error=error,
                )
            except Exception:
                self.log.exception(
                    "Failed to record maintenance_failed for product=%s run=%s",
                    product,
                    run_id,
                )

    def execute_deletion(
        self,
        plan: DeletionPlan,
        delete_storage: bool = True,
        delete_manifest: bool = True,
        storage_config: StorageConfig | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Execute a deletion plan."""
        if dry_run:
            return {
                "dry_run": True,
                "would_delete_chunks": plan.count,
                "would_delete_size_bytes": plan.total_size,
                "products_affected": list(plan.products_affected),
            }

        affected_products = sorted(
            {p for p in plan.products_affected if p}
            or {chunk.product for chunk in plan.chunks if chunk.product}
        )
        claims = self._acquire_maintenance_claims(
            products=affected_products,
            operation="delete_chunks",
        )

        run_id = f"maintenance-delete-{uuid.uuid4().hex}"

        def _scope_for(product: str) -> dict[str, Any]:
            product_chunks = [c for c in plan.chunks if c.product == product]
            return {
                "chunks_count": len(product_chunks),
                "size_bytes": sum(int(c.size or 0) for c in product_chunks),
                "delete_storage": bool(delete_storage),
                "delete_manifest": bool(delete_manifest),
                "products_affected": affected_products,
            }

        started_products: list[str] = []
        try:
            started_products = self._record_maintenance_started(
                products=affected_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
            )

            deleted_chunks = 0
            deleted_size = 0
            storage_errors = []
            manifest_errors = []

            if delete_storage and (storage_config or self.filesystem):
                storage_errors = self._delete_from_storage(plan.chunks, storage_config)
                if not storage_errors:
                    pass

            if delete_manifest:
                by_manifest = {}
                for c in plan.chunks:
                    by_manifest.setdefault(c.manifest_path, []).append(c)

                for m_uri, chunks in by_manifest.items():
                    try:
                        count, size = self.repo.remove_from_manifest(m_uri, chunks)
                        deleted_chunks += count
                        deleted_size += size
                    except Exception as e:
                        msg = f"Failed to update manifest {m_uri}: {e}"
                        self.log.error(msg)
                        manifest_errors.append(msg)

            self._record_maintenance_completed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
            )

            return {
                "deleted_chunks": deleted_chunks,
                "deleted_size_bytes": deleted_size,
                "storage_errors": storage_errors,
                "manifest_errors": manifest_errors,
            }
        except Exception as exc:
            self._record_maintenance_failed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
                error=str(exc),
            )
            raise
        finally:
            self._clear_maintenance_claims(claims)

    def _delete_from_storage(
        self, chunks: list[ChunkInfo], storage_config: StorageConfig | None
    ) -> list[str]:
        """Dispatch to S3 or Local deletion."""
        if self.filesystem is not None:
            return self._delete_with_filesystem(chunks, self.filesystem)

        from firecube.core.uris import infer_target_protocol

        proto = infer_target_protocol(self.repo.base_uri or "")
        storage_type = storage_config.storage_type if storage_config is not None else None
        if proto == "s3" or storage_type == "s3":
            return self._delete_from_s3(chunks)
        else:
            return self._delete_from_local(chunks, storage_config)

    def _delete_with_filesystem(self, chunks: list[ChunkInfo], filesystem: Any) -> list[str]:
        errors: list[str] = []
        if not self.repo.base_uri:
            return ["Missing base URI; cannot delete without a base URI"]
        base_path = self.repo.base_uri.rstrip("/")
        for chunk in chunks:
            path = f"{base_path}/{chunk.product.strip('/')}/{chunk.key.lstrip('/')}".replace(
                "//", "/"
            )
            try:
                if filesystem.exists(path):
                    filesystem.rm(path, recursive=True)
            except Exception as e:
                errors.append(f"Failed to delete {path}: {e}")
        return errors

    def _delete_from_s3(self, chunks: list[ChunkInfo]) -> list[str]:
        """Delete chunks from S3 storage."""
        errors: list[str] = []

        if not self.repo.base_uri:
            return ["Missing base URI; cannot delete from S3 without a base URI"]

        base_uri = self.repo.base_uri.rstrip("/")
        fs, base_path = self.repo._get_fs(base_uri)
        base_path = str(base_path).rstrip("/")

        s3_paths: list[str] = []
        for chunk in chunks:
            p = f"{base_path}/{chunk.product.strip('/')}/{chunk.key.lstrip('/')}".replace("//", "/")
            s3_paths.append(p)

        batch_size = 1000  # S3 delete limit
        for i in range(0, len(s3_paths), batch_size):
            batch_paths = s3_paths[i : i + batch_size]
            try:
                # TODO(uri-refactor): S3 deletion still operates on raw str paths;
                # migrate to StorageUri once a base-relative URI builder lands.
                existing_paths = [path for path in batch_paths if fs.exists(path)]  # pyright: ignore[reportArgumentType]
                if existing_paths:
                    fs.rm(existing_paths, recursive=False)  # pyright: ignore[reportArgumentType]
                    self.log.info("Deleted %s files from S3", len(existing_paths))
            except Exception as e:
                error_msg = f"Failed to delete S3 batch: {e}"
                self.log.error(error_msg)
                errors.append(error_msg)

        return errors

    def _delete_from_local(
        self, chunks: list[ChunkInfo], storage_config: StorageConfig | None
    ) -> list[str]:
        errors = []
        base = _local_base_from_storage_config(storage_config)
        if base is None:
            return [
                "Missing local target base; cannot delete local storage without a file:// product URI"
            ]
        for c in chunks:
            try:
                p = base / c.product / c.key
                if p.exists():
                    if p.is_file():
                        p.unlink()
                    else:
                        import shutil

                        shutil.rmtree(p)
            except Exception as e:
                errors.append(str(e))
        return errors

    def _measure_span_alignment(
        self,
        *,
        store_uri: str,
        arrays: list[str],
        time_ranges: list[Any],
        expected_time_dim_name: str,
        span_key: str,
        wal_aligned: bool,
    ) -> tuple[bool, dict[str, tuple[list[str], list[int], list[int], int]], list[str]]:
        """Measure alignment from the STORED chunk grid per data array.

        The WAL-recorded ``aligned`` flag can be wrong: it may have been
        computed against the REQUESTED chunk size rather than the STORED
        chunk size. To keep deletion decisions truthful,
        this helper opens each array's chunk grid metadata and checks
        whether every span time range lands on chunk boundaries along the
        resolved time dimension. Grids are cached so the caller can reuse
        them without re-reading zarr.json inside the deletion loop.

        Returns:
            ``(measured_aligned, per_array_grids, read_errors)`` where
            ``per_array_grids`` maps ``array_path -> (dim_names, shape,
            chunk_shape, time_dim_index)``. If no grid could be read,
            falls back to the WAL flag rather than silently deleting.
        """
        from firecube.core.zarr.validation import read_chunk_grid

        per_array_grids: dict[str, tuple[list[str], list[int], list[int], int]] = {}
        read_errors: list[str] = []
        array_flags: list[bool] = []

        normalised: list[tuple[int, int]] = []
        for pair in time_ranges:
            if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
                continue
            try:
                s_i, e_i = int(pair[0]), int(pair[1])
            except Exception:
                continue
            if e_i < s_i:
                continue
            normalised.append((s_i, e_i))

        for array_path in arrays:
            try:
                dim_names, shape, chunk_shape = read_chunk_grid(
                    store_uri,
                    array_path,
                    storage_config=self.repo.storage_config,
                )
            except Exception as exc:
                read_errors.append(f"Failed to read zarr.json for {array_path}: {exc}")
                continue

            if not shape or not chunk_shape:
                continue

            time_dim = resolve_time_dim_index(dim_names, expected_time_dim_name)
            per_array_grids[array_path] = (dim_names, shape, chunk_shape, time_dim)

            time_chunk = int(chunk_shape[time_dim] or 0)
            time_length = int(shape[time_dim] or 0)
            if time_chunk <= 0 or time_length <= 0 or not normalised:
                continue

            array_aligned = True
            for s_i, e_i in normalised:
                lo_chunk = s_i // time_chunk
                hi_chunk = e_i // time_chunk
                chunk_span_lo = lo_chunk * time_chunk
                chunk_span_hi = min(time_length - 1, (hi_chunk + 1) * time_chunk - 1)
                if chunk_span_lo != s_i or chunk_span_hi != e_i:
                    array_aligned = False
                    break
            array_flags.append(array_aligned)

        if array_flags:
            measured_aligned = all(array_flags)
        else:
            # No grid could be measured. Falling back to the WAL flag is
            # safer than silently allowing deletion: if the caller passes
            # ``force``, they can still override the guard.
            measured_aligned = wal_aligned

        self.log.debug(
            "span %s: WAL alignment=%s, measured alignment=%s",
            span_key,
            wal_aligned,
            measured_aligned,
        )
        return measured_aligned, per_array_grids, read_errors

    @staticmethod
    def _render_collateral_entry(span: ChunkInfo) -> str:
        meta = span.meta if isinstance(span.meta, dict) else {}
        t_min = meta.get("time_min")
        t_max = meta.get("time_max")
        if t_min and t_max:
            return f"{span.key} ({t_min}..{t_max})"
        return span.key

    def _find_collateral_spans(
        self,
        *,
        arrays: list[str],
        time_ranges: list[Any],
        per_array_grids: dict[str, tuple[list[str], list[int], list[int], int]],
        product: str,
        target_span_keys: set[str],
        all_spans: Sequence[ChunkInfo] | None = None,
    ) -> list[ChunkInfo]:
        """Return OTHER active spans whose slot data lives in the physical
        chunks about to be deleted.

        For each array in the target span, the requested time-index ranges are
        expanded to full physical chunk boundaries using the stored
        ``chunk_shape`` on the time axis. Any other active span for the same
        product that (a) writes to the same array AND (b) has a
        ``time_index_ranges`` entry that intersects the expanded physical
        range is treated as collateral: force-deleting the target span will
        also destroy its data because they share the same physical chunk keys
        on disk.
        """
        per_array_physical: dict[str, list[tuple[int, int]]] = {}
        for array_path in arrays:
            grid = per_array_grids.get(array_path)
            if grid is None:
                continue
            _dim_names, shape, chunk_shape, time_dim = grid
            if not shape or not chunk_shape:
                continue
            time_chunk = int(chunk_shape[time_dim] or 0)
            time_length = int(shape[time_dim] or 0)
            if time_chunk <= 0 or time_length <= 0:
                continue

            physical_ranges: list[tuple[int, int]] = []
            for pair in time_ranges:
                if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
                    continue
                try:
                    s_i, e_i = int(pair[0]), int(pair[1])
                except Exception:
                    continue
                if e_i < s_i:
                    continue
                lo_chunk = s_i // time_chunk
                hi_chunk = e_i // time_chunk
                phys_lo = lo_chunk * time_chunk
                phys_hi = min(time_length - 1, (hi_chunk + 1) * time_chunk - 1)
                physical_ranges.append((phys_lo, phys_hi))
            if physical_ranges:
                per_array_physical[array_path] = physical_ranges

        if not per_array_physical:
            return []

        if all_spans is None:
            all_spans = self.repo.list_chunks(product=product, chunk_type="span")

        collateral: list[ChunkInfo] = []
        for other in all_spans:
            if other.key in target_span_keys:
                continue
            if not other.is_active:
                continue
            payload = other.record if isinstance(other.record, dict) else {}
            other_spec = payload.get("span") if isinstance(payload, dict) else None
            if not isinstance(other_spec, dict):
                continue
            other_arrays = other_spec.get("arrays")
            other_ranges = other_spec.get("time_index_ranges")
            if not isinstance(other_arrays, list) or not isinstance(other_ranges, list):
                continue

            hit = False
            for array_path, physical_ranges in per_array_physical.items():
                if array_path not in other_arrays:
                    continue
                for pair in other_ranges:
                    if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
                        continue
                    try:
                        o_start, o_end = int(pair[0]), int(pair[1])
                    except Exception:
                        continue
                    if o_end < o_start:
                        continue
                    for phys_lo, phys_hi in physical_ranges:
                        if o_start <= phys_hi and o_end >= phys_lo:
                            hit = True
                            break
                    if hit:
                        break
                if hit:
                    break
            if hit:
                collateral.append(other)

        return collateral

    def _delete_span_by_region_fill(
        self,
        *,
        span: ChunkInfo,
        group_name: str,
        store_uri: str,
        fs: Any,
        spec: dict[str, Any],
        dry_run: bool,
        force: bool,
        collateral_spans: Sequence[ChunkInfo],
        arrays: list[Any],
        time_indices: list[int],
        expected_time_dim_name: str,
        update_state: bool,
        update_manifest: bool,
    ) -> dict[str, Any]:
        del fs, force, collateral_spans
        errors: list[str] = []
        replacement_meta_updates_by_key: dict[str, dict[str, Any]] = {}

        state_array_name = _span_state_array_name(spec=spec, group=group_name)
        if not dry_run:
            try:
                delete_span_via_region_nan_fill(
                    store_uri=store_uri,
                    group=group_name,
                    time_indices=time_indices,
                    storage_config=self.repo.storage_config,
                    state_array_name=state_array_name,
                    array_paths=[str(array_path) for array_path in arrays],
                    time_dim_name=expected_time_dim_name,
                    update_state=update_state,
                    state_deleted_value=int(
                        spec.get("state_deleted_value", STATE_DELETED_BY_FIRECUBE)
                        or STATE_DELETED_BY_FIRECUBE
                    ),
                )
            except Exception as e:
                errors.append(f"Failed to region-fill span {span.key}: {e}")
                return {"deleted_keys": 0, "deleted_spans": 0, "errors": errors}

        replaced_span_keys: list[str] = []
        if update_manifest:
            replaced_span_keys.append(span.key)
            replacement_meta_updates_by_key[span.key] = {"write_strategy": "region_nan_fill"}
        return {
            "deleted_keys": 0,
            "deleted_spans": 1,
            "region_filled_spans": 1,
            "errors": errors,
            "warnings": [],
            "replaced_span_keys": replaced_span_keys,
            "replacement_meta_updates_by_key": replacement_meta_updates_by_key,
        }

    def _delete_span_by_chunk_keys(
        self,
        *,
        span: ChunkInfo,
        group_name: str,
        store_uri: str,
        fs: Any,
        spec: dict[str, Any],
        dry_run: bool,
        force: bool,
        collateral_spans: Sequence[ChunkInfo],
        product: str,
        base_path: str,
        arrays: list[str],
        time_ranges: list[Any],
        expected_time_dim_name: str,
        update_state: bool,
        update_manifest: bool,
        yes_i_really_mean_it: bool,
        all_spans: Sequence[ChunkInfo],
        target_span_keys: set[str],
    ) -> dict[str, Any]:
        del group_name
        from firecube.core.zarr.validation import read_chunk_grid

        deleted_keys = 0
        already_absent_keys = 0
        span_paths_added = 0
        errors: list[str] = []
        warnings: list[str] = []
        collateral_span_keys: list[str] = []
        pending_paths: list[str] = []
        pending_limit = 1000
        wal_aligned = bool(spec.get("aligned", True))
        measured_aligned, per_array_grids, grid_read_errors = self._measure_span_alignment(
            store_uri=store_uri,
            arrays=arrays,
            time_ranges=time_ranges,
            expected_time_dim_name=expected_time_dim_name,
            span_key=span.key,
            wal_aligned=wal_aligned,
        )
        errors.extend(grid_read_errors)

        if not measured_aligned and not force:
            errors.append(f"Span {span.key} is not time-chunk aligned; rerun with force=True")
            return {"deleted_keys": 0, "deleted_spans": 0, "errors": errors, "warnings": warnings}

        if not measured_aligned and force:
            collateral_spans = self._find_collateral_spans(
                arrays=arrays,
                time_ranges=time_ranges,
                per_array_grids=per_array_grids,
                product=product,
                target_span_keys=target_span_keys,
                all_spans=all_spans,
            )

        def _flush_pending() -> None:
            nonlocal pending_paths, deleted_keys, already_absent_keys
            if not pending_paths:
                return
            if dry_run:
                deleted_keys += len(pending_paths)
                pending_paths = []
                return
            for path in pending_paths:
                uri = StorageUri.parse(path) if "://" in path else StorageUri.from_local_path(path)
                try:
                    fs.rm(uri, recursive=False)
                    deleted_keys += 1
                except FileNotFoundError:
                    already_absent_keys += 1
                    self.log.warning(
                        "chunk key already absent (fill-only or already removed); "
                        "not counted as deleted: %s",
                        path,
                    )
                except Exception as e:
                    errors.append(f"Failed to delete {path}: {e}")
            pending_paths = []

        if not measured_aligned and force and collateral_spans:
            collateral_summary = ", ".join(
                self._render_collateral_entry(c) for c in collateral_spans
            )
            if dry_run:
                for c in collateral_spans:
                    warnings.append(
                        f"WARNING: force-deleting {span.key} will also destroy "
                        f"{self._render_collateral_entry(c)}"
                    )
                    self.log.warning(
                        "force-deleting %s will also destroy %s",
                        span.key,
                        self._render_collateral_entry(c),
                    )
            elif not yes_i_really_mean_it:
                errors.append(
                    f"Span {span.key} shares physical chunks with other active spans: "
                    f"{collateral_summary}. Re-run with yes_i_really_mean_it=True to "
                    f"acknowledge the collateral destruction."
                )
                return {
                    "deleted_keys": 0,
                    "deleted_spans": 0,
                    "errors": errors,
                    "warnings": warnings,
                }
            else:
                collateral_span_keys = [c.key for c in collateral_spans]

        for array_path in arrays:
            grid = per_array_grids.get(array_path)
            if grid is None:
                continue
            _dim_names, shape, chunk_shape, time_dim = grid
            if not shape or not chunk_shape:
                continue
            time_chunk = int(chunk_shape[time_dim] or 0)
            if time_chunk <= 0:
                continue
            expected_chunks = [math.ceil(s / c) for s, c in zip(shape, chunk_shape, strict=False)]
            time_chunk_indices: set[int] = set()
            for pair in time_ranges:
                if len(pair) != 2:
                    continue
                try:
                    start_i, end_i = int(pair[0]), int(pair[1])
                except Exception:
                    continue
                if end_i < start_i:
                    continue
                time_chunk_indices.update(range(start_i // time_chunk, end_i // time_chunk + 1))
            time_chunk_indices = {
                i for i in time_chunk_indices if 0 <= i < expected_chunks[time_dim]
            }
            if not time_chunk_indices:
                continue
            other_ranges = [
                range(exp) for dim_i, exp in enumerate(expected_chunks) if dim_i != time_dim
            ]
            for t_idx in time_chunk_indices:
                for combo in itertools.product(*other_ranges):
                    other_iter = iter(combo)
                    indices = [
                        t_idx if dim_i == time_dim else next(other_iter)
                        for dim_i in range(len(expected_chunks))
                    ]
                    rel = f"{product}/{array_path.strip('/')}/c/" + "/".join(
                        str(i) for i in indices
                    )
                    pending_paths.append(f"{base_path.rstrip('/')}/{rel}")
                    span_paths_added += 1
                    if len(pending_paths) >= pending_limit:
                        _flush_pending()
        _flush_pending()

        if update_state:
            state_path = spec.get("state_array")
            state_value = spec.get("state_deleted_value", STATE_DELETED_BY_FIRECUBE)
            if state_path:
                try:
                    from firecube.core.zarr.state import (
                        expand_time_index_ranges_to_chunk_boundaries,
                        update_timestamp_state,
                    )

                    effective_ranges = time_ranges
                    if not measured_aligned and force:
                        try:
                            dim_names, shape, chunk_shape = read_chunk_grid(
                                store_uri, state_path, storage_config=self.repo.storage_config
                            )
                            time_dim = resolve_time_dim_index(dim_names, expected_time_dim_name)
                            time_chunk = int(chunk_shape[time_dim] or 0)
                            time_length = int(shape[time_dim] or 0)
                            if time_chunk > 0 and time_length > 0:
                                expanded = expand_time_index_ranges_to_chunk_boundaries(
                                    time_ranges, chunk_len=time_chunk, length=time_length
                                )
                                if expanded:
                                    effective_ranges = expanded
                        except (FileNotFoundError, KeyError, ValueError) as exc:
                            errors.append(
                                f"Span {span.key}: could not expand state ranges to chunk "
                                f"boundaries from {state_path}: {exc}"
                            )
                    update_timestamp_state(
                        store_uri=store_uri,
                        array_path=state_path,
                        time_index_ranges=effective_ranges,
                        value=int(state_value),
                        storage_config=self.repo.storage_config,
                    )
                except Exception as e:
                    errors.append(f"Failed to update state: {e}")

        if errors or span_paths_added <= 0:
            deleted_spans = 0
            replaced_span_keys: list[str] = []
        else:
            deleted_spans = 1
            replaced_span_keys = [span.key] if update_manifest else []
        return {
            "deleted_keys": deleted_keys,
            "already_absent_keys": already_absent_keys,
            "deleted_spans": deleted_spans,
            "errors": errors,
            "warnings": warnings,
            "collateral_spans": collateral_span_keys,
            "replaced_span_keys": replaced_span_keys,
        }

    def delete_spans(
        self,
        spans: Iterable[ChunkInfo],
        *,
        dry_run: bool = False,
        force: bool = False,
        yes_i_really_mean_it: bool = False,
        update_manifest: bool = True,
        update_state: bool = True,
        time_dim_name: str | None = None,
    ) -> dict[str, Any]:
        """Delete Zarr concrete chunks from spans."""
        spans_list = list(spans)
        if not spans_list:
            return {
                "deleted_keys": 0,
                "deleted_spans": 0,
                "errors": [],
                "warnings": [],
                "collateral_spans": [],
            }

        products = {s.product for s in spans_list if s.product}
        if len(products) != 1:
            raise ManifestError(
                f"delete_spans requires spans for a single product, got {sorted(products)}"
            )

        product = next(iter(products))
        if not self.repo.base_uri:
            raise ManifestError("No base URI configured")

        claims = self._acquire_maintenance_claims(products=[product], operation="delete_spans")

        run_id = f"maintenance-delete-spans-{uuid.uuid4().hex}"

        def _scope_for(_product: str) -> dict[str, Any]:
            return {
                "spans_count": len(spans_list),
                "force": bool(force),
                "update_manifest": bool(update_manifest),
                "update_state": bool(update_state),
            }

        started_products: list[str] = []
        try:
            if not dry_run:
                started_products = self._record_maintenance_started(
                    products=[product],
                    run_id=run_id,
                    op=MAINTENANCE_OP_DELETE,
                    scope_for=_scope_for,
                )
            base_uri = self.repo.base_uri.rstrip("/")
            store_uri = f"{base_uri}/{product}"
            span_time_dims = resolve_span_time_dims(
                spans_list,
                store_uri=store_uri,
                storage_config=self.repo.storage_config,
                explicit=time_dim_name,
                default=self.time_dim_name,
            )
            fs, base_uri = self.repo._get_fs(self.repo.base_uri)
            base_uri_obj: Any = base_uri
            base_path = (
                base_uri_obj.path if base_uri_obj.protocol == "file" else base_uri_obj.to_str()
            )
            counts = {"deleted_keys": 0, "deleted_spans": 0, "region_filled_spans": 0}
            errors: list[str] = []
            warnings: list[str] = []
            collateral_span_keys: list[str] = []
            replaced_span_keys: list[str] = []
            replacement_meta_updates_by_key: dict[str, dict[str, Any]] = {}
            target_span_keys = {s.key for s in spans_list}
            all_spans = self.repo.list_chunks(product=product, chunk_type="span")
            group_array_cache: dict[tuple[str, str], bool] = {}
            outcomes: list[dict[str, Any]] = []

            for span in spans_list:
                if _is_completed_region_fill(span) and not force:
                    outcomes.append(
                        {
                            "warnings": [
                                f"Span {span.key} is already replaced by an in-place fill (write_strategy=region_nan_fill); skipped. Rerun with force=True to fill it again."
                            ]
                        }
                    )
                    continue
                payload: dict[str, Any] = span.record if isinstance(span.record, dict) else {}
                raw_spec = payload.get("span")
                spec: dict[str, Any] = raw_spec if isinstance(raw_spec, dict) else {}
                expected_time_dim_name = span_time_dims[span.key]
                arrays = spec.get("arrays")
                if not isinstance(arrays, list) or not arrays:
                    outcomes.append({"errors": [f"Span {span.key} missing span.arrays"]})
                    continue
                time_ranges = spec.get("time_index_ranges")
                if not isinstance(time_ranges, list) or not time_ranges:
                    continue
                time_indices = _span_time_indices(time_ranges)
                group_name = _span_group_name(span)
                state_array_name = _span_state_array_name(spec=spec, group=group_name)
                cache_key = (group_name, state_array_name)
                if cache_key not in group_array_cache:
                    group_array_cache[cache_key] = _zarr_group_has_array(
                        store_uri=store_uri,
                        group=group_name,
                        array_name=state_array_name,
                        storage_config=self.repo.storage_config,
                    )
                common = {
                    "span": span,
                    "group_name": group_name,
                    "store_uri": store_uri,
                    "fs": fs,
                    "spec": spec,
                    "dry_run": dry_run,
                    "force": force,
                    "collateral_spans": [],
                }
                if time_indices and group_array_cache[cache_key]:
                    outcomes.append(
                        self._delete_span_by_region_fill(
                            **common,
                            arrays=arrays,
                            time_indices=time_indices,
                            expected_time_dim_name=expected_time_dim_name,
                            update_state=update_state,
                            update_manifest=update_manifest,
                        )
                    )
                    continue
                outcomes.append(
                    self._delete_span_by_chunk_keys(
                        **common,
                        product=product,
                        base_path=base_path,
                        arrays=arrays,
                        time_ranges=time_ranges,
                        expected_time_dim_name=expected_time_dim_name,
                        update_state=update_state,
                        update_manifest=update_manifest,
                        yes_i_really_mean_it=yes_i_really_mean_it,
                        all_spans=all_spans,
                        target_span_keys=target_span_keys,
                    )
                )

            collateral_replaced_keys: set[str] = set()
            for outcome in outcomes:
                errors.extend(outcome.get("errors", []))
                warnings.extend(outcome.get("warnings", []))
                for key in counts:
                    counts[key] += int(outcome.get(key, 0))
                replaced_span_keys.extend(outcome.get("replaced_span_keys", []))
                replacement_meta_updates_by_key.update(
                    outcome.get("replacement_meta_updates_by_key", {})
                )
                for key in outcome.get("collateral_spans", []):
                    if key not in collateral_replaced_keys:
                        collateral_span_keys.append(key)
                        collateral_replaced_keys.add(key)

            all_replaced_keys = list(dict.fromkeys([*replaced_span_keys, *collateral_span_keys]))

            if update_manifest and all_replaced_keys and not dry_run:
                import time

                try:
                    self.repo.mark_chunks_replaced(
                        all_replaced_keys,
                        product,
                        time.time(),
                        meta_updates_by_key=replacement_meta_updates_by_key,
                    )
                except ManifestError as exc:
                    errors.append(f"Manifest update failed: {exc}")

            if not dry_run:
                self._record_maintenance_completed(
                    products=started_products,
                    run_id=run_id,
                    op=MAINTENANCE_OP_DELETE,
                    scope_for=_scope_for,
                )

            return {
                "product": product,
                "deleted_keys": counts["deleted_keys"],
                "deleted_spans": counts["deleted_spans"],
                "region_filled_spans": counts["region_filled_spans"],
                "dry_run": dry_run,
                "errors": errors,
                "warnings": warnings,
                "collateral_spans": collateral_span_keys,
            }
        except Exception as exc:
            self._record_maintenance_failed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
                error=str(exc),
            )
            raise
        finally:
            self._clear_maintenance_claims(claims)
