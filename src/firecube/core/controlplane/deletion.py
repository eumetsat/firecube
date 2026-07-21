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
from collections.abc import Callable, Iterable
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

from firecube.core.config import StorageConfig
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.time_dim import resolve_span_time_dims, resolve_time_dim_index
from firecube.core.controlplane.types import (
    MAINTENANCE_OP_DELETE,
    ChunkInfo,
    DeletionPlan,
    WriteDomain,
)
from firecube.core.errors import ClaimConflictError, ManifestError
from firecube.core.runtime import identity_from_storage_config
from firecube.core.storage.uri import StorageUri

log = logging.getLogger(__name__)


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

    def delete_spans(
        self,
        spans: Iterable[ChunkInfo],
        *,
        dry_run: bool = False,
        force: bool = False,
        update_manifest: bool = True,
        update_state: bool = True,
        time_dim_name: str | None = None,
    ) -> dict[str, Any]:
        """Delete Zarr concrete chunks from spans.

        ``time_dim_name`` is an explicit operator-supplied time dimension; see
        :func:`firecube.core.controlplane.time_dim.resolve_span_time_dims` for
        how it combines with the dim name recorded in span specs and
        discovered from the timestamp-state array.
        """
        from firecube.core.zarr.validation import read_chunk_grid

        spans_list = list(spans)
        if not spans_list:
            return {"deleted_keys": 0, "deleted_spans": 0, "errors": []}

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

            deleted_keys = 0
            deleted_spans = 0
            errors = []
            replaced_span_keys = []

            pending_paths = []
            pending_limit = 1000

            def _flush_pending():
                nonlocal pending_paths, deleted_keys
                if not pending_paths:
                    return
                if dry_run:
                    deleted_keys += len(pending_paths)
                    pending_paths = []
                    return
                for p in pending_paths:
                    uri = StorageUri.parse(p) if "://" in p else StorageUri.from_local_path(p)
                    try:
                        fs.rm(uri, recursive=False)
                        deleted_keys += 1
                    except Exception as e:
                        errors.append(f"Failed to delete {p}: {e}")
                pending_paths = []

            for span in spans_list:
                span_error_count_before = len(errors)
                span_paths_added = 0
                payload: dict[str, Any] = span.record if isinstance(span.record, dict) else {}
                raw_spec = payload.get("span")
                spec: dict[str, Any] = raw_spec if isinstance(raw_spec, dict) else {}
                expected_time_dim_name = span_time_dims[span.key]

                arrays = spec.get("arrays")
                if not isinstance(arrays, list) or not arrays:
                    errors.append(f"Span {span.key} missing span.arrays")
                    continue

                time_ranges = spec.get("time_index_ranges")
                if not isinstance(time_ranges, list) or not time_ranges:
                    continue

                aligned = bool(spec.get("aligned", True))
                if not aligned and not force:
                    errors.append(
                        f"Span {span.key} is not time-chunk aligned; rerun with force=True"
                    )
                    continue

                for array_path in arrays:
                    try:
                        dim_names, shape, chunk_shape = read_chunk_grid(
                            store_uri,
                            array_path,
                            storage_config=self.repo.storage_config,
                        )
                    except Exception as exc:
                        errors.append(f"Failed to read zarr.json for {array_path}: {exc}")
                        continue

                    if not shape or not chunk_shape:
                        continue

                    time_dim = resolve_time_dim_index(dim_names, expected_time_dim_name)

                    time_chunk = int(chunk_shape[time_dim] or 0)
                    if time_chunk <= 0:
                        continue

                    expected_chunks = [
                        math.ceil(s / c) for s, c in zip(shape, chunk_shape, strict=False)
                    ]

                    time_chunk_indices = set()
                    for pair in time_ranges:
                        if len(pair) != 2:
                            continue
                        start, end = pair
                        try:
                            start_i, end_i = int(start), int(end)
                        except Exception:
                            continue
                        if end_i < start_i:
                            continue

                        time_chunk_indices.add(start_i // time_chunk)
                        time_chunk_indices.add(end_i // time_chunk)
                        for i in range((start_i // time_chunk) + 1, (end_i // time_chunk)):
                            time_chunk_indices.add(i)

                    time_chunk_indices = {
                        i for i in time_chunk_indices if 0 <= i < expected_chunks[time_dim]
                    }
                    if not time_chunk_indices:
                        continue

                    other_ranges = []
                    for dim_i, exp in enumerate(expected_chunks):
                        if dim_i != time_dim:
                            other_ranges.append(range(exp))

                    for t_idx in time_chunk_indices:
                        for combo in itertools.product(*other_ranges):
                            indices = []
                            other_iter = iter(combo)
                            for dim_i in range(len(expected_chunks)):
                                if dim_i == time_dim:
                                    indices.append(t_idx)
                                else:
                                    indices.append(next(other_iter))

                            rel = f"{product}/{array_path.strip('/')}/c/" + "/".join(
                                str(i) for i in indices
                            )
                            p = f"{base_path.rstrip('/')}/{rel}"
                            pending_paths.append(p)
                            span_paths_added += 1
                            if len(pending_paths) >= pending_limit:
                                _flush_pending()

                _flush_pending()

                # State update logic
                if update_state:
                    state_path = spec.get("state_array")
                    state_value = spec.get("state_deleted_value", 2)
                    if state_path:
                        try:
                            from firecube.core.zarr.state import (
                                expand_time_index_ranges_to_chunk_boundaries,
                                update_timestamp_state,
                            )

                            effective_ranges = time_ranges
                            if not aligned and force:
                                # Expand ranges logic from ChunkManager
                                try:
                                    d, s, cs = read_chunk_grid(
                                        store_uri,
                                        state_path,
                                        storage_config=self.repo.storage_config,
                                    )
                                    td = resolve_time_dim_index(d, expected_time_dim_name)
                                    tc = int(cs[td] or 0)
                                    tl = int(s[td] or 0)
                                    if tc > 0 and tl > 0:
                                        expanded = expand_time_index_ranges_to_chunk_boundaries(
                                            time_ranges, chunk_len=tc, length=tl
                                        )
                                        if expanded:
                                            effective_ranges = expanded
                                except Exception:
                                    pass

                            update_timestamp_state(
                                store_uri=store_uri,
                                array_path=state_path,
                                time_index_ranges=effective_ranges,
                                value=int(state_value),
                                storage_config=self.repo.storage_config,
                            )
                        except Exception as e:
                            errors.append(f"Failed to update state: {e}")

                span_had_errors = len(errors) != span_error_count_before
                if update_manifest and not span_had_errors and span_paths_added > 0:
                    replaced_span_keys.append(span.key)

                if not span_had_errors and span_paths_added > 0:
                    deleted_spans += 1

            if update_manifest and replaced_span_keys and not dry_run:
                import time

                try:
                    self.repo.mark_chunks_replaced(replaced_span_keys, product, time.time())
                except ManifestError as exc:
                    errors.append(f"Manifest update failed: {exc}")

            self._record_maintenance_completed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
            )

            return {
                "product": product,
                "deleted_keys": deleted_keys,
                "deleted_spans": deleted_spans,
                "dry_run": dry_run,
                "errors": errors,
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
