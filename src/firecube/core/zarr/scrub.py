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

"""Zarr scrub orchestration (mutating) used by CLI and API.

This module *acts* on validation results: it maps debris chunk keys back to
manifest entries and deletes them via ChunkManager.

Keep this separate from `firecube.core.zarr.validation`, which is read-only.
"""

from __future__ import annotations

from dataclasses import dataclass

from firecube.core.controlplane import ChunkInfo, ChunkManager
from firecube.core.filesystem import create_filesystem
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.session import StorageSession
from firecube.core.zarr.validation import validate_group_with_fs


@dataclass
class ScrubResult:
    """Result of a scrub operation for a single group."""

    product: str
    group: str
    extra_chunks: list[str]
    deleted_chunks: int
    deleted_size_bytes: int
    storage_errors: list[str]
    manifest_errors: list[str]


def run_scrub(
    session: StorageSession,
    group: str,
    *,
    max_chunks: int | None = None,
    include_orphans: bool = False,
) -> ScrubResult:
    """Scrub debris chunks for a given array group using ChunkManager.

    The ``session`` carries the product binding (URI, driver, credentials);
    no string-concat URI building or raw credential dicts live here.

    This helper:
      1) Validates the group to find out-of-range chunks.
      2) Maps those chunk paths back to manifest keys.
      3) Uses ChunkManager to delete only matching chunks from storage
         and manifests.
    """
    product_uri = session.product.product_uri
    product_name = session.product.product_name
    fs = session.fs()

    try:
        report = validate_group_with_fs(
            fs,
            product_uri,
            group,
        )
    except ValueError as exc:
        if "is a Zarr group (container)" not in str(exc):
            raise
        return ScrubResult(
            product=product_name,
            group=group,
            extra_chunks=[],
            deleted_chunks=0,
            deleted_size_bytes=0,
            storage_errors=[],
            manifest_errors=[],
        )
    extra_paths = list(report.extra_chunks)

    if max_chunks is not None and max_chunks >= 0:
        extra_paths = extra_paths[:max_chunks]

    prefix = f"{product_name}/"
    manifest_keys: list[str] = []
    for path in extra_paths:
        key = path
        if prefix in path:
            key = path.split(prefix, 1)[1]
        manifest_keys.append(key)

    if not manifest_keys:
        return ScrubResult(
            product=product_name,
            group=group,
            extra_chunks=[],
            deleted_chunks=0,
            deleted_size_bytes=0,
            storage_errors=[],
            manifest_errors=[],
        )

    binding = StorageBinding(identity=session.product, driver=session.driver)
    manager = ChunkManager(binding=binding, filesystem=create_filesystem(binding))
    try:
        all_chunks: list[ChunkInfo] = []
        for key in manifest_keys:
            matches = manager.list_chunks(pattern=key, product=product_name)
            all_chunks.extend(matches)

        seen = set()
        unique_chunks: list[ChunkInfo] = []
        for chunk in all_chunks:
            k = (chunk.product, chunk.key, chunk.manifest_path)
            if k not in seen:
                seen.add(k)
                unique_chunks.append(chunk)

        total_deleted_chunks = 0
        total_deleted_size = 0
        storage_errors: list[str] = []
        manifest_errors: list[str] = []

        if unique_chunks:
            plan = manager.create_deletion_plan()
            plan.chunks = unique_chunks
            plan.total_size = sum(c.size for c in unique_chunks)
            plan.products_affected = {product_name}
            plan.manifest_files = {c.manifest_path for c in unique_chunks}

            result = manager.execute_deletion(
                plan,
                delete_storage=True,
                delete_manifest=True,
                storage_config=None,
                dry_run=False,
            )
            total_deleted_chunks += result.get("deleted_chunks", 0)
            total_deleted_size += result.get("deleted_size_bytes", 0)
            storage_errors.extend(result.get("storage_errors", []))
            manifest_errors.extend(result.get("manifest_errors", []))

        if include_orphans:
            manifest_key_set = {c.key for c in unique_chunks}
            orphan_keys = [k for k in manifest_keys if k not in manifest_key_set]

            if orphan_keys:
                from time import time as _time

                now_ts = _time()
                orphan_chunks: list[ChunkInfo] = [
                    ChunkInfo(
                        key=key,
                        product=product_name,
                        chunk_type="chunk",
                        size=0,
                        timestamp=now_ts,
                        manifest_path="",
                    )
                    for key in orphan_keys
                ]

                orphan_plan = manager.create_deletion_plan()
                orphan_plan.chunks = orphan_chunks
                orphan_plan.total_size = 0
                orphan_plan.products_affected = {product_name}
                orphan_plan.manifest_files = set()

                orphan_result = manager.execute_deletion(
                    orphan_plan,
                    delete_storage=True,
                    delete_manifest=False,
                    storage_config=None,
                    dry_run=False,
                )
                if not orphan_result.get("storage_errors"):
                    total_deleted_chunks += len(orphan_keys)
                total_deleted_size += orphan_result.get("deleted_size_bytes", 0)
                storage_errors.extend(orphan_result.get("storage_errors", []))
                manifest_errors.extend(orphan_result.get("manifest_errors", []))

        return ScrubResult(
            product=product_name,
            group=group,
            extra_chunks=manifest_keys,
            deleted_chunks=total_deleted_chunks,
            deleted_size_bytes=total_deleted_size,
            storage_errors=storage_errors,
            manifest_errors=manifest_errors,
        )
    finally:
        manager.close()
