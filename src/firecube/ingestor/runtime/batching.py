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

"""Batch creation logic for ingestion pipelines.

This module provides the `BatchPlanner` service, which is responsible for
discovering source files, filtering them, grouping them, and chunking them
into batches for processing.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from firecube.ingestor.contracts.interfaces import SourceFile
from firecube.ingestor.types.context import PipelineBatch, PluginContext

log = logging.getLogger("firecube.ingestor.batching")


@runtime_checkable
class BatchPlanHost(Protocol):
    """Protocol for the host that drives the batch planning process.

    The host provides the source files and the logic for filtering and grouping them.
    Typically implemented by `BaseIngestor` or a subclass.
    """

    @property
    def batch_id_prefix(self) -> str:
        """prefix for batch IDs (e.g., 'plugin_name_')."""
        ...

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        """Yield source files (paths, strings, or objects) to be processed."""
        ...

    def filter_item(self, item: Any, ctx: PluginContext) -> bool:
        """Return True if the item should be processed, False otherwise."""
        ...

    def item_size_bytes(self, item: Any) -> int | None:
        """Return the size of the item in bytes, or None if unknown."""
        ...

    def get_batch_groups(self, items: list[Any], ctx: PluginContext) -> list[str]:
        """Return logical groups for a batch of items (e.g., horizon, product).

        MUST be deterministic (return a sorted/stable list) to ensure consistent
        spans and batch grouping across runs.
        """
        ...


class BatchPlanner:
    """Service that creates batches from discovered source files."""

    def create_batches(
        self, host: BatchPlanHost, ctx: PluginContext, batch_size: int
    ) -> Iterator[PipelineBatch]:
        """Yield batches via hooks (iter -> filter -> batch).

        Args:
            host: The host object providing hooks and source files.
            ctx: The ingestion context.
            batch_size: Maximum number of items per batch.

        Yields:
            PipelineBatch objects containing the items to be processed.
        """
        items_iter = host.discover_source_files(ctx)

        current_batch_items: list[Any] = []
        current_size = 0
        batch_idx = 0

        def _yield_batch(b_items: list[Any], b_size: int, b_idx: int) -> PipelineBatch:
            # Deterministic grouping is enforced by the host contract,
            # but we trust the host to return a stable list.
            groups = host.get_batch_groups(b_items, ctx)

            # ID Strategy: {prefix}batch_{index:04d} (Simple IDs)
            batch_id = f"{host.batch_id_prefix}batch_{b_idx:04d}"

            # Safety: Store item URIs in metadata for logging/manifests.
            # We assume items are str/Path/SourceFile, so str(item) or .uri is reasonable.
            item_uris = []
            for item in b_items:
                if isinstance(item, SourceFile):
                    item_uris.append(item.uri)
                else:
                    item_uris.append(str(item))
            item_uris_hash = hashlib.sha256(
                "\n".join(sorted(item_uris)).encode("utf-8")
            ).hexdigest()

            # Limit metadata size for safety
            preview_limit = 100
            item_uris_total = len(item_uris)
            item_uris_truncated = False
            if item_uris_total > preview_limit:
                item_uris_truncated = True
                item_uris = [*item_uris[:preview_limit], "..."]
                log.warning(
                    "Batch metadata item_uris truncated (batch_id=%s total=%d preview_limit=%d).",
                    batch_id,
                    item_uris_total,
                    preview_limit,
                )

            return PipelineBatch(
                batch_id=batch_id,
                data_path=Path(ctx.source),
                items=list(b_items),  # Copy to avoid mutation issues
                metadata={
                    "item_uris": item_uris,
                    "item_uris_total": item_uris_total,
                    "item_uris_preview_limit": preview_limit,
                    "item_uris_truncated": item_uris_truncated,
                    "files_hash": item_uris_hash,
                },
                size_bytes=b_size,
                files_count=len(b_items),
                groups=groups,
            )

        for item in items_iter:
            if not host.filter_item(item, ctx):
                continue

            # Calculate size (Best effort)
            size = host.item_size_bytes(item)
            if size is None:
                size = 0

            current_batch_items.append(item)
            current_size += size

            if len(current_batch_items) >= batch_size:
                yield _yield_batch(current_batch_items, current_size, batch_idx)
                batch_idx += 1
                current_batch_items = []
                current_size = 0

        if current_batch_items:
            yield _yield_batch(current_batch_items, current_size, batch_idx)
