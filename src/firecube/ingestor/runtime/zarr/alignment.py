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

"""Run-scoped chunk-alignment monitor for Zarr append writes.

One :class:`AlignmentMonitor` lives for the whole ingestion run. Every
append write reports its slot range through :meth:`AlignmentMonitor.check`;
the first unaligned write of a ``(group, chunk_len)`` pair logs a warning,
later ones are counted silently, and :meth:`AlignmentMonitor.emit_summary`
logs one line per pair at the end of the run. The final short write of a
run (the tail the planner marks with ``is_last``) is never counted: a
dataset whose length is not a multiple of the chunk length always ends
with one.
"""

from __future__ import annotations

import logging


class AlignmentMonitor:
    """Memo and counters for unaligned Zarr append writes across a run.

    Attributes are keyed by ``(group, chunk_len)`` so a store whose groups
    carry different chunk layouts reports each layout separately.
    """

    def __init__(self) -> None:
        self._warned: set[tuple[str, int]] = set()
        self._unaligned_counts: dict[tuple[str, int], int] = {}

    def check(
        self,
        *,
        start_i: int,
        count: int,
        chunk_len: int | None,
        group: str,
        is_final: bool,
        logger: logging.Logger,
    ) -> bool:
        """Record one write's alignment against the group's chunk layout.

        Args:
            start_i: Index on the append dimension where the write starts.
            count: Number of slots the write covers.
            chunk_len: Chunk length on the append dimension; ``None`` or a
                non-positive value means the layout is unknown and the write
                is reported as aligned.
            group: Zarr group the write targets.
            is_final: ``True`` when the write is the last of the run, so a
                short tail is not reported as unaligned.
            logger: Logger that receives the first warning per pair.

        Returns:
            ``True`` when both the start index and the count are multiples of
            ``chunk_len``.
        """
        if not chunk_len or chunk_len <= 0:
            return True
        aligned = start_i % chunk_len == 0 and count % chunk_len == 0
        if aligned:
            return True
        if is_final and count < chunk_len:
            return False

        pair = (str(group), chunk_len)
        self._unaligned_counts[pair] = self._unaligned_counts.get(pair, 0) + 1
        if pair not in self._warned:
            self._warned.add(pair)
            logger.warning(
                "Zarr write is unaligned with chunk layout. "
                "This may reduce performance due to Read-Modify-Write cycles. "
                "Recommendation: align pipeline_batch_size with Zarr chunk_len.",
                extra={
                    "group": str(group),
                    "chunk_len": chunk_len,
                    "batch_count": count,
                    "start_index": start_i,
                },
            )
        return False

    def emit_summary(self, logger: logging.Logger) -> None:
        """Log one end-of-run line per unaligned ``(group, chunk_len)`` pair.

        Logs nothing when every write of the run was aligned.

        Args:
            logger: Logger that receives the summary lines.
        """
        for (group, chunk_len), count in self._unaligned_counts.items():
            logger.warning(
                "Alignment summary: %d unaligned batch(es) in group %r "
                "(chunk=%d). Consider aligning pipeline_batch_size to chunk size.",
                count,
                group,
                chunk_len,
            )

    @property
    def unaligned_total(self) -> int:
        """Number of unaligned writes counted across all groups this run."""
        return sum(self._unaligned_counts.values())
