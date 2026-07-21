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

"""Structured evidence logging for parallel ingestion slot-range operations.

Every filter/assertion/schema operation emits a single key=value log line
(parseable as grep-friendly structured log) at INFO level for production
observability. The lines are not suppressed when no_progress=True, but
they can be captured as DEBUG when needed.
"""

from __future__ import annotations

import logging


def log_filter_evidence(
    logger: logging.Logger,
    *,
    stage: str,
    planned_range: tuple[int, int],
    original_count: int,
    filtered_count: int,
    dropped_count: int,
) -> None:
    """Emit a single structured evidence log line for a filter operation.

    All 5 fields are always present — even if dropped_count=0 (zero is valid evidence).

    Format: "Parallel evidence: stage={stage} planned_range=[{ss},{se}) original_count={n} filtered_count={n} dropped_count={n}"
    """
    slot_start, slot_end = planned_range
    logger.info(
        "Parallel evidence: stage=%s planned_range=[%s,%s) original_count=%s filtered_count=%s dropped_count=%s",
        stage,
        slot_start,
        slot_end,
        original_count,
        filtered_count,
        dropped_count,
    )


def log_schema_evidence(
    logger: logging.Logger,
    *,
    stage: str,
    group: str,
    existing_shape: tuple[int, ...] | None,
    expected_shape: int,
    status: str,
) -> None:
    """Emit schema verification evidence.

    Format: "Parallel evidence: stage={stage} group={group} existing_shape={...} expected_shape={n} status={status}"
    """
    logger.info(
        "Parallel evidence: stage=%s group=%s existing_shape=%s expected_shape=%s status=%s",
        stage,
        group,
        existing_shape,
        expected_shape,
        status,
    )
