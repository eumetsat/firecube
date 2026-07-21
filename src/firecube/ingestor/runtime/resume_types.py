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

"""Resume authority decision types.

These types model the outcome of ``ResumeGuard.enforce()`` — the safety
check that runs before every ingestion to decide whether to proceed,
resume, or block.  ``enforce()`` logs a ``ResumeDecision`` at each
decision point for diagnostics, while still raising
``ResumeConflictError`` on blocking verdicts to preserve the existing API.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class ResumeVerdict(Enum):
    """Outcome of a resume authority decision."""

    PROCEED_FRESH = "proceed_fresh"
    PROCEED_RESUME = "proceed_resume"
    BLOCK_STALE_RUN = "block_stale_run"
    BLOCK_CONFLICT = "block_conflict"


@dataclass(frozen=True)
class ResumeDecision:
    """Immutable result of a resume authority check."""

    verdict: ResumeVerdict
    reason: str
    blocking_run_ids: list[str] = field(default_factory=list)
    overlap_groups: list[str] = field(default_factory=list)
    time_coverage: dict[str, Any] = field(default_factory=dict)


class ResumeAuthority(Protocol):
    """Contract for objects that can make resume decisions."""

    def decide(
        self,
        *,
        product: str,
        plugin: str,
        slice_meta: dict[str, Any],
        force_reingest: bool,
        resume_existing: bool,
    ) -> ResumeDecision: ...


__all__ = ["ResumeAuthority", "ResumeDecision", "ResumeVerdict"]
