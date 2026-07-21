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

from dataclasses import FrozenInstanceError

import pytest

from firecube.ingestor.runtime.resume_types import ResumeDecision, ResumeVerdict


@pytest.mark.unit
def test_resume_verdict_exposes_required_values():
    assert ResumeVerdict.PROCEED_FRESH.value == "proceed_fresh"
    assert ResumeVerdict.PROCEED_RESUME.value == "proceed_resume"
    assert ResumeVerdict.BLOCK_STALE_RUN.value == "block_stale_run"
    assert ResumeVerdict.BLOCK_CONFLICT.value == "block_conflict"


@pytest.mark.unit
def test_resume_decision_constructs_with_all_fields():
    decision = ResumeDecision(
        verdict=ResumeVerdict.BLOCK_CONFLICT,
        reason="overlapping spans detected",
        blocking_run_ids=["run-1", "run-2"],
        overlap_groups=["group-a", "group-b"],
        time_coverage={
            "time_min": "2024-01-01T00:00:00Z",
            "time_max": "2024-01-02T00:00:00Z",
        },
    )

    assert decision.verdict is ResumeVerdict.BLOCK_CONFLICT
    assert decision.reason == "overlapping spans detected"
    assert decision.blocking_run_ids == ["run-1", "run-2"]
    assert decision.overlap_groups == ["group-a", "group-b"]
    assert decision.time_coverage == {
        "time_min": "2024-01-01T00:00:00Z",
        "time_max": "2024-01-02T00:00:00Z",
    }


@pytest.mark.unit
def test_resume_decision_is_frozen():
    decision = ResumeDecision(
        verdict=ResumeVerdict.PROCEED_FRESH,
        reason="no existing data",
    )

    with pytest.raises(FrozenInstanceError):
        attr_name = "reason"
        setattr(decision, attr_name, "mutated")


@pytest.mark.unit
def test_resume_decision_defaults_are_empty_collections():
    decision = ResumeDecision(
        verdict=ResumeVerdict.PROCEED_RESUME,
        reason="existing data is resumable",
    )

    assert decision.blocking_run_ids == []
    assert decision.overlap_groups == []
    assert decision.time_coverage == {}


@pytest.mark.unit
def test_block_stale_run_decision_carries_blocking_run_ids():
    decision = ResumeDecision(
        verdict=ResumeVerdict.BLOCK_STALE_RUN,
        reason="non-terminal run requires operator action",
        blocking_run_ids=["run-stale-1", "run-stale-2"],
    )

    assert decision.verdict is ResumeVerdict.BLOCK_STALE_RUN
    assert decision.blocking_run_ids == ["run-stale-1", "run-stale-2"]
