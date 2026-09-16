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

"""Tests that ResumeGuard decision messages reflect actual template semantics.

Verifies that the reason strings logged via _log_decision accurately describe
what the user's configured flags will do (overwrite vs skip), not just which
flag was set.
"""

from unittest.mock import MagicMock, patch

import pytest

from firecube.ingestor.runtime.resume_guard import ResumeGuard
from firecube.ingestor.runtime.resume_types import ResumeDecision, ResumeVerdict


def _make_ctx(*, force_reingest: bool = False, **options):
    ctx = MagicMock()
    ctx.force_reingest = force_reingest
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_span(*, meta=None):
    span = MagicMock()
    span.meta = meta or {"plugin": "test_product"}
    span.record = {"span": {}}
    return span


def _make_guard(chunk_manager: MagicMock | None = None) -> ResumeGuard:
    return ResumeGuard(
        plugin_name="test_product",
        chunk_manager=chunk_manager or MagicMock(),
        log=MagicMock(),
        slice_meta_keys=(),
    )


@pytest.mark.unit
def test_append_force_reingest_message_says_overwrite():
    """With force_reingest=True on an append run, the decision reason must say 'overwrite'."""
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [_make_span()]
    guard = _make_guard(chunk_manager)

    captured: list[ResumeDecision] = []

    original = ResumeGuard._log_decision

    def capturing(self: ResumeGuard, decision: ResumeDecision) -> None:
        captured.append(decision)
        original(self, decision)

    with patch.object(ResumeGuard, "_log_decision", capturing):
        guard.enforce(ctx=_make_ctx(force_reingest=True), product="P")

    assert captured, "Expected at least one ResumeDecision to be logged"
    proceed_decisions = [d for d in captured if d.verdict == ResumeVerdict.PROCEED_RESUME]
    assert proceed_decisions, "Expected a PROCEED_RESUME decision"
    reason = proceed_decisions[-1].reason
    assert "overwrite" in reason, (
        f"Expected 'overwrite' in reason for force_reingest=True, got: {reason!r}"
    )
    assert "append" in reason, (
        f"Expected 'append' in reason for force_reingest=True, got: {reason!r}"
    )


@pytest.mark.unit
def test_append_resume_existing_message_says_skip():
    """With resume_existing=True on an append run, the decision reason must say 'skip'."""
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [_make_span()]
    guard = _make_guard(chunk_manager)

    captured: list[ResumeDecision] = []

    original = ResumeGuard._log_decision

    def capturing(self: ResumeGuard, decision: ResumeDecision) -> None:
        captured.append(decision)
        original(self, decision)

    with patch.object(ResumeGuard, "_log_decision", capturing):
        guard.enforce(ctx=_make_ctx(resume_existing=True), product="P")

    assert captured, "Expected at least one ResumeDecision to be logged"
    proceed_decisions = [d for d in captured if d.verdict == ResumeVerdict.PROCEED_RESUME]
    assert proceed_decisions, "Expected a PROCEED_RESUME decision"
    reason = proceed_decisions[-1].reason
    assert "skip" in reason, f"Expected 'skip' in reason for resume_existing=True, got: {reason!r}"
    assert "append" in reason, (
        f"Expected 'append' in reason for resume_existing=True, got: {reason!r}"
    )
