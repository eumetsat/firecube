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

"""``ResumeGuard.enforce`` decisions against a real ``ChunkManager`` WAL.

Each test drives the guard against a fresh product WAL populated via
``ChunkManager.record_run_started`` / ``record_span`` / ``record_run_terminal``
and asserts the observable outcome (raised error type + message, or
clean return) rather than which methods the guard called in what order.
Method-call ordering is an implementation detail and would repin every
refactor of the guard.

Also owns ``test_resume_guard_fail_loud_end_to_end``: the failed-run
blocks-resume path (previously covered by
``test_v2_b2_unified_fail_loud_and_message.py``, dropped from the tree),
now exercised end-to-end against a real WAL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit

_PRODUCT = "test_product"
_PLUGIN = "test_product"


@dataclass
class _GuardCtx:
    """Minimal ctx surface `ResumeGuard.enforce` reads from.

    The guard reads ``ctx.force_reingest`` directly and ``ctx.option(name,
    default)`` for ``force_reingest``, ``resume_existing``, ``validate_zarr``,
    ``validate_zarr_group``, ``validate_zarr_timeout_s``,
    ``validate_zarr_max_chunks``, ``validate_zarr_on_timeout``. It never
    touches ``ctx.storage`` unless ``validate_zarr=True``, which these tests
    do not set.
    """

    force_reingest: bool = False
    options: dict[str, Any] = field(default_factory=dict)

    def option(self, name: str, default: Any = None) -> Any:
        return self.options.get(name, default)


def _fresh_manager(tmp_path: Path) -> ChunkManager:
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    return ChunkManager(binding=binding, workspace=tmp_path)


def _make_guard(chunk_manager: ChunkManager) -> ResumeGuard:
    return ResumeGuard(
        plugin_name=_PLUGIN,
        chunk_manager=chunk_manager,
        log=logging.getLogger("test.resume_guard"),
        slice_meta_keys=(),
    )


def _record_running_run(manager: ChunkManager, *, run_id: str) -> None:
    """Seed a non-terminal (``started``, no terminal event) run in the WAL."""
    output_path = f"{manager.base_uri.rstrip('/')}/{_PRODUCT}"
    manager.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": _PLUGIN},
    )


def _record_completed_span(
    manager: ChunkManager,
    *,
    run_id: str,
    batch_id: str,
    group: str = "G",
    time_min: str = "2024-01-01T00:00:00Z",
    time_max: str = "2024-01-02T00:00:00Z",
) -> None:
    output_path = f"{manager.base_uri.rstrip('/')}/{_PRODUCT}"
    meta = {
        "plugin": _PLUGIN,
        "group": group,
        "time_min": time_min,
        "time_max": time_max,
        "batch_id": batch_id,
    }
    manager.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": _PLUGIN},
    )
    manager.record_span(
        product=_PRODUCT,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/data"],
            time_index_ranges=[[0, 1]],
            time_min=time_min,
            time_max=time_max,
        ),
        meta=meta,
    )
    manager.record_run_terminal(
        product=_PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": _PLUGIN},
        status="complete",
    )


def _record_failed_run_with_completed_span(
    manager: ChunkManager,
    *,
    run_id: str,
    batch_id: str,
    group: str = "G",
    time_min: str = "2024-01-01T00:00:00Z",
    time_max: str = "2024-01-02T00:00:00Z",
) -> None:
    """A run that wrote a span successfully then failed at terminal.

    The active span survives in the WAL projection (spans are only sealed
    by a subsequent replacement run), and the owning run's terminal status
    is ``failed``. That combination is the ``failed-run-spans exist``
    conflict the guard must refuse loudly.
    """
    output_path = f"{manager.base_uri.rstrip('/')}/{_PRODUCT}"
    meta = {
        "plugin": _PLUGIN,
        "group": group,
        "time_min": time_min,
        "time_max": time_max,
        "batch_id": batch_id,
    }
    manager.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": _PLUGIN},
    )
    manager.record_span(
        product=_PRODUCT,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/data"],
            time_index_ranges=[[0, 1]],
            time_min=time_min,
            time_max=time_max,
        ),
        meta=meta,
    )
    manager.record_run_terminal(
        product=_PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": _PLUGIN},
        status="failed",
        error="simulated batch failure",
    )


def test_non_terminal_run_blocks_and_names_abandon_command(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    _record_running_run(manager, run_id="run-alive")
    guard = _make_guard(manager)

    with pytest.raises(ResumeConflictError) as excinfo:
        guard.enforce(ctx=_GuardCtx(), product=_PRODUCT)

    message = str(excinfo.value)
    assert "run-alive" in message
    assert (
        f"firecube chunks runs abandon --product-name {_PRODUCT} --run-id run-alive "
        '--reason "<reason>"'
    ) in message


def test_resume_existing_does_not_bypass_non_terminal_run(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    _record_running_run(manager, run_id="run-alive")
    guard = _make_guard(manager)

    with pytest.raises(ResumeConflictError, match="run-alive"):
        guard.enforce(ctx=_GuardCtx(options={"resume_existing": True}), product=_PRODUCT)


def test_force_reingest_bypasses_non_terminal_run(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    _record_running_run(manager, run_id="run-alive")
    guard = _make_guard(manager)

    guard.enforce(ctx=_GuardCtx(force_reingest=True), product=_PRODUCT)


def test_fresh_product_proceeds(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    guard = _make_guard(manager)

    result = guard.enforce(ctx=_GuardCtx(), product=_PRODUCT)

    assert result is None


def test_existing_completed_span_blocks_without_resume_or_force(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    _record_completed_span(manager, run_id="run-past", batch_id="b01")
    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(_PRODUCT)
    guard = _make_guard(manager)

    with pytest.raises(ResumeConflictError, match=f"Existing entries for product '{_PRODUCT}'"):
        guard.enforce(ctx=_GuardCtx(), product=_PRODUCT)


def test_force_reingest_bypasses_existing_completed_span(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    _record_completed_span(manager, run_id="run-past", batch_id="b01")
    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(_PRODUCT)
    guard = _make_guard(manager)

    guard.enforce(ctx=_GuardCtx(force_reingest=True), product=_PRODUCT)


def test_resume_existing_bypasses_existing_completed_span(tmp_path: Path) -> None:
    manager = _fresh_manager(tmp_path)
    _record_completed_span(manager, run_id="run-past", batch_id="b01")
    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(_PRODUCT)
    guard = _make_guard(manager)

    guard.enforce(ctx=_GuardCtx(options={"resume_existing": True}), product=_PRODUCT)


def test_resume_guard_fail_loud_end_to_end(tmp_path: Path) -> None:
    """A failed run that left a completed span blocks plain re-runs loudly.

    Setup mirrors the production hazard: a run terminated with status=failed
    but wrote at least one span before dying. On the next attempt without
    ``resume_existing`` or ``force_reingest`` the guard must refuse and
    name both escape hatches so the operator can pick the safe one.
    """
    manager = _fresh_manager(tmp_path)
    _record_failed_run_with_completed_span(manager, run_id="run-failed", batch_id="b02")
    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(_PRODUCT)
    guard = _make_guard(manager)

    with pytest.raises(ResumeConflictError) as excinfo:
        guard.enforce(ctx=_GuardCtx(), product=_PRODUCT)

    message = str(excinfo.value)
    assert "'run-failed'" in message
    assert "succeeded span(s)" in message
    assert "b02" in message
    assert "resume_existing=true" in message
    assert "force_reingest=true" in message


def test_resume_guard_fail_loud_bypassed_by_resume_existing(tmp_path: Path) -> None:
    """The failed-run block is a soft failure: resume_existing must clear it."""
    manager = _fresh_manager(tmp_path)
    _record_failed_run_with_completed_span(manager, run_id="run-failed", batch_id="b02")
    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(_PRODUCT)
    guard = _make_guard(manager)

    guard.enforce(ctx=_GuardCtx(options={"resume_existing": True}), product=_PRODUCT)


def test_resume_guard_fail_loud_bypassed_by_force_reingest(tmp_path: Path) -> None:
    """force_reingest also clears the failed-run block."""
    manager = _fresh_manager(tmp_path)
    _record_failed_run_with_completed_span(manager, run_id="run-failed", batch_id="b02")
    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(_PRODUCT)
    guard = _make_guard(manager)

    guard.enforce(ctx=_GuardCtx(force_reingest=True), product=_PRODUCT)
