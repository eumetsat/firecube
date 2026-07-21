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

import json
from pathlib import Path
from typing import Any

from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import (
    EVENT_REPLACEMENT_COMMITTED,
    EVENT_RUN_STARTED_WITH_REPLACEMENT,
)
from tests.helpers.storage import make_test_binding


def _record_run_started(repo: ManifestRepository, *, product: str, run_id: str) -> None:
    assert repo.workspace is not None
    repo.record_run_started(
        product=product,
        run_id=run_id,
        output_path=str(repo.workspace / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )


def _read_run_wal_events(
    temp_workspace: Path, *, product: str, run_id: str
) -> list[dict[str, Any]]:
    run_dir = temp_workspace / product / ".firecube" / "runs" / run_id
    events: list[dict[str, Any]] = []
    for path in sorted(run_dir.glob("events-*.jsonl")):
        events.extend(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return events


def test_run_started_with_replacement_event_written(temp_workspace):
    product = "product"
    run_id = "run-001"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    repo.record_run_started_with_replacement(
        product=product,
        run_id=run_id,
        replaces=["run-000", "run-000b"],
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_RUN_STARTED_WITH_REPLACEMENT
    ]

    assert len(replacement_events) == 1
    assert replacement_events[0]["record"]["replaces"] == ["run-000", "run-000b"]


def test_replacement_committed_event_written(temp_workspace):
    product = "product"
    run_id = "run-002"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    repo.record_replacement_committed(
        product=product,
        run_id=run_id,
        replacing_run_id="run-002",
        replaced_span_keys=["span_old_1", "span_old_2"],
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]

    assert len(replacement_events) == 1
    assert replacement_events[0]["record"]["replacing_run_id"] == "run-002"
    assert replacement_events[0]["record"]["replaced_span_keys"] == ["span_old_1", "span_old_2"]


def test_replacement_committed_idempotent(temp_workspace):
    product = "product"
    run_id = "run-003"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    repo.record_replacement_committed(
        product=product,
        run_id=run_id,
        replacing_run_id=run_id,
        replaced_span_keys=["span_old_1"],
    )
    repo.record_replacement_committed(
        product=product,
        run_id=run_id,
        replacing_run_id=run_id,
        replaced_span_keys=["span_old_1"],
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]

    assert len(replacement_events) == 1
