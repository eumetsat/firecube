# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""``list_runs`` must project terminal run metrics from ``run.json`` only."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _fresh_manager(tmp_path: Path) -> ChunkManager:
    binding = make_test_binding(tmp_path)
    return ChunkManager(binding=binding, workspace=tmp_path)


def _record_terminal_run(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    timestamps_skipped: int,
) -> None:
    output_path = f"{manager.base_uri.rstrip('/')}/{product}"
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": product},
    )
    terminal_meta: dict[str, Any] = {"plugin": product}
    if timestamps_skipped:
        terminal_meta["timestamps_skipped"] = timestamps_skipped
    manager.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta=terminal_meta,
        status="complete",
    )


def test_list_runs_reads_terminal_timestamps_skipped_without_wal_scan(
    tmp_path: Path,
) -> None:
    product = "test_product"
    run_id = "run-with-skips"
    manager = _fresh_manager(tmp_path)
    _record_terminal_run(
        manager,
        product=product,
        run_id=run_id,
        timestamps_skipped=5,
    )

    manager.close()
    manager = _fresh_manager(tmp_path)

    with patch.object(
        manager.repo,
        "_read_run_events",
        wraps=manager.repo._read_run_events,
    ) as spy:
        runs = manager.list_runs(product=product)

    assert spy.call_count == 0, "terminal list_runs must not scan run WAL events"
    assert len(runs) == 1
    assert runs[0].run_id == run_id
    assert runs[0].timestamps_skipped == 5


def test_list_runs_defaults_missing_timestamps_skipped_to_zero(tmp_path: Path) -> None:
    product = "test_product"
    run_id = "run-without-skips"
    manager = _fresh_manager(tmp_path)
    _record_terminal_run(
        manager,
        product=product,
        run_id=run_id,
        timestamps_skipped=0,
    )

    run_json = tmp_path / product / ".firecube" / "runs" / run_id / "run.json"
    payload = json.loads(run_json.read_text(encoding="utf-8"))
    payload.pop("timestamps_skipped", None)
    run_json.write_text(json.dumps(payload), encoding="utf-8")

    manager.close()
    manager = _fresh_manager(tmp_path)

    runs = manager.list_runs(product=product)

    assert len(runs) == 1
    assert runs[0].timestamps_skipped == 0
