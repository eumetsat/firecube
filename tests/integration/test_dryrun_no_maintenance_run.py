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

"""`chunks delete-span --dry-run` must not record a maintenance run."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


PRODUCT = "product.zarr"
GROUP = "default"
ARRAY_NAME = "precipitation"
ARRAY_PATH = f"{GROUP}/{ARRAY_NAME}"
STATE_NAME = "firecube_timestamp_state"
STATE_PATH = f"{GROUP}/{STATE_NAME}"
RUN_ID = "run-under-test"


def _manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=PRODUCT), workspace=tmp_path)


def _store_root(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT


def _seed_store_with_span(tmp_path: Path) -> None:
    """Create a minimal append-store zarr and record one active span."""
    root = zarr.open_group(store=str(_store_root(tmp_path)), mode="w", zarr_format=3)
    group = root.require_group(GROUP)
    group.create_array(
        ARRAY_NAME,
        shape=(30, 2),
        chunks=(365, 2),
        dtype="f4",
        fill_value=np.nan,
        dimension_names=("timestamp", "x"),
        overwrite=True,
    )
    group.create_array(
        "timestamp",
        shape=(30,),
        chunks=(365,),
        dtype="i8",
        fill_value=0,
        dimension_names=("timestamp",),
        overwrite=True,
    )
    state = group.create_array(
        STATE_NAME,
        shape=(30,),
        chunks=(365,),
        dtype="u1",
        fill_value=0,
        dimension_names=("timestamp",),
        overwrite=True,
    )
    state[:] = np.uint8(1)

    manager = _manager(tmp_path)
    try:
        base_time = datetime(2024, 1, 1)
        coverage = SpanCoverage(
            group=GROUP,
            arrays=[ARRAY_PATH],
            time_index_ranges=[[0, 29]],
            aligned=False,
            state_array=STATE_PATH,
            state_deleted_value=2,
            time_min=base_time.isoformat(),
            time_max=(base_time + timedelta(days=29)).isoformat(),
            time_dim_name="timestamp",
        )
        output_path = str(_store_root(tmp_path))
        manager.record_run_started(
            product=PRODUCT,
            run_id=RUN_ID,
            output_path=output_path,
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_span(
            product=PRODUCT,
            run_id=RUN_ID,
            batch_id="batch-a",
            group=GROUP,
            status="active",
            coverage=coverage,
            meta={"plugin": "test", "group": GROUP},
        )
        manager.record_run_terminal(
            product=PRODUCT,
            run_id=RUN_ID,
            output_path=output_path,
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
        )
    finally:
        manager.close()


def _run_ids_for(tmp_path: Path) -> list[str]:
    manager = _manager(tmp_path)
    try:
        return [r.run_id for r in manager.list_runs(product=PRODUCT)]
    finally:
        manager.close()


def _delete_span_cli(tmp_path: Path, *extra_flags: str):
    return CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(tmp_path),
            "delete-span",
            "--product-name",
            _store_root(tmp_path).as_uri(),
            "--run-id",
            RUN_ID,
            *extra_flags,
        ],
    )


def test_dry_run_does_not_record_maintenance_run(tmp_path: Path) -> None:
    """dry-run must leave the run list unchanged."""
    _seed_store_with_span(tmp_path)
    baseline_runs = _run_ids_for(tmp_path)
    assert baseline_runs == [RUN_ID], baseline_runs

    result = _delete_span_cli(tmp_path, "--dry-run")

    assert result.exit_code == 0, result.output
    assert "DRY RUN" in result.output, result.output
    after_runs = _run_ids_for(tmp_path)
    assert after_runs == baseline_runs, (
        f"dry-run must not add a maintenance run; before={baseline_runs}, after={after_runs}"
    )
    assert not any(rid.startswith("maintenance-delete-spans-") for rid in after_runs)


def test_real_delete_still_records_maintenance_run(tmp_path: Path) -> None:
    """Positive control: guard skips only dry-run, real delete still audits."""
    _seed_store_with_span(tmp_path)

    result = _delete_span_cli(tmp_path, "--force", "--yes-i-really-mean-it")

    assert result.exit_code == 0, result.output
    after_runs = _run_ids_for(tmp_path)
    assert any(rid.startswith("maintenance-delete-spans-") for rid in after_runs), after_runs
