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

"""Appends commit in planner batch order with ``pipeline_workers > 1``.

Behaviour under test (data correctness): when workers finish preparing
batches out of order, the ordered write gate still commits them in batch
order, so the append axis is strictly monotonic and the control-plane spans
cover consecutive index ranges in batch order.
"""

from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
import xarray as xr
from precip_daily_test_plugin.ingestor import PrecipDailyIngestor

from firecube.core.controlplane import ChunkManager
from firecube.ingestor.types.context import (
    IngestContext,
    PipelineBatch,
    PluginContext,
    StorageContext,
)
from tests.helpers.storage import make_test_binding, make_test_session

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"
_DAYS = 30
_BATCH_SIZE = 10
_WORKERS = 3
_BATCHES = _DAYS // _BATCH_SIZE


class _SlowEarlyBatchesIngestor(PrecipDailyIngestor):
    """precip_daily where batch N blocks on batch N+1 to force reverse preparation order."""

    PRODUCT_NAME: ClassVar[str] = "precip_daily"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.prepared_order: list[int] = []
        self._order_lock = threading.Lock()
        self._prepared_events: list[threading.Event] = [threading.Event() for _ in range(_BATCHES)]

    def prepare_batch_data(self, batch: PipelineBatch, ctx: PluginContext) -> dict[str, Any] | None:
        index = int(batch.metadata["batch_index"])
        if index < _BATCHES - 1 and not self._prepared_events[index + 1].wait(timeout=30.0):
            raise TimeoutError(f"batch {index} timed out waiting for batch {index + 1} to prepare")
        with self._order_lock:
            self.prepared_order.append(index)
        self._prepared_events[index].set()
        return super().prepare_batch_data(batch, ctx)


def _generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    result = subprocess.run(
        [sys.executable, str(_SYNTHETIC_PRECIP), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_parallel_workers_commit_appends_in_batch_order(tmp_path: Path) -> None:
    product = "precip_daily.zarr"
    source = tmp_path / "input"
    _generate_days(source, 1, _DAYS)

    chunk_manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=tmp_path / "cm-work",
    )
    ingestor = _SlowEarlyBatchesIngestor(name="precip_daily", chunk_manager=chunk_manager)
    session = make_test_session(tmp_path, product=product)
    ctx = IngestContext(
        source=str(source),
        target=session.product.product_uri.to_str(),
        output_format="zarr",
        options={
            "pipeline_batch_size": _BATCH_SIZE,
            "pipeline_workers": _WORKERS,
            "write_mode": "direct",
            "no_progress": True,
            "layout": "areastats",
        },
        storage=StorageContext(output=session),
        run_id="ordered-commit-run",
    )

    result = ingestor.run(ctx)

    # Preparation genuinely finished out of order; only the gate put it right.
    assert ingestor.prepared_order == list(reversed(range(_BATCHES)))
    assert result.metrics["pipeline"]["batches_failed"] == 0
    assert result.metrics["pipeline"]["batches_not_attempted"] == 0

    ds = xr.open_zarr(str(tmp_path / product), group="default", consolidated=False, zarr_format=3)
    try:
        times = np.asarray(ds["time"].values)
        precipitation = ds["precipitation"]
        assert precipitation.shape[0] == _DAYS
    finally:
        ds.close()

    assert len(times) == _DAYS
    assert np.all(np.diff(times) > np.timedelta64(0, "ns")), "time axis is not strictly monotonic"
    assert np.array_equal(
        times.astype("datetime64[D]"),
        np.arange("2024-01-01", "2024-01-31", dtype="datetime64[D]"),
    )

    spans = chunk_manager.list_chunks(product=product, chunk_type="span")
    by_batch = sorted(
        ((span.meta or {})["batch_id"], (span.record or {})["span"]["time_index_ranges"])
        for span in spans
    )
    assert [ranges for _batch_id, ranges in by_batch] == [
        [[0, 9]],
        [[10, 19]],
        [[20, 29]],
    ]
