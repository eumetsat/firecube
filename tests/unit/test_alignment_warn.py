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

"""The chunk-alignment warning fires once per run and is summarised once.

Drives ``append_time_groups`` against a real local store with one
``AlignmentMonitor`` shared across calls, the way ``GenericZarrIngestor``
shares it across planner batches.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.alignment import AlignmentMonitor
from firecube.ingestor.runtime.zarr.append import append_time_groups
from tests.helpers.storage import local_zarr_handle, make_local_session

pytestmark = pytest.mark.unit

_LOGGER_NAME = "test.v2_m6_alignment"
_ROWS_PER_CALL = 10


def _dataset_for_batch(group: str, batch_ts) -> xr.Dataset:
    _ = group
    ts = pd.to_datetime(list(batch_ts))
    data = np.arange(len(ts) * 2 * 3, dtype=np.float32).reshape((len(ts), 2, 3))
    return xr.Dataset(
        {"precip": (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(2), "lon": np.arange(3)},
    )


def _append_call(
    store: Path,
    *,
    call_index: int,
    chunk_len: int,
    alignment: AlignmentMonitor,
    is_final_batch: bool,
) -> None:
    """Append ``_ROWS_PER_CALL`` consecutive days as one planner batch."""
    start = pd.Timestamp("2024-01-01") + pd.Timedelta(days=call_index * _ROWS_PER_CALL)
    timestamps = pd.date_range(start, periods=_ROWS_PER_CALL, freq="D")
    append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={"default": list(timestamps)},
        dataset_for_batch=_dataset_for_batch,
        chunk_shape={"timestamp": chunk_len, "lat": 2, "lon": 3},
        compression=False,
        consolidate=False,
        logger=logging.getLogger(_LOGGER_NAME),
        alignment=alignment,
        is_final_batch=is_final_batch,
    )


def _alignment_records(caplog: pytest.LogCaptureFixture) -> tuple[list[str], list[str]]:
    warnings = [
        rec.getMessage()
        for rec in caplog.records
        if rec.name == _LOGGER_NAME and "unaligned with chunk layout" in rec.getMessage()
    ]
    summaries = [
        rec.getMessage()
        for rec in caplog.records
        if rec.name == _LOGGER_NAME and rec.getMessage().startswith("Alignment summary")
    ]
    return warnings, summaries


def test_unaligned_run_warns_once_and_summarises_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Three 10-row batches against chunk 365: one warning, tail suppressed, summary of 2."""
    store = tmp_path / "unaligned.zarr"
    monitor = AlignmentMonitor()

    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        _append_call(store, call_index=0, chunk_len=365, alignment=monitor, is_final_batch=False)
        warnings_after_first, _ = _alignment_records(caplog)
        _append_call(store, call_index=1, chunk_len=365, alignment=monitor, is_final_batch=False)
        _append_call(store, call_index=2, chunk_len=365, alignment=monitor, is_final_batch=True)
        warnings, summaries_before = _alignment_records(caplog)
        monitor.emit_summary(logging.getLogger(_LOGGER_NAME))
        _, summaries = _alignment_records(caplog)

    assert len(warnings_after_first) == 1
    assert len(warnings) == 1, "second and third batches must not re-warn"
    assert summaries_before == [], "summary is emitted only by emit_summary"
    assert len(summaries) == 1
    assert "2 unaligned batch(es)" in summaries[0]
    assert "'default'" in summaries[0]
    assert "chunk=365" in summaries[0]
    assert monitor.unaligned_total == 2

    written = xr.open_zarr(str(store), group="default", consolidated=False)
    assert written.sizes["timestamp"] == 3 * _ROWS_PER_CALL


def test_final_tail_alone_is_not_unaligned(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A run whose only short write is the planner's last batch logs nothing."""
    store = tmp_path / "tail_only.zarr"
    monitor = AlignmentMonitor()

    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        _append_call(store, call_index=0, chunk_len=365, alignment=monitor, is_final_batch=True)
        monitor.emit_summary(logging.getLogger(_LOGGER_NAME))

    warnings, summaries = _alignment_records(caplog)
    assert warnings == []
    assert summaries == []
    assert monitor.unaligned_total == 0


def test_chunk_aligned_run_logs_nothing(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Batches that are multiples of the chunk length never warn or summarise."""
    store = tmp_path / "aligned.zarr"
    monitor = AlignmentMonitor()

    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        for call_index in range(3):
            _append_call(
                store,
                call_index=call_index,
                chunk_len=_ROWS_PER_CALL,
                alignment=monitor,
                is_final_batch=call_index == 2,
            )
        monitor.emit_summary(logging.getLogger(_LOGGER_NAME))

    warnings, summaries = _alignment_records(caplog)
    assert warnings == []
    assert summaries == []
    assert monitor.unaligned_total == 0


def test_monitor_not_shared_means_each_call_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Without a shared monitor every call allocates its own and re-warns.

    This is the pre-fix behaviour, kept observable so the run-scoped
    plumbing in ``GenericZarrIngestor`` is the thing under test, not the
    memo alone.
    """
    store = tmp_path / "per_call.zarr"

    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        for call_index in range(2):
            _append_call(
                store,
                call_index=call_index,
                chunk_len=365,
                alignment=AlignmentMonitor(),
                is_final_batch=False,
            )

    warnings, _ = _alignment_records(caplog)
    assert len(warnings) == 2
