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

"""Regression tests for append_time_groups() behavior.

These tests freeze the current behavior before any decomposition begins.
If any test breaks during refactoring, a regression has been introduced.

Covers: single-group append, multi-group append, resume cursor inference,
empty batch handling, coverage entry generation, timestamp-state initialization,
multires follow-up, chunk alignment warnings, shard validation.
"""

from __future__ import annotations

import logging
import warnings
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.resume_cache import (
    ResumeCacheEntry,
    clear_resume_cache,
    get_resume_cache_entry,
    put_resume_cache_entry,
)
from tests.helpers.storage import local_zarr_handle, make_local_session

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset(timestamps, nlat=2, nlon=3, var_name="FWI"):
    """Build a simple xr.Dataset with given timestamps."""
    ts = pd.to_datetime(list(timestamps))
    data = np.arange(len(ts) * nlat * nlon, dtype=np.float32).reshape(len(ts), nlat, nlon)
    return xr.Dataset(
        {var_name: (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


def _write_initial_store(store_path, group, n_timestamps, nlat=2, nlon=3):
    """Write an initial zarr store with n_timestamps for pre-populating."""
    ts = pd.date_range("2024-01-01", periods=n_timestamps, freq="h")
    ds = xr.Dataset(
        {
            "FWI": (
                ("timestamp", "lat", "lon"),
                np.zeros((n_timestamps, nlat, nlon), dtype=np.float32),
            )
        },
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        ds.to_zarr(
            str(store_path),
            group=group,
            mode="w",
            zarr_format=3,
            safe_chunks=False,
            align_chunks=True,
        )
    return ts


def _dataset_factory(var_name="FWI", nlat=2, nlon=3):
    """Return a dataset_for_batch callable."""

    def _build(group: str, batch_ts):
        return _make_dataset(batch_ts, nlat=nlat, nlon=nlon, var_name=var_name)

    return _build


@pytest.fixture(autouse=True)
def _clear_resume_cache():
    """Ensure resume cache is empty between tests."""
    clear_resume_cache()
    yield
    clear_resume_cache()


# ===========================================================================
# Single-group append
# ===========================================================================


@pytest.mark.unit
def test_single_group_basic_append(tmp_path):
    """1 group, 3 timestamps → all 3 written, store has correct size."""
    store = str(tmp_path / "single.zarr")
    ts = pd.date_range("2024-01-01", periods=3, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        batch_size=10,
    )

    assert metrics["batch_processing"]["timestamps_written"] == 3
    ds = xr.open_zarr(store, group="G1", consolidated=False)
    assert ds.sizes["timestamp"] == 3


@pytest.mark.unit
def test_single_group_coverage_entry(tmp_path):
    """Coverage entry time_index_ranges are correct for a single-batch write."""
    store = str(tmp_path / "cov.zarr")
    ts = pd.date_range("2024-01-01", periods=5, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        batch_size=10,
    )

    assert len(metrics["coverage"]) == 1
    cov = metrics["coverage"][0]
    assert cov["group"] == "G1"
    assert cov["arrays"] == ["G1/FWI"]
    assert cov["time_index_ranges"] == [[0, 4]]
    assert cov["state_array"] == "G1/firecube_timestamp_state"


# ===========================================================================
# Multi-group append
# ===========================================================================


@pytest.mark.unit
def test_multi_group_append(tmp_path):
    """3 groups → all groups written independently."""
    store = str(tmp_path / "multi.zarr")
    ts1 = pd.date_range("2024-01-01", periods=2, freq="h")
    ts2 = pd.date_range("2024-02-01", periods=3, freq="h")
    ts3 = pd.date_range("2024-03-01", periods=4, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"A": list(ts1), "B": list(ts2), "C": list(ts3)},
        dataset_for_batch=_dataset_factory(),
        batch_size=10,
    )

    assert metrics["batch_processing"]["timestamps_written"] == 2 + 3 + 4
    for grp, expected in [("A", 2), ("B", 3), ("C", 4)]:
        ds = xr.open_zarr(store, group=grp, consolidated=False)
        assert ds.sizes["timestamp"] == expected, f"Group {grp} mismatch"


@pytest.mark.unit
def test_multi_group_independent_coverage(tmp_path):
    """Each group produces its own coverage entry."""
    store = str(tmp_path / "multi_cov.zarr")
    ts1 = pd.date_range("2024-01-01", periods=2, freq="h")
    ts2 = pd.date_range("2024-02-01", periods=3, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"X": list(ts1), "Y": list(ts2)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        batch_size=10,
    )

    cov = metrics["coverage"]
    assert len(cov) == 2
    groups = {c["group"] for c in cov}
    assert groups == {"X", "Y"}
    for entry in cov:
        assert len(entry["time_index_ranges"]) >= 1


# ===========================================================================
# Resume / cursor inference
# ===========================================================================


@pytest.mark.unit
def test_resume_cursor_inference(tmp_path):
    """Pre-populated store with 5 timestamps → append starts at index 5."""
    store = str(tmp_path / "resume_cursor.zarr")
    group = "G1"

    _write_initial_store(tmp_path / "resume_cursor.zarr", group, n_timestamps=5)
    new_ts = pd.date_range("2024-01-01T05:00:00", periods=3, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={group: list(new_ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=True,
        batch_size=10,
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[5, 7]]
    ds = xr.open_zarr(store, group=group, consolidated=False)
    assert ds.sizes["timestamp"] == 8
    assert float(ds["FWI"].isel(timestamp=0, lat=0, lon=0).values) == 0.0


@pytest.mark.unit
def test_resume_cache_used_when_available(tmp_path):
    """When ResumeCacheEntry is pre-seeded, cursor comes from the cache entry."""
    store = str(tmp_path / "cache_hit.zarr")
    group = "G1"

    existing_ts = _write_initial_store(tmp_path / "cache_hit.zarr", group, n_timestamps=5)
    cache_key = (store, group, "timestamp")
    put_resume_cache_entry(
        cache_key,
        ResumeCacheEntry(
            cursor=5,
            chunk_len=2,
            state_initialized=True,
            preexisting_values=frozenset(pd.to_datetime(existing_ts).to_pydatetime()),  # type: ignore[union-attr]
        ),
    )

    new_ts = pd.date_range("2024-01-01T05:00:00", periods=2, freq="h")

    with patch(
        "firecube.ingestor.runtime.zarr.append._read_existing_group_meta",
        wraps=lambda *a, **kw: (True, ["timestamp", "lat", "lon"], [0, 2, 3], [99, 2, 3], None),
    ):
        metrics = append_time_groups(
            store=store,
            zarr_store=local_zarr_handle(store),
            session=make_local_session(store),
            group_to_timestamps={group: list(new_ts)},
            dataset_for_batch=_dataset_factory(),
            arrays_for_group=lambda g: [f"{g}/FWI"],
            resume_existing=True,
            batch_size=10,
        )

    assert metrics["coverage"][0]["time_index_ranges"] == [[5, 6]]
    assert zarr.open_array(store, path=f"{group}/FWI", mode="r").shape == (7, 2, 3)
    assert zarr.open_array(store, path=f"{group}/timestamp", mode="r").shape == (7,)
    resume_entry = get_resume_cache_entry(cache_key)
    assert resume_entry is not None
    assert resume_entry.cursor == 7


@pytest.mark.unit
def test_overlap_detection_raises(tmp_path):
    """Overlapping timestamps with resume_existing raises ResumeConflictError."""
    store = str(tmp_path / "overlap.zarr")
    group = "G1"

    _write_initial_store(tmp_path / "overlap.zarr", group, n_timestamps=4)
    overlap_ts = pd.date_range("2024-01-01T02:00:00", periods=2, freq="h")

    with pytest.raises(ResumeConflictError, match="overlapping resume append"):
        append_time_groups(
            store=store,
            zarr_store=local_zarr_handle(store),
            session=make_local_session(store),
            group_to_timestamps={group: list(overlap_ts)},
            dataset_for_batch=_dataset_factory(),
            arrays_for_group=lambda g: [f"{g}/FWI"],
            resume_existing=True,
            batch_size=10,
        )


# ===========================================================================
# Empty batch handling
# ===========================================================================


@pytest.mark.unit
def test_empty_batch_no_writes(tmp_path):
    """Zero items → no zarr writes, coverage is empty."""
    store = str(tmp_path / "empty.zarr")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": []},
        dataset_for_batch=_dataset_factory(),
        batch_size=10,
    )

    assert metrics["batch_processing"]["timestamps_written"] == 0
    assert metrics["batch_processing"]["batches_written"] == 0
    assert "coverage" not in metrics


@pytest.mark.unit
def test_empty_group_no_writes(tmp_path):
    """Group with empty timestamp list produces no coverage entry."""
    store = str(tmp_path / "empty_group.zarr")
    ts = pd.date_range("2024-01-01", periods=2, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"has_data": list(ts), "empty": []},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        batch_size=10,
    )

    cov_groups = [c["group"] for c in metrics.get("coverage", [])]
    assert "has_data" in cov_groups
    assert "empty" not in cov_groups


@pytest.mark.unit
def test_dataset_none_skipped(tmp_path):
    """When dataset_for_batch returns None, batch is skipped gracefully."""
    store = str(tmp_path / "none_ds.zarr")
    ts = pd.date_range("2024-01-01", periods=3, freq="h")

    def _returns_none(group, batch_ts):
        return None

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_returns_none,
        batch_size=10,
    )

    assert metrics["batch_processing"]["timestamps_written"] == 0
    assert "coverage" not in metrics


# ===========================================================================
# Coverage entries
# ===========================================================================


@pytest.mark.unit
def test_coverage_entry_time_index_ranges_single_batch(tmp_path):
    """Single batch of contiguous timestamps → one range [[0, N-1]]."""
    store = str(tmp_path / "cov_contig.zarr")
    ts = pd.date_range("2024-01-01", periods=5, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        batch_size=100,  # All in one batch
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[0, 4]]


@pytest.mark.unit
def test_coverage_entry_multiple_batches(tmp_path):
    """Multiple batches produce separate ranges in coverage."""
    store = str(tmp_path / "cov_multi_batch.zarr")
    ts = pd.date_range("2024-01-01", periods=6, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        batch_size=2,  # 3 batches of 2
    )

    ranges = metrics["coverage"][0]["time_index_ranges"]
    assert ranges == [[0, 1], [2, 3], [4, 5]]


@pytest.mark.unit
def test_coverage_entry_time_bounds(tmp_path):
    """Coverage entry captures correct time_min and time_max."""
    store = str(tmp_path / "cov_bounds.zarr")
    ts = pd.date_range("2024-06-15T08:00", periods=4, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        batch_size=10,
    )

    cov = metrics["coverage"][0]
    assert cov["time_min"] is not None
    assert cov["time_max"] is not None
    assert pd.Timestamp(cov["time_min"]) == ts[0]
    assert pd.Timestamp(cov["time_max"]) == ts[-1]


# ===========================================================================
# Timestamp-state
# ===========================================================================


@pytest.mark.unit
def test_timestamp_state_initialized(tmp_path):
    """firecube_timestamp_state array is created in the output zarr store."""
    store = str(tmp_path / "state.zarr")
    ts = pd.date_range("2024-01-01", periods=3, freq="h")

    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        batch_size=10,
    )

    ds = xr.open_zarr(store, group="G1", consolidated=False)
    assert "firecube_timestamp_state" in ds
    state_vals = ds["firecube_timestamp_state"].values
    assert np.all(state_vals == 1)


@pytest.mark.unit
def test_timestamp_state_on_resume_legacy_store(tmp_path):
    """When resuming into a legacy store (no state array), state is backfilled."""
    store = str(tmp_path / "legacy_state.zarr")
    group = "G1"

    _write_initial_store(tmp_path / "legacy_state.zarr", group, n_timestamps=3)

    new_ts = pd.date_range("2024-01-01T03:00:00", periods=2, freq="h")

    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={group: list(new_ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=True,
        batch_size=10,
    )

    ds = xr.open_zarr(store, group=group, consolidated=False)
    assert "firecube_timestamp_state" in ds
    assert ds.sizes["timestamp"] == 5


@pytest.mark.unit
def test_resume_append_tolerates_clamped_first_write_chunk(tmp_path):
    """Resume must not reject a store whose first write clamped its chunks.

    dask clamps a configured chunk down to the data extent on the initial write
    (a chunk cannot exceed the array size). So when the first batch is smaller
    than the configured chunk in any dimension -- a small first batch on the
    append dim, or a configured spatial chunk larger than the grid -- the
    stored chunk differs from the raw configured value. A later resume must
    still append, not raise a chunk-shape mismatch.
    """
    import zarr

    store = str(tmp_path / "clamped_chunk.zarr")
    # lon chunk (1000) >> grid (3) and timestamp chunk (24) >> first batch (2):
    # both are clamped on the initial write.
    chunk_shape = {"timestamp": 24, "lat": 2, "lon": 1000}

    first = pd.date_range("2024-01-01", periods=2, freq="h")
    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(first)},
        dataset_for_batch=_dataset_factory(),
        chunk_shape=chunk_shape,
        batch_size=10,
    )

    # Precondition: the oversized configured chunks were clamped to the data.
    stored = zarr.open_array(store, path="G1/FWI", mode="r")
    assert tuple(stored.chunks) == (2, 2, 3)

    # Simulate a fresh run: no in-process resume cache, so the resume path
    # reads stored metadata and validates chunks (this is the bug's trigger).
    clear_resume_cache()

    later = pd.date_range("2024-01-01T02:00:00", periods=1, freq="h")
    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(later)},
        dataset_for_batch=_dataset_factory(),
        chunk_shape=chunk_shape,
        resume_existing=True,
        batch_size=10,
    )

    assert metrics["batch_processing"]["timestamps_written"] == 1
    ds = xr.open_zarr(store, group="G1", consolidated=False)
    assert ds.sizes["timestamp"] == 3


# ===========================================================================
# Alignment warnings
# ===========================================================================


@pytest.mark.unit
def test_alignment_warning_logged(tmp_path):
    """Misaligned chunk boundaries produce a warning log entry."""
    store = str(tmp_path / "unaligned.zarr")
    ts = pd.date_range("2024-01-01", periods=3, freq="h")

    logger = logging.getLogger("test.alignment")
    with patch.object(logger, "warning") as mock_warn:
        append_time_groups(
            store=store,
            zarr_store=local_zarr_handle(store),
            session=make_local_session(store),
            group_to_timestamps={"G1": list(ts)},
            dataset_for_batch=_dataset_factory(),
            chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
            batch_size=10,
            logger=logger,
        )

    assert mock_warn.call_count >= 1
    warn_msg = mock_warn.call_args_list[0][0][0]
    assert "unaligned" in warn_msg.lower()


@pytest.mark.unit
def test_aligned_no_warning(tmp_path):
    """Aligned chunk boundaries produce no alignment warning."""
    store = str(tmp_path / "aligned.zarr")
    ts = pd.date_range("2024-01-01", periods=4, freq="h")

    logger = logging.getLogger("test.aligned")
    with patch.object(logger, "warning") as mock_warn:
        append_time_groups(
            store=store,
            zarr_store=local_zarr_handle(store),
            session=make_local_session(store),
            group_to_timestamps={"G1": list(ts)},
            dataset_for_batch=_dataset_factory(),
            chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
            batch_size=2,
            logger=logger,
        )
    for call in mock_warn.call_args_list:
        if call[0]:
            assert "unaligned" not in call[0][0].lower()


# ===========================================================================
# Shard validation
# ===========================================================================


@pytest.mark.unit
def test_shard_validation_runs(tmp_path):
    """Sharding configured → first write succeeds, second with same config succeeds."""
    store = str(tmp_path / "shard_ok.zarr")
    ts1 = pd.date_range("2024-01-01", periods=2, freq="h")
    ts2 = pd.date_range("2024-01-01T02:00:00", periods=2, freq="h")

    shard = {"timestamp": 2, "lat": 2, "lon": 3}

    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts1)},
        dataset_for_batch=_dataset_factory(),
        shard_shape=shard,
        batch_size=10,
    )

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts2)},
        dataset_for_batch=_dataset_factory(),
        shard_shape=shard,
        batch_size=10,
    )

    assert metrics["batch_processing"]["timestamps_written"] == 2


@pytest.mark.unit
def test_shard_validation_mismatch_raises(tmp_path):
    """Mismatched shard shape on append raises ValueError."""
    store = str(tmp_path / "shard_bad.zarr")
    ts1 = pd.date_range("2024-01-01", periods=2, freq="h")
    ts2 = pd.date_range("2024-01-01T02:00:00", periods=2, freq="h")

    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts1)},
        dataset_for_batch=_dataset_factory(nlat=20, nlon=20),
        shard_shape={"timestamp": 2, "lat": 20, "lon": 20},
        batch_size=10,
    )
    with pytest.raises(ValueError, match="shard_shape"):
        append_time_groups(
            store=store,
            zarr_store=local_zarr_handle(store),
            session=make_local_session(store),
            group_to_timestamps={"G1": list(ts2)},
            dataset_for_batch=_dataset_factory(nlat=20, nlon=20),
            shard_shape={"timestamp": 2, "lat": 10, "lon": 10},
            batch_size=10,
        )


# ===========================================================================
# Metrics structure
# ===========================================================================


@pytest.mark.unit
def test_metrics_structure(tmp_path):
    """Returned metrics dict has expected keys and structure."""
    store = str(tmp_path / "metrics.zarr")
    ts = pd.date_range("2024-01-01", periods=3, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        batch_size=10,
    )

    assert "duration_s" in metrics
    assert isinstance(metrics["duration_s"], float)
    assert "timestamps_per_group" in metrics
    assert metrics["timestamps_per_group"]["G1"] == 3
    assert "batch_processing" in metrics
    bp = metrics["batch_processing"]
    assert bp["batch_size"] == 10
    assert bp["batches_attempted"] >= 1
    assert bp["batches_written"] >= 1
    assert bp["timestamps_requested"] == 3
    assert bp["timestamps_written"] == 3


# ===========================================================================
# Resume cache update after write
# ===========================================================================


@pytest.mark.unit
def test_resume_cache_updated_after_write(tmp_path):
    """After writing, the resume cache entry is updated with new cursor."""
    store = str(tmp_path / "cache_update.zarr")
    ts = pd.date_range("2024-01-01", periods=4, freq="h")

    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        batch_size=10,
    )

    cache_key = (store, "G1", "timestamp")
    entry = get_resume_cache_entry(cache_key)
    assert entry is not None
    assert entry.cursor == 4


@pytest.mark.unit
def test_coverage_aligned_field(tmp_path):
    """Coverage entry 'aligned' field reflects actual alignment status."""
    store = str(tmp_path / "aligned_field.zarr")
    ts = pd.date_range("2024-01-01", periods=4, freq="h")

    metrics = append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G1": list(ts)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
        batch_size=2,
    )

    assert metrics["coverage"][0]["aligned"] is True

    store2 = str(tmp_path / "unaligned_field.zarr")
    ts2 = pd.date_range("2024-01-01", periods=3, freq="h")

    metrics2 = append_time_groups(
        store=store2,
        zarr_store=local_zarr_handle(store2),
        session=make_local_session(store2),
        group_to_timestamps={"G1": list(ts2)},
        dataset_for_batch=_dataset_factory(),
        arrays_for_group=lambda g: [f"{g}/FWI"],
        chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
        batch_size=10,
    )

    assert metrics2["coverage"][0]["aligned"] is False
