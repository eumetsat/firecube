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

import warnings
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.zarr.append import _read_existing_append_values, append_time_groups
from tests.helpers.storage import local_zarr_handle, make_local_session


@pytest.mark.unit
def test_append_time_groups_writes_state_and_coverage(tmp_path):
    store = tmp_path / "out.zarr"
    timestamps = pd.date_range("2024-01-01", periods=4, freq="h")

    def dataset_for_batch(group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.arange(len(batch_ts) * 2 * 3, dtype=np.float32).reshape((len(batch_ts), 2, 3))
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={"F024": list(timestamps)},
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
        compression=False,
        consolidate=False,
        resume_existing=False,
        batch_size=2,
    )

    assert "coverage" in metrics
    cov = metrics["coverage"][0]
    assert cov["group"] == "F024"
    assert cov["arrays"] == ["F024/FWI"]
    assert cov["time_index_ranges"] == [[0, 1], [2, 3]]
    assert cov["state_array"] == "F024/firecube_timestamp_state"

    ds = xr.open_zarr(str(store), group="F024", consolidated=False)
    assert ds.sizes["timestamp"] == 4
    assert "firecube_timestamp_state" in ds


@pytest.mark.unit
def test_append_time_groups_resume_creates_state_for_legacy_store(tmp_path):
    store = tmp_path / "legacy.zarr"
    group = "F024"

    # Create a legacy store with only FWI, no firecube_timestamp_state.
    ts0 = pd.date_range("2024-01-01", periods=2, freq="h")
    ds0 = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((2, 2, 3), dtype=np.float32))},
        coords={"timestamp": ts0, "lat": np.arange(2), "lon": np.arange(3)},
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        ds0.to_zarr(
            str(store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
        )

    ts1 = pd.date_range("2024-01-01T02:00:00", periods=3, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts1)},
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        chunk_shape=None,
        resume_existing=True,
        batch_size=10,
    )

    cov = metrics["coverage"][0]
    assert cov["time_index_ranges"] == [[2, 4]]

    ds = xr.open_zarr(str(store), group=group, consolidated=False)
    assert ds.sizes["timestamp"] == 5
    assert "firecube_timestamp_state" in ds


@pytest.mark.unit
def test_append_time_groups_raises_on_spatial_chunk_mismatch(tmp_path):
    """A genuine non-append-dim chunk drift must still fail loudly on resume.

    The append-dimension chunk is immutable and only advisory on resume (it is
    fixed at array creation and appends reuse existing metadata; see
    ``AppendResumeService._effective_chunk``), so a smaller-than-configured
    append chunk is tolerated. But a configured *spatial* chunk that the
    existing array cannot hold signals real config drift and must raise.
    """
    store = tmp_path / "mismatch.zarr"
    group = "F024"

    root = zarr.open_group(store=str(store), mode="a")
    grp = root.require_group(group)
    grp.create_array(
        "FWI",
        shape=(2, 2, 3),
        chunks=(1, 2, 3),  # lon chunk = 3
        dtype="f4",
        dimension_names=("timestamp", "lat", "lon"),
        overwrite=True,
    )

    ts = pd.date_range("2024-01-01", periods=1, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    with pytest.raises(ValueError, match="chunk_shape"):
        append_time_groups(
            store=str(store),
            zarr_store=local_zarr_handle(store),
            session=make_local_session(str(store)),
            group_to_timestamps={group: list(ts)},
            dataset_for_batch=dataset_for_batch,
            arrays_for_group=lambda g: [f"{g}/FWI"],
            # lon configured to 2 but the existing array holds chunk 3 -> drift.
            chunk_shape={"timestamp": 1, "lat": 2, "lon": 2},
            resume_existing=True,
        )


@pytest.mark.unit
def test_append_time_groups_raises_on_resume_overlap(tmp_path):
    store = tmp_path / "overlap.zarr"
    group = "F024"

    ts0 = pd.date_range("2024-01-01", periods=4, freq="h")
    ds0 = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((4, 2, 3), dtype=np.float32))},
        coords={"timestamp": ts0, "lat": np.arange(2), "lon": np.arange(3)},
    )
    ds0.to_zarr(
        str(store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
    )

    ts_overlap = pd.date_range("2024-01-01T02:00:00", periods=2, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    with pytest.raises(ResumeConflictError, match="overlapping resume append"):
        append_time_groups(
            store=str(store),
            zarr_store=local_zarr_handle(store),
            session=make_local_session(str(store)),
            group_to_timestamps={group: list(ts_overlap)},
            dataset_for_batch=dataset_for_batch,
            arrays_for_group=lambda g: [f"{g}/FWI"],
            resume_existing=True,
            batch_size=10,
        )


@pytest.mark.unit
def test_append_time_groups_resume_accepts_strictly_new_timestamps(tmp_path):
    store = tmp_path / "resume_ok.zarr"
    group = "F024"

    ts0 = pd.date_range("2024-01-01", periods=3, freq="h")
    ds0 = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((3, 2, 3), dtype=np.float32))},
        coords={"timestamp": ts0, "lat": np.arange(2), "lon": np.arange(3)},
    )
    ds0.to_zarr(
        str(store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
    )

    ts1 = pd.date_range("2024-01-01T03:00:00", periods=2, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts1)},
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=True,
        batch_size=10,
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[3, 4]]


@pytest.mark.unit
def test_append_time_groups_resume_target_uri_reads_cursor_from_final_target(tmp_path):
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp.zarr"
    group = "F024"

    existing_ts = pd.date_range("2024-01-01", periods=10, freq="h")
    existing_ds = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((10, 2, 3), dtype=np.float32))},
        coords={"timestamp": existing_ts, "lat": np.arange(2), "lon": np.arange(3)},
    )
    existing_ds.to_zarr(
        str(final_store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
    )

    new_ts = pd.date_range("2024-01-01T10:00:00", periods=2, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    with (
        patch(
            "firecube.ingestor.runtime.zarr.append.write_dataset_to_zarr",
            autospec=True,
        ) as mock_write,
        patch(
            "firecube.ingestor.runtime.zarr.append._read_existing_append_values",
            wraps=_read_existing_append_values,
        ) as mock_read_values,
    ):
        metrics = append_time_groups(
            store=str(temp_store),
            zarr_store=local_zarr_handle(temp_store),
            session=make_local_session(str(temp_store)),
            resume_zarr_store=local_zarr_handle(final_store, mode="r"),
            group_to_timestamps={group: list(new_ts)},
            dataset_for_batch=dataset_for_batch,
            arrays_for_group=lambda g: [f"{g}/FWI"],
            resume_existing=True,
            batch_size=10,
        )

    assert mock_write.call_count == 1
    assert mock_read_values.call_args.kwargs["store_uri"] == str(final_store)
    assert metrics["coverage"][0]["time_index_ranges"] == [[10, 11]]


@pytest.mark.unit
def test_append_time_groups_without_resume_target_uri_keeps_store_uri_reads(tmp_path):
    store = tmp_path / "resume_target_default.zarr"
    group = "F024"

    existing_ts = pd.date_range("2024-01-01", periods=3, freq="h")
    existing_ds = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((3, 2, 3), dtype=np.float32))},
        coords={"timestamp": existing_ts, "lat": np.arange(2), "lon": np.arange(3)},
    )
    existing_ds.to_zarr(
        str(store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
    )

    new_ts = pd.date_range("2024-01-01T03:00:00", periods=2, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    with patch(
        "firecube.ingestor.runtime.zarr.append._read_existing_append_values",
        wraps=_read_existing_append_values,
    ) as mock_read_values:
        metrics = append_time_groups(
            store=str(store),
            zarr_store=local_zarr_handle(store),
            session=make_local_session(str(store)),
            group_to_timestamps={group: list(new_ts)},
            dataset_for_batch=dataset_for_batch,
            arrays_for_group=lambda g: [f"{g}/FWI"],
            resume_existing=True,
            batch_size=10,
        )

    assert mock_read_values.call_args.kwargs["store_uri"] == str(store)
    assert metrics["coverage"][0]["time_index_ranges"] == [[3, 4]]


@pytest.mark.unit
def test_append_time_groups_reader_uses_storage_driver_factory_for_default_session(tmp_path):
    store = tmp_path / "factory.zarr"
    group = "F024"

    ds = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.ones((1, 2, 3), dtype=np.float32))},
        coords={
            "timestamp": pd.date_range("2024-01-01", periods=1, freq="h"),
            "lat": np.arange(2),
            "lon": np.arange(3),
        },
    )
    ds.to_zarr(
        str(store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
    )

    with patch(
        "firecube.ingestor.runtime.zarr.append.StorageDriverConfig.from_storage_config_or_default",
        wraps=StorageDriverConfig.from_storage_config_or_default,
    ) as mock_factory:
        values = _read_existing_append_values(
            store_uri=str(store),
            group=group,
            append_dim="timestamp",
            session=None,
        )

    mock_factory.assert_called_once_with(None)
    assert values == {pd.Timestamp("2024-01-01T00:00:00")}


@pytest.mark.unit
def test_append_time_groups_resume_allows_non_overlapping_earlier_window(tmp_path):
    store = tmp_path / "resume_earlier_ok.zarr"
    group = "F024"

    ts0 = pd.date_range("2024-06-10", periods=3, freq="h")
    ds0 = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((3, 2, 3), dtype=np.float32))},
        coords={"timestamp": ts0, "lat": np.arange(2), "lon": np.arange(3)},
    )
    ds0.to_zarr(
        str(store), group=group, mode="w", zarr_format=3, safe_chunks=False, align_chunks=True
    )

    ts1 = pd.date_range("2024-01-01", periods=2, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts1)},
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=True,
        batch_size=10,
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[3, 4]]


@pytest.mark.unit
def test_append_time_groups_resume_uses_preexisting_baseline_not_same_run_data(tmp_path):
    store = tmp_path / "resume_baseline_only.zarr"
    group = "F024"

    # First write creates a new store with a sparse timestamp selection.
    ts_first = pd.to_datetime(["2024-01-01T12:00:00", "2024-03-22T12:00:00", "2024-06-09T12:00:00"])

    def ds_for_first(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts_first)},
        dataset_for_batch=ds_for_first,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=True,
        batch_size=10,
    )

    # Second write is disjoint from first write values but overlaps in min/max range.
    ts_second = pd.to_datetime(["2024-02-10T12:00:00", "2024-04-30T12:00:00"])

    def ds_for_second(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts_second)},
        dataset_for_batch=ds_for_second,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=True,
        batch_size=10,
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[3, 4]]


@pytest.mark.unit
def test_append_time_groups_non_resume_appends_across_calls(tmp_path):
    store = tmp_path / "no_resume_multi_call.zarr"
    group = "F024"

    ts_first = pd.date_range("2024-01-01", periods=2, freq="h")
    ts_second = pd.date_range("2024-01-01T02:00:00", periods=2, freq="h")

    def dataset_for_batch(_group: str, batch_ts):
        batch_ts = pd.to_datetime(list(batch_ts))
        data = np.ones((len(batch_ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts_first)},
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=False,
        batch_size=10,
    )
    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={group: list(ts_second)},
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/FWI"],
        resume_existing=False,
        batch_size=10,
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[2, 3]]
    ds = xr.open_zarr(str(store), group=group, consolidated=False)
    assert ds.sizes["timestamp"] == 4


@pytest.mark.unit
def test_append_time_groups_shard_mismatch_raises_on_append(tmp_path):
    """Flaw 4: _validate_shard_shape deduplication — both paths raise ValueError."""
    from firecube.ingestor.runtime.zarr.append import append_time_groups

    store = str(tmp_path / "mismatch.zarr")

    def make_ds(g, items):
        return xr.Dataset(
            {
                "v": xr.DataArray(
                    np.zeros((len(items), 20, 20), dtype="float32"),
                    dims=["timestamp", "ny", "nx"],
                    coords={"timestamp": list(range(len(items)))},
                )
            }
        )

    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"G": [0, 1]},
        dataset_for_batch=make_ds,
        shard_shape={"timestamp": 1, "ny": 20, "nx": 20},
        batch_size=2,
    )

    with pytest.raises(ValueError, match="shard_shape"):
        append_time_groups(
            store=store,
            zarr_store=local_zarr_handle(store),
            session=make_local_session(store),
            group_to_timestamps={"G": [2, 3]},
            dataset_for_batch=make_ds,
            shard_shape={"timestamp": 1, "ny": 10, "nx": 10},
            batch_size=2,
        )
