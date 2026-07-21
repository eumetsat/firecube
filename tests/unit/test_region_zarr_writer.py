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

import warnings
from datetime import UTC, datetime

import numpy as np
import pytest
import zarr

from firecube.core.zarr.region_writer import (
    RegionZarrWriter,
    RegionZarrWriterProtocol,
)

try:
    from zarr.errors import ZarrUserWarning

    warnings.filterwarnings("ignore", category=ZarrUserWarning)
except ImportError:
    pass


@pytest.fixture()
def store_path(tmp_path):
    store_dir = tmp_path / "test.zarr"
    zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    return str(store_dir)


@pytest.fixture()
def writer(store_path):
    return RegionZarrWriter(store_path)


class TestLazyOpen:
    def test_file_uri_opens_successfully(self, store_path):
        w = RegionZarrWriter(f"file://{store_path}")
        root = w._open_root()
        assert root is not None


class TestEnsureGroup:
    def test_create_top_level_group(self, writer):
        grp = writer.ensure_group("data_1km")
        root = writer._open_root()

        assert "data_1km" in root
        assert root["data_1km"].path == grp.path

    def test_create_array_path(self, writer):
        arr = writer.ensure_group(
            "data_1km/counts",
            shape=(4, 100, 200),
            dtype=np.float32,
            fill_value=0.0,
            chunks=(1, 100, 200),
        )
        assert arr.shape == (4, 100, 200)
        assert arr.dtype == np.float32

    def test_existing_array_returned(self, writer):
        arr1 = writer.ensure_group(
            "grp/vals",
            shape=(2, 10),
            dtype=np.int16,
            fill_value=-1,
        )
        arr2 = writer.ensure_group(
            "grp/vals",
            shape=(2, 10),
            dtype=np.int16,
            fill_value=-1,
        )
        assert arr1.shape == arr2.shape

    def test_missing_shape_dtype_raises(self, writer):
        with pytest.raises(ValueError, match="shape and dtype"):
            writer.ensure_group("grp/arr", shape=(10,))


class TestEnsureTimestampSlot:
    def test_accepts_preallocated_timestamped_arrays(self, writer):
        writer.ensure_group(
            "grp/data",
            shape=(5, 10),
            dtype=np.float32,
            fill_value=0.0,
        )
        writer.ensure_timestamp_slot("grp", ts_index=4)
        arr = writer._open_root()["grp/data"]
        assert arr.shape == (5, 10)

    def test_skips_default_coord_names(self, writer):
        writer.ensure_group("grp")
        writer.ensure_group("grp/y", shape=(100,), dtype=np.float64, fill_value=0.0)
        writer.ensure_group("grp/data", shape=(4, 100), dtype=np.float32, fill_value=0.0)
        writer.ensure_timestamp_slot("grp", ts_index=3)
        y_arr = writer._open_root()["grp/y"]
        assert y_arr.shape == (100,)

    def test_custom_coord_names(self, store_path):
        w = RegionZarrWriter(store_path, coord_names=frozenset({"lat", "lon"}))
        w.ensure_group("grp")
        w.ensure_group("grp/lat", shape=(50,), dtype=np.float64, fill_value=0.0)
        w.ensure_group("grp/data", shape=(3, 50), dtype=np.float32, fill_value=0.0)
        w.ensure_timestamp_slot("grp", ts_index=2)
        assert w._open_root()["grp/lat"].shape == (50,)
        assert w._open_root()["grp/data"].shape == (3, 50)

    def test_skips_scalar_arrays(self, writer):
        root = writer._open_root()
        grp = root.require_group("grp")
        grp.create_array("scalar", shape=(), dtype=np.int32, fill_value=0)
        writer.ensure_timestamp_slot("grp", ts_index=5)
        assert root["grp/scalar"].shape == ()

    def test_undersized_timestamped_array_raises(self, writer):
        writer.ensure_group(
            "grp/data",
            shape=(1, 10),
            dtype=np.float32,
            fill_value=0.0,
        )

        with pytest.raises(ValueError, match="Preallocate timestamped arrays"):
            writer.ensure_timestamp_slot("grp", ts_index=4)

        assert writer._open_root()["grp/data"].shape == (1, 10)


class TestWriteRegion:
    def test_3d_write(self, writer):
        writer.ensure_group(
            "grp/data",
            shape=(2, 10, 20),
            dtype=np.float32,
            fill_value=np.nan,
            chunks=(1, 10, 20),
        )
        patch = np.ones((5, 20), dtype=np.float32) * 42.0
        writer.write_region("grp", "data", ts_index=0, y_slice=slice(0, 5), data=patch)
        arr = writer._open_root()["grp/data"]
        result = np.asarray(arr[0, 0:5, :])
        np.testing.assert_array_equal(result, patch)

    def test_4d_write_with_channel(self, writer):
        writer.ensure_group(
            "grp/data",
            shape=(2, 10, 20, 3),
            dtype=np.float32,
            fill_value=np.nan,
            chunks=(1, 10, 20, 3),
        )
        patch = np.ones((5, 20), dtype=np.float32) * 7.0
        writer.write_region(
            "grp", "data", ts_index=1, y_slice=slice(0, 5), data=patch, channel_index=2
        )
        result = np.asarray(writer._open_root()["grp/data"][1, 0:5, :, 2])
        np.testing.assert_array_equal(result, patch)

    def test_unsupported_rank_raises(self, writer):
        writer.ensure_group(
            "grp/vec",
            shape=(10,),
            dtype=np.float32,
            fill_value=0.0,
        )
        with pytest.raises(ValueError, match="Unsupported array rank"):
            writer.write_region("grp", "vec", ts_index=0, y_slice=slice(0, 5), data=np.zeros(5))


class TestWrite1D:
    def test_writes_slot(self, writer):
        writer.ensure_group(
            "grp/cal",
            shape=(4, 8),
            dtype=np.float64,
            fill_value=0.0,
        )
        data = np.arange(8, dtype=np.float64)
        writer.write_1d("grp", "cal", ts_index=2, data=data)
        result = np.asarray(writer._open_root()["grp/cal"][2])
        np.testing.assert_array_equal(result, data)

    def test_1d_datetime64_single_element_ndarray_writes_slot(self, writer):
        """Regression: numpy>=2 rejects arr[i]=1-elem-array for datetime64."""
        writer.ensure_group(
            "grp/scans_start",
            shape=(288,),
            dtype=np.dtype("datetime64[s]"),
            fill_value=np.datetime64("NaT", "s"),
            chunks=(288,),
        )
        data = np.array(["2023-12-01T00:00:00"], dtype="datetime64[s]")
        writer.write_1d("grp", "scans_start", ts_index=42, data=data)
        arr = writer._open_root()["grp/scans_start"]
        assert np.asarray(arr[42]) == np.datetime64("2023-12-01T00:00:00", "s")

    @pytest.mark.parametrize(
        "dtype,fill,scalar_val,array_val",
        [
            pytest.param(
                np.float64,
                0.0,
                np.float64(42.5),
                np.array([42.5], dtype=np.float64),
                id="float64",
            ),
            pytest.param(
                np.int64,
                0,
                np.int64(7),
                np.array([7], dtype=np.int64),
                id="int64",
            ),
            pytest.param(
                np.dtype("datetime64[s]"),
                np.datetime64("NaT", "s"),
                np.datetime64("2024-01-01T12:00:00", "s"),
                np.array(["2024-01-01T12:00:00"], dtype="datetime64[s]"),
                id="datetime64",
            ),
        ],
    )
    def test_1d_scalar_or_1elem_payload_writes_slot(
        self, writer, dtype, fill, scalar_val, array_val
    ):
        writer.ensure_group(
            "grp/vec_scalar",
            shape=(10,),
            dtype=dtype,
            fill_value=fill,
            chunks=(10,),
        )
        writer.write_1d("grp", "vec_scalar", ts_index=3, data=scalar_val)
        assert np.asarray(writer._open_root()["grp/vec_scalar"][3]) == scalar_val

        writer.ensure_group(
            "grp/vec_array",
            shape=(10,),
            dtype=dtype,
            fill_value=fill,
            chunks=(10,),
        )
        writer.write_1d("grp", "vec_array", ts_index=5, data=array_val)
        assert np.asarray(writer._open_root()["grp/vec_array"][5]) == scalar_val

    @pytest.mark.parametrize(
        "bad_data",
        [
            pytest.param(np.array([], dtype=np.float64), id="empty"),
            pytest.param(np.array([1.0, 2.0, 3.0], dtype=np.float64), id="multi-element"),
        ],
    )
    def test_1d_target_rejects_non_single_element_payload(self, writer, bad_data):
        writer.ensure_group(
            "grp/reject",
            shape=(10,),
            dtype=np.float64,
            fill_value=0.0,
            chunks=(10,),
        )
        with pytest.raises(ValueError, match="exactly one slot"):
            writer.write_1d("grp", "reject", ts_index=0, data=bad_data)

    def test_higher_rank_target_rejects_shape_mismatch(self, writer):
        writer.ensure_group(
            "grp/calib_bad",
            shape=(4, 8),
            dtype=np.float64,
            fill_value=0.0,
            chunks=(1, 8),
        )
        with pytest.raises(ValueError, match="payload shape"):
            writer.write_1d(
                "grp",
                "calib_bad",
                ts_index=0,
                data=np.arange(5, dtype=np.float64),
            )


class TestResolveTimestampIndex:
    def test_returns_zero_when_no_timestamp_array(self, writer):
        writer.ensure_group("grp")
        idx = writer.resolve_timestamp_index("grp", "2024-01-01T00:00:00")
        assert idx == 0

    def test_idempotent_for_existing_timestamp(self, writer):
        ts = np.datetime64("2024-06-15T12:00:00", "s")
        writer.write_timestamp("grp", ts_index=0, timestamp_val=ts)
        idx = writer.resolve_timestamp_index("grp", ts)
        assert idx == 0

    def test_returns_next_slot_for_new_timestamp(self, writer):
        ts1 = np.datetime64("2024-01-01T00:00:00", "s")
        writer.write_timestamp("grp", ts_index=0, timestamp_val=ts1)
        ts2 = np.datetime64("2024-01-02T00:00:00", "s")
        idx = writer.resolve_timestamp_index("grp", ts2)
        assert idx == 1


class TestWriteTimestamp:
    def test_creates_and_writes_timestamp(self, writer):
        ts = np.datetime64("2024-03-10T06:00:00", "s")
        writer.write_timestamp("grp", ts_index=0, timestamp_val=ts)
        arr = writer._open_root()["grp/timestamp"]
        assert np.asarray(arr[0]) == ts

    def test_normalizes_datetime_object(self, writer):
        dt = datetime(2024, 7, 4, 12, 0, 0, tzinfo=UTC)
        writer.write_timestamp("grp", ts_index=0, timestamp_val=dt)
        arr = writer._open_root()["grp/timestamp"]
        expected = np.datetime64("2024-07-04T12:00:00", "s")
        assert np.asarray(arr[0]) == expected

    def test_writes_multiple_slots(self, writer):
        ts0 = np.datetime64("2024-01-01T00:00:00", "s")
        ts1 = np.datetime64("2024-01-02T00:00:00", "s")
        writer.ensure_group(
            "grp/timestamp",
            shape=(2,),
            dtype=np.dtype("datetime64[s]"),
            fill_value=np.datetime64("NaT", "s"),
            chunks=(2,),
        )
        writer.write_timestamp("grp", ts_index=0, timestamp_val=ts0)
        writer.write_timestamp("grp", ts_index=1, timestamp_val=ts1)
        arr = writer._open_root()["grp/timestamp"]
        assert arr.shape == (2,)
        assert np.asarray(arr[0]) == ts0
        assert np.asarray(arr[1]) == ts1


class TestNormalizeTimestamp:
    def test_datetime64_passthrough(self):
        val = np.datetime64("2024-01-01T00:00:00", "ns")
        result = RegionZarrWriter._normalize_timestamp_value(val)
        assert result.dtype == np.dtype("datetime64[s]")

    def test_string_conversion(self):
        result = RegionZarrWriter._normalize_timestamp_value("2024-06-15")
        assert result == np.datetime64("2024-06-15T00:00:00", "s")
        assert result.dtype == np.dtype("datetime64[s]")


class TestProtocol:
    def test_writer_satisfies_protocol(self, store_path):
        w = RegionZarrWriter(store_path)
        assert isinstance(w, RegionZarrWriterProtocol)
