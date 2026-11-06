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

import contextlib
import threading
import warnings
from datetime import UTC, datetime

import cftime
import numpy as np
import pytest
import zarr
import zarr.core.array as zarr_array
from zarr.buffer import default_buffer_prototype
from zarr.core.sync import sync
from zarr.storage import LocalStore, MemoryStore

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.core.zarr.guard.guarded_write import WriteResult, unit_store_key
from firecube.core.zarr.guard.metadata_authority import (
    AuthorityBasis,
    MetadataAuthorityError,
    hold_authority,
)
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
    def test_stale_extent_does_not_erase_slots_another_batch_wrote(self, store_path, monkeypatch):
        from firecube.core.zarr.guard._unit_locks import _UNIT_LOCKS

        kwargs = {"dtype": np.float32, "fill_value": np.nan, "chunks": (4, 4, 4)}
        writer_a = RegionZarrWriter(store_path)
        writer_a.ensure_group("grp/data", shape=(9, 4, 4), **kwargs)
        reached, release = threading.Event(), threading.Event()
        errors: list[BaseException] = []
        holder: list[threading.Thread] = []
        original_hold = _UNIT_LOCKS.hold
        first = [True]

        def hold(keys):
            if threading.current_thread() is holder[0] and first[0]:
                first[0] = False
                reached.set()
                if not release.wait(30.0):
                    raise AssertionError("writer A was never released")
            return original_hold(keys)

        monkeypatch.setattr(_UNIT_LOCKS, "hold", hold)

        def write(w, slot, value):
            data = np.full((4, 4), value, dtype=np.float32)
            w.write_region("grp", "data", ts_index=slot, y_slice=slice(None), data=data)

        def run_a():
            try:
                write(writer_a, 8, 8.0)
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run_a)
        holder.append(thread)
        thread.start()
        try:
            assert reached.wait(30.0), "writer A never reached its unit locks"
            writer_b = RegionZarrWriter(store_path)
            writer_b.ensure_group("grp/data", shape=(12, 4, 4), allow_grow=True, **kwargs)
            for slot in (9, 10, 11):
                write(writer_b, slot, float(slot))
        finally:
            release.set()
            thread.join(30.0)
        assert not thread.is_alive(), "writer A never finished"
        assert errors == []

        stored = zarr.open_array(f"{store_path}/grp/data", mode="r")
        assert stored.shape == (12, 4, 4)
        slots = np.asarray(stored[8:12, 0, 0]).tolist()
        assert slots == [8.0, 9.0, 10.0, 11.0], f"slots 8..11 read back {slots}"

    def test_fixed_extent_writer_drops_its_handle_when_it_grows_the_array(self, store_path):
        kwargs = {"dtype": np.float32, "fill_value": np.nan, "chunks": (4, 4, 4)}
        w = RegionZarrWriter(store_path, fixed_extent=True)
        w.ensure_group("grp/data", shape=(9, 4, 4), **kwargs)
        data = np.full((4, 4), 5.0, dtype=np.float32)
        w.write_region("grp", "data", ts_index=0, y_slice=slice(None), data=data)

        w.ensure_group("grp/data", shape=(12, 4, 4), allow_grow=True, **kwargs)
        w.write_region("grp", "data", ts_index=10, y_slice=slice(None), data=data)

        stored = zarr.open_array(f"{store_path}/grp/data", mode="r")
        assert stored.shape == (12, 4, 4)
        assert np.asarray(stored[10, 0, 0]).item() == 5.0

    def test_fixed_extent_writer_reads_no_metadata_after_its_first_write(
        self, store_path, monkeypatch
    ):
        kwargs = {"dtype": np.float32, "fill_value": np.nan, "chunks": (4, 4, 4)}
        RegionZarrWriter(store_path).ensure_group("grp/data", shape=(9, 4, 4), **kwargs)
        data = np.full((4, 4), 1.0, dtype=np.float32)
        reads = {"fixed": 0, "default": 0}
        original_get = LocalStore.get

        async def counting_get(self, key, *args, **kw):
            if key.endswith("zarr.json"):
                reads[mode] += 1
            return await original_get(self, key, *args, **kw)

        monkeypatch.setattr(LocalStore, "get", counting_get)
        for mode, fixed_extent in (("fixed", True), ("default", False)):
            w = RegionZarrWriter(store_path, fixed_extent=fixed_extent)
            w.write_region("grp", "data", ts_index=0, y_slice=slice(None), data=data)
            reads[mode] = 0
            for slot in (1, 2, 3):
                w.write_region("grp", "data", ts_index=slot, y_slice=slice(None), data=data)

        assert reads == {"fixed": 0, "default": 3}

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


@pytest.mark.parametrize("entry", ["write_region", "write_1d"])
@pytest.mark.parametrize(
    ("ts_index", "slot"), [(-1, 11), (10, 10)], ids=["negative_index", "newly_grown_index"]
)
def test_cached_handle_follows_another_writers_growth(store_path, entry, ts_index, slot):
    shape, chunks = ((9, 4, 4), (4, 4, 4)) if entry == "write_region" else ((9,), (4,))
    kwargs = {"dtype": np.float32, "fill_value": np.nan, "chunks": chunks}
    writer_a = RegionZarrWriter(store_path)
    writer_a.ensure_group("grp/data", shape=shape, **kwargs)

    def write(w, index, value):
        if entry == "write_region":
            data = np.full((4, 4), value, dtype=np.float32)
            w.write_region("grp", "data", ts_index=index, y_slice=slice(None), data=data)
        else:
            w.write_1d("grp", "data", ts_index=index, data=np.float32(value))

    write(writer_a, 0, 1.0)
    RegionZarrWriter(store_path).ensure_group(
        "grp/data", shape=(12, *shape[1:]), allow_grow=True, **kwargs
    )
    write(writer_a, ts_index, 7.0)

    stored = zarr.open_array(f"{store_path}/grp/data", mode="r")
    cell = (slot, 0, 0) if entry == "write_region" else (slot,)
    assert np.asarray(stored[cell]).item() == 7.0


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


def _stored(arr, unit):
    return bool(sync(arr.store.exists(unit_store_key(arr, unit))))


class TestOnWrite:
    """Every payload write goes through guarded_write and reports its WriteResult."""

    def test_every_payload_site_reports(self, store_path):
        seen: list[WriteResult] = []
        writer = RegionZarrWriter(store_path, on_write=seen.append)
        writer.ensure_group("grp/img", shape=(2, 2, 2), dtype=np.float32, fill_value=0.0)
        writer.ensure_group("grp/vec", shape=(2,), dtype=np.float32, fill_value=0.0)
        writer.ensure_group("grp/lat", shape=(2,), dtype=np.float64, fill_value=0.0)
        writer.ensure_group(
            "grp/pub", shape=(2, 2, 2), dtype=np.float32, fill_value=0.0, chunks=(1, 2, 2)
        )
        ts = np.datetime64("2024-01-01T00:00:00", "s")

        writer.write_region("grp", "img", 0, slice(0, 2), np.ones((2, 2), dtype=np.float32))
        description = writer.describe_array("grp", "pub")
        writer.publish_unit("grp", "pub", (1, 0, 0), None, description=description)
        writer.write_1d("grp", "vec", 1, np.asarray(5.0, dtype=np.float32))
        writer.write_static("grp", "lat", np.array([1.5, 2.5]))
        writer.write_timestamp("grp", ts_index=0, timestamp_val=ts)

        assert [(r.footprint.array_path, r.footprint.box) for r in seen] == [
            ("grp/img", ((0, 1), (0, 2), (0, 2))),
            ("grp/pub", ((1, 2), (0, 2), (0, 2))),
            ("grp/vec", ((1, 2),)),
            ("grp/lat", ((0, 2),)),
            ("grp/timestamp", ((0, 1),)),
        ]
        assert [r.tracked for r in seen] == [False] * 5
        assert all(r.units for r in seen)
        root = writer._open_root()
        np.testing.assert_array_equal(root["grp/vec"][...], [0.0, 5.0])
        np.testing.assert_array_equal(root["grp/lat"][...], [1.5, 2.5])
        assert root["grp/timestamp"][0] == ts

    @pytest.mark.parametrize("keep", [True, False])
    def test_write_empty_chunks_is_the_writers_not_the_process(self, store_path, keep):
        writer = RegionZarrWriter(store_path, write_empty_chunks=keep)
        writer.ensure_group(
            "grp/img", shape=(2, 2, 2), dtype=np.float32, fill_value=0.0, chunks=(1, 2, 2)
        )

        with zarr.config.set({"array.write_empty_chunks": not keep}):
            writer.write_region("grp", "img", 0, slice(0, 2), np.zeros((2, 2), dtype=np.float32))
            assert zarr.config.get("array.write_empty_chunks") is (not keep)

        assert _stored(writer._open_root()["grp/img"], (0, 0, 0)) is keep

    def test_track_units_without_on_write_is_refused(self, store_path):
        with pytest.raises(ValueError, match="on_write"):
            RegionZarrWriter(store_path, track_units=True)

    def test_results_reach_the_sink_in_write_order_when_a_sink_is_slow(self, store_path):
        """A slow sink cannot let a later write of the same unit be recorded first.

        A stores a value in the one chunk and its sink stalls; B then empties
        the chunk. B's result must reach the sink after A's, as the chunk is
        absent on disk, whatever the sinks' speed.
        """
        recorded: list[str] = []
        a_in_sink, release_a = threading.Event(), threading.Event()

        def sink(name: str):
            def record(_result: WriteResult) -> None:
                if name == "A":
                    a_in_sink.set()
                    assert release_a.wait(10)
                recorded.append(name)

            return record

        def writer_for(name: str) -> RegionZarrWriter:
            return RegionZarrWriter(
                store_path, write_empty_chunks=False, on_write=sink(name), track_units=True
            )

        writer_for("setup").ensure_group(
            "grp/img", shape=(2, 2, 2), dtype=np.float32, fill_value=0.0, chunks=(2, 2, 2)
        )
        errors: list[BaseException] = []

        def write(name: str, value: float) -> None:
            try:
                writer_for(name).write_region(
                    "grp", "img", 0, slice(0, 2), np.full((2, 2), value, dtype=np.float32)
                )
            except BaseException as exc:
                errors.append(exc)

        a = threading.Thread(target=write, args=("A", 1.0))
        a.start()
        assert a_in_sink.wait(10), "A never reached its sink"
        b = threading.Thread(target=write, args=("B", 0.0))
        b.start()
        b.join(1.0)  # B may finish here only if the sink runs outside the unit lock
        release_a.set()
        a.join(10)
        b.join(10)

        assert errors == []
        assert not _stored(_on_disk(store_path, "grp/img"), (0, 0, 0))
        assert recorded == ["A", "B"], "results reached the sink out of write order"


def _on_disk(store_path, path) -> zarr.Array:
    """The array at *path* read from the store now, not through any writer's handle."""
    return zarr.open_array(store=LocalStore(store_path), path=path, mode="r", zarr_format=3)


def _refusal(change) -> str:
    """The MetadataAuthorityError message *change* raised, or ``"not refused"``."""
    try:
        change()
    except MetadataAuthorityError as exc:
        return str(exc)
    return "not refused"


class TestMetadataAuthority:
    """Every zarr.json change reads the node under the writer's authority, never a stale handle."""

    def test_concurrent_growth_keeps_the_larger_shape(self, store_path, monkeypatch):
        RegionZarrWriter(store_path).ensure_group("g/t", shape=(1,), dtype="int32", fill_value=0)
        a_in_resize, b_in_resize, a_done = threading.Event(), threading.Event(), threading.Event()
        resize = zarr_array.Array.resize

        def paused(self, *args, **kwargs):
            if threading.current_thread().name == "A":
                a_in_resize.set()
                b_in_resize.wait(1)  # B gets here only without per-node exclusion
                try:
                    return resize(self, *args, **kwargs)
                finally:
                    a_done.set()
            b_in_resize.set()
            a_done.wait(5)  # B's resize, if it comes, lands after A's
            return resize(self, *args, **kwargs)

        monkeypatch.setattr(zarr_array.Array, "resize", paused)

        def grow(n):
            RegionZarrWriter(store_path).ensure_group(
                "g/t", shape=(n,), dtype="int32", fill_value=0, allow_grow=True
            )

        a = threading.Thread(target=grow, args=(6,), name="A")
        b = threading.Thread(target=grow, args=(3,), name="B")
        a.start()
        assert a_in_resize.wait(5)
        b.start()
        a.join(10)
        b.join(10)

        assert _on_disk(store_path, "g/t").shape == (6,)

    def test_array_creation_is_one_metadata_write(self, store_path, monkeypatch):
        follow_ups: list[list[str]] = []

        def crash(self, new_attributes):
            follow_ups.append(sorted(new_attributes))
            raise RuntimeError("simulated crash after the array's first metadata write")

        monkeypatch.setattr(zarr_array.Array, "update_attributes", crash)
        writer = RegionZarrWriter(store_path)
        with contextlib.suppress(RuntimeError):
            writer.ensure_group(
                "g/a",
                shape=(2,),
                dtype="int32",
                fill_value=7,
                attrs={"units": "K"},
                engine_attrs={"firecube_engine_owned": True},
            )

        assert dict(_on_disk(store_path, "g/a").attrs) == {
            "_FillValue": 7,
            "units": "K",
            "firecube_engine_owned": True,
        }
        assert follow_ups == []

    def test_no_stale_handle_is_written(self, store_path, monkeypatch):
        """A marker stamped while a growth is in flight survives it, and the growth survives it."""
        writer = RegionZarrWriter(store_path)
        writer.ensure_group("g/a", shape=(2,), dtype="int32", fill_value=0)
        in_resize, stamped = threading.Event(), threading.Event()
        resize = zarr_array.Array.resize

        def paused(self, *args, **kwargs):
            in_resize.set()
            stamped.wait(1)  # the stamp lands first only without per-node exclusion
            return resize(self, *args, **kwargs)

        monkeypatch.setattr(zarr_array.Array, "resize", paused)

        def stamp():
            assert in_resize.wait(5)
            writer.update_array_attrs("g/a", {"firecube_static_written": True})
            stamped.set()

        stamper = threading.Thread(target=stamp)
        stamper.start()
        writer.ensure_group("g/a", shape=(5,), dtype="int32", fill_value=0, allow_grow=True)
        stamper.join(10)

        stored = _on_disk(store_path, "g/a")
        assert stored.shape == (5,)
        assert dict(stored.attrs) == {"_FillValue": 0, "firecube_static_written": True}
        assert writer.update_array_attrs("g/a", {}).shape == (5,)  # the node as stored

    def test_default_authority_is_process_local_over_the_whole_store(self, tmp_path):
        writer = RegionZarrWriter(str(tmp_path / "fresh.zarr"))

        assert writer.authority.basis is AuthorityBasis.PROCESS_LOCAL
        assert writer.authority.scope.prefix == ""
        assert writer.authority.scope.store == str((tmp_path / "fresh.zarr").resolve())
        writer.ensure_group("g")  # creates the missing root under it
        assert (tmp_path / "fresh.zarr" / "zarr.json").is_file()

    @pytest.mark.parametrize("released", [True, False], ids=["released", "group-scoped"])
    def test_refused_authority_leaves_a_fresh_store_without_zarr_json(self, tmp_path, released):
        uri = str(tmp_path / "fresh.zarr")
        scope = RegionZarrWriter(uri).metadata_scope("" if released else "g")
        with hold_authority(scope, claim=None) as authority:
            writer = RegionZarrWriter(uri, authority=authority)
            if not released:  # only a store-wide authority may create the root group
                with pytest.raises(MetadataAuthorityError, match="outside the scope"):
                    writer.ensure_group("g/a", shape=(2,), dtype="int32", fill_value=0)
        if released:
            with pytest.raises(MetadataAuthorityError, match="was released"):
                writer.ensure_group("g/a", shape=(2,), dtype="int32", fill_value=0)
            with pytest.raises(MetadataAuthorityError, match="was released"):
                writer.set_group_attrs("g", {"title": "t"})

        assert list(tmp_path.rglob("zarr.json")) == []

    @pytest.mark.parametrize(
        ("call", "group"),
        [
            pytest.param(
                lambda w, auth: w.ensure_group(
                    "g/a", shape=(2,), dtype="int32", fill_value=0, authority=auth
                ),
                False,
                id="ensure_group-array",
            ),
            pytest.param(lambda w, auth: w.ensure_group("g", authority=auth), False, id="group"),
            pytest.param(lambda w, auth: w.ensure_group("", authority=auth), False, id="root"),
            pytest.param(
                lambda w, auth: w.set_group_attrs("g", {"title": "t"}, authority=auth),
                False,
                id="set_group_attrs",
            ),
            pytest.param(
                lambda w, auth: w.update_array_attrs("g/a", {"k": 1}, authority=auth),
                False,
                id="update_array_attrs",
            ),
            pytest.param(
                lambda w, auth: w.ensure_group(
                    "g/a", shape=(2,), dtype="int32", fill_value=0, authority=auth
                ),
                True,
                id="group-scoped-ensure_group-array",
            ),
            pytest.param(
                lambda w, auth: w.set_group_attrs("g", {"title": "t"}, authority=auth),
                True,
                id="group-scoped-set_group_attrs",
            ),
        ],
    )
    def test_refused_call_authority_leaves_a_fresh_store_without_zarr_json(
        self, tmp_path, call, group
    ):
        """A per-call authority is the one root creation runs under: refused, nothing is written."""
        uri = str(tmp_path / "fresh.zarr")
        writer = RegionZarrWriter(uri)  # its own authority is active and store-wide
        with hold_authority(writer.metadata_scope("g" if group else ""), claim=None) as auth:
            if group:  # active, but cannot create the root group
                assert "outside the scope" in _refusal(lambda: call(writer, auth))
        if not group:
            assert "was released" in _refusal(lambda: call(writer, auth))

        assert list(tmp_path.rglob("zarr.json")) == []

    def test_released_call_authority_is_refused_on_an_existing_root(self, store_path):
        writer = RegionZarrWriter(store_path)
        with hold_authority(writer.metadata_scope(), claim=None) as auth:
            pass

        assert "was released" in _refusal(lambda: writer.ensure_group("", authority=auth))


@pytest.mark.parametrize(
    "value",
    ["2500-01-01", "2262-04-11T23:47:16.854775808", np.datetime64("1600-01-01", "D")],
)
def test_legacy_timestamp_write_outside_ns_range_is_refused_and_array_unchanged(writer, value):
    arr = writer.ensure_group(
        "grp/timestamp",
        shape=(1,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(1,),
        dimension_names=("timestamp",),
    )
    arr[0] = np.datetime64("2024-01-01T00:00:00", "ns")

    with pytest.raises(ValueError, match=r"datetime64\[ns\] range"):
        writer.write_timestamp("grp", ts_index=0, timestamp_val=value)

    assert arr[0] == np.datetime64("2024-01-01T00:00:00", "ns")


def test_gregorian_coordinate_refuses_a_360_day_value_and_creates_nothing(writer):
    with pytest.raises(ValueError, match="360_day"):
        writer.write_timestamp("grp", ts_index=0, timestamp_val=cftime.Datetime360Day(2049, 2, 30))

    assert "grp/timestamp" not in writer._open_root()


_UNITS = "seconds since 2049-01-01 00:00:00"
_DAY_S = 86400
_INT64_FILL = np.iinfo(np.int64).min


def _encoded_timestamp(writer, slots, *, preallocated):
    arr = writer.ensure_group(
        "grp/timestamp",
        shape=(slots,),
        dtype=np.dtype("int64"),
        fill_value=_INT64_FILL,
        chunks=(slots,),
        attrs={"units": _UNITS, "calendar": "360_day"},
        dimension_names=("timestamp",),
    )
    if preallocated:
        arr[...] = np.arange(slots, dtype=np.int64) * _DAY_S
        arr.attrs[ATTR_PREALLOCATED] = True
    return arr


class TestWriteTimestampEncoded:
    """write_timestamp on an encoded (calendar) coordinate verifies the slot and never writes."""

    def test_matching_value_is_a_noop(self, writer):
        arr = _encoded_timestamp(writer, 10, preallocated=True)

        writer.write_timestamp("grp", ts_index=3, timestamp_val=cftime.Datetime360Day(2049, 1, 4))

        assert arr[...].tolist() == [i * _DAY_S for i in range(10)]

    def test_off_by_one_cadence_raises_schema_drift(self, writer):
        _encoded_timestamp(writer, 10, preallocated=True)

        with pytest.raises(SchemaDriftError, match="diverged") as excinfo:
            writer.write_timestamp(
                "grp", ts_index=3, timestamp_val=cftime.Datetime360Day(2049, 1, 5)
            )

        assert "360_day" in str(excinfo.value)

    def test_unwritten_slot_reports_preallocation_incomplete_not_drift(self, writer):
        arr = _encoded_timestamp(writer, 3, preallocated=True)
        arr[1] = _INT64_FILL

        with pytest.raises(SchemaDriftError, match="preallocation incomplete") as excinfo:
            writer.write_timestamp(
                "grp", ts_index=1, timestamp_val=cftime.Datetime360Day(2049, 1, 2)
            )

        message = str(excinfo.value)
        assert "grp/timestamp" in message
        assert "360_day" in message

    def test_unpreallocated_encoded_array_is_refused_and_left_unwritten(self, writer):
        arr = _encoded_timestamp(writer, 3, preallocated=False)

        with pytest.raises(SchemaDriftError) as excinfo:
            writer.write_timestamp(
                "grp", ts_index=0, timestamp_val=cftime.Datetime360Day(2049, 1, 1)
            )

        message = str(excinfo.value)
        assert "grp/timestamp" in message
        assert _UNITS in message
        assert int(arr[0]) == _INT64_FILL


_K = {"dtype": np.float32, "fill_value": np.nan, "chunks": (4, 4, 4)}


@pytest.mark.parametrize(
    ("described", "noop_attrs", "refused"),
    [
        pytest.param("stale", False, True, id="before-growth"),
        pytest.param("stale", True, True, id="before-growth-set_group_attrs-between"),
        pytest.param("other", False, True, id="other-stored-unit-grid"),
        pytest.param("data", False, False, id="after-growth"),
    ],
)
def test_publish_unit_judges_its_description_against_the_grown_array(
    store_path, described, noop_attrs, refused
):
    w = RegionZarrWriter(store_path)
    w.ensure_group("grp/data", shape=(9, 4, 4), **_K)
    w.ensure_group("grp/other", shape=(12, 4, 4), **{**_K, "chunks": (4, 8, 4)})
    w.write_region("grp", "data", 0, slice(None), np.ones((4, 4), np.float32))  # warms the handle
    stale = w.describe_array("grp", "data")
    grower = RegionZarrWriter(store_path)
    grower.ensure_group("grp/data", shape=(12, 4, 4), allow_grow=True, **_K)
    slots = np.arange(8, 12, dtype=np.float32)[:, None, None] * np.ones((4, 4, 4), np.float32)
    grower.write_box("grp", "data", ((8, 12), (0, 4), (0, 4)), slots)
    if noop_attrs:
        w.set_group_attrs("grp", {})
    description = stale if described == "stale" else w.describe_array("grp", described)
    store = MemoryStore()
    zarr.create_array(store, shape=(4, 4, 4), **_K)[...] = slots + 100
    unit = sync(store.get("c/0/0/0", default_buffer_prototype()))
    assert unit is not None
    payload = unit.to_bytes()

    with pytest.raises(ValueError) if refused else contextlib.nullcontext():
        w.publish_unit("grp", "data", (2, 0, 0), payload, description=description)

    expected = [8.0, 9.0, 10.0, 11.0] if refused else [108.0, 109.0, 110.0, 111.0]
    assert np.asarray(_on_disk(store_path, "grp/data"))[8:12, 0, 0].tolist() == expected


def _record(monkeypatch, method, wanted):
    """Keys of the `LocalStore.<method>` calls whose key satisfies *wanted*."""
    keys, original = [], getattr(LocalStore, method)

    async def recording(self, key, *args, **kw):
        keys.extend(k for k in (key,) if wanted(k))
        return await original(self, key, *args, **kw)

    monkeypatch.setattr(LocalStore, method, recording)
    return keys


def test_fixed_extent_publish_unit_reads_no_metadata_after_its_description(store_path, monkeypatch):
    RegionZarrWriter(store_path).ensure_group("grp/data", shape=(9, 4, 4), **_K)
    w = RegionZarrWriter(store_path, fixed_extent=True)
    description = w.describe_array("grp", "data")
    reads = _record(monkeypatch, "get", lambda key: key.endswith("zarr.json"))

    for unit in ((0, 0, 0), (1, 0, 0), (2, 0, 0)):
        w.publish_unit("grp", "data", unit, None, description=description)

    assert reads == []


@pytest.mark.parametrize(
    ("box", "data_shape", "stored_units"),
    [
        pytest.param(((0, 1), (0, 4), (0, 4)), (1, 4, 4), [(0, 0, 0)], id="inside-one-unit"),
        pytest.param(((0, 1), (1, 3), (2, 6)), (1, 2, 4), [(0, 0, 0), (0, 0, 1)], id="two-units"),
        pytest.param(((0, 1), (0, 4), (0, 4)), (1, 4, 3), [], id="other-shape-refused"),
    ],
)
def test_write_box_changes_only_the_cells_and_units_of_its_box(
    store_path, monkeypatch, box, data_shape, stored_units
):
    w = RegionZarrWriter(store_path)
    w.ensure_group("grp/data", shape=(1, 4, 8), **{**_K, "chunks": (1, 4, 4)})
    w.write_region("grp", "data", 0, slice(None), np.ones((4, 8), dtype=np.float32))
    sets = _record(monkeypatch, "set", lambda key: True)
    refusal = pytest.raises(ValueError, match="does not match")
    with contextlib.nullcontext() if stored_units else refusal:
        w.write_box("grp", "data", box, np.full(data_shape, 7.0, dtype=np.float32))

    expected = np.ones((1, 4, 8), dtype=np.float32)
    if stored_units:
        expected[tuple(slice(start, stop) for start, stop in box)] = 7.0
    stored = _on_disk(store_path, "grp/data")
    np.testing.assert_array_equal(stored[...], expected)
    assert sorted(sets) == sorted(unit_store_key(stored, unit) for unit in stored_units)
