"""Tests for marker-aware ``RegionZarrWriter.write_timestamp`` behavior."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriter


def _store_files(store_path: Path) -> dict[str, bytes]:
    return {
        path.relative_to(store_path).as_posix(): path.read_bytes()
        for path in store_path.rglob("*")
        if path.is_file()
    }


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    store_dir = tmp_path / "cube.zarr"
    zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    return store_dir


@pytest.fixture()
def writer(store_path: Path) -> RegionZarrWriter:
    return RegionZarrWriter(str(store_path))


def _preallocate_timestamp(
    writer: RegionZarrWriter,
    group: str,
    values: list[np.datetime64],
    *,
    dtype: str = "datetime64[ns]",
) -> None:
    arr = writer.ensure_group(
        f"{group}/timestamp",
        shape=(len(values),),
        dtype=np.dtype(dtype),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(len(values),),
        dimension_names=("timestamp",),
    )
    arr[:] = np.asarray(values, dtype=np.dtype(dtype))
    arr.attrs[ATTR_PREALLOCATED] = True


def test_marker_absent_legacy_write_proceeds(writer: RegionZarrWriter) -> None:
    timestamp = np.datetime64("2026-01-01T00:00:00", "s")

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp)

    arr = writer._open_root()["data_1km/timestamp"]
    assert np.asarray(arr[0]) == timestamp


def test_marker_present_match_noop(store_path: Path, writer: RegionZarrWriter) -> None:
    timestamp = np.datetime64("2026-01-01T00:00:00", "ns")
    _preallocate_timestamp(writer, "data_1km", [timestamp])
    before = _store_files(store_path)

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp)

    assert _store_files(store_path) == before


def test_marker_present_mismatch_raises(writer: RegionZarrWriter) -> None:
    current = np.datetime64("2026-01-01T00:00:00", "ns")
    incoming = np.datetime64("2026-01-01T00:05:00", "ns")
    _preallocate_timestamp(writer, "data_1km", [current])

    with pytest.raises(SchemaDriftError, match=r"data_1km.*slot 0"):
        writer.write_timestamp("data_1km", ts_index=0, timestamp_val=incoming)


def test_marker_present_both_nat_noop(store_path: Path, writer: RegionZarrWriter) -> None:
    _preallocate_timestamp(writer, "data_1km", [np.datetime64("NaT", "ns")])
    before = _store_files(store_path)

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=np.datetime64("NaT", "s"))

    assert _store_files(store_path) == before


def test_dtype_normalization_treats_seconds_and_nanoseconds_as_equal(
    store_path: Path,
    writer: RegionZarrWriter,
) -> None:
    timestamp_ns = np.datetime64("2026-01-01T00:10:00", "ns")
    timestamp_s = np.datetime64("2026-01-01T00:10:00", "s")
    _preallocate_timestamp(writer, "data_1km", [timestamp_ns])
    before = _store_files(store_path)

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp_s)

    assert _store_files(store_path) == before


def test_drift_error_message_fields(writer: RegionZarrWriter) -> None:
    current = np.datetime64("2026-01-01T00:00:00", "ns")
    incoming = np.datetime64("2026-01-01T00:05:00", "ns")
    _preallocate_timestamp(writer, "data_1km", [current])

    with pytest.raises(SchemaDriftError) as exc_info:
        writer.write_timestamp("data_1km", ts_index=0, timestamp_val=incoming)

    message = str(exc_info.value)
    assert "data_1km" in message
    assert "slot 0" in message
    assert "2026-01-01T00:00:00" in message
    assert "2026-01-01T00:05:00" in message
