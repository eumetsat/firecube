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

from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
import zarr

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.core.zarr.coord_materialization import (
    build_irregular_coord_attrs,
    materialize_irregular_coord_array,
    values_all_nat,
)
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec

_BASE = np.datetime64("2026-01-01T00:00:00", "ns")
_STEP = np.timedelta64(600, "s").astype("timedelta64[ns]")


@pytest.fixture()
def writer_and_root(tmp_path: Any) -> tuple[RegionZarrWriter, Any]:
    store_path = tmp_path / "cube.zarr"
    zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    writer = RegionZarrWriter(str(store_path))
    return writer, writer._open_root()


def axis(slot_count: int, coordinate: str = "timestamp") -> SimpleNamespace:
    values = tuple(_BASE + i * _STEP for i in range(slot_count))
    return SimpleNamespace(coordinate=coordinate, values=values)


def expected_values(slot_count: int) -> np.ndarray:
    return np.asarray([_BASE + i * _STEP for i in range(slot_count)], dtype="datetime64[ns]")


def coord_spec(
    *,
    chunks: tuple[int, ...] | None = None,
    dtype: Any = None,
    dimension_names: tuple[str, ...] | None = ("timestamp",),
    attrs: dict[str, Any] | None = None,
) -> ZarrArraySpec:
    default_attrs: dict[str, Any] = {
        "long_name": "slot time",
        "units": "seconds since 1970-01-01",
        "calendar": "proleptic_gregorian",
    }
    return ZarrArraySpec(
        name="timestamp",
        shape=(0,),
        dtype=dtype if dtype is not None else np.dtype("datetime64[ns]"),
        chunks=chunks,
        fill_value=np.datetime64("NaT", "ns"),
        attrs=attrs if attrs is not None else default_attrs,
        dimension_names=dimension_names,
    )


def test_fresh_creation_stamps_marker_backward_compat(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root

    materialize_irregular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(5),
    )

    arr = root["data/timestamp"]
    assert arr.shape == (5,)
    assert arr.dtype == np.dtype("datetime64[ns]")
    assert tuple(arr.chunks) == (5,)
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert arr.attrs["standard_name"] == "time"
    assert arr.attrs["axis"] == "T"
    assert np.array_equal(arr[:], expected_values(5))


def test_fresh_creation_with_spec_merges_attrs_and_honors_chunks(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root

    materialize_irregular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(1000),
        spec=coord_spec(chunks=(128,)),
    )

    arr = root["data/timestamp"]
    assert arr.shape == (1000,)
    assert tuple(arr.chunks) == (128,)
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert arr.attrs["long_name"] == "slot time"
    assert arr.attrs["standard_name"] == "time"
    assert arr.attrs["axis"] == "T"
    assert "units" not in arr.attrs
    assert "calendar" not in arr.attrs


def test_fill_and_stamp_existing_nat_array(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root
    writer.ensure_group(
        "data/timestamp",
        shape=(5,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(1,),
        attrs={"existing": "kept"},
        dimension_names=("timestamp",),
    )

    materialize_irregular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(5),
        spec=coord_spec(chunks=(1,)),
    )

    arr = root["data/timestamp"]
    assert arr.shape == (5,)
    assert np.array_equal(arr[:], expected_values(5))
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert arr.attrs["existing"] == "kept"
    assert arr.attrs["long_name"] == "slot time"
    assert "units" not in arr.attrs
    assert "calendar" not in arr.attrs


def test_idempotent_replay_matches_and_stamps_marker_if_absent(
    writer_and_root: tuple[RegionZarrWriter, Any],
    capsys: pytest.CaptureFixture[str],
) -> None:
    writer, root = writer_and_root

    materialize_irregular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(5),
        report=print,
    )
    arr = root["data/timestamp"]
    del arr.attrs[ATTR_PREALLOCATED]

    materialize_irregular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(5),
        report=print,
    )

    captured = capsys.readouterr()
    assert "existing irregular coord array matches; no-op" in captured.out
    arr = root["data/timestamp"]
    assert arr.attrs[ATTR_PREALLOCATED] is True


def test_idempotent_replay_when_marker_already_stamped(
    writer_and_root: tuple[RegionZarrWriter, Any],
    capsys: pytest.CaptureFixture[str],
) -> None:
    writer, root = writer_and_root

    materialize_irregular_coord_array(
        writer=writer, root=root, group_name="data", axis=axis(5), report=print
    )
    materialize_irregular_coord_array(
        writer=writer, root=root, group_name="data", axis=axis(5), report=print
    )

    captured = capsys.readouterr()
    assert captured.out.count("existing irregular coord array matches; no-op") == 1
    arr = root["data/timestamp"]
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert np.array_equal(arr[:], expected_values(5))


def test_drift_error_when_existing_values_differ_and_not_nat(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root
    writer.ensure_group(
        "data/timestamp",
        shape=(5,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(5,),
        dimension_names=("timestamp",),
    )
    arr = root["data/timestamp"]
    arr[...] = np.asarray([_BASE + (i + 100) * _STEP for i in range(5)], dtype="datetime64[ns]")

    with pytest.raises(SchemaDriftError, match="values that differ"):
        materialize_irregular_coord_array(writer=writer, root=root, group_name="data", axis=axis(5))


def test_schema_mismatch_shape_raises(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root
    writer.ensure_group(
        "data/timestamp",
        shape=(4,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(4,),
        dimension_names=("timestamp",),
    )

    with pytest.raises(SchemaDriftError, match="shape: expected"):
        materialize_irregular_coord_array(writer=writer, root=root, group_name="data", axis=axis(5))


def test_spec_dimension_names_honored(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root

    materialize_irregular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(5),
        spec=coord_spec(dimension_names=("t",)),
    )

    arr = root["data/timestamp"]
    assert tuple(arr.metadata.dimension_names) == ("t",)


def testbuild_irregular_coord_attrs_minimal_when_spec_none() -> None:
    assert build_irregular_coord_attrs(None, axis(1)) == {
        "standard_name": "time",
        "axis": "T",
    }


def testbuild_irregular_coord_attrs_strips_reserved_and_cf_encoding_keys() -> None:
    spec = coord_spec(
        attrs={
            "long_name": "slot time",
            "units": "seconds since 1970-01-01",
            "calendar": "proleptic_gregorian",
            "extra": 42,
        }
    )
    result = build_irregular_coord_attrs(spec, axis(1))
    assert result == {
        "standard_name": "time",
        "axis": "T",
        "long_name": "slot time",
        "extra": 42,
    }


def testvalues_all_nat_true_for_all_nat_array() -> None:
    arr = np.array([np.datetime64("NaT", "ns"), np.datetime64("NaT", "ns")], dtype="datetime64[ns]")
    assert values_all_nat(arr) is True


def testvalues_all_nat_false_for_mixed_array() -> None:
    arr = np.array(
        [np.datetime64("NaT", "ns"), np.datetime64("2026-01-01", "ns")],
        dtype="datetime64[ns]",
    )
    assert values_all_nat(arr) is False


def testvalues_all_nat_false_for_non_datetime_dtype() -> None:
    arr = np.zeros(3, dtype=np.int64)
    assert values_all_nat(arr) is False
