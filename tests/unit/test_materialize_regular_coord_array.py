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
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.coord_materialization import materialize_regular_coord_array
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec


@pytest.fixture()
def writer_and_root(tmp_path: Any) -> tuple[RegionZarrWriter, Any]:
    store_path = tmp_path / "cube.zarr"
    zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    writer = RegionZarrWriter(str(store_path))
    return writer, writer._open_root()


def axis(
    slot_count: int | None,
    *,
    mode: str = "exact",
) -> SimpleNamespace:
    return SimpleNamespace(
        coordinate="time",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        mode=mode,
        slot_count=slot_count,
    )


def expected_values(slot_count: int) -> np.ndarray:
    epoch = np.datetime64("2024-01-01T00:00:00", "ns")
    cadence = np.timedelta64(600, "s")
    return epoch + np.arange(slot_count, dtype=np.int64) * cadence


def coord_spec(*, chunks: tuple[int, ...] | None = None) -> ZarrArraySpec:
    return ZarrArraySpec(
        name="time",
        shape=(0,),
        dtype=np.dtype("datetime64[ns]"),
        chunks=chunks,
        fill_value=np.datetime64("NaT", "ns"),
        attrs={
            "long_name": "slot time",
            "units": "seconds since 1970-01-01",
            "calendar": "proleptic_gregorian",
        },
        dimension_names=("time",),
    )


def test_fresh_creation_happy_path(writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
    writer, root = writer_and_root

    materialize_regular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(100),
        spec=None,
    )

    arr = root["data/time"]
    assert arr.shape == (100,)
    assert tuple(arr.chunks) == (100,)
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert arr.attrs["standard_name"] == "time"
    assert arr.attrs["axis"] == "T"
    assert "units" not in arr.attrs
    assert "calendar" not in arr.attrs
    assert np.array_equal(arr[:], expected_values(100))
    assert np.all(np.diff(arr[:].astype("datetime64[ns]").astype(np.int64)) > 0)


def test_fill_and_stamp_existing_array(writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
    writer, root = writer_and_root
    writer.ensure_group(
        "data/time",
        shape=(100,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(1,),
        attrs={"existing": "kept"},
        dimension_names=("time",),
    )

    materialize_regular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(100),
        spec=coord_spec(chunks=(1,)),
    )

    arr = root["data/time"]
    assert arr.shape == (100,)
    assert tuple(arr.chunks) == (1,)
    assert np.array_equal(arr[:], expected_values(100))
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert arr.attrs["existing"] == "kept"
    assert arr.attrs["long_name"] == "slot time"
    assert "units" not in arr.attrs
    assert "calendar" not in arr.attrs


def test_slot_count_none_raises(writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
    writer, root = writer_and_root

    with pytest.raises(ValueError, match=r"(?=.*slot_count)(?=.*index_spec)"):
        materialize_regular_coord_array(
            writer=writer,
            root=root,
            group_name="data",
            axis=axis(None),
            spec=None,
        )


@pytest.mark.parametrize(
    ("slot_count", "expected_chunks"),
    [
        (0, (1,)),
        (1, (1,)),
        (256, (256,)),
        (257, (256,)),
        (4320, (256,)),
    ],
)
def test_boundary_sizes(
    writer_and_root: tuple[RegionZarrWriter, Any],
    slot_count: int,
    expected_chunks: tuple[int, ...],
) -> None:
    writer, root = writer_and_root

    materialize_regular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(slot_count),
        spec=None,
    )

    arr = root["data/time"]
    assert arr.shape == (slot_count,)
    assert tuple(arr.chunks) == expected_chunks
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert np.array_equal(arr[:], expected_values(slot_count))


def test_spec_chunks_honored(writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
    writer, root = writer_and_root

    materialize_regular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(1000),
        spec=coord_spec(chunks=(512,)),
    )

    arr = root["data/time"]
    assert arr.shape == (1000,)
    assert tuple(arr.chunks) == (512,)
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert "units" not in arr.attrs
    assert "calendar" not in arr.attrs


def test_floor_mode_fresh_creation_is_unsealed_nat(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root

    materialize_regular_coord_array(
        writer=writer,
        root=root,
        group_name="data",
        axis=axis(100, mode="floor"),
        spec=None,
    )

    arr = root["data/time"]
    assert arr.shape == (100,)
    assert tuple(arr.chunks) == (100,)
    assert arr.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in arr.attrs
    assert arr.attrs["standard_name"] == "time"
    assert arr.attrs["axis"] == "T"
    assert np.all(np.isnat(arr[:]))


def test_floor_mode_existing_unmarked_array_refuses_legacy_shell(
    writer_and_root: tuple[RegionZarrWriter, Any],
) -> None:
    writer, root = writer_and_root
    writer.ensure_group(
        "data/time",
        shape=(100,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(100,),
        attrs={"existing": "kept"},
        dimension_names=("time",),
    )
    ingested = np.datetime64("2024-01-01T00:00:02", "ns")
    root["data/time"][10] = ingested

    with pytest.raises(SchemaDriftError, match=r"legacy.*firecube chunks"):
        materialize_regular_coord_array(
            writer=writer,
            root=root,
            group_name="data",
            axis=axis(100, mode="floor"),
            spec=coord_spec(chunks=(100,)),
        )

    arr = root["data/time"]
    assert ATTR_COORD_MANAGED not in arr.attrs
    assert ATTR_PREALLOCATED not in arr.attrs
    assert arr.attrs["existing"] == "kept"
    assert arr[10] == ingested, "floor-mode rerun must not clobber ingested values"
    assert np.all(np.isnat(np.delete(arr[:], 10)))


def test_existing_shape_mismatch_raises(writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
    writer, root = writer_and_root
    writer.ensure_group(
        "data/time",
        shape=(99,),
        dtype=np.dtype("datetime64[ns]"),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(1,),
        dimension_names=("time",),
    )

    with pytest.raises(ValueError, match=r"shape.*expected.*Refuse to resize silently"):
        materialize_regular_coord_array(
            writer=writer,
            root=root,
            group_name="data",
            axis=axis(100),
            spec=coord_spec(chunks=(1,)),
        )
