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

"""Behavioral tests for calendar-declared (non-Gregorian) coordinate materialization.

Covers the encoded branches of ``materialize_regular_coord_array`` and
``materialize_irregular_coord_array``: real Zarr stores under ``tmp_path``,
real ``cftime``-decoded values (via ``decode_time_array``), no mocks of
firecube internals.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
import zarr

from firecube.core.errors import ConfigurationError, SchemaDriftError
from firecube.core.index_spec import IrregularTimeAxis, RegularTimeAxis
from firecube.core.zarr._reserved_attrs import FIRECUBE_GROUP_IDENTITY_HASH_ATTR
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.core.zarr.coord_materialization import (
    _array_is_all_fill,
    _scalar_is_fill,
    materialize_irregular_coord_array,
    materialize_regular_coord_array,
    values_all_nat,
)
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.core.zarr.time_decode import decode_time_array
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec

pytestmark = pytest.mark.unit


@pytest.fixture()
def writer_and_root(tmp_path: Any) -> tuple[RegionZarrWriter, Any]:
    store_path = tmp_path / "cube.zarr"
    zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    writer = RegionZarrWriter(str(store_path))
    return writer, writer._open_root()


# ---------------------------------------------------------------------------
# Regular calendar axis: fresh creation
# ---------------------------------------------------------------------------


class TestMaterializeRegularCalendarCoordArray:
    @pytest.mark.parametrize("calendar", ["360_day", "noleap"])
    def test_fresh_creation_is_encoded_int64(
        self, writer_and_root: tuple[RegionZarrWriter, Any], calendar: str
    ) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar=calendar,
        )

        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        assert arr.dtype == np.dtype("int64")
        assert np.array_equal(arr[:], np.arange(90, dtype=np.int64) * 86400)
        assert arr.fill_value == np.iinfo(np.int64).min
        assert arr.attrs[ATTR_PREALLOCATED] is True
        assert arr.attrs["units"] == "seconds since 2049-01-01 00:00:00"
        assert arr.attrs["calendar"] == calendar
        assert arr.attrs["standard_name"] == "time"
        assert arr.attrs["axis"] == "T"
        assert FIRECUBE_GROUP_IDENTITY_HASH_ATTR in arr.attrs

    def test_360_day_decodes_february_30(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=90,
            calendar="360_day",
        )
        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        decoded = decode_time_array(np.asarray(arr[:]), dict(arr.attrs))
        assert decoded.dtype.kind == "O"
        assert decoded[0].isoformat().startswith("2049-01-01")
        assert decoded[59].isoformat().startswith("2049-02-30")

    def test_noleap_decodes_no_february_29(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=60,
            calendar="noleap",
        )
        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        decoded = decode_time_array(np.asarray(arr[:]), dict(arr.attrs))
        # noleap: Jan has 31 days (indices 0..30), so index 31 is Feb 1.
        assert decoded[31].isoformat().startswith("2049-02-01")
        isoformats = [item.isoformat() for item in decoded]
        assert not any(iso.startswith("2049-02-29") for iso in isoformats)

    def test_idempotent_rerun_is_noop(self, writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            calendar="360_day",
        )
        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )
        first_values = np.asarray(root["data/time"][:]).copy()

        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        assert np.array_equal(arr[:], first_values)
        assert arr.attrs[ATTR_PREALLOCATED] is True

    def test_fresh_all_fill_shell_gets_filled(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        writer.ensure_group(
            "data/time",
            shape=(10,),
            dtype=np.int64,
            fill_value=np.iinfo(np.int64).min,
            chunks=(10,),
            dimension_names=("time",),
        )
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            calendar="360_day",
        )

        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        assert np.array_equal(arr[:], np.arange(10, dtype=np.int64) * 86400)
        assert arr.attrs[ATTR_PREALLOCATED] is True

    def test_existing_different_values_raise_drift(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        writer.ensure_group(
            "data/time",
            shape=(10,),
            dtype=np.int64,
            fill_value=np.iinfo(np.int64).min,
            chunks=(10,),
            dimension_names=("time",),
        )
        root["data/time"][3] = 999_999
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            calendar="360_day",
        )

        with pytest.raises(SchemaDriftError, match="diverged from nominal grid"):
            materialize_regular_coord_array(
                writer=writer, root=root, group_name="data", axis=axis, spec=None
            )

    def test_datetime64_spec_dtype_contradicts_calendar_axis(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            calendar="360_day",
        )
        spec = ZarrArraySpec(name="time", shape=(0,), dtype=np.dtype("datetime64[ns]"))

        with pytest.raises(ConfigurationError, match="datetime64"):
            materialize_regular_coord_array(
                writer=writer, root=root, group_name="data", axis=axis, spec=spec
            )

    def test_spec_attrs_calendar_contradicts_axis_calendar(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            calendar="360_day",
        )
        spec = ZarrArraySpec(name="time", shape=(0,), dtype=np.int64, attrs={"calendar": "noleap"})

        with pytest.raises(ConfigurationError, match="contradicts"):
            materialize_regular_coord_array(
                writer=writer, root=root, group_name="data", axis=axis, spec=spec
            )

    def test_spec_attrs_equal_units_and_calendar_are_fine(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            calendar="360_day",
        )
        spec = ZarrArraySpec(
            name="time",
            shape=(0,),
            dtype=np.int64,
            attrs={
                "units": "seconds since 2049-01-01 00:00:00",
                "calendar": "360_day",
            },
        )

        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=spec
        )

        arr = root["data/time"]
        assert arr.attrs["calendar"] == "360_day"

    def test_spec_may_widen_to_float64(self, writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
        writer, root = writer_and_root
        axis = RegularTimeAxis(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=5,
            calendar="360_day",
        )
        spec = ZarrArraySpec(name="time", shape=(0,), dtype=np.float64)

        materialize_regular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=spec
        )

        arr = root["data/time"]
        assert arr.dtype == np.dtype("float64")
        assert np.array_equal(arr[:], np.arange(5, dtype=np.float64) * 86400)

    def test_calendar_axis_with_floor_policy_guard_is_unreachable(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        """`RegularTimeAxis` construction rejects mode='floor' with calendar set; this
        proves the materializer's own defense-in-depth guard also fires if that
        invariant is ever bypassed (e.g. a hand-built axis-like object)."""
        writer, root = writer_and_root
        fake_axis = SimpleNamespace(
            coordinate="time",
            epoch="2049-01-01T00:00:00Z",
            cadence_s=86400,
            slot_count=10,
            mode="floor",
            calendar="360_day",
        )

        with pytest.raises(ConfigurationError, match="only support mode='exact'"):
            materialize_regular_coord_array(
                writer=writer, root=root, group_name="data", axis=fake_axis, spec=None
            )


# ---------------------------------------------------------------------------
# Irregular calendar axis
# ---------------------------------------------------------------------------


class TestMaterializeIrregularCalendarCoordArray:
    def test_half_day_explicit_values_are_float64_with_nan_fill(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[71640.5, 71699.5, 71729.5],
            calendar="360_day",
            units="days since 1850-01-01",
        )

        materialize_irregular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        assert arr.dtype == np.dtype("float64")
        assert np.array_equal(arr[:], np.array([71640.5, 71699.5, 71729.5]))
        assert np.isnan(arr.fill_value)
        assert arr.attrs["units"] == "days since 1850-01-01"
        assert arr.attrs["calendar"] == "360_day"
        assert arr.attrs[ATTR_PREALLOCATED] is True

    def test_idempotent_rerun_is_noop(self, writer_and_root: tuple[RegionZarrWriter, Any]) -> None:
        writer, root = writer_and_root
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[71640.5, 71699.5],
            calendar="360_day",
            units="days since 1850-01-01",
        )
        materialize_irregular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )
        materialize_irregular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )
        arr = root["data/time"]
        assert np.array_equal(arr[:], np.array([71640.5, 71699.5]))

    def test_fresh_all_fill_shell_gets_filled(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        writer.ensure_group(
            "data/time",
            shape=(2,),
            dtype=np.float64,
            fill_value=float("nan"),
            chunks=(2,),
            dimension_names=("time",),
        )
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[71640.5, 71699.5],
            calendar="360_day",
            units="days since 1850-01-01",
        )

        materialize_irregular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        assert np.array_equal(arr[:], np.array([71640.5, 71699.5]))
        assert arr.attrs[ATTR_PREALLOCATED] is True

    def test_existing_different_values_raise_drift(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        writer.ensure_group(
            "data/time",
            shape=(2,),
            dtype=np.float64,
            fill_value=float("nan"),
            chunks=(2,),
            dimension_names=("time",),
        )
        root["data/time"][:] = np.array([1.0, 2.0])
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[71640.5, 71699.5],
            calendar="360_day",
            units="days since 1850-01-01",
        )

        with pytest.raises(SchemaDriftError, match="differ from the"):
            materialize_irregular_coord_array(
                writer=writer, root=root, group_name="data", axis=axis, spec=None
            )

    def test_int64_spec_dtype_with_fractional_values_is_configuration_error(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[71640.5, 71699.5],
            calendar="360_day",
            units="days since 1850-01-01",
        )
        spec = ZarrArraySpec(name="time", shape=(0,), dtype=np.int64)

        with pytest.raises(ConfigurationError, match="not all integral"):
            materialize_irregular_coord_array(
                writer=writer, root=root, group_name="data", axis=axis, spec=spec
            )

    def test_spec_attrs_units_contradicts_axis_units(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[0, 1, 2],
            calendar="360_day",
            units="days since 1850-01-01",
        )
        spec = ZarrArraySpec(
            name="time", shape=(0,), dtype=np.int64, attrs={"units": "days since 1900-01-01"}
        )

        with pytest.raises(ConfigurationError, match="contradicts"):
            materialize_irregular_coord_array(
                writer=writer, root=root, group_name="data", axis=axis, spec=spec
            )

    def test_all_integral_values_default_to_int64(
        self, writer_and_root: tuple[RegionZarrWriter, Any]
    ) -> None:
        writer, root = writer_and_root
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[0, 1, 2],
            calendar="360_day",
            units="days since 1850-01-01",
        )

        materialize_irregular_coord_array(
            writer=writer, root=root, group_name="data", axis=axis, spec=None
        )

        arr = root["data/time"]
        assert arr.dtype == np.dtype("int64")
        assert arr.fill_value == np.iinfo(np.int64).min


# ---------------------------------------------------------------------------
# is-all-fill predicate: generalises `values_all_nat` without changing it.
# ---------------------------------------------------------------------------


class TestIsAllFillPredicate:
    def test_values_all_nat_unchanged_for_datetime64(self) -> None:
        all_nat = np.array(
            [np.datetime64("NaT", "ns"), np.datetime64("NaT", "ns")], dtype="datetime64[ns]"
        )
        mixed = np.array(
            [np.datetime64("NaT", "ns"), np.datetime64("2026-01-01", "ns")],
            dtype="datetime64[ns]",
        )
        non_datetime = np.zeros(3, dtype=np.int64)

        assert values_all_nat(all_nat) is True
        assert values_all_nat(mixed) is False
        assert values_all_nat(non_datetime) is False

    def test_array_is_all_fill_recognises_int64_min(self) -> None:
        fill = np.iinfo(np.int64).min
        all_fill = np.full(3, fill, dtype=np.int64)
        mixed = np.array([fill, fill, 42], dtype=np.int64)

        assert _array_is_all_fill(all_fill, fill) is True
        assert _array_is_all_fill(mixed, fill) is False

    def test_array_is_all_fill_recognises_nan(self) -> None:
        all_nan = np.full(3, np.nan, dtype=np.float64)
        mixed = np.array([np.nan, 1.5, np.nan], dtype=np.float64)

        assert _array_is_all_fill(all_nan, np.nan) is True
        assert _array_is_all_fill(mixed, np.nan) is False

    def test_scalar_is_fill_int_and_float(self) -> None:
        assert _scalar_is_fill(np.iinfo(np.int64).min, np.iinfo(np.int64).min) is True
        assert _scalar_is_fill(np.int64(42), np.iinfo(np.int64).min) is False
        assert _scalar_is_fill(np.float64("nan"), np.nan) is True
        assert _scalar_is_fill(np.float64(1.5), np.nan) is False
