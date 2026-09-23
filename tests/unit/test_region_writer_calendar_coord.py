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

"""Behavioral tests for ``RegionZarrWriter`` on encoded (calendar) time coordinates.

Real Zarr stores under ``tmp_path``, real ``cftime`` values -- no mocks of
firecube internals.
"""

from __future__ import annotations

from pathlib import Path

import cftime
import numpy as np
import pytest
import zarr

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.unit

_UNITS = "seconds since 2049-01-01 00:00:00"
_CADENCE_S = 86400


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    store_dir = tmp_path / "cube.zarr"
    zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    return store_dir


@pytest.fixture()
def writer(store_path: Path) -> RegionZarrWriter:
    return RegionZarrWriter(str(store_path))


def _preallocate_encoded_timestamp(
    writer: RegionZarrWriter,
    group: str,
    slot_count: int,
    *,
    calendar: str = "360_day",
    units: str = _UNITS,
    cadence_s: int = _CADENCE_S,
    dtype: str = "int64",
) -> None:
    values = np.arange(slot_count, dtype=np.int64) * cadence_s
    fill_value = np.iinfo(np.int64).min if dtype == "int64" else float("nan")
    arr = writer.ensure_group(
        f"{group}/timestamp",
        shape=(slot_count,),
        dtype=np.dtype(dtype),
        fill_value=fill_value,
        chunks=(slot_count,),
        attrs={"standard_name": "time", "axis": "T", "units": units, "calendar": calendar},
        dimension_names=("timestamp",),
    )
    arr[...] = values.astype(dtype)
    arr.attrs[ATTR_PREALLOCATED] = True


class TestWriteTimestampEncoded:
    def test_matching_cftime_value_is_ok(self, writer: RegionZarrWriter) -> None:
        _preallocate_encoded_timestamp(writer, "grp", 10)
        value = cftime.Datetime360Day(2049, 1, 4)  # slot 3: 3 * 86400s

        writer.write_timestamp("grp", ts_index=3, timestamp_val=value)  # must not raise

        arr = writer._open_root()["grp/timestamp"]
        assert int(arr[3]) == 3 * _CADENCE_S

    def test_matching_already_encoded_number_is_ok(self, writer: RegionZarrWriter) -> None:
        _preallocate_encoded_timestamp(writer, "grp", 10)

        writer.write_timestamp("grp", ts_index=5, timestamp_val=5 * _CADENCE_S)  # must not raise

    def test_off_by_one_cadence_raises_schema_drift(self, writer: RegionZarrWriter) -> None:
        _preallocate_encoded_timestamp(writer, "grp", 10)
        wrong_value = cftime.Datetime360Day(2049, 1, 5)  # slot 4, written at slot 3

        with pytest.raises(SchemaDriftError, match="diverged"):
            writer.write_timestamp("grp", ts_index=3, timestamp_val=wrong_value)

    def test_mismatched_calendar_raises_value_error(self, writer: RegionZarrWriter) -> None:
        _preallocate_encoded_timestamp(writer, "grp", 10, calendar="360_day")
        noleap_value = cftime.DatetimeNoLeap(2049, 1, 4)

        with pytest.raises(ValueError, match="calendar"):
            writer.write_timestamp("grp", ts_index=3, timestamp_val=noleap_value)

    def test_gregorian_datetime64_raises_type_error(self, writer: RegionZarrWriter) -> None:
        _preallocate_encoded_timestamp(writer, "grp", 10)

        with pytest.raises(TypeError, match="Gregorian"):
            writer.write_timestamp(
                "grp", ts_index=3, timestamp_val=np.datetime64("2049-01-04", "ns")
            )

    def test_unfilled_slot_error_names_preallocation_incomplete(
        self, writer: RegionZarrWriter
    ) -> None:
        # Preallocate creates a fully-filled array (grid values everywhere), so
        # an unfilled slot only occurs if the array was hand-built with a fill
        # hole -- exercise that directly. The message must name the actual
        # state ("preallocation incomplete"/"unwritten slot") so the operator
        # runs ``firecube zarr preallocate`` instead of chasing a phantom
        # coordinate-drift bug.
        arr = writer.ensure_group(
            "grp/timestamp",
            shape=(3,),
            dtype=np.int64,
            fill_value=np.iinfo(np.int64).min,
            chunks=(3,),
            attrs={"units": _UNITS, "calendar": "360_day"},
            dimension_names=("timestamp",),
        )
        arr[...] = np.array([0, np.iinfo(np.int64).min, 2 * _CADENCE_S], dtype=np.int64)
        arr.attrs[ATTR_PREALLOCATED] = True

        with pytest.raises(
            SchemaDriftError, match=r"preallocation incomplete|unwritten slot"
        ) as exc_info:
            writer.write_timestamp(
                "grp", ts_index=1, timestamp_val=cftime.Datetime360Day(2049, 1, 2)
            )
        assert "diverged" not in str(exc_info.value)

    def test_coord_managed_state_on_encoded_array_is_unreachable_error(
        self, writer: RegionZarrWriter
    ) -> None:
        arr = writer.ensure_group(
            "grp/timestamp",
            shape=(3,),
            dtype=np.int64,
            fill_value=np.iinfo(np.int64).min,
            chunks=(3,),
            attrs={"units": _UNITS, "calendar": "360_day"},
            dimension_names=("timestamp",),
        )
        arr.attrs[ATTR_COORD_MANAGED] = True

        with pytest.raises(SchemaDriftError, match="unreachable"):
            writer.write_timestamp(
                "grp", ts_index=0, timestamp_val=cftime.Datetime360Day(2049, 1, 1)
            )

    def test_legacy_state_on_encoded_array_is_unreachable_error(
        self, writer: RegionZarrWriter
    ) -> None:
        # A numeric array with units+calendar but no sealing marker at all
        # (LEGACY state): this should never occur in practice (see
        # `RegionZarrWriter._raise_encoded_unreachable_state`'s docstring)
        # but must fail loudly, not silently create/grow it.
        writer.ensure_group(
            "grp/timestamp",
            shape=(3,),
            dtype=np.int64,
            fill_value=np.iinfo(np.int64).min,
            chunks=(3,),
            attrs={"units": _UNITS, "calendar": "360_day"},
            dimension_names=("timestamp",),
        )

        with pytest.raises(SchemaDriftError, match="unreachable"):
            writer.write_timestamp(
                "grp", ts_index=0, timestamp_val=cftime.Datetime360Day(2049, 1, 1)
            )


class TestResolveTimestampIndexEncoded:
    def test_matches_existing_encoded_slot(self, writer: RegionZarrWriter) -> None:
        _preallocate_encoded_timestamp(writer, "grp", 10)
        value = cftime.Datetime360Day(2049, 1, 4)  # slot 3

        assert writer.resolve_timestamp_index("grp", value) == 3

    def test_gregorian_coordinate_still_resolves_normally(self, writer: RegionZarrWriter) -> None:
        # A plain datetime64 timestamp array (today's, unaffected) still
        # resolves through the unchanged path.
        ts = np.datetime64("2024-06-15T12:00:00", "s")
        writer.write_timestamp("grp", ts_index=0, timestamp_val=ts)

        assert writer.resolve_timestamp_index("grp", ts) == 0
