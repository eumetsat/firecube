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

"""Dtype-normalization regression: `datetime64[s]` coord + sub-second incoming does not raise.

The pre-fix compare cast both sides to `datetime64[ns]`; an on-disk
`datetime64[s]` slot then differed from an in-memory `datetime64[ns]`
candidate that shared the same second but carried sub-second precision.
That produced a spurious ``SchemaDriftError`` on the `COORD_MANAGED`
verify branch and on the observed-coord materializer's verify branch.

The unified normalizer truncates both sides to the on-disk resolution
before the equality check, so a same-second candidate is a no-op and a
different-second candidate still raises.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.api import ATTR_COORD_MANAGED
from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.coord_materialization import write_observed_regular_coord_values
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.integration


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    store_dir = tmp_path / "cube.zarr"
    zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    return store_dir


@pytest.fixture()
def writer(store_path: Path) -> RegionZarrWriter:
    return RegionZarrWriter(str(store_path))


def _make_seconds_coord_array(
    writer: RegionZarrWriter,
    group: str,
    stored_seconds: list[np.datetime64],
) -> None:
    arr = writer.ensure_group(
        f"{group}/timestamp",
        shape=(len(stored_seconds),),
        dtype=np.dtype("datetime64[s]"),
        fill_value=np.datetime64("NaT", "s"),
        chunks=(len(stored_seconds),),
        dimension_names=("timestamp",),
    )
    arr[:] = np.asarray(stored_seconds, dtype="datetime64[s]")
    arr.attrs[ATTR_COORD_MANAGED] = True


class TestPodVerifyCoordManagedSubsecondRoundtrip:
    def test_same_second_ns_incoming_is_no_op(self, writer: RegionZarrWriter) -> None:
        stored = np.datetime64("2026-03-15T10:20:30", "s")
        incoming_ns = np.datetime64("2026-03-15T10:20:30.123456789", "ns")
        _make_seconds_coord_array(writer, "data_1km", [stored])

        writer.write_timestamp("data_1km", ts_index=0, timestamp_val=incoming_ns)

        arr = writer._open_root()["data_1km/timestamp"]
        assert np.asarray(arr[0]) == stored
        assert np.asarray(arr[0]).dtype == np.dtype("datetime64[s]")

    def test_different_second_still_raises(self, writer: RegionZarrWriter) -> None:
        stored = np.datetime64("2026-03-15T10:20:30", "s")
        incoming_ns = np.datetime64("2026-03-15T10:20:31.000000001", "ns")
        _make_seconds_coord_array(writer, "data_1km", [stored])

        with pytest.raises(SchemaDriftError, match=r"data_1km.*slot 0"):
            writer.write_timestamp("data_1km", ts_index=0, timestamp_val=incoming_ns)


class TestMaterializerSubsecondRoundtrip:
    def test_ns_discovery_against_seconds_slot_marks_match(self, writer: RegionZarrWriter) -> None:
        stored = np.datetime64("2026-03-15T10:20:30", "s")
        _make_seconds_coord_array(writer, "data_1km", [stored])
        arr = writer._open_root()["data_1km/timestamp"]
        subsecond_value = np.datetime64("2026-03-15T10:20:30.987654321", "ns")

        written, matched = write_observed_regular_coord_values(
            arr=arr,
            coord_path="data_1km/timestamp",
            values_by_slot={0: subsecond_value},
            target_dtype=np.dtype("datetime64[s]"),
        )

        assert written == 0
        assert matched == 1
        assert np.asarray(arr[0]) == stored

    def test_ns_discovery_into_nat_slot_writes_truncated(self, writer: RegionZarrWriter) -> None:
        _make_seconds_coord_array(writer, "data_1km", [np.datetime64("NaT", "s")])
        arr = writer._open_root()["data_1km/timestamp"]
        subsecond_value = np.datetime64("2026-03-15T10:20:30.987654321", "ns")

        written, matched = write_observed_regular_coord_values(
            arr=arr,
            coord_path="data_1km/timestamp",
            values_by_slot={0: subsecond_value},
            target_dtype=np.dtype("datetime64[s]"),
        )

        assert written == 1
        assert matched == 0
        assert np.asarray(arr[0]) == np.datetime64("2026-03-15T10:20:30", "s")

    def test_ns_discovery_with_different_second_still_raises(
        self, writer: RegionZarrWriter
    ) -> None:
        stored = np.datetime64("2026-03-15T10:20:30", "s")
        _make_seconds_coord_array(writer, "data_1km", [stored])
        arr = writer._open_root()["data_1km/timestamp"]
        divergent_value = np.datetime64("2026-03-15T10:20:31.000000001", "ns")

        with pytest.raises(SchemaDriftError, match=r"data_1km/timestamp.*slot 0"):
            write_observed_regular_coord_values(
                arr=arr,
                coord_path="data_1km/timestamp",
                values_by_slot={0: divergent_value},
                target_dtype=np.dtype("datetime64[s]"),
            )
