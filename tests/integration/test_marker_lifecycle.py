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

"""Marker lifecycle state machine for `RegionZarrWriter.write_timestamp`.

Covers four states of a time-coordinate array's reserved marker attrs:

- (a) only ``firecube_preallocated`` set — dense-preallocate lifecycle
- (b) only ``firecube_coord_managed`` set — coord-managed lifecycle
- (c) both markers set — invalid, mutually exclusive
- (d) neither marker set — legacy per-slot write

Scenarios (a) and (d) regress the dense-preallocate and legacy lifecycles.
Scenarios (b) and (c) pin the ``firecube_coord_managed`` verify-or-error
policy and the mutual-exclusion enforcement between the two markers.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.api import ATTR_COORD_MANAGED
from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.integration


def _store_files(store_path: Path) -> dict[str, bytes]:
    return {
        path.relative_to(store_path).as_posix(): path.read_bytes()
        for path in store_path.rglob("*")
        if path.is_file()
    }


def _store_mtimes(store_path: Path) -> dict[str, int]:
    return {
        path.relative_to(store_path).as_posix(): path.stat().st_mtime_ns
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


def _make_timestamp_array(
    writer: RegionZarrWriter,
    group: str,
    values: list[np.datetime64],
    *,
    markers: dict[str, bool],
    dtype: str = "datetime64[ns]",
) -> None:
    """Materialize a 1-D `timestamp` coord array with an explicit marker set.

    The array shape/chunks/dtype match how ``firecube zarr preallocate`` stamps
    time coords today; only the ``attrs`` markers change per scenario.
    """
    arr = writer.ensure_group(
        f"{group}/timestamp",
        shape=(len(values),),
        dtype=np.dtype(dtype),
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(len(values),),
        dimension_names=("timestamp",),
    )
    arr[:] = np.asarray(values, dtype=np.dtype(dtype))
    for name, value in markers.items():
        arr.attrs[name] = value


def test_preallocated_only_match_noop(store_path: Path, writer: RegionZarrWriter) -> None:
    timestamp = np.datetime64("2026-01-01T00:00:00", "ns")
    _make_timestamp_array(
        writer,
        "data_1km",
        [timestamp],
        markers={ATTR_PREALLOCATED: True},
    )
    before = _store_files(store_path)

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp)

    assert _store_files(store_path) == before


def test_preallocated_only_mismatch_raises(writer: RegionZarrWriter) -> None:
    current = np.datetime64("2026-01-01T00:00:00", "ns")
    incoming = np.datetime64("2026-01-01T00:05:00", "ns")
    _make_timestamp_array(
        writer,
        "data_1km",
        [current],
        markers={ATTR_PREALLOCATED: True},
    )

    with pytest.raises(SchemaDriftError, match=r"data_1km.*slot 0"):
        writer.write_timestamp("data_1km", ts_index=0, timestamp_val=incoming)


def test_coord_managed_only_match_noop(
    store_path: Path,
    writer: RegionZarrWriter,
) -> None:
    """Coord-managed marker + matching incoming timestamp → no-op.

    The coord-managed slot must not fall through to the legacy per-slot
    write path; a matching incoming value leaves every persisted byte and
    file mtime untouched.
    """
    timestamp = np.datetime64("2026-01-01T00:00:00", "ns")
    _make_timestamp_array(
        writer,
        "data_1km",
        [timestamp],
        markers={ATTR_COORD_MANAGED: True},
    )
    before_bytes = _store_files(store_path)
    before_mtimes = _store_mtimes(store_path)

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp)

    assert _store_files(store_path) == before_bytes
    assert _store_mtimes(store_path) == before_mtimes


def test_coord_managed_only_nat_raises(writer: RegionZarrWriter) -> None:
    """Coord-managed marker + stored NaT → structured error naming missing materialization.

    A coord-managed slot that still holds NaT is un-materialized; the write
    must raise instead of silently overwriting NaT with the incoming
    timestamp.
    """
    _make_timestamp_array(
        writer,
        "data_1km",
        [np.datetime64("NaT", "ns")],
        markers={ATTR_COORD_MANAGED: True},
    )
    incoming = np.datetime64("2026-01-01T00:00:00", "ns")

    with pytest.raises(
        SchemaDriftError,
        match=r"(materiali[sz]ation|firecube_coord_managed)",
    ):
        writer.write_timestamp("data_1km", ts_index=0, timestamp_val=incoming)


def test_both_markers_raises(writer: RegionZarrWriter) -> None:
    """Both markers set on the same slot → SchemaDriftError with mutual-exclusion message.

    The two lifecycles are mutually exclusive; combined presence indicates a
    corrupted lifecycle state that must be surfaced, not silently accepted.
    """
    timestamp = np.datetime64("2026-01-01T00:00:00", "ns")
    _make_timestamp_array(
        writer,
        "data_1km",
        [timestamp],
        markers={
            ATTR_PREALLOCATED: True,
            ATTR_COORD_MANAGED: True,
        },
    )

    with pytest.raises(SchemaDriftError, match=r"mutually exclusive"):
        writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp)


def test_no_markers_legacy_write(writer: RegionZarrWriter) -> None:
    timestamp = np.datetime64("2026-01-01T00:00:00", "s")

    writer.write_timestamp("data_1km", ts_index=0, timestamp_val=timestamp)

    arr = writer._open_root()["data_1km/timestamp"]
    assert np.asarray(arr[0]) == timestamp
