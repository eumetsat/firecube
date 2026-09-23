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

"""Guards against silently mislabeling a non-Gregorian calendar value.

Covers:

* `firecube.core.zarr._calendar_guard.reject_non_gregorian_calendar_value` and
  its three call sites (`coord_to_datetime64`,
  `RegionZarrWriter._normalize_timestamp_value(_ns)`).
* The `_ensure_index_identity_at_startup` "calendar axis requires
  preallocate" guard on `DirectZarrIngestor`
  (`_verify_calendar_axes_preallocated_at_startup`).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, ClassVar, cast

import cftime
import numpy as np
import pytest
import zarr

from firecube.core.errors import ConfigurationError
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, RegularTimeAxis
from firecube.core.zarr._calendar_guard import reject_non_gregorian_calendar_value
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.core.zarr.coord_materialization import coord_to_datetime64
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor, WriteIntent, ZarrGroupSpec

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# reject_non_gregorian_calendar_value / coord_to_datetime64 / writer normalizers
# ---------------------------------------------------------------------------


class TestRejectNonGregorianCalendarValue:
    def test_non_calendar_valued_input_passes_through(self) -> None:
        reject_non_gregorian_calendar_value("2024-01-01T00:00:00Z")  # must not raise
        reject_non_gregorian_calendar_value(np.datetime64("2024-01-01"))  # must not raise
        reject_non_gregorian_calendar_value(42)  # must not raise

    @pytest.mark.parametrize(
        "value",
        [
            cftime.DatetimeGregorian(2024, 1, 1),
            cftime.DatetimeProlepticGregorian(2024, 1, 1),
        ],
    )
    def test_gregorian_like_calendar_value_passes_through(self, value: Any) -> None:
        reject_non_gregorian_calendar_value(value)  # must not raise

    def test_360_day_calendar_value_raises_value_error(self) -> None:
        value = cftime.Datetime360Day(2049, 2, 30)
        with pytest.raises(ValueError, match="360_day"):
            reject_non_gregorian_calendar_value(value)

    def test_error_message_mentions_calendar_kwarg(self) -> None:
        value = cftime.DatetimeNoLeap(2049, 1, 1)
        with pytest.raises(ValueError, match="calendar="):
            reject_non_gregorian_calendar_value(value)


class TestCoordToDatetime64Guard:
    def test_gregorian_calendar_value_converts_normally(self) -> None:
        value = cftime.DatetimeGregorian(2024, 1, 1, 12, 0, 0)
        result = coord_to_datetime64(value)
        assert result == np.datetime64("2024-01-01T12:00:00", "ns")

    def test_proleptic_gregorian_calendar_value_converts_normally(self) -> None:
        value = cftime.DatetimeProlepticGregorian(2024, 1, 1, 12, 0, 0)
        result = coord_to_datetime64(value)
        assert result == np.datetime64("2024-01-01T12:00:00", "ns")

    def test_360_day_calendar_value_raises(self) -> None:
        value = cftime.Datetime360Day(2049, 2, 30)
        with pytest.raises(ValueError, match="360_day"):
            coord_to_datetime64(value)

    def test_plain_string_still_converts_normally(self) -> None:
        assert coord_to_datetime64("2024-01-01T00:00:00Z") == np.datetime64(
            "2024-01-01T00:00:00", "ns"
        )


class TestRegionZarrWriterNormalizerGuards:
    def test_normalize_timestamp_value_rejects_360_day(self) -> None:
        value = cftime.Datetime360Day(2049, 2, 30)
        with pytest.raises(ValueError, match="360_day"):
            RegionZarrWriter._normalize_timestamp_value(value)

    def test_normalize_timestamp_value_ns_rejects_noleap(self) -> None:
        value = cftime.DatetimeNoLeap(2049, 1, 1)
        with pytest.raises(ValueError, match="noleap"):
            RegionZarrWriter._normalize_timestamp_value_ns(value)

    def test_normalize_timestamp_value_accepts_gregorian(self) -> None:
        value = cftime.DatetimeGregorian(2024, 7, 4, 12, 0, 0)
        result = RegionZarrWriter._normalize_timestamp_value(value)
        assert result == np.datetime64("2024-07-04T12:00:00", "s")

    def test_normalize_timestamp_value_ns_accepts_proleptic_gregorian(self) -> None:
        value = cftime.DatetimeProlepticGregorian(2024, 7, 4, 12, 0, 0)
        result = RegionZarrWriter._normalize_timestamp_value_ns(value)
        assert result == np.datetime64("2024-07-04T12:00:00", "ns")

    def test_write_timestamp_legacy_branch_rejects_360_day(self, tmp_path: Any) -> None:
        """The LEGACY (unpreallocated) creation branch must not silently stamp a
        calendar value as datetime64."""
        store_path = tmp_path / "cube.zarr"
        zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
        writer = RegionZarrWriter(str(store_path))
        value = cftime.Datetime360Day(2049, 2, 30)

        with pytest.raises(ValueError, match="360_day"):
            writer.write_timestamp("grp", ts_index=0, timestamp_val=value)


# ---------------------------------------------------------------------------
# DirectZarrIngestor: calendar-axis-requires-preallocate startup guard
# ---------------------------------------------------------------------------


class _ChunkManager:
    storage_config = None


class _CalendarStartupIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "calendar_startup_guard_test"

    def __init__(self, *, chunk_manager: _ChunkManager) -> None:
        super().__init__(name="calendar_startup_guard_test", chunk_manager=cast(Any, chunk_manager))
        self.engine_config = cast(Any, SimpleNamespace(write_mode="direct"))

    def zarr_schema(self, ctx: Any) -> list[ZarrGroupSpec]:
        _ = ctx
        return []

    def build_write_intents(self, batch: Any, ctx: Any) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


def _resolved_calendar_index(axis: RegularTimeAxis) -> Any:
    spec = IndexSpec(name="calendar_startup_guard_test_v1", groups={"data": axis})
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _calendar_axis() -> RegularTimeAxis:
    return RegularTimeAxis(
        coordinate="timestamp",
        epoch="2049-01-01T00:00:00Z",
        cadence_s=86400,
        slot_count=10,
        calendar="360_day",
    )


class TestCalendarAxesPreallocatedAtStartup:
    def test_no_calendar_axis_is_noop(self) -> None:
        ingestor = _CalendarStartupIngestor(chunk_manager=_ChunkManager())
        axis = RegularTimeAxis(
            coordinate="timestamp", epoch="2049-01-01T00:00:00Z", cadence_s=86400, slot_count=10
        )
        resolved = _resolved_calendar_index(axis)

        # Must not attempt any store lookup (no resolve_output_uri override
        # needed): would raise AttributeError/NotImplementedError if it did.
        ingestor._verify_calendar_axes_preallocated_at_startup(
            cast(Any, SimpleNamespace()), resolved
        )

    def test_missing_store_raises_configuration_error(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ingestor = _CalendarStartupIngestor(chunk_manager=_ChunkManager())
        monkeypatch.setattr(
            _CalendarStartupIngestor,
            "resolve_output_uri",
            lambda self, ctx, write_mode: str(tmp_path / "does_not_exist.zarr"),
        )
        resolved = _resolved_calendar_index(_calendar_axis())

        with pytest.raises(ConfigurationError, match="firecube zarr preallocate"):
            ingestor._verify_calendar_axes_preallocated_at_startup(
                cast(Any, SimpleNamespace()), resolved
            )

    def test_unmarked_coord_array_raises_configuration_error(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store_path = tmp_path / "cube.zarr"
        root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
        root.create_group("data").create_array(
            "timestamp", shape=(10,), dtype=np.int64, fill_value=np.iinfo(np.int64).min
        )
        ingestor = _CalendarStartupIngestor(chunk_manager=_ChunkManager())
        monkeypatch.setattr(
            _CalendarStartupIngestor,
            "resolve_output_uri",
            lambda self, ctx, write_mode: str(store_path),
        )
        resolved = _resolved_calendar_index(_calendar_axis())

        with pytest.raises(ConfigurationError, match="data") as excinfo:
            ingestor._verify_calendar_axes_preallocated_at_startup(
                cast(Any, SimpleNamespace()), resolved
            )
        assert "360_day" in str(excinfo.value)
        assert "firecube zarr preallocate" in str(excinfo.value)

    def test_preallocated_coord_array_passes(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store_path = tmp_path / "cube.zarr"
        root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
        arr = root.create_group("data").create_array(
            "timestamp", shape=(10,), dtype=np.int64, fill_value=np.iinfo(np.int64).min
        )
        arr.attrs[ATTR_PREALLOCATED] = True
        ingestor = _CalendarStartupIngestor(chunk_manager=_ChunkManager())
        monkeypatch.setattr(
            _CalendarStartupIngestor,
            "resolve_output_uri",
            lambda self, ctx, write_mode: str(store_path),
        )
        resolved = _resolved_calendar_index(_calendar_axis())

        # Must not raise.
        ingestor._verify_calendar_axes_preallocated_at_startup(
            cast(Any, SimpleNamespace()), resolved
        )
