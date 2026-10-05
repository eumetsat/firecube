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

"""GenericZarrIngestor datasets that mix time-indexed and static data variables.

Regression for issue #83: static data variables (no time dimension, e.g.
``lat_bnds (lat, nv)``) were listed in ``span.arrays`` and could be picked as
the primary array whose shape seeds the resume cursor. ``chunks delete-span``
then failed on them, and a resumed run started appending at the wrong slot.

The datasets deliberately put ``lat_bnds`` FIRST in ``data_vars`` and give the
static arrays lengths that differ from the number of time slots, so a static
array chosen as primary yields a visibly wrong cursor.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from firecube.ingestor.runtime.zarr.resume_cache import clear_resume_cache
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration

_PRODUCT = "static_vars_span.zarr"
_GROUP = "default"
_NLAT = 4
_NLON = 5
_T0 = np.datetime64("2024-01-01", "ns")

_LAT_BNDS = np.arange(_NLAT * 2, dtype=np.float64).reshape(_NLAT, 2)
_LON_BNDS = (100.0 + np.arange(_NLON * 2, dtype=np.float64)).reshape(_NLON, 2)


def _precip(day: int) -> np.ndarray:
    return np.full((_NLAT, _NLON), 10.0 + day, dtype=np.float32)


def _dataset(days: list[int]) -> xr.Dataset:
    """Dataset with ``lat_bnds`` first, then time-indexed vars, then ``lon_bnds``."""
    times = _T0 + np.asarray(days).astype("timedelta64[D]")
    time_bnds = np.stack([[d, d + 1] for d in days]).astype(np.float64)
    ds = xr.Dataset(
        {
            "lat_bnds": (("lat", "nv"), _LAT_BNDS),
            "precipitation": (
                ("time", "lat", "lon"),
                np.stack([_precip(d) for d in days]),
            ),
            "time_bnds": (("time", "nv"), time_bnds),
            "lon_bnds": (("lon", "nv"), _LON_BNDS),
        },
        coords={
            "time": times,
            "lat": np.arange(_NLAT, dtype=np.float64),
            "lon": np.arange(_NLON, dtype=np.float64),
        },
    )
    ds["time"].encoding.update(dtype="int64", units="nanoseconds since 1970-01-01")
    assert next(iter(ds.data_vars)) == "lat_bnds"
    return ds


def _static_only_dataset(days: list[int]) -> xr.Dataset:
    """Dataset whose time coordinate exists but no data variable carries it."""
    times = _T0 + np.asarray(days).astype("timedelta64[D]")
    ds = xr.Dataset(
        {
            "lat_bnds": (("lat", "nv"), _LAT_BNDS),
            "lon_bnds": (("lon", "nv"), _LON_BNDS),
        },
        coords={
            "time": times,
            "lat": np.arange(_NLAT, dtype=np.float64),
            "lon": np.arange(_NLON, dtype=np.float64),
        },
    )
    ds["time"].encoding.update(dtype="int64", units="nanoseconds since 1970-01-01")
    return ds


class _StaticMixIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "static_vars_span"
    name = "static_vars_span"
    time_dim_name = "time"

    def discover_source_files(self, ctx: PluginContext) -> list[int]:
        return list(ctx.option("x_days") or [])

    def build_dataset(self, group: str, items: list[int], ctx: PluginContext) -> xr.Dataset:
        _ = group
        if ctx.option("x_static_only", False):
            return _static_only_dataset(list(items))
        return _dataset(list(items))


def _engine_run(
    tmp_path: Path,
    days: list[int],
    *,
    resume_existing: bool = False,
    static_only: bool = False,
) -> None:
    options: dict[str, Any] = {
        "write_mode": "direct",
        "x_days": days,
        "x_static_only": static_only,
        "pipeline_workers": 1,
        "pipeline_batch_size": 8,
        "no_progress": True,
    }
    if resume_existing:
        options["resume_existing"] = True
    ctx = make_test_context(tmp_path, product=_PRODUCT, options=options)
    _StaticMixIngestor().run(ctx)


def _spans(tmp_path: Path) -> list[dict[str, Any]]:
    """Read span payloads back through ``firecube chunks list --include-span``."""
    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--quiet",
            "list",
            "--product-name",
            (tmp_path / _PRODUCT).as_uri(),
            "--include-span",
            "--format",
            "json",
        ],
        env={"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_STORAGE_DRIVER": "fsspec"},
    )
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    return [row["span"] for row in rows if "span" in row]


def _ranges(span: dict[str, Any]) -> list[list[int]]:
    return [list(r) for r in span["time_index_ranges"]]


def test_span_arrays_list_only_time_indexed_variables(tmp_path: Path) -> None:
    _engine_run(tmp_path, days=[0, 1])

    spans = _spans(tmp_path)
    assert len(spans) == 1, spans
    expected = {f"{_GROUP}/precipitation", f"{_GROUP}/time_bnds"}
    assert set(spans[0]["arrays"]) == expected
    assert len(spans[0]["arrays"]) == len(expected)
    assert _ranges(spans[0]) == [[0, 1]]

    # The static arrays are still stored, just not tracked as span coverage.
    with xr.open_zarr(str(tmp_path / _PRODUCT), group=_GROUP, consolidated=False) as stored:
        np.testing.assert_array_equal(stored["lat_bnds"].values, _LAT_BNDS)
        np.testing.assert_array_equal(stored["lon_bnds"].values, _LON_BNDS)
        assert stored["precipitation"].sizes["time"] == 2


def test_resume_cursor_comes_from_time_indexed_primary_array(tmp_path: Path) -> None:
    """The resume cursor must come from a time-indexed array, not ``lat_bnds``.

    ``lat_bnds`` sorts first and has ``shape[0] == 4`` while the store holds 2
    time slots. Chunk length cannot reveal the wrong primary (it is refreshed
    from the stored time coordinate), but the cursor can: the appended slot must
    be 2, not 4.
    """
    _engine_run(tmp_path, days=[0, 1])
    # A real resume happens in a fresh process: drop the process-local cursor
    # cache so the cursor is re-derived from the stored primary array.
    clear_resume_cache()
    _engine_run(tmp_path, days=[2], resume_existing=True)

    spans = _spans(tmp_path)
    assert sorted(_ranges(s) for s in spans) == [[[0, 1]], [[2, 2]]]
    for span in spans:
        assert set(span["arrays"]) == {f"{_GROUP}/precipitation", f"{_GROUP}/time_bnds"}

    with xr.open_zarr(str(tmp_path / _PRODUCT), group=_GROUP, consolidated=False) as stored:
        assert stored["precipitation"].sizes["time"] == 3
        expected_times = _T0 + np.arange(3).astype("timedelta64[D]")
        np.testing.assert_array_equal(stored["time"].values, expected_times)
        assert len(set(stored["time"].values.tolist())) == 3
        values = stored["precipitation"].values
        for slot in range(3):
            np.testing.assert_array_equal(values[slot], _precip(slot))
        np.testing.assert_array_equal(stored["time_bnds"].values[:, 0], [0.0, 1.0, 2.0])
        np.testing.assert_array_equal(stored["lat_bnds"].values, _LAT_BNDS)
        np.testing.assert_array_equal(stored["lon_bnds"].values, _LON_BNDS)


def test_static_only_dataset_is_skipped_and_span_covers_nothing(tmp_path: Path) -> None:
    """No variable carries the time dim: the batch is skipped, not written.

    The run still records a span for the batch, but it must claim no arrays and
    no time slots, so ``chunks delete-span`` has nothing to act on.
    """
    _engine_run(tmp_path, days=[0, 1], static_only=True)

    for span in _spans(tmp_path):
        assert span["arrays"] == []
        assert span["time_index_ranges"] == []
        assert span["timestamps_written"] == 0
    store = tmp_path / _PRODUCT
    assert not (store / _GROUP / "lat_bnds").exists()
    assert not (store / _GROUP / "lon_bnds").exists()
