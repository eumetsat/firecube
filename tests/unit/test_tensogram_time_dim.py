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

from typing import Any

import numpy as np
import xarray as xr

from firecube.core.archive.tensogram.converter import _find_time_dim


def _ds_with_dims(*dim_names: str) -> xr.Dataset:
    return xr.Dataset(
        coords={dim: ((dim,), np.array([0.0, 1.0])) for dim in dim_names},
    )


_SCAN_DS = xr.Dataset(
    coords={
        "scan_time": (
            ("scan_time",),
            np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]"),
        ),
    },
)
_OBS_AND_SCAN_DS = xr.Dataset(
    coords={
        "obs_time": (("obs_time",), np.array([0.0, 1.0])),
        "scan_time": _SCAN_DS["scan_time"].variable.copy(),
    },
)
_FIND_TIME_DIM_ROWS: tuple[tuple[str, xr.Dataset, dict[str, Any], str | None], ...] = (
    # (row id, dataset, preferred_time_dim kwargs, expected dimension)
    (
        "preferred_name_wins_when_present",
        _ds_with_dims("obs_time"),
        {"preferred_time_dim": "obs_time"},
        "obs_time",
    ),
    (
        "preferred_name_wins_over_legacy_order",
        _ds_with_dims("obs_time", "time"),
        {"preferred_time_dim": "obs_time"},
        "obs_time",
    ),
    (
        "preferred_name_wins_over_timestamp",
        _ds_with_dims("obs_time", "timestamp", "time"),
        {"preferred_time_dim": "obs_time"},
        "obs_time",
    ),
    (
        "preferred_falls_through_when_absent",
        _ds_with_dims("time"),
        {"preferred_time_dim": "obs_time"},
        "time",
    ),
    (
        "preferred_overrides_datetime64_scan",
        _OBS_AND_SCAN_DS,
        {"preferred_time_dim": "obs_time"},
        "obs_time",
    ),
    ("default_picks_timestamp_first", _ds_with_dims("timestamp", "time"), {}, "timestamp"),
    ("default_picks_time_when_timestamp_missing", _ds_with_dims("time"), {}, "time"),
    (
        "default_picks_datetime64_coord_when_no_canonical_name",
        _SCAN_DS.copy(deep=True),
        {},
        "scan_time",
    ),
    ("default_returns_none_when_no_match", _ds_with_dims("x", "y"), {}, None),
    (
        "none_preference_equivalent_to_default",
        _ds_with_dims("timestamp", "time"),
        {"preferred_time_dim": None},
        "timestamp",
    ),
)


def test_find_time_dim(subtests):
    for row_id, ds, kwargs, expected in _FIND_TIME_DIM_ROWS:
        with subtests.test(msg=f"test_{row_id}"):
            assert _find_time_dim(ds, **kwargs) == expected


_STRATEGY_ROWS: tuple[tuple[str, xr.Dataset, dict[str, Any], str], ...] = (
    # (row id, dataset, strategy kwargs, expected archived time_dim)
    (
        "strategy_writes_preferred_time_dim_to_archive_metadata",
        xr.Dataset(
            {"value": (("obs_time",), np.array([1.0, 2.0], dtype="float32"))},
            coords={
                "obs_time": (("obs_time",), np.array([0.0, 1.0])),
                "time": (("time",), np.array([10.0, 11.0])),
            },
        ),
        {"time_dim_name": "obs_time"},
        "obs_time",
    ),
    (
        "strategy_default_time_dim_discovery_is_written_to_archive_metadata",
        xr.Dataset(
            {"value": (("timestamp",), np.array([1.0, 2.0], dtype="float32"))},
            coords={"timestamp": (("timestamp",), np.array([0.0, 1.0]))},
        ),
        {},
        "timestamp",
    ),
)


def test_strategy_writes_resolved_time_dim_to_archive_metadata(subtests, tmp_path):
    import tensogram

    from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

    for row_id, ds, kwargs, expected in _STRATEGY_ROWS:
        with subtests.test(msg=f"test_{row_id}"):
            target = tmp_path / f"{row_id}.tgm"
            strategy = TensogramWriteStrategy(target=str(target), **kwargs)
            strategy.write(ds)
            strategy.close()

            with tensogram.TensogramFile.open(str(target)) as archive:  # type: ignore[attr-defined]
                meta = archive.file_decode_metadata(0)
            assert meta.extra["firecube"]["time_dim"] == expected
