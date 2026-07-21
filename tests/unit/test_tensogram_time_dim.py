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

import numpy as np
import xarray as xr

from firecube.core.tensogram.converter import _find_time_dim


def _ds_with_dims(*dim_names: str) -> xr.Dataset:
    return xr.Dataset(
        coords={dim: ((dim,), np.array([0.0, 1.0])) for dim in dim_names},
    )


class TestFindTimeDimPreferred:
    def test_preferred_name_wins_when_present(self):
        ds = _ds_with_dims("obs_time")
        assert _find_time_dim(ds, preferred_time_dim="obs_time") == "obs_time"

    def test_preferred_name_wins_over_legacy_order(self):
        ds = _ds_with_dims("obs_time", "time")
        assert _find_time_dim(ds, preferred_time_dim="obs_time") == "obs_time"

    def test_preferred_name_wins_over_timestamp(self):
        ds = _ds_with_dims("obs_time", "timestamp", "time")
        assert _find_time_dim(ds, preferred_time_dim="obs_time") == "obs_time"

    def test_preferred_falls_through_when_absent(self):
        ds = _ds_with_dims("time")
        assert _find_time_dim(ds, preferred_time_dim="obs_time") == "time"

    def test_preferred_overrides_datetime64_scan(self):
        ds = xr.Dataset(
            coords={
                "obs_time": (("obs_time",), np.array([0.0, 1.0])),
                "scan_time": (
                    ("scan_time",),
                    np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]"),
                ),
            },
        )
        assert _find_time_dim(ds, preferred_time_dim="obs_time") == "obs_time"


class TestFindTimeDimFallbackUnchanged:
    def test_default_picks_timestamp_first(self):
        ds = _ds_with_dims("timestamp", "time")
        assert _find_time_dim(ds) == "timestamp"

    def test_default_picks_time_when_timestamp_missing(self):
        ds = _ds_with_dims("time")
        assert _find_time_dim(ds) == "time"

    def test_default_picks_datetime64_coord_when_no_canonical_name(self):
        ds = xr.Dataset(
            coords={
                "scan_time": (
                    ("scan_time",),
                    np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]"),
                ),
            },
        )
        assert _find_time_dim(ds) == "scan_time"

    def test_default_returns_none_when_no_match(self):
        ds = _ds_with_dims("x", "y")
        assert _find_time_dim(ds) is None

    def test_none_preference_equivalent_to_default(self):
        ds = _ds_with_dims("timestamp", "time")
        assert _find_time_dim(ds, preferred_time_dim=None) == "timestamp"


class TestTensogramStrategyTimeDimThreading:
    def test_strategy_writes_preferred_time_dim_to_archive_metadata(self, tmp_path):
        import tensogram

        from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

        target = tmp_path / "out.tgm"
        ds = xr.Dataset(
            {"value": (("obs_time",), np.array([1.0, 2.0], dtype="float32"))},
            coords={
                "obs_time": (("obs_time",), np.array([0.0, 1.0])),
                "time": (("time",), np.array([10.0, 11.0])),
            },
        )
        strategy = TensogramWriteStrategy(
            target=str(target),
            time_dim_name="obs_time",
        )
        strategy.write(ds)
        strategy.close()

        with tensogram.TensogramFile.open(str(target)) as archive:  # type: ignore[attr-defined]
            meta = archive.file_decode_metadata(0)
        assert meta.extra["firecube"]["time_dim"] == "obs_time"

    def test_strategy_default_time_dim_discovery_is_written_to_archive_metadata(self, tmp_path):
        import tensogram

        from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy

        target = tmp_path / "out.tgm"
        ds = xr.Dataset(
            {"value": (("timestamp",), np.array([1.0, 2.0], dtype="float32"))},
            coords={"timestamp": (("timestamp",), np.array([0.0, 1.0]))},
        )
        strategy = TensogramWriteStrategy(target=str(target))
        strategy.write(ds)
        strategy.close()

        with tensogram.TensogramFile.open(str(target)) as archive:  # type: ignore[attr-defined]
            meta = archive.file_decode_metadata(0)
        assert meta.extra["firecube"]["time_dim"] == "timestamp"
