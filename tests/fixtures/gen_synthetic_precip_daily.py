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

"""Fixture-only synthetic precipitation NetCDF generator."""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir")
    parser.add_argument("start_day", type=int)
    parser.add_argument("end_day", type=int)
    parser.add_argument(
        "--time-resolution",
        default="ns",
        choices=["ns", "s", "ms", "D"],
        help="numpy datetime64 resolution for the time coordinate",
    )
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    lat = np.arange(-89.5, 90, 1.0, dtype=np.float32)
    lon = np.arange(-179.5, 180, 1.0, dtype=np.float32)
    rng = np.random.default_rng(42)
    for d in range(args.start_day, args.end_day + 1):
        t = pd.Timestamp("2024-01-01") + pd.Timedelta(days=d - 1)
        time_val = np.datetime64(t.strftime("%Y-%m-%d"), args.time_resolution)
        data = rng.gamma(2.0, 1.5, size=(1, lat.size, lon.size)).astype(np.float32)
        ds = xr.Dataset(
            {
                "precipitation": (
                    ("time", "lat", "lon"),
                    data,
                    {"units": "mm/day", "long_name": "daily precipitation"},
                )
            },
            coords={"time": [time_val], "lat": lat, "lon": lon},
        )
        ds.to_netcdf(out / f"precip_{t:%Y%m%d}.nc")
    print(f"wrote days {args.start_day}-{args.end_day} to {out}")


if __name__ == "__main__":
    main()
