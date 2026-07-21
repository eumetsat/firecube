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
import pandas as pd
import xarray as xr

__all__ = [
    "make_ambiguous_dataset",
    "make_broken_dataset",
    "make_cf_compliant_dataset",
    "make_legacy_timestamp_dataset",
]


def make_cf_compliant_dataset(
    time_dim: str = "time",
    n_t: int = 3,
    n_y: int = 4,
    n_x: int = 5,
) -> xr.Dataset:
    """Fully CF-1.8 compliant dataset."""
    times = np.arange(n_t, dtype=np.float64)
    lats = np.linspace(-30.0, 30.0, n_y)
    lons = np.linspace(-20.0, 20.0, n_x)
    data = np.zeros((n_t, n_y, n_x), dtype=np.float32)

    time_coord = xr.DataArray(
        pd.Index(times, name=time_dim).to_numpy(dtype=np.float64),
        dims=[time_dim],
        attrs={
            "units": "days since 2000-01-01",
            "calendar": "standard",
            "standard_name": "time",
            "axis": "T",
        },
    )
    lat_coord = xr.DataArray(
        lats,
        dims=["lat"],
        attrs={"units": "degrees_north", "standard_name": "latitude", "axis": "Y"},
    )
    lon_coord = xr.DataArray(
        lons,
        dims=["lon"],
        attrs={"units": "degrees_east", "standard_name": "longitude", "axis": "X"},
    )

    return xr.Dataset(
        {
            "temperature": (
                [time_dim, "lat", "lon"],
                data,
                {
                    "units": "K",
                    "standard_name": "air_temperature",
                    "long_name": "Air Temperature",
                },
            )
        },
        coords={time_dim: time_coord, "lat": lat_coord, "lon": lon_coord},
        attrs={"Conventions": "CF-1.8", "title": "Test dataset"},
    )


def make_broken_dataset(missing: str) -> xr.Dataset:
    """Dataset with one deliberate CF defect."""
    ds = make_cf_compliant_dataset()

    if missing == "conventions":
        ds.attrs = {k: v for k, v in ds.attrs.items() if k != "Conventions"}
    elif missing == "time_units":
        ds["time"].attrs = {k: v for k, v in ds["time"].attrs.items() if k != "units"}
    elif missing == "var_units":
        ds["temperature"].attrs = {k: v for k, v in ds["temperature"].attrs.items() if k != "units"}
    elif missing == "var_names_only":
        ds["temperature"].attrs = {"units": "K", "long_name": "Air Temperature"}
    elif missing == "broken_reference":
        ds["temperature"].attrs = dict(ds["temperature"].attrs)
        ds["temperature"].attrs["coordinates"] = "nonexistent_var"
    else:
        raise ValueError(f"Unknown missing={missing!r}")

    return ds


def make_legacy_timestamp_dataset(
    n_t: int = 3,
    n_y: int = 4,
    n_x: int = 5,
) -> xr.Dataset:
    """Dataset with 'timestamp' dim (firecube legacy convention)."""
    return make_cf_compliant_dataset(time_dim="timestamp", n_t=n_t, n_y=n_y, n_x=n_x)


def make_ambiguous_dataset() -> xr.Dataset:
    """Dataset that has BOTH 'time' AND 'timestamp' dims."""
    ds_time = make_cf_compliant_dataset(time_dim="time", n_t=2)
    ds_ts = make_cf_compliant_dataset(time_dim="timestamp", n_t=2)
    ds_ts = ds_ts.rename({"temperature": "temperature_ts"})
    return xr.merge([ds_time, ds_ts])
