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

from typing import ClassVar

import numpy as np
import xarray as xr

from firecube.ingestor.api import (
    DirectZarrIngestor,
    GenericZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


@register_ingestor("cf_time_dim")
class CFTimeDimIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "cf_time_dim"
    time_dim_name: ClassVar[str] = "time"

    def build_dataset(
        self, group: str, items: list[object], ctx: PluginContext
    ) -> xr.Dataset | None:
        n = max(1, len(items))
        # Use numeric time values with a CF "<unit> since <reference>" attrs.units string.
        # Mirrors tests/fixtures/cf_dataset_fixtures.py: this layout lets xarray write the
        # array as-is (no CF time encoding kicks in) while keeping the units attr CF-shaped.
        times = np.arange(n, dtype=np.float64)
        lats = np.linspace(30.0, 60.0, 4)
        lons = np.linspace(-10.0, 20.0, 5)
        data = np.zeros((n, 4, 5), dtype=np.float32)

        return xr.Dataset(
            {
                "temperature": (
                    ["time", "lat", "lon"],
                    data,
                    {
                        "units": "K",
                        "standard_name": "air_temperature",
                        "long_name": "Air Temperature",
                    },
                )
            },
            coords={
                "time": xr.DataArray(
                    times,
                    dims=["time"],
                    attrs={
                        "units": "days since 2000-01-01",
                        "calendar": "standard",
                        "standard_name": "time",
                        "axis": "T",
                    },
                ),
                "lat": xr.DataArray(
                    lats,
                    dims=["lat"],
                    attrs={
                        "units": "degrees_north",
                        "standard_name": "latitude",
                        "axis": "Y",
                    },
                ),
                "lon": xr.DataArray(
                    lons,
                    dims=["lon"],
                    attrs={
                        "units": "degrees_east",
                        "standard_name": "longitude",
                        "axis": "X",
                    },
                ),
            },
            attrs={"Conventions": "CF-1.8", "title": "CF Time Dim Test Cube"},
        )

    def _aggregate_metrics(self, ctx, state):
        return self.default_aggregate_metrics(ctx, state)


_VDEDUP_SOURCE_TOKEN = "dummy"
_VDEDUP_T1 = "2024-10-01T00:00:00"
_VDEDUP_SENTINEL_A = 100.0


@register_ingestor("cf_time_dim_value_dedup")
class CFTimeDimValueDedupIngestor(DirectZarrIngestor):
    """DirectZarr fixture with time_dim_name="time" for staged-mode value-dedup tests.

    Proves that the custom time_dim_name ClassVar flows through the engine's
    _zarr_pre_batch_hook into the seeder, which copies data/time/c/* chunks
    to the workspace so that build_write_intents() can dedup by value.
    """

    PRODUCT_NAME: ClassVar[str] = "cf_time_dim_value_dedup"
    time_dim_name: ClassVar[str] = "time"

    def discover_source_files(self, ctx: PluginContext) -> list[object]:
        return [_VDEDUP_SOURCE_TOKEN]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="time",
                        shape=(0,),
                        dtype="datetime64[s]",
                        chunks=(1,),
                    ),
                    ZarrArraySpec(
                        name="val",
                        shape=(0,),
                        dtype="float32",
                        chunks=(1,),
                        fill_value=float("nan"),
                    ),
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        import zarr

        write_mode = self.engine_config.write_mode
        store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
        ts_iso = str(ctx.option("x_ts_iso", _VDEDUP_T1))
        ts_val = np.datetime64(ts_iso, "s")

        ts_index = 0
        try:
            arr = zarr.open_array(f"{store_uri}/data/time", mode="r")
            raw = np.asarray(arr[:]).astype("datetime64[s]")
            for i, v in enumerate(raw):
                if v == ts_val:
                    ts_index = i
                    break
            else:
                ts_index = int(arr.shape[0])
        except Exception:
            ts_index = 0

        sentinel = float(ctx.option("x_sentinel", _VDEDUP_SENTINEL_A))
        return [
            WriteIntent(
                group="data",
                array="time",
                ts_index=ts_index,
                data=None,
                kind="timestamp",
                timestamp_val=ts_val,
            ),
            WriteIntent(
                group="data",
                array="val",
                ts_index=ts_index,
                data=np.array([sentinel], dtype="float32"),
                kind="1d",
            ),
        ]
