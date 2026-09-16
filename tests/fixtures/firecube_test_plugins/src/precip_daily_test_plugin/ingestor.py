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

"""Generic Zarr ingestor for precip_daily (synthetic daily 1x1 deg precipitation).

Built from the public docs only:
  - guides/plugins/generic-zarr/      -> build_dataset + time_dim_name
  - guides/plugins/add-config-options/ -> PluginConfig dataclass + plugin_config_class
  - reference/templates/ (get_zarr_config) + reference/config/ (zarr_chunk_shape,
    zarr_sharding, zarr_shard_shape) -> per-layout chunking
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginConfig,
    PluginContext,
    register_ingestor,
)

LAYOUTS: dict[str, dict[str, int]] = {
    "timeseries": {"time": 365, "lat": 10, "lon": 10},
    "areastats": {"time": 1, "lat": 180, "lon": 360},
}


@dataclass
class PrecipConfig(PluginConfig):
    layout: str = "timeseries"


@register_ingestor("precip_daily")
class PrecipDailyIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "precip_daily"
    time_dim_name: ClassVar[str] = "time"
    plugin_config_class = PrecipConfig

    def get_zarr_config(self, ctx: PluginContext) -> dict[str, Any]:
        options = super().get_zarr_config(ctx)
        config = self.plugin_config
        assert isinstance(config, PrecipConfig)
        if config.layout not in LAYOUTS:
            raise ValueError(f"layout must be one of {sorted(LAYOUTS)}, got {config.layout!r}")
        if options.get("chunk_shape") is None:
            options["chunk_shape"] = dict(LAYOUTS[config.layout])
        return options

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = group
        if not items:
            return None
        paths = [ctx.materialize(item) for item in items]
        dataset = xr.open_mfdataset(paths, combine="by_coords", engine="h5netcdf")
        return dataset[["precipitation"]].sortby(self.time_dim_name).load()


@register_ingestor("test_giraffe_override")
class InvalidShardingPlugin(PrecipDailyIngestor):
    PRODUCT_NAME: ClassVar[str] = "test_giraffe_override"

    def get_zarr_config(self, ctx: PluginContext) -> dict[str, Any]:
        return {"sharding": True, "chunk_shape": {"time": 365}, "shard_shape": None}
