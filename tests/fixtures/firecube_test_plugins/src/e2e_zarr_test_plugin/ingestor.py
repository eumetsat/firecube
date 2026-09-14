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

"""End-to-end zarr ingestor fixture.

Reads NetCDFs with dims (time, y, x), renames time -> timestamp, appends into a
Zarr store. Used by the end-to-end ingestion tests and by out-of-tree
reproduction scripts.
"""

from __future__ import annotations

from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)


@register_ingestor("e2e_zarr")
class E2eZarrIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "e2e_zarr"

    def build_dataset(
        self,
        group: str,
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = group
        if not items:
            return None
        datasets = [xr.open_dataset(ctx.materialize(item), engine="h5netcdf") for item in items]
        renamed = [ds.rename({"time": "timestamp"}) if "time" in ds.dims else ds for ds in datasets]
        combined = xr.concat(
            renamed,
            dim=self.time_dim_name,
            data_vars="minimal",
            coords="minimal",
            compat="equals",
            join="exact",
        )
        return combined.sortby(self.time_dim_name).load()
