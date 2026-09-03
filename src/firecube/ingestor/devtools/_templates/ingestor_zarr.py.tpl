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

"""Generic Zarr ingestor for {plugin_name}.

Only ``read_dataset`` knows the source format. Implement it; ``build_dataset``
concatenates what it returns along the time dimension.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext, register_ingestor


def read_dataset(path: Path) -> xr.Dataset:
    """Open one source file as an ``xarray.Dataset`` with a time dimension."""
    raise NotImplementedError(
        f"read_dataset() is not implemented (called for {{path}}). Open the file and "
        "return an xarray.Dataset whose time dimension matches time_dim_name."
    )


@register_ingestor("{plugin_name}")
class {class_name}(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    # Must equal the time dimension of the dataset ``build_dataset`` returns.
    time_dim_name: ClassVar[str] = "timestamp"
    # To accept ``--option key=value`` flags, attach a PluginConfig subclass;
    # see the Firecube "Add Plugin Configuration Options" guide.

    def build_dataset(
        self,
        group: str,  # Called once per output group; most plugins ignore this.
        items: list[Any],
        ctx: PluginContext,
    ) -> xr.Dataset | None:
        _ = group
        if not items:
            return None

        datasets = [read_dataset(ctx.materialize(item)) for item in items]
        dataset = xr.concat(datasets, dim=self.time_dim_name)
        return dataset.sortby(self.time_dim_name)
