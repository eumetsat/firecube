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
import pandas as pd
import xarray as xr

from firecube.ingestor.api import (
    GenericZarrIngestor,
    PluginContext,
    register_ingestor,
)


@register_ingestor("fc_test_stub")
class FcTestStubIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "fc_test_stub"
    name = "fc_test_stub"

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        return ["stub-item"]

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset:
        timestamps = pd.date_range("2024-01-01", periods=5)
        return xr.Dataset(
            {"x": (("timestamp",), np.arange(5, dtype=np.int64))},
            coords={"timestamp": timestamps},
        )
