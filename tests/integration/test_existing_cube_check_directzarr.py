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
import pytest
import xarray as xr

from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.errors import ConfigurationError
from tests.helpers.storage import make_test_context


class _DirectZarrTestIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "directzarr_check_test"
    time_dim_name: ClassVar[str] = "time"

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return ["synthetic_item_1"]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="SCHEMA_A",
                arrays=[
                    ZarrArraySpec(name="data", chunks=(10,), shape=(100,), dtype=np.float32),
                ],
            ),
            ZarrGroupSpec(
                group="SCHEMA_B",
                arrays=[
                    ZarrArraySpec(name="data", chunks=(10,), shape=(100,), dtype=np.float32),
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.mark.integration
def test_direct_zarr_existing_cube_check_uses_schema_declared_groups(tmp_path):
    target = tmp_path / "out.zarr"
    bad_ds = xr.Dataset(
        {"data": (("timestamp",), np.zeros((3,), dtype=np.float32))},
        coords={"timestamp": np.array([0.0, 1.0, 2.0])},
    )
    bad_ds.to_zarr(target, group="SCHEMA_B", mode="w", zarr_format=3, consolidated=False)

    ctx = make_test_context(
        tmp_path,
        source=str(tmp_path),
        product="out.zarr",
        options={"no_progress": True, "pipeline_batch_size": 1, "write_mode": "direct"},
    )
    plugin = _DirectZarrTestIngestor()  # type: ignore[abstract]

    with pytest.raises(ConfigurationError) as exc_info:
        plugin.run(ctx)
    msg = str(exc_info.value)
    assert "SCHEMA_B" in msg
    assert "timestamp" in msg
    assert "'time'" in msg
