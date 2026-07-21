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

from collections.abc import Sequence
from typing import Any, ClassVar

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from firecube.ingestor.errors import ConfigurationError
from tests.helpers.storage import make_test_context


class _MultiGroupTestIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "multigroup_test"
    time_dim_name: ClassVar[str] = "time"

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        return ["GROUP_A", "GROUP_B"]

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return ["item1"]

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> None:
        return None


@pytest.mark.integration
def test_multigroup_plugin_existing_cube_check_uses_actual_write_groups(tmp_path):
    target = tmp_path / "out.zarr"
    bad_ds = xr.Dataset(
        {"data": (("timestamp", "y"), np.zeros((2, 3), dtype=np.float32))},
        coords={"timestamp": np.array([0.0, 1.0])},
    )
    bad_ds.to_zarr(target, group="GROUP_A", mode="w", zarr_format=3, consolidated=False)

    ctx = make_test_context(
        tmp_path,
        source=str(tmp_path),
        product="out.zarr",
        options={"no_progress": True, "pipeline_batch_size": 1},
    )
    plugin = _MultiGroupTestIngestor()  # type: ignore[abstract]

    with pytest.raises(ConfigurationError) as exc_info:
        plugin.run(ctx)
    msg = str(exc_info.value)
    assert "GROUP_A" in msg
    assert "timestamp" in msg
    assert "'time'" in msg
