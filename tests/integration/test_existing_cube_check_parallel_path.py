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

import datetime as dt
import logging
from typing import Any, ClassVar

import numpy as np
import pytest
import zarr

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    ExecutionParams,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.errors import ConfigurationError
from tests.helpers.storage import make_test_context


class _ParallelDirectZarrTestIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "directzarr_parallel_test"
    time_dim_name: ClassVar[str] = "time"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="PARALLEL_GROUP",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(1,),
                        shape=(100,),
                        dtype=np.float32,
                    )
                ],
            )
        ]

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return ["synthetic_item_0", "synthetic_item_1"]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="directzarr_parallel_test_v1",
            groups={
                "PARALLEL_GROUP": RegularTimeAxis(
                    coordinate="time",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=1,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        if not isinstance(item, str):
            return None
        return ItemInfo(coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC))

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.mark.integration
def test_slot_range_parallel_path_runs_existing_cube_mismatch_check(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="firecube.ingestor.engine")

    target = tmp_path / "out.zarr"
    # Same schema as the plugin (one-slot chunks, so the [0, 1) window is
    # aligned) except for the time dimension name: only the existing-cube
    # check can object.
    root = zarr.open_group(store=str(target), mode="w", zarr_format=3)
    root.require_group("PARALLEL_GROUP").create_array(
        "data",
        shape=(100,),
        chunks=(1,),
        dtype=np.float32,
        dimension_names=("timestamp",),
        fill_value=0.0,
    )

    ctx = make_test_context(
        tmp_path,
        source=str(tmp_path),
        product="out.zarr",
        options={
            "no_progress": True,
            "pipeline_batch_size": 1,
            "pipeline_workers": 2,
        },
        execution=ExecutionParams(slot_start=0, slot_end=1),
    )
    plugin = _ParallelDirectZarrTestIngestor()  # type: ignore[abstract]

    with pytest.raises(ConfigurationError) as exc_info:
        plugin.run(ctx)
    msg = str(exc_info.value)
    assert "PARALLEL_GROUP" in msg
    assert any("pre_batch_filter" in r.message for r in caplog.records)
