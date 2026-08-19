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
import xarray as xr

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
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
                        chunks=(10,),
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
                    size=1,
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
def test_slot_range_parallel_path_runs_existing_cube_mismatch_check(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="firecube.ingestor.engine")

    target = tmp_path / "out.zarr"
    bad_ds = xr.Dataset(
        {"data": (("timestamp",), np.zeros((3,), dtype=np.float32))},
        coords={"timestamp": np.array([0.0, 1.0, 2.0])},
    )
    bad_ds.to_zarr(target, group="PARALLEL_GROUP", mode="w", zarr_format=3, consolidated=False)

    # This RED test isolates the existing-cube-check bypass. The required narrow
    # [0, 1) slot range is intentionally not chunk-aligned for chunks=(10,), so
    # bypass alignment validation to reach the engine's pre-batch filter branch.
    monkeypatch.setattr(
        "firecube.ingestor.types.planned_range.validate_chunk_alignment",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "firecube.ingestor.types.planned_range.warn_if_misaligned",
        lambda *args, **kwargs: None,
    )

    ctx = make_test_context(
        tmp_path,
        source=str(tmp_path),
        product="out.zarr",
        options={
            "no_progress": True,
            "pipeline_batch_size": 1,
            "pipeline_workers": 2,
            "slot_start": 0,
            "slot_end": 1,
        },
    )
    plugin = _ParallelDirectZarrTestIngestor()  # type: ignore[abstract]

    with pytest.raises(ConfigurationError) as exc_info:
        plugin.run(ctx)
    msg = str(exc_info.value)
    assert "PARALLEL_GROUP" in msg
    assert any("pre_batch_filter" in r.message for r in caplog.records)
