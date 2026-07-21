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

import logging
from collections.abc import Sequence
from typing import Any, ClassVar

import numpy as np
import pytest
import xarray as xr

from firecube.core.api import SlotAxis, SlotIndexModel
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
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

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

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        return list(items[slot_start:slot_end])

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return 0

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {"PARALLEL_GROUP": 1}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return SlotIndexModel(
            name="directzarr_parallel_test_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={"PARALLEL_GROUP": SlotAxis(cadence_s=1, mode="exact")},
        )

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
