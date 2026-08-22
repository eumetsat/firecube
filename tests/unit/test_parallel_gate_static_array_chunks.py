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
from types import SimpleNamespace
from typing import Any, ClassVar

import numpy as np
import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import DirectZarrIngestor, PipelineBatch, PluginContext, WriteIntent
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.unit


class _CapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "parallel_gate_static_array_chunks"

    def __init__(self, *, global_expected: dict[str, int], schema: list[ZarrGroupSpec]) -> None:
        super().__init__(name="parallel_gate_static_array_chunks")
        self._global_expected = global_expected
        self._schema = schema

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="parallel_gate_static_array_chunks_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=self._global_expected["data"],
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return self._schema

    def ingest(self, ctx: Any):  # pragma: no cover
        _ = ctx
        raise NotImplementedError

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


def _ctx() -> Any:
    return SimpleNamespace(_ctx=object())


def test_static_array_chunks_excluded_from_alignment_check() -> None:
    """Regression: static (time_indexed=False) array chunks must NOT participate
    in time-axis alignment validation. Their spatial shape was bleeding through
    and demanding slot ranges be multiples of the spatial dim. Reference:
    parallel_gate.py:107 + plugin handoff firecube-parallel-gate-includes-static-array-chunks.md.
    """
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(1000, 4),
                    dtype=np.float32,
                    chunks=(10, 4),
                ),
                ZarrArraySpec(
                    name="lat",
                    shape=(100, 4),
                    dtype=np.float64,
                    chunks=(100, 4),
                    time_indexed=False,
                ),
            ],
        )
    ]
    ingestor = _CapableIngestor(global_expected={"data": 1000}, schema=schema)

    # Without T10's fix, lat's (100, 4) chunks would force slot_start/end
    # to be multiples of 100. With the fix, only `values` chunks (10, 4) drive
    # alignment, so [0, 50) is valid (50 % 10 == 0).
    validate_parallel_capability(ingestor, 0, 50, ctx=_ctx())


def test_time_indexed_chunks_still_checked() -> None:
    """T10's filter must not break legitimate misalignment detection for
    time-indexed arrays."""
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(1000, 4),
                    dtype=np.float32,
                    chunks=(100, 4),  # forces alignment to multiples of 100
                ),
            ],
        )
    ]
    ingestor = _CapableIngestor(global_expected={"data": 1000}, schema=schema)

    with pytest.raises(ConfigurationError, match=r"misaligned|alignment"):
        validate_parallel_capability(ingestor, 0, 50, ctx=_ctx())
