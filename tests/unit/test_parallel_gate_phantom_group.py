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

from typing import Any, ClassVar

import numpy as np
import pytest

from firecube.core.api import SlotAxis, SlotIndexModel
from firecube.ingestor.api import DirectZarrIngestor, PipelineBatch, PluginContext, WriteIntent
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_gate import (
    validate_global_expected_subset_of_schema,
    validate_parallel_capability,
)
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.unit


def _group(name: str) -> ZarrGroupSpec:
    return ZarrGroupSpec(
        group=name,
        arrays=[ZarrArraySpec(name="values", shape=(100, 4), dtype=np.float32, chunks=(10, 4))],
    )


class _CapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "parallel_gate_phantom"
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def __init__(self, *, global_expected: dict[str, int], schema: list[ZarrGroupSpec]) -> None:
        super().__init__(name="parallel_gate_phantom")
        self._global_expected = global_expected
        self._schema = schema

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        _ = group
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        _ = ctx
        return self._global_expected

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        _ = ctx
        return SlotIndexModel(
            name="parallel_gate_phantom_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=1, mode="exact")},
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return self._schema

    def ingest(self, ctx: Any):  # pragma: no cover - abstract hook not used here
        _ = ctx
        raise NotImplementedError

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


def test_subset_validator_passes_when_all_global_groups_in_schema() -> None:
    validate_global_expected_subset_of_schema({"data": 100}, [_group("data")])


def test_subset_validator_fails_when_phantom_group() -> None:
    with pytest.raises(ConfigurationError, match="phantom"):
        validate_global_expected_subset_of_schema({"data": 100, "phantom": 50}, [_group("data")])


def test_subset_validator_fails_multi_phantom() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_global_expected_subset_of_schema(
            {"data": 100, "phantom_a": 50, "phantom_b": 25},
            [_group("data")],
        )

    message = str(exc_info.value)
    assert "phantom_a" in message
    assert "phantom_b" in message


def test_subset_validator_called_by_capability_gate() -> None:
    ingestor = _CapableIngestor(
        global_expected={"data": 100, "phantom": 50},
        schema=[_group("data")],
    )

    with pytest.raises(ConfigurationError, match="phantom"):
        validate_parallel_capability(ingestor, 0, 10, ctx=None)  # type: ignore[arg-type]


def test_subset_validator_passes_when_schema_has_extras() -> None:
    validate_global_expected_subset_of_schema(
        {"data": 100},
        [_group("data"), _group("lat"), _group("lon")],
    )
