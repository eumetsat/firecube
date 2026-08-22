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
from firecube.ingestor.api import (
    DirectZarrIngestor,
    IngestContext,
    IngestResult,
    PipelineBatch,
    PluginContext,
)
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.unit


def _group(group: str, chunk_size: int) -> ZarrGroupSpec:
    return ZarrGroupSpec(
        group=group,
        arrays=[
            ZarrArraySpec(
                name="values",
                shape=(100, 4),
                dtype=np.float32,
                chunks=(chunk_size, 4),
                fill_value=0.0,
            )
        ],
    )


class SlotGroupCapable(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "slot_group_capable"

    def __init__(self, schema: list[ZarrGroupSpec]) -> None:
        super().__init__()
        self._schema = schema

    def ingest(self, ctx: IngestContext) -> IngestResult:
        raise AssertionError("capability gate must not run ingestion")

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="slot_group_capable_v1",
            groups={
                spec.group: RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=100,
                )
                for spec in self._schema
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

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        raise AssertionError("capability gate must not build write intents or perform writes")


def _ctx() -> Any:
    return SimpleNamespace(_ctx=object())


def test_unknown_slot_group_fails_before_writes() -> None:
    ingestor = SlotGroupCapable([_group("group_a", 5), _group("group_b", 5)])

    with pytest.raises(ConfigurationError) as exc_info:
        validate_parallel_capability(ingestor, 0, 10, ctx=_ctx(), slot_group="missing_grp")

    message = str(exc_info.value)
    assert "missing_grp" in message
    assert "group_a" in message
    assert "group_b" in message


def test_known_slot_group_passes_capability_gate() -> None:
    ingestor = SlotGroupCapable([_group("group_a", 5), _group("group_b", 6)])

    result = validate_parallel_capability(ingestor, 0, 10, ctx=_ctx(), slot_group="group_a")

    assert result is not None
    assert result.resolved.size("group_a") == 100
    assert result.resolved.size("group_b") == 100


def test_slot_group_none_validates_all_groups_chunks() -> None:
    ingestor = SlotGroupCapable([_group("group_a", 5), _group("group_b", 6)])

    with pytest.raises(ConfigurationError, match="group_b"):
        validate_parallel_capability(ingestor, 0, 10, ctx=_ctx(), slot_group=None)


def test_slot_group_set_validates_only_that_group_chunks() -> None:
    ingestor = SlotGroupCapable([_group("group_a", 5), _group("group_b", 6)])

    result = validate_parallel_capability(ingestor, 0, 10, ctx=_ctx(), slot_group="group_a")

    assert result is not None
    assert result.resolved.size("group_a") == 100
    assert result.resolved.size("group_b") == 100


def test_slot_group_set_validates_misalignment_for_that_group() -> None:
    ingestor = SlotGroupCapable([_group("group_a", 6), _group("group_b", 5)])

    with pytest.raises(ConfigurationError, match="group_a"):
        validate_parallel_capability(ingestor, 0, 10, ctx=_ctx(), slot_group="group_a")


def test_capability_gate_skipped_when_no_slot_flags() -> None:
    class ExplodingIngestor(SlotGroupCapable):
        PRODUCT_NAME: ClassVar[str] = "slot_group_exploding"

        def index_spec(self, ctx: PluginContext) -> IndexSpec:
            raise AssertionError("index_spec should not be queried")

        def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
            raise AssertionError("zarr schema should not be queried")

    result = validate_parallel_capability(
        ExplodingIngestor([_group("group_a", 5)]),
        None,
        None,
        ctx=_ctx(),
        slot_group="group_a",
    )

    assert result is None


def test_unknown_group_error_message_includes_available_groups() -> None:
    ingestor = SlotGroupCapable([_group("aaa", 5), _group("zzz", 5)])

    with pytest.raises(ConfigurationError) as exc_info:
        validate_parallel_capability(ingestor, 0, 10, ctx=_ctx(), slot_group="bbb")

    assert "available: ['aaa', 'zzz']" in str(exc_info.value)
