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
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import numpy as np
import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)

pytestmark = pytest.mark.unit


class _ChunkManager:
    storage_config = None

    def __init__(self) -> None:
        self.audit_records: list[dict[str, Any]] = []

    def acquire_claim(self, *, product: str, domain: Any, owner_id: str):
        _ = (product, domain, owner_id)
        return nullcontext()

    def record_schema_verification(self, **kwargs: Any) -> None:
        self.audit_records.append(kwargs)

    def ensure_slot_index_model(self, *, product: str, model: Any, run_id: str) -> Any:
        _ = (product, model, run_id)
        return SimpleNamespace(identity_hash=model.identity_hash, model=model)


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(10, 4, 5),
                    dtype=np.float32,
                    chunks=(5, 4, 5),
                )
            ],
        )
    ]


class _PodStartupIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "pod_startup_phantom"

    def __init__(self, *, chunk_manager: _ChunkManager) -> None:
        super().__init__(name="pod_startup_phantom", chunk_manager=cast(Any, chunk_manager))
        self.engine_config = cast(
            Any,
            SimpleNamespace(write_mode="direct", slot_start=0, slot_end=10, slot_group=None),
        )

    def index_spec(self, ctx: Any) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="pod_startup_phantom_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=10,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: Any) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: Any) -> list[ZarrGroupSpec]:
        _ = ctx
        return _schema()

    def ingest(self, ctx: Any):  # pragma: no cover - abstract hook not used here
        _ = ctx
        raise NotImplementedError

    def build_write_intents(self, batch: Any, ctx: Any) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


def _ctx() -> Any:
    return SimpleNamespace(
        _ctx=object(), run_id="run-1", storage=None, option=lambda key, default=None: default
    )


def _patch_strategy(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    import firecube.ingestor.api as api

    class _FakeStrategy:
        def __init__(self, **kwargs: Any) -> None:
            self._store_uri = kwargs["store_uri"]
            self._storage_config = kwargs.get("storage_config")
            self._coord_names_by_group = kwargs.get("coord_names_by_group", {})

    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    monkeypatch.setattr(
        _PodStartupIngestor,
        "resolve_output_uri",
        lambda self, ctx, write_mode: str(tmp_path / "out.zarr"),
    )


def test_pod_startup_hard_fails_on_phantom_group(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_strategy(monkeypatch, tmp_path)
    ingestor = _PodStartupIngestor(chunk_manager=_ChunkManager())
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"phantom": 10})

    with pytest.raises(ConfigurationError, match="phantom"):
        ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))


def test_pod_startup_succeeds_for_valid_groups(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    manager = _ChunkManager()
    ingestor = _PodStartupIngestor(chunk_manager=manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: None)

    ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))

    assert manager.audit_records[0]["group"] == "data"


def test_pod_startup_no_audit_record_on_phantom(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_strategy(monkeypatch, tmp_path)
    manager = _ChunkManager()
    ingestor = _PodStartupIngestor(chunk_manager=manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"phantom": 10})

    with pytest.raises(ConfigurationError):
        ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))

    assert manager.audit_records == []


def test_pod_startup_hard_fail_message_actionable(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_strategy(monkeypatch, tmp_path)
    ingestor = _PodStartupIngestor(chunk_manager=_ChunkManager())
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"phantom": 10})

    with pytest.raises(ConfigurationError) as exc_info:
        ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))

    message = str(exc_info.value)
    assert "phantom" in message
    assert "zarr_schema" in message
    assert "capability gate" in message
