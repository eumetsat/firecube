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
        self.fail_audit = False
        self.slot_index_model_calls: list[dict[str, Any]] = []

    def acquire_claim(self, *, product: str, domain: Any, owner_id: str):
        _ = (product, domain, owner_id)
        return nullcontext()

    def record_schema_verification(self, **kwargs: Any) -> None:
        if self.fail_audit:
            raise RuntimeError("audit unavailable")
        self.audit_records.append(kwargs)

    def ensure_slot_index_model(self, *, product: str, model: Any, run_id: str) -> Any:
        self.slot_index_model_calls.append({"product": product, "model": model, "run_id": run_id})
        return SimpleNamespace(identity_hash=model.identity_hash, model=model)


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    fill_value=0.0,
                )
            ],
        )
    ]


class _CapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "schema_startup_test"

    def __init__(
        self, *, chunk_manager: _ChunkManager, intents: list[WriteIntent] | None = None
    ) -> None:
        super().__init__(name="schema_startup_test", chunk_manager=cast(Any, chunk_manager))
        self.engine_config = cast(
            Any,
            SimpleNamespace(write_mode="direct", slot_start=0, slot_end=10, slot_group=None),
        )
        self._intents = intents if intents is not None else [_intent()]

    def index_spec(self, ctx: Any) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="schema_startup_test_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=10,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: Any) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def ingest(self, ctx: Any):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx: Any) -> list[ZarrGroupSpec]:
        _ = ctx
        return _schema()

    def build_write_intents(self, batch: Any, ctx: Any) -> list[WriteIntent]:
        _ = (batch, ctx)
        return self._intents


def _intent() -> WriteIntent:
    return WriteIntent(
        group="data",
        array="values",
        ts_index=0,
        data=np.zeros((4, 5), dtype=np.float32),
        y_slice=slice(0, 4),
    )


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

        def write_groups(self, **kwargs: Any) -> dict[str, Any]:
            _ = kwargs
            return {"coverage": []}

    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    monkeypatch.setattr(
        _CapableIngestor,
        "resolve_output_uri",
        lambda self, ctx, write_mode: str(tmp_path / "out.zarr"),
    )


def test_setup_called_once_not_per_batch(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    manager = _ChunkManager()
    ingestor = _CapableIngestor(chunk_manager=manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: calls.append(kwargs)
    )

    ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))
    ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))
    result = ingestor._process_batch(cast(Any, SimpleNamespace(items=["a"])), cast(Any, _ctx()))

    assert result.success is True
    assert len(calls) == 1


def test_setup_skipped_in_non_parallel_mode(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    ingestor = _CapableIngestor(chunk_manager=_ChunkManager())
    monkeypatch.setattr(
        direct_zarr,
        "_setup_global_zarr_schema",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))


def test_schema_verified_flag_set_after_success(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    ingestor = _CapableIngestor(chunk_manager=_ChunkManager())
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: None)

    ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))

    assert ingestor._parallel_execution_state.schema_verified == {"data": True}


def test_audit_record_written_on_success(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    manager = _ChunkManager()
    ingestor = _CapableIngestor(chunk_manager=manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: None)

    ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))

    assert manager.audit_records[0]["group"] == "data"
    assert manager.audit_records[0]["plugin"] == "schema_startup_test"
    assert len(manager.audit_records[0]["schema_hash"]) == 16


def test_audit_record_failure_does_not_block_ingestion(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    manager = _ChunkManager()
    manager.fail_audit = True
    ingestor = _CapableIngestor(chunk_manager=manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: None)

    with caplog.at_level("WARNING"):
        ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))
    result = ingestor._process_batch(cast(Any, SimpleNamespace(items=["a"])), cast(Any, _ctx()))

    assert result.success is True
    assert "Failed to record schema verification audit event" in caplog.text


def test_phantom_group_now_hard_fails(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Previously-silent phantom group case now raises ConfigurationError."""
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    _patch_strategy(monkeypatch, tmp_path)
    manager = _ChunkManager()
    ingestor = _CapableIngestor(chunk_manager=manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"phantom": 10})
    monkeypatch.setattr(
        direct_zarr,
        "_setup_global_zarr_schema",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not run for phantom")),
    )

    with pytest.raises(ConfigurationError, match="phantom"):
        ingestor._verify_schema_at_pod_startup(cast(Any, _ctx()))

    assert manager.audit_records == []
