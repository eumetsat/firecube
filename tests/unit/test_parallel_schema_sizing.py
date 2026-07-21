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

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr

from firecube.core.api import SlotAxis, SlotIndexModel
from firecube.core.controlplane import WriteDomain
from firecube.ingestor.errors import ConfigurationError, SchemaSizeMismatchError
from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    _setup_global_zarr_schema,
)

pytestmark = pytest.mark.unit


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


class _ChunkManager:
    storage_config = None

    def __init__(self) -> None:
        self.domains: list[WriteDomain] = []

    def acquire_claim(self, *, product: str, domain: WriteDomain, owner_id: str):
        _ = (product, owner_id)
        self.domains.append(domain)
        return nullcontext()


class _CapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "schema_test"
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def __init__(self, *, intents: list[WriteIntent], chunk_manager: Any) -> None:
        super().__init__(name="schema_test", chunk_manager=chunk_manager)
        self.engine_config = cast(
            Any,
            SimpleNamespace(write_mode="direct", slot_start=0, slot_end=10, slot_group=None),
        )
        self._intents = intents

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx) -> dict[str, int] | None:
        return {"data": 1000}

    def slot_index_model(self, ctx) -> SlotIndexModel:
        return SlotIndexModel(
            name="parallel_schema_sizing_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=1, mode="exact")},
        )

    def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx):
        return _schema()

    def build_write_intents(self, batch, ctx):
        return self._intents


def _intent(ts_index: int) -> WriteIntent:
    return WriteIntent(
        group="data",
        array="values",
        ts_index=ts_index,
        data=np.zeros((4, 5), dtype=np.float32),
        y_slice=slice(0, 4),
    )


def test_parallel_schema_uses_global_expected_not_batch_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import firecube.ingestor.api as api
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    captured: dict[str, Any] = {}

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            self._store_uri = kwargs["store_uri"]

        def write_groups(self, **kwargs):
            captured["slot_range"] = kwargs["slot_range"]
            return {"coverage": []}

    def fail_setup(**kwargs) -> None:
        _ = kwargs
        raise AssertionError("global schema setup must run at pod startup, not per batch")

    manager = _ChunkManager()
    ingestor = _CapableIngestor(intents=[_intent(3)], chunk_manager=manager)
    monkeypatch.setattr(
        ingestor, "resolve_output_uri", lambda ctx, write_mode: str(tmp_path / "out.zarr")
    )
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", fail_setup)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 1000})

    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        option=lambda key, default=None: default,
    )
    result = ingestor._process_batch(cast(Any, SimpleNamespace(items=["x"])), cast(Any, ctx))

    assert result.success is True
    assert captured["slot_range"] == (0, 10)


def test_empty_global_dict_fails(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="empty"):
        _setup_global_zarr_schema(
            strategy=SimpleNamespace(_store_uri=str(tmp_path / "out.zarr"), _storage_config=None),
            schema=_schema(),
            global_expected={},
            product="product",
            run_id="run-1",
            chunk_manager=_ChunkManager(),
        )


def test_non_positive_global_count_fails(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="non-positive"):
        _setup_global_zarr_schema(
            strategy=SimpleNamespace(_store_uri=str(tmp_path / "out.zarr"), _storage_config=None),
            schema=_schema(),
            global_expected={"data": 0},
            product="product",
            run_id="run-1",
            chunk_manager=_ChunkManager(),
        )


def test_global_schema_creates_at_correct_size(tmp_path: Path) -> None:
    store_path = str(tmp_path / "out.zarr")
    manager = _ChunkManager()

    _setup_global_zarr_schema(
        strategy=SimpleNamespace(_store_uri=store_path, _storage_config=None),
        schema=_schema(),
        global_expected={"data": 25},
        product="product",
        run_id="run-1",
        chunk_manager=manager,
    )

    arr = cast(Any, zarr.open_group(store=store_path, mode="r", zarr_format=3)["data/values"])
    assert arr.shape == (25, 4, 5)
    assert manager.domains == [
        WriteDomain(product="product", category="zarr_schema_global", name="data:setup")
    ]


def test_existing_smaller_array_raises_schema_size_mismatch(tmp_path: Path) -> None:
    store_path = str(tmp_path / "out.zarr")
    root = zarr.open_group(store=store_path, mode="w", zarr_format=3)
    group = root.require_group("data")
    group.create_array("values", shape=(5, 4, 5), dtype=np.float32, chunks=(1, 4, 5))

    with pytest.raises(SchemaSizeMismatchError, match="existing array shape"):
        _setup_global_zarr_schema(
            strategy=SimpleNamespace(_store_uri=store_path, _storage_config=None),
            schema=_schema(),
            global_expected={"data": 25},
            product="product",
            run_id="run-1",
            chunk_manager=_ChunkManager(),
        )


def test_no_slot_range_uses_phase2_batch_local_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import firecube.ingestor.api as api
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    captured: dict[str, Any] = {}

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            pass

        def write_groups(self, **kwargs):
            captured.update(kwargs)
            return {"coverage": []}

    def fail_setup(**kwargs) -> None:
        raise AssertionError("global schema setup must not run")

    manager = _ChunkManager()
    ingestor = _CapableIngestor(intents=[_intent(3)], chunk_manager=manager)
    ingestor.engine_config = cast(
        Any,
        SimpleNamespace(write_mode="direct", slot_start=None, slot_end=None, slot_group=None),
    )
    monkeypatch.setattr(
        ingestor, "resolve_output_uri", lambda ctx, write_mode: str(tmp_path / "out.zarr")
    )
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", fail_setup)

    ctx = SimpleNamespace(
        _ctx=SimpleNamespace(_parallel_global_schema={"data": 1000}),
        run_id="run-1",
        storage=None,
        option=lambda key, default=None: default,
    )
    result = ingestor._process_batch(cast(Any, SimpleNamespace(items=["x"])), cast(Any, ctx))

    assert result.success is True
    assert captured["slot_range"] is None
