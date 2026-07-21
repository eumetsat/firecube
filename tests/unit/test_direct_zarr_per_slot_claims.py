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
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrGroupSpec,
)
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


class _DummyDirectIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "dummy_direct"

    def __init__(self, *, intents: list[WriteIntent], chunk_manager: Any) -> None:
        super().__init__(name="dummy_direct", chunk_manager=chunk_manager)
        self.engine_config = cast(Any, SimpleNamespace(write_mode="direct", slot_group=None))
        self._intents = intents

    def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx):
        return [ZarrGroupSpec(group="F024", arrays=[])]

    def build_write_intents(self, batch, ctx):
        return self._intents


class _Writer:
    def ensure_group(self, group: str, **kwargs) -> None:
        _ = (group, kwargs)


def _intent(ts_index: int, *, group: str = "F024") -> WriteIntent:
    return WriteIntent(
        group=group,
        array="data",
        ts_index=ts_index,
        data=None,
        y_slice=slice(0, 1),
    )


def _patch_strategy_io(monkeypatch: pytest.MonkeyPatch, calls: list[object] | None = None) -> None:
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        lambda *args, **kwargs: _Writer(),
    )

    def _dispatch(writer, intent) -> None:
        if calls is not None:
            calls.append(("dispatch", intent.ts_index))

    monkeypatch.setattr(IndexedRegionStrategy, "_dispatch_intent", staticmethod(_dispatch))


def _run_direct_process_batch(monkeypatch: pytest.MonkeyPatch, intents: list[WriteIntent]):
    import firecube.ingestor.api as api

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

        def write_groups(self, **kwargs):
            captured.update(kwargs)
            group_to_intents = kwargs["group_to_intents"]
            kwargs["claim_for_group"]("F024")
            for ts_index in sorted({intent.ts_index for intent in group_to_intents["F024"]}):
                kwargs["claim_for_slot"]("F024", ts_index)
            return {"coverage": []}

    captured: dict[str, object] = {}
    chunk_manager = SimpleNamespace(
        storage_config=SimpleNamespace(),
        acquire_claim=MagicMock(return_value=nullcontext()),
    )
    ingestor = _DummyDirectIngestor(intents=intents, chunk_manager=chunk_manager)
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: "out.zarr")
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)

    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        option=lambda key, default=None: default,
    )
    batch = SimpleNamespace(items=["source"], metadata={})

    result = ingestor._process_batch(cast(Any, batch), cast(Any, ctx))
    return result, captured, chunk_manager.acquire_claim


def test_schema_claim_acquired_once_per_group(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_strategy_io(monkeypatch)
    calls: list[str] = []

    def claim_for_group(group_name: str):
        calls.append(group_name)
        return nullcontext()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(index) for index in range(5)]},
        claim_for_group=claim_for_group,
        claim_for_slot=lambda group_name, ts_index: nullcontext(),
    )

    assert calls == ["F024"]


def test_same_ts_index_shares_slot_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_strategy_io(monkeypatch)
    calls: list[tuple[str, int]] = []

    def claim_for_slot(group_name: str, ts_index: int):
        calls.append((group_name, ts_index))
        return nullcontext()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(5), _intent(5), _intent(5)]},
        claim_for_slot=claim_for_slot,
    )

    assert calls == [("F024", 5)]


def test_schema_claim_uses_correct_domain_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, acquire_claim = _run_direct_process_batch(monkeypatch, [_intent(5)])

    schema_call = acquire_claim.call_args_list[0]
    assert schema_call.kwargs["domain"] == WriteDomain(
        product="dummy_direct", category="zarr_region", name="F024:schema"
    )
    assert schema_call.kwargs["owner_id"] == "run-1:F024"


def test_slot_claim_uses_correct_domain_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, acquire_claim = _run_direct_process_batch(monkeypatch, [_intent(5)])

    slot_call = acquire_claim.call_args_list[1]
    assert slot_call.kwargs["domain"] == WriteDomain(
        product="dummy_direct", category="zarr_region", name="F024:slot=5"
    )
    assert slot_call.kwargs["owner_id"] == "run-1:F024:slot=5"


def test_empty_intents_acquires_no_slot_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    import firecube.ingestor.api as api

    slot_claim = MagicMock(return_value=nullcontext())

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            pass

        def write_groups(self, **kwargs):
            return {"coverage": []}

    chunk_manager = SimpleNamespace(storage_config=SimpleNamespace(), acquire_claim=slot_claim)
    ingestor = _DummyDirectIngestor(intents=[], chunk_manager=chunk_manager)
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: "out.zarr")
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    ctx = SimpleNamespace(run_id="run-1", storage=None, option=lambda key, default=None: default)

    result = ingestor._process_batch(
        cast(Any, SimpleNamespace(items=[], metadata={})), cast(Any, ctx)
    )

    assert result.success is True
    slot_claim.assert_not_called()


def test_fallback_chain_claim_for_slot_none(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    _patch_strategy_io(monkeypatch, calls)

    def claim_for_group(group_name: str):
        calls.append(("group_claim", group_name))
        return nullcontext()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(0), _intent(1)]},
        claim_for_group=claim_for_group,
        claim_for_slot=None,
    )

    assert calls == [
        ("group_claim", "F024"),
        ("group_claim", "F024"),
        ("dispatch", 0),
        ("group_claim", "F024"),
        ("dispatch", 1),
    ]


def test_fallback_chain_both_none(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    _patch_strategy_io(monkeypatch, calls)

    IndexedRegionStrategy(store_uri="/tmp/test.zarr").write_groups(
        group_to_intents={"F024": [_intent(0)]},
        claim_for_group=None,
        claim_for_slot=None,
    )

    assert calls == [("dispatch", 0)]


def test_high_slot_count_no_leftover_claims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_strategy_io(monkeypatch)
    product = "product.zarr"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product), workspace=workspace
    )

    def claim_for_slot(group_name: str, ts_index: int):
        domain = WriteDomain(
            product=product, category="zarr_region", name=f"{group_name}:slot={ts_index}"
        )
        return manager.acquire_claim(
            product=product,
            domain=domain,
            owner_id=f"run-1:{group_name}:slot={ts_index}",
        )

    try:
        IndexedRegionStrategy(store_uri=str(tmp_path / product)).write_groups(
            group_to_intents={"F024": [_intent(index) for index in range(100)]},
            claim_for_slot=claim_for_slot,
        )
        claim_files = list((tmp_path / product / ".firecube" / "claims").glob("*.json"))
        assert claim_files == []
    finally:
        manager.close()
