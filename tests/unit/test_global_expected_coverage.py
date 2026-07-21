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

from firecube.core.api import SlotAxis, SlotIndexModel
from firecube.ingestor.api import DirectZarrIngestor
from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.unit


def _schema(groups: list[str]) -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group=group,
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
        for group in groups
    ]


def _intent(group: str, ts_index: int = 0) -> WriteIntent:
    return WriteIntent(
        group=group,
        array="values",
        ts_index=ts_index,
        data=np.zeros((4, 5), dtype=np.float32),
        y_slice=slice(0, 4),
    )


class _ChunkManager:
    storage_config = None

    def acquire_claim(self, *, product: str, domain: Any, owner_id: str):
        _ = (product, domain, owner_id)
        return nullcontext()


def _make_ingestor(
    *,
    schema_groups: list[str],
    global_expected: dict[str, int],
    intents: list[WriteIntent],
    slot_start: int | None = 0,
    slot_end: int | None = 10,
) -> DirectZarrIngestor:
    class _Ingestor(DirectZarrIngestor):
        PRODUCT_NAME: ClassVar[str] = "global_expected_test"
        SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

        def __init__(self) -> None:
            super().__init__(name="global_expected_test", chunk_manager=cast(Any, _ChunkManager()))
            self.engine_config = cast(
                Any,
                SimpleNamespace(
                    write_mode="direct",
                    slot_start=slot_start,
                    slot_end=slot_end,
                    slot_group=None,
                ),
            )

        def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
            _ = group
            return int(timestamp_val)

        def global_expected_time_count(self, ctx):
            _ = ctx
            return global_expected

        def slot_index_model(self, ctx):
            _ = ctx
            return SlotIndexModel(
                name="global_expected_test_v1",
                epoch="2026-01-01T00:00:00Z",
                groups={g: SlotAxis(cadence_s=1, mode="exact") for g in schema_groups},
            )

        def zarr_schema(self, ctx):
            _ = ctx
            return _schema(schema_groups)

        def build_write_intents(self, batch, ctx):
            _ = (batch, ctx)
            return intents

        def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
            raise NotImplementedError

    return _Ingestor()


def _run_process_batch(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ingestor: DirectZarrIngestor,
    global_schema: dict[str, int],
    setup_stub: Any,
):
    import firecube.ingestor.api as api
    import firecube.ingestor.templates.direct_zarr as direct_zarr

    captured: dict[str, Any] = {}

    class _FakeStrategy:
        def __init__(self, **kwargs) -> None:
            captured["strategy_kwargs"] = kwargs

        def write_groups(self, **kwargs):
            captured["write_groups_kwargs"] = kwargs
            return {"coverage": []}

    monkeypatch.setattr(
        ingestor, "resolve_output_uri", lambda ctx, write_mode: str(tmp_path / "out.zarr")
    )
    monkeypatch.setattr(api, "IndexedRegionStrategy", _FakeStrategy)
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", setup_stub)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected=global_schema)

    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        option=lambda key, default=None: default,
    )
    result = ingestor._process_batch(cast(Any, SimpleNamespace(items=["x"])), cast(Any, ctx))
    return result, captured


def test_intent_group_missing_from_global_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor = _make_ingestor(
        schema_groups=["A", "B"],
        global_expected={"A": 1000},
        intents=[_intent("B")],
    )

    result, _ = _run_process_batch(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        ingestor=ingestor,
        global_schema={"A": 1000},
        setup_stub=lambda **kwargs: None,
    )

    assert result.success is False
    assert "B" in cast(str, result.error)
    assert "global_expected_time_count()" in cast(str, result.error)


def test_intent_group_missing_from_schema_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor = _make_ingestor(
        schema_groups=["A", "B"],
        global_expected={"A": 1000, "B": 1000, "G_unknown": 1000},
        intents=[_intent("G_unknown")],
    )

    result, _ = _run_process_batch(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        ingestor=ingestor,
        global_schema={"A": 1000, "B": 1000, "G_unknown": 1000},
        setup_stub=lambda **kwargs: None,
    )

    assert result.success is False
    assert "G_unknown" in cast(str, result.error)


def test_extra_groups_in_global_allowed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor = _make_ingestor(
        schema_groups=["A"],
        global_expected={"A": 1000, "G_extra": 1000},
        intents=[_intent("A")],
    )

    result, captured = _run_process_batch(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        ingestor=ingestor,
        global_schema={"A": 1000, "G_extra": 1000},
        setup_stub=lambda **kwargs: None,
    )

    assert result.success is True
    assert "write_groups_kwargs" in captured


def test_all_groups_match_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor = _make_ingestor(
        schema_groups=["A", "B"],
        global_expected={"A": 1000, "B": 1000},
        intents=[_intent("A"), _intent("B")],
    )

    result, captured = _run_process_batch(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        ingestor=ingestor,
        global_schema={"A": 1000, "B": 1000},
        setup_stub=lambda **kwargs: None,
    )

    assert result.success is True
    assert set(captured["write_groups_kwargs"]["group_to_intents"]) == {"A", "B"}


def test_no_intents_skip_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor = _make_ingestor(
        schema_groups=["A", "B"],
        global_expected={"A": 1000, "B": 1000},
        intents=[],
    )

    result, captured = _run_process_batch(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        ingestor=ingestor,
        global_schema={"A": 1000, "B": 1000},
        setup_stub=lambda **kwargs: (_ for _ in ()).throw(AssertionError("setup must not run")),
    )

    assert result.success is True
    assert captured == {}


def test_non_parallel_mode_skip_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor = _make_ingestor(
        schema_groups=["A", "B"],
        global_expected={"A": 1000, "B": 1000},
        intents=[_intent("A")],
        slot_start=None,
        slot_end=None,
    )

    result, captured = _run_process_batch(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        ingestor=ingestor,
        global_schema={"A": 1000, "B": 1000},
        setup_stub=lambda **kwargs: (_ for _ in ()).throw(AssertionError("setup must not run")),
    )

    assert result.success is True
    assert captured["write_groups_kwargs"]["slot_range"] is None
