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

"""Integration tests for DirectZarr pod-startup slot-index model routing.

Verifies that ``DirectZarrIngestor._ensure_slot_index_model_at_startup``
resolves and persists the slot-index model BEFORE
``_verify_schema_at_pod_startup`` runs schema mutation, and that:

* Non-opt-in plugins never trigger the gate.
* Plugin-side and control-plane-side gate failures leave the target store and
  the on-disk ``current.json`` file in their pre-call state (no partial writes).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import numpy as np
import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    SlotIndexModelRecord,
)
from firecube.core.errors import SlotIndexModelConflictError
from firecube.core.product.identity import ProductIdentity
from firecube.core.slot_index import SlotAxis, SlotIndexModel
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState
from firecube.ingestor.templates import direct_zarr

pytestmark = pytest.mark.integration


PRODUCT_NAME = "startup_slot_index_test_product"


class _CapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def __init__(self, *, chunk_manager: ChunkManager, model_name: str = "startup_v1") -> None:
        super().__init__(name=PRODUCT_NAME, chunk_manager=chunk_manager)
        self._model_name = model_name
        self.engine_config = cast(
            Any,
            SimpleNamespace(write_mode="direct", slot_start=0, slot_end=10, slot_group=None),
        )

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        _ = group
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        _ = ctx
        return {"data": 10}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        _ = ctx
        return SlotIndexModel(
            name=self._model_name,
            epoch="2026-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=1, mode="exact")},
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(10, 4),
                        dtype=np.float32,
                        chunks=(5, 4),
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []

    def ingest(self, ctx: Any) -> Any:
        _ = ctx
        raise NotImplementedError


class _NonCapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME

    def __init__(self, *, chunk_manager: ChunkManager) -> None:
        super().__init__(name=PRODUCT_NAME, chunk_manager=chunk_manager)
        self.engine_config = cast(
            Any,
            SimpleNamespace(write_mode="direct", slot_start=0, slot_end=10, slot_group=None),
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(10, 4),
                        dtype=np.float32,
                        chunks=(5, 4),
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []

    def ingest(self, ctx: Any) -> Any:
        _ = ctx
        raise NotImplementedError


def _make_chunk_manager(tmp_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(tmp_path / PRODUCT_NAME)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name=PRODUCT_NAME),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    return ChunkManager(binding=binding, workspace=tmp_path)


def _slot_index_current_path(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT_NAME / ".firecube" / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME


def _store_path(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT_NAME


def _zarr_metadata_files(tmp_path: Path) -> list[Path]:
    store = _store_path(tmp_path)
    if not store.exists():
        return []
    return [p for p in store.rglob("zarr.json") if ".firecube" not in p.parts]


def _plugin_ctx() -> Any:
    return SimpleNamespace(
        run_id="run-1",
        storage=None,
        option=lambda key, default=None: default,
    )


def _patch_resolve(
    monkeypatch: pytest.MonkeyPatch,
    ingestor: DirectZarrIngestor,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        ingestor,
        "resolve_output_uri",
        lambda ctx, write_mode: str(_store_path(tmp_path)),
    )


def test_startup_routes_through_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """On a fresh store, the slot-index gate runs BEFORE schema mutation."""
    chunk_manager = _make_chunk_manager(tmp_path)
    ingestor = _CapableIngestor(chunk_manager=chunk_manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    _patch_resolve(monkeypatch, ingestor, tmp_path)

    current_json = _slot_index_current_path(tmp_path)
    ordering_witness: dict[str, bool] = {"current_json_present_at_setup": False}

    def spy_setup_global_schema(**kwargs: Any) -> None:
        _ = kwargs
        ordering_witness["current_json_present_at_setup"] = current_json.exists()

    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", spy_setup_global_schema)

    assert not current_json.exists(), "preconditions: control-plane record absent"

    ingestor._ensure_slot_index_model_at_startup(cast(Any, _plugin_ctx()))
    ingestor._verify_schema_at_pod_startup(cast(Any, _plugin_ctx()))

    assert current_json.exists(), "slot-index current.json must be written by the gate"
    record = SlotIndexModelRecord.from_json_bytes(current_json.read_bytes())
    assert record.model.name == "startup_v1"
    assert record.recorded_by_run_id == "run-1"
    assert ordering_witness["current_json_present_at_setup"] is True, (
        "_setup_global_zarr_schema must be called AFTER the slot-index gate writes current.json"
    )


def test_non_opt_in_plugin_bypasses_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plugins without SUPPORTS_SLOT_RANGE_PARALLELISM never reach the gate."""
    chunk_manager = _make_chunk_manager(tmp_path)
    ingestor = _NonCapableIngestor(chunk_manager=chunk_manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    _patch_resolve(monkeypatch, ingestor, tmp_path)
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: None)

    slot_model_attr = _NonCapableIngestor.slot_index_model

    def fail_if_called(self, ctx):  # type: ignore[no-untyped-def]
        _ = (self, ctx)
        raise AssertionError("slot_index_model must NOT be called for non-opt-in plugins")

    monkeypatch.setattr(_NonCapableIngestor, "slot_index_model", fail_if_called)
    try:
        ingestor._verify_schema_at_pod_startup(cast(Any, _plugin_ctx()))
    finally:
        monkeypatch.setattr(_NonCapableIngestor, "slot_index_model", slot_model_attr)

    assert not _slot_index_current_path(tmp_path).exists(), (
        "non-opt-in plugin must not create slot_index/current.json"
    )


def test_failure_before_mutation_under_cp_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pre-seeded conflicting record causes the gate to raise before any Zarr write."""
    chunk_manager = _make_chunk_manager(tmp_path)
    prior_model = SlotIndexModel(
        name="prior_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"data": SlotAxis(cadence_s=1, mode="exact")},
    )
    chunk_manager.ensure_slot_index_model(
        product=PRODUCT_NAME, model=prior_model, run_id="prior-run"
    )
    current_json = _slot_index_current_path(tmp_path)
    assert current_json.exists(), "precondition: prior record was seeded"
    prior_record_bytes = current_json.read_bytes()
    prior_record = SlotIndexModelRecord.from_json_bytes(prior_record_bytes)
    assert prior_record.identity_hash == prior_model.identity_hash
    pre_conflict_zarr_files = {p: p.read_bytes() for p in _zarr_metadata_files(tmp_path)}

    ingestor = _CapableIngestor(chunk_manager=chunk_manager, model_name="new_v1")
    new_model = ingestor.slot_index_model(cast(Any, _plugin_ctx()))
    assert new_model.identity_hash != prior_model.identity_hash, (
        "precondition: new model identity differs from prior"
    )
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    _patch_resolve(monkeypatch, ingestor, tmp_path)

    setup_witness = {"called": False}

    def fail_if_called(**kwargs: Any) -> None:
        _ = kwargs
        setup_witness["called"] = True
        raise AssertionError("_setup_global_zarr_schema must NOT run after a slot-model conflict")

    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", fail_if_called)

    with pytest.raises(SlotIndexModelConflictError):
        ingestor._ensure_slot_index_model_at_startup(cast(Any, _plugin_ctx()))
        ingestor._verify_schema_at_pod_startup(cast(Any, _plugin_ctx()))

    assert setup_witness["called"] is False
    post_conflict_zarr_files = {p: p.read_bytes() for p in _zarr_metadata_files(tmp_path)}
    assert post_conflict_zarr_files == pre_conflict_zarr_files, (
        "the refused conflict must not create or modify any zarr.json files; "
        f"pre={sorted(pre_conflict_zarr_files)} post={sorted(post_conflict_zarr_files)}"
    )
    assert current_json.read_bytes() == prior_record_bytes, (
        "current.json must remain unchanged after a refused conflict"
    )


def test_failure_before_mutation_under_plugin_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plugin-side slot_index_model failure leaves the store untouched."""
    chunk_manager = _make_chunk_manager(tmp_path)
    ingestor = _CapableIngestor(chunk_manager=chunk_manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    _patch_resolve(monkeypatch, ingestor, tmp_path)

    def slot_model_raises(self, ctx):  # type: ignore[no-untyped-def]
        _ = (self, ctx)
        raise ConfigurationError("plugin-induced slot_index_model failure")

    monkeypatch.setattr(_CapableIngestor, "slot_index_model", slot_model_raises)

    setup_witness = {"called": False}

    def fail_if_called(**kwargs: Any) -> None:
        _ = kwargs
        setup_witness["called"] = True
        raise AssertionError("_setup_global_zarr_schema must NOT run after plugin failure")

    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", fail_if_called)

    current_json = _slot_index_current_path(tmp_path)
    assert not current_json.exists(), "precondition: no prior slot-index record"

    with pytest.raises(ConfigurationError, match="plugin-induced"):
        ingestor._ensure_slot_index_model_at_startup(cast(Any, _plugin_ctx()))
        ingestor._verify_schema_at_pod_startup(cast(Any, _plugin_ctx()))

    assert setup_witness["called"] is False
    assert not current_json.exists(), (
        "slot-index current.json must NOT be created when the plugin raises"
    )
    zarr_files = _zarr_metadata_files(tmp_path)
    assert zarr_files == [], (
        f"Expected 0 product-side zarr.json files after plugin failure, found: {zarr_files}"
    )


def test_second_call_returns_existing_record_without_rewrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Within one pod, the gate is idempotent and does not re-call the plugin hook."""
    chunk_manager = _make_chunk_manager(tmp_path)
    ingestor = _CapableIngestor(chunk_manager=chunk_manager)
    ingestor._parallel_execution_state = _ParallelExecutionState(global_expected={"data": 10})
    _patch_resolve(monkeypatch, ingestor, tmp_path)
    monkeypatch.setattr(direct_zarr, "_setup_global_zarr_schema", lambda **kwargs: None)

    plugin_calls = {"count": 0}
    original_slot_model = _CapableIngestor.slot_index_model

    def counting_slot_model(self, ctx):  # type: ignore[no-untyped-def]
        plugin_calls["count"] += 1
        return original_slot_model(self, ctx)

    monkeypatch.setattr(_CapableIngestor, "slot_index_model", counting_slot_model)

    ingestor._ensure_slot_index_model_at_startup(cast(Any, _plugin_ctx()))
    ingestor._verify_schema_at_pod_startup(cast(Any, _plugin_ctx()))
    first_record_bytes = _slot_index_current_path(tmp_path).read_bytes()

    ingestor._ensure_slot_index_model_at_startup(cast(Any, _plugin_ctx()))
    ingestor._verify_schema_at_pod_startup(cast(Any, _plugin_ctx()))
    second_record_bytes = _slot_index_current_path(tmp_path).read_bytes()

    assert plugin_calls["count"] == 1, (
        f"slot_index_model(ctx) must be called exactly once per pod, got {plugin_calls['count']}"
    )
    assert first_record_bytes == second_record_bytes, (
        "current.json must be byte-identical across repeated startup calls"
    )
