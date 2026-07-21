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

"""Integration tests exercising the in-tree slot-index fixture plugins.

Covers the ``tests/fixtures/slot_shape_test_plugin/`` package directly
through ``plugin.slot_index_model(ctx)`` + ``ChunkManager.ensure_slot_index_model``,
NOT via the ``firecube zarr preallocate`` CLI (that surface is locked down
in ``test_cli_zarr_preallocate_slot_index.py``).

Asserts:

* Fixed-epoch fixture: 4 groups, 600s/floor cadence, fixed 2024-09-24Z epoch.
* Option-epoch fixture: 5 groups, mixed 300s/900s exact cadence, epoch read
  from ``ctx.options["reference_epoch"]`` and normalized so ``"Z"`` vs
  ``"+00:00"`` inputs converge on the same ``identity_hash``.
* Idempotency: a second call with the same epoch emits VERIFIED only.
* Conflict: a second call with a different epoch raises
  ``SlotIndexModelConflictError`` (different epoch ⇒ different identity_hash).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
)
from firecube.core.errors import SlotIndexModelConflictError
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.types.context import (
    IngestContext,
    PluginContext,
    RuntimeIngestContext,
)

pytestmark = [pytest.mark.integration, pytest.mark.plugin]


def _make_manager(tmp_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(tmp_path / "__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=tmp_path)


def _make_plugin_ctx(**options: Any) -> PluginContext:
    ictx = IngestContext(source="/dev/null", options=dict(options))
    rctx = RuntimeIngestContext.from_ingest_context(
        ictx, run_id="fixture-test", temp_root=None, materializer=None
    )
    return PluginContext(rctx)


def _start_run(cm: ChunkManager, tmp_path: Path, product: str, run_id: str) -> None:
    cm.record_run_started(
        product=product,
        run_id=run_id,
        output_path=str(tmp_path / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "slot_index_fixture_plugin_test"},
    )


def _current_json(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube" / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME


def _event_counts(spy: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for call in spy.call_args_list:
        et = call.kwargs.get("event_type")
        if et is None:
            continue
        counts[et] = counts.get(et, 0) + 1
    return counts


def test_fixed_epoch_shape_records_four_groups(tmp_path: Path) -> None:
    from slot_shape_test_plugin import FixedEpochShapeIngestor

    plugin = FixedEpochShapeIngestor()
    ctx = _make_plugin_ctx()
    model = plugin.slot_index_model(ctx)

    cm = _make_manager(tmp_path)
    product = FixedEpochShapeIngestor.PRODUCT_NAME
    _start_run(cm, tmp_path, product, "run-1")
    record = cm.ensure_slot_index_model(product=product, model=model, run_id="run-1")

    assert record.identity_hash == model.identity_hash
    on_disk = json.loads(_current_json(tmp_path, product).read_text())
    groups = on_disk["model"]["groups"]
    assert set(groups.keys()) == {
        "groupA/data_hi",
        "groupA/data_lo",
        "groupB/data_hi",
        "groupB/data_lo",
    }, groups
    for axis in groups.values():
        assert axis["cadence_s"] == 600
        assert axis["mode"] == "floor"
    assert on_disk["model"]["epoch"] == "2024-09-24T00:00:00Z"
    assert on_disk["model"]["name"] == "fixed_epoch_shape_v1"


def test_option_epoch_shape_records_five_groups_with_option_epoch(
    tmp_path: Path,
) -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    model = plugin.slot_index_model(ctx)

    cm = _make_manager(tmp_path)
    product = OptionEpochShapeIngestor.PRODUCT_NAME
    _start_run(cm, tmp_path, product, "run-1")
    record = cm.ensure_slot_index_model(product=product, model=model, run_id="run-1")

    assert record.identity_hash == model.identity_hash
    on_disk = json.loads(_current_json(tmp_path, product).read_text())
    groups = on_disk["model"]["groups"]
    assert set(groups.keys()) == {
        "fast_a/data",
        "fast_b/data",
        "slow_a/data",
        "slow_b/data",
        "slow_c/data",
    }, groups
    assert groups["fast_a/data"] == {"cadence_s": 300, "mode": "exact"}
    assert groups["fast_b/data"] == {"cadence_s": 300, "mode": "exact"}
    assert groups["slow_a/data"] == {"cadence_s": 900, "mode": "exact"}
    assert groups["slow_b/data"] == {"cadence_s": 900, "mode": "exact"}
    assert groups["slow_c/data"] == {"cadence_s": 900, "mode": "exact"}
    assert on_disk["model"]["epoch"] == "2024-01-01T00:00:00Z"
    assert on_disk["model"]["name"] == "option_epoch_shape_v1"


def test_option_epoch_idempotent_same_epoch_emits_verified(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    cm = _make_manager(tmp_path)
    product = OptionEpochShapeIngestor.PRODUCT_NAME

    _start_run(cm, tmp_path, product, "run-1")
    first = cm.ensure_slot_index_model(
        product=product, model=plugin.slot_index_model(ctx), run_id="run-1"
    )

    spy = mocker.spy(cm.repo, "record_slot_index_model_event")
    _start_run(cm, tmp_path, product, "run-2")
    second = cm.ensure_slot_index_model(
        product=product, model=plugin.slot_index_model(ctx), run_id="run-2"
    )

    assert first.identity_hash == second.identity_hash
    counts = _event_counts(spy)
    assert counts == {EVENT_SLOT_INDEX_MODEL_VERIFIED: 1}, (
        f"second call must emit exactly one VERIFIED (no RECORDED); got {counts!r}"
    )


def test_option_epoch_different_epoch_raises_conflict(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx_a = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    ctx_b = _make_plugin_ctx(reference_epoch="2025-01-01T00:00:00Z")
    cm = _make_manager(tmp_path)
    product = OptionEpochShapeIngestor.PRODUCT_NAME

    model_a = plugin.slot_index_model(ctx_a)
    model_b = plugin.slot_index_model(ctx_b)
    assert model_a.identity_hash != model_b.identity_hash, (
        "different epochs must produce different identity hashes"
    )

    _start_run(cm, tmp_path, product, "run-1")
    cm.ensure_slot_index_model(product=product, model=model_a, run_id="run-1")

    spy = mocker.spy(cm.repo, "record_slot_index_model_event")
    _start_run(cm, tmp_path, product, "run-2")
    with pytest.raises(SlotIndexModelConflictError):
        cm.ensure_slot_index_model(product=product, model=model_b, run_id="run-2")

    counts = _event_counts(spy)
    assert counts.get(EVENT_SLOT_INDEX_MODEL_RECORDED, 0) == 0, (
        f"conflict must not write RECORDED; got {counts!r}"
    )


def test_option_epoch_z_and_plus_zero_epoch_converge_via_normalize(tmp_path: Path) -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx_z = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    ctx_plus = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00+00:00")

    model_z = plugin.slot_index_model(ctx_z)
    model_plus = plugin.slot_index_model(ctx_plus)

    assert model_z.identity_hash == model_plus.identity_hash, (
        "fixture must normalize epoch ISO so 'Z' and '+00:00' converge; "
        f"got Z={model_z.epoch!r} +00:00={model_plus.epoch!r}"
    )

    cm = _make_manager(tmp_path)
    product = OptionEpochShapeIngestor.PRODUCT_NAME
    _start_run(cm, tmp_path, product, "run-1")
    cm.ensure_slot_index_model(product=product, model=model_z, run_id="run-1")
    _start_run(cm, tmp_path, product, "run-2")
    record = cm.ensure_slot_index_model(product=product, model=model_plus, run_id="run-2")
    assert record.identity_hash == model_z.identity_hash
