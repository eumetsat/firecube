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

"""Integration tests for the slot-index precedence matrix.

Locks in the 6-row decision table implemented by
``ChunkManager._apply_slot_model_precedence`` in
``src/firecube/core/controlplane/manager.py``.  Each row is its own test so
the matrix is auditable from the test names; collapsing rows would obscure
which precedence branch a regression broke.

Matrix (CP = control-plane record at ``.firecube/slot_index/current.json``,
Attrs = ``firecube_slot_index_model_identity_hash`` root attr):

+-----+-----+--------+-----------+--------------------------------------+
| Row | CP  | Attrs  | Plugin    | Action                               |
+=====+=====+========+===========+======================================+
| 1   |  -  |   -    |     X     | Write CP + mirror attrs + RECORDED   |
| 2   |  X  |   X    |     X     | Happy path; emit VERIFIED            |
| 3   |  X  |   -    |     X     | Crash-recovery: re-mirror + VERIFIED |
| 4   |  X  |   Y    |     X     | Drift detected; refuse               |
| 5   |  X  |   *    |     Y     | Plugin incompatibility; refuse       |
| 6   |  -  | present|     X     | Unmanaged store; refuse              |
+-----+-----+--------+-----------+--------------------------------------+

Event accounting is verified via a spy on ``repo.record_slot_index_model_event``
so the test does NOT depend on the WAL writer's batched flush behavior.  Claim
release is verified after both success and exception by attempting a second
``ensure_slot_index_model`` call with the same run_id — a leaked claim would
surface as ``ClaimConflictError`` on the follow-up call.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import zarr
from pytest_mock import MockerFixture
from zarr.storage import LocalStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
)
from firecube.core.errors import (
    SlotIndexModelConflictError,
    SlotIndexUnmanagedStoreError,
)
from firecube.core.product.identity import ProductIdentity
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
    SlotAxis,
    SlotIndexModel,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

pytestmark = [pytest.mark.integration, pytest.mark.architecture]

_PRODUCT = "prod1"


def _make_manager(tmp_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(tmp_path / "__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=tmp_path)


def _model(name: str = "matrix_model_v1") -> SlotIndexModel:
    return SlotIndexModel(
        name=name,
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


def _current_json(tmp_path: Path) -> Path:
    return tmp_path / _PRODUCT / ".firecube" / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME


def _zarr_root(tmp_path: Path) -> zarr.Group:
    store = LocalStore(str(tmp_path / _PRODUCT))
    return zarr.open_group(store=store, mode="a", zarr_format=3)


def _read_root_attrs_hash(tmp_path: Path) -> Any:
    try:
        store = LocalStore(str(tmp_path / _PRODUCT))
        root = zarr.open_group(store=store, mode="r", zarr_format=3)
    except Exception:
        return None
    return root.attrs.get(SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR)


def _start_run(cm: ChunkManager, tmp_path: Path, run_id: str) -> None:
    cm.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=str(tmp_path / _PRODUCT),
        output_format="zarr",
        size=0,
        meta={"plugin": "precedence_matrix_test"},
    )


def _event_counts(spy: Any) -> dict[str, int]:
    """Bucket the spy's ``record_slot_index_model_event`` calls by ``event_type``."""
    counts: dict[str, int] = {}
    for call in spy.call_args_list:
        event_type = call.kwargs.get("event_type")
        if event_type is None and len(call.args) >= 3:
            event_type = call.args[2]
        if event_type is None:
            continue
        counts[event_type] = counts.get(event_type, 0) + 1
    return counts


def _assert_claim_released(
    cm: ChunkManager, run_id: str, model: SlotIndexModel | None = None
) -> None:
    """A subsequent call with the SAME run_id must not raise ClaimConflictError.

    Idempotent same-model re-entry is safe (rows 2 and 3 both return), so this
    is a clean probe for "did the prior claim release cleanly?". A leaked claim
    file would re-raise ``ClaimConflictError`` from inside ``acquire_claim``.
    """
    cm.ensure_slot_index_model(
        product=_PRODUCT,
        model=model if model is not None else _model(),
        run_id=run_id,
        max_retries=1,
        initial_backoff_s=0.01,
    )


def test_row1_cp_absent_attrs_absent_writes_records_and_recorded(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Row 1 — fresh store: write CP, mirror attrs, emit RECORDED only."""
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "run-1")
    spy = mocker.spy(cm.repo, "record_slot_index_model_event")
    model = _model()

    record = cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")

    assert record.identity_hash == model.identity_hash
    assert _current_json(tmp_path).is_file(), "current.json must be created"
    assert _read_root_attrs_hash(tmp_path) == model.identity_hash, (
        "zarr root identity-hash attr must be mirrored"
    )
    counts = _event_counts(spy)
    assert counts == {EVENT_SLOT_INDEX_MODEL_RECORDED: 1}, (
        f"Row 1 must emit exactly one RECORDED event and nothing else; got {counts!r}"
    )
    _assert_claim_released(cm, run_id="run-1")


def test_row2_cp_present_attrs_present_emits_verified(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Row 2 — happy path: second call emits VERIFIED, never RECORDED."""
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "run-1")
    model = _model()
    cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")

    spy = mocker.spy(cm.repo, "record_slot_index_model_event")
    record = cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")

    assert record.identity_hash == model.identity_hash
    counts = _event_counts(spy)
    assert counts == {EVENT_SLOT_INDEX_MODEL_VERIFIED: 1}, (
        f"Row 2 second call must emit exactly one VERIFIED event (no RECORDED); got {counts!r}"
    )
    _assert_claim_released(cm, run_id="run-1")


def test_row3_cp_present_attrs_absent_remirrors_and_verifies(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """Row 3 — crash-recovery: attrs deleted between runs; re-mirror + VERIFIED."""
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "run-1")
    model = _model()
    cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")
    # Simulate post-crash: zarr root attrs were lost but CP record survived.
    root = _zarr_root(tmp_path)
    if SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR in root.attrs:
        del root.attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR]
    assert _read_root_attrs_hash(tmp_path) is None, "attrs must be absent before retry"

    spy = mocker.spy(cm.repo, "record_slot_index_model_event")
    record = cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")

    assert record.identity_hash == model.identity_hash
    assert _read_root_attrs_hash(tmp_path) == model.identity_hash, (
        "Row 3 must re-mirror the identity-hash attr from the CP record"
    )
    counts = _event_counts(spy)
    assert counts == {EVENT_SLOT_INDEX_MODEL_VERIFIED: 1}, (
        f"Row 3 must emit exactly one VERIFIED event (no RECORDED); got {counts!r}"
    )
    _assert_claim_released(cm, run_id="run-1")


def test_row4_cp_present_attrs_drift_raises_conflict(tmp_path: Path) -> None:
    """Row 4 — drift: attrs were tampered to a different hash; refuse."""
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "run-1")
    model = _model()
    cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")
    # Corrupt the mirrored attr to a wrong (but well-formed) identity hash.
    bogus_hash = "0" * 64
    root = _zarr_root(tmp_path)
    root.attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] = bogus_hash
    assert _read_root_attrs_hash(tmp_path) == bogus_hash, "drift must be in place"

    with pytest.raises(SlotIndexModelConflictError):
        cm.ensure_slot_index_model(product=_PRODUCT, model=model, run_id="run-1")

    # Restore attrs so the claim-release probe can run idempotently (Row 3).
    root.attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] = model.identity_hash
    _assert_claim_released(cm, run_id="run-1")


def test_row5_cp_present_plugin_declares_incompatible_model_raises(
    tmp_path: Path,
) -> None:
    """Row 5 — plugin declares a different model than the persisted one; refuse."""
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "run-1")
    model_a = _model(name="alpha_v1")
    model_b = _model(name="beta_v1")
    assert model_a.identity_hash != model_b.identity_hash, (
        "model A and B must hash differently for this row"
    )
    cm.ensure_slot_index_model(product=_PRODUCT, model=model_a, run_id="run-1")

    _start_run(cm, tmp_path, "run-2")
    with pytest.raises(SlotIndexModelConflictError):
        cm.ensure_slot_index_model(product=_PRODUCT, model=model_b, run_id="run-2")

    _assert_claim_released(cm, run_id="run-2", model=model_a)


def test_row6_cp_absent_attrs_present_unmanaged_store_raises(tmp_path: Path) -> None:
    """Row 6 — unmanaged store: root attrs exist without a CP record; refuse."""
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "run-1")
    # Manually stamp the reserved attr WITHOUT creating current.json.
    bogus_hash = "f" * 64
    root = _zarr_root(tmp_path)
    root.attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] = bogus_hash
    assert _read_root_attrs_hash(tmp_path) == bogus_hash
    assert not _current_json(tmp_path).exists(), "CP record must be absent for Row 6"

    with pytest.raises(SlotIndexUnmanagedStoreError):
        cm.ensure_slot_index_model(product=_PRODUCT, model=_model(), run_id="run-1")

    # Confirm claim released after the failure. Cleaning the bogus attr first
    # turns this into a Row 1 fresh-store probe, which idempotently succeeds.
    del root.attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR]
    cm.ensure_slot_index_model(
        product=_PRODUCT,
        model=_model(),
        run_id="run-1",
        max_retries=1,
        initial_backoff_s=0.01,
    )
    assert _read_root_attrs_hash(tmp_path) == _model().identity_hash
    on_disk = json.loads(_current_json(tmp_path).read_text())
    assert on_disk["identity_hash"] == _model().identity_hash
