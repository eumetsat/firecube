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

"""Convergence-policy alignment between ``ensure_resolved_index`` and
``ensure_slot_index_model``.

Both primitives must apply the SAME 5-row policy in their ClaimConflictError
loser branch — divergence causes one primitive to self-heal via re-mirror
while the other refuses loudly, breaking operator expectations and pre-mirror
cube startup:

+-----+------------+---------------------+-----------------------+
| Row | CP record  | Attrs hash          | Action                |
+=====+============+=====================+=======================+
| 1   | matches    | absent (None)       | re-mirror + accept    |
| 2   | matches    | transient read err  | propagate (raise)     |
| 3   | matches    | matches             | accept                |
| 4   | matches    | mismatches          | reject (drift error)  |
| 5   | mismatches | *                   | reject (CP conflict)  |
+-----+------------+---------------------+-----------------------+

Each row is parametrized over both primitives so the assertion pair proves the
policies stayed aligned. The transient-error row exercises the D14e exception
categorization landed by task 29.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    ResolvedIndexRecord,
    SlotIndexModelRecord,
    canonical_index_bytes,
)
from firecube.core.errors import (
    ClaimConflictError,
    ManifestError,
    ResolvedIndexConflictError,
    SlotIndexModelConflictError,
)
from firecube.core.slot_index import SlotAxis, SlotIndexModel
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.architecture]

_PRODUCT = "prod-align"


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


def _resolved_index(*, name: str = "align_index") -> dict[str, Any]:
    return {
        "schema_version": "v1",
        "name": name,
        "groups": {"data_1km": {"kind": "integer", "size": 3, "params": {}}},
    }


def _resolved_record(
    *, index: dict[str, Any] | None = None, run_id: str = "run-loser"
) -> ResolvedIndexRecord:
    payload = _resolved_index() if index is None else index
    return ResolvedIndexRecord(
        recorded_at="2026-08-20T00:00:00+00:00",
        recorded_by_run_id=run_id,
        identity_hash=hashlib.sha256(canonical_index_bytes(payload)).hexdigest(),
        index=payload,
    )


def _slot_model(*, name: str = "align_model_v1") -> SlotIndexModel:
    return SlotIndexModel(
        name=name,
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


def _slot_cp_record(model: SlotIndexModel) -> SlotIndexModelRecord:
    return SlotIndexModelRecord(
        model=model,
        identity_hash=model.identity_hash,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00+00:00",
        recorded_by_run_id="winner",
    )


class _ResolvedInvocation:
    """Bindings for ``ChunkManager.ensure_resolved_index``."""

    conflict_error: type[BaseException] = ResolvedIndexConflictError
    drift_error: type[BaseException] = ManifestError
    get_cp_method: str = "get_resolved_index"
    read_attrs_method: str = "read_resolved_index_attrs_hash"
    mirror_method: str = "_mirror_resolved_index_attrs"

    def __init__(self, cm: ChunkManager) -> None:
        self.cm = cm
        self.declared = _resolved_record()

    def call(self) -> Any:
        return self.cm.ensure_resolved_index(
            product=_PRODUCT,
            record=self.declared,
            max_retries=0,
            initial_backoff_s=0.001,
        )

    def matching_cp(self) -> Any:
        return self.declared

    def mismatched_cp(self) -> Any:
        return _resolved_record(index=_resolved_index(name="different_index"), run_id="run-other")

    def declared_hash(self) -> str:
        return self.declared.identity_hash

    def extract_record(self, result: Any) -> Any:
        record, outcome = result
        assert outcome == "matched_existing"
        return record


class _SlotInvocation:
    """Bindings for ``ChunkManager.ensure_slot_index_model``."""

    conflict_error: type[BaseException] = SlotIndexModelConflictError
    drift_error: type[BaseException] = SlotIndexModelConflictError
    get_cp_method: str = "get_slot_index_model"
    read_attrs_method: str = "read_slot_index_attrs_hash"
    mirror_method: str = "_mirror_attrs"

    def __init__(self, cm: ChunkManager) -> None:
        self.cm = cm
        self.model = _slot_model()

    def call(self) -> Any:
        return self.cm.ensure_slot_index_model(
            product=_PRODUCT,
            model=self.model,
            run_id="loser",
            max_retries=0,
            initial_backoff_s=0.001,
        )

    def matching_cp(self) -> Any:
        return _slot_cp_record(self.model)

    def mismatched_cp(self) -> Any:
        return _slot_cp_record(_slot_model(name="other_variant_v1"))

    def declared_hash(self) -> str:
        return self.model.identity_hash

    def extract_record(self, result: Any) -> Any:
        return result


@pytest.fixture(
    params=[
        pytest.param(_ResolvedInvocation, id="ensure_resolved_index"),
        pytest.param(_SlotInvocation, id="ensure_slot_index_model"),
    ]
)
def invocation_cls(request: pytest.FixtureRequest) -> type[Any]:
    return request.param


def _install_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_conflict(self: ChunkManager, *_args: Any, **_kwargs: Any) -> Any:
        _ = self
        raise ClaimConflictError("winner-holds-claim")

    monkeypatch.setattr(ChunkManager, "acquire_claim", _raise_conflict)


def _suppress_slot_event(monkeypatch: pytest.MonkeyPatch, cm: ChunkManager) -> list[str]:
    """Suppress ``record_slot_index_model_event`` and capture invocations.

    The slot loser branch emits ``EVENT_SLOT_INDEX_MODEL_VERIFIED`` after
    accepting; the resolved-index primitive has no equivalent WAL event on
    the loser branch. Returning a shared capture list lets Row 1 / Row 3
    tests verify the slot primitive emitted VERIFIED without polluting the
    resolved-index parametrization.
    """
    events: list[str] = []

    def _record(*, event_type: str, **_: Any) -> None:
        events.append(event_type)

    monkeypatch.setattr(cm.repo, "record_slot_index_model_event", _record)
    return events


def _suppress_resolved_conflict_wal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ChunkManager,
        "_record_conflict_refused_index_ensured_event",
        lambda self, *, product, record: None,
    )


def test_row1_cp_match_attrs_absent_remirrors_and_accepts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invocation_cls: type[Any],
) -> None:
    """Row 1: CP matches + attrs absent (None) → re-mirror + accept.

    Backward-compat for pre-attrs-mirror cubes and post-crash stale-claim
    scenarios: the loser must NOT refuse; it must re-mirror attrs from the
    authoritative CP record so older cubes converge cleanly on startup.
    """
    cm = _make_manager(tmp_path)
    inv = invocation_cls(cm)
    _install_conflict(monkeypatch)

    cp = inv.matching_cp()
    monkeypatch.setattr(ChunkManager, inv.get_cp_method, lambda self, *, product: cp)
    monkeypatch.setattr(ChunkManager, inv.read_attrs_method, lambda self, *, product: None)

    mirror_calls: list[tuple[str, Any]] = []

    def _mirror(self: ChunkManager, product: str, record: Any) -> None:
        _ = self
        mirror_calls.append((product, record))

    monkeypatch.setattr(ChunkManager, inv.mirror_method, _mirror)
    slot_events = _suppress_slot_event(monkeypatch, cm)

    result = inv.call()

    assert mirror_calls == [(_PRODUCT, cp)], (
        f"Row 1 must call re-mirror exactly once with the CP record; got {mirror_calls!r}"
    )
    assert inv.extract_record(result) == cp

    if isinstance(inv, _SlotInvocation):
        assert slot_events == [EVENT_SLOT_INDEX_MODEL_VERIFIED], (
            f"slot loser must emit VERIFIED after re-mirror; got {slot_events!r}"
        )


@pytest.mark.parametrize(
    "transient_exc",
    [
        PermissionError("denied"),
        OSError("io error"),
        TimeoutError("timed out"),
        ValueError("parse error"),
    ],
    ids=["permission", "oserror", "timeout", "value-error"],
)
def test_row2_cp_match_attrs_transient_error_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invocation_cls: type[Any],
    transient_exc: BaseException,
) -> None:
    """Row 2: CP matches + attrs read raises transient error → propagate.

    Task 29 (D14e) categorized these as transient (permission/IO/parse) —
    silently swallowing them here would mask cluster-wide storage faults.
    """
    cm = _make_manager(tmp_path)
    inv = invocation_cls(cm)
    _install_conflict(monkeypatch)

    cp = inv.matching_cp()
    monkeypatch.setattr(ChunkManager, inv.get_cp_method, lambda self, *, product: cp)

    def _raise_transient(self: ChunkManager, *, product: str) -> Any:
        _ = (self, product)
        raise transient_exc

    monkeypatch.setattr(ChunkManager, inv.read_attrs_method, _raise_transient)

    mirror_calls: list[Any] = []
    monkeypatch.setattr(
        ChunkManager,
        inv.mirror_method,
        lambda self, product, record: mirror_calls.append((product, record)),
    )
    _suppress_slot_event(monkeypatch, cm)

    with pytest.raises(type(transient_exc)):
        inv.call()

    assert mirror_calls == [], (
        "Row 2 must NOT re-mirror on transient errors (attrs state undetermined)"
    )


def test_row3_cp_match_attrs_match_accepts_without_remirror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invocation_cls: type[Any],
) -> None:
    """Row 3: CP matches + attrs matches → accept (no re-mirror needed)."""
    cm = _make_manager(tmp_path)
    inv = invocation_cls(cm)
    _install_conflict(monkeypatch)

    cp = inv.matching_cp()
    monkeypatch.setattr(ChunkManager, inv.get_cp_method, lambda self, *, product: cp)
    monkeypatch.setattr(
        ChunkManager,
        inv.read_attrs_method,
        lambda self, *, product: inv.declared_hash(),
    )

    mirror_calls: list[Any] = []
    monkeypatch.setattr(
        ChunkManager,
        inv.mirror_method,
        lambda self, product, record: mirror_calls.append((product, record)),
    )
    slot_events = _suppress_slot_event(monkeypatch, cm)

    result = inv.call()

    assert mirror_calls == [], "Row 3 must NOT re-mirror when attrs already match"
    assert inv.extract_record(result) == cp

    if isinstance(inv, _SlotInvocation):
        assert slot_events == [EVENT_SLOT_INDEX_MODEL_VERIFIED], (
            f"slot loser must emit VERIFIED on full convergence; got {slot_events!r}"
        )


def test_row4_cp_match_attrs_mismatch_rejects_with_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invocation_cls: type[Any],
) -> None:
    """Row 4: CP matches + attrs mismatch → reject with drift error naming both hashes."""
    cm = _make_manager(tmp_path)
    inv = invocation_cls(cm)
    _install_conflict(monkeypatch)

    cp = inv.matching_cp()
    drifted_hash = "d" * 64
    monkeypatch.setattr(ChunkManager, inv.get_cp_method, lambda self, *, product: cp)
    monkeypatch.setattr(ChunkManager, inv.read_attrs_method, lambda self, *, product: drifted_hash)

    mirror_calls: list[Any] = []
    monkeypatch.setattr(
        ChunkManager,
        inv.mirror_method,
        lambda self, product, record: mirror_calls.append((product, record)),
    )
    _suppress_slot_event(monkeypatch, cm)

    with pytest.raises(inv.drift_error) as excinfo:
        inv.call()

    message = str(excinfo.value)
    assert cp.identity_hash[:16] in message, (
        f"drift error must name the CP hash prefix; got {message!r}"
    )
    assert drifted_hash[:16] in message, (
        f"drift error must name the attrs hash prefix; got {message!r}"
    )
    assert mirror_calls == [], "Row 4 must NOT re-mirror when attrs disagree"


def test_row5_cp_mismatch_rejects_with_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invocation_cls: type[Any],
) -> None:
    """Row 5: CP mismatch → reject with conflict error (existing behavior)."""
    cm = _make_manager(tmp_path)
    inv = invocation_cls(cm)
    _install_conflict(monkeypatch)

    other_cp = inv.mismatched_cp()
    monkeypatch.setattr(ChunkManager, inv.get_cp_method, lambda self, *, product: other_cp)
    _suppress_resolved_conflict_wal(monkeypatch)

    read_attrs_calls: list[str] = []

    def _read_attrs(self: ChunkManager, *, product: str) -> Any:
        _ = self
        read_attrs_calls.append(product)
        return None

    monkeypatch.setattr(ChunkManager, inv.read_attrs_method, _read_attrs)
    _suppress_slot_event(monkeypatch, cm)

    with pytest.raises(inv.conflict_error) as excinfo:
        inv.call()

    message = str(excinfo.value)
    assert other_cp.identity_hash[:16] in message
    assert inv.declared_hash()[:16] in message
    assert read_attrs_calls == [], (
        "Row 5 must short-circuit on CP mismatch — attrs must not be read"
    )
