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

"""Integration tests for the resolved-index precedence matrix."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    IndexEnsuredEvent,
    ResolvedIndexRecord,
    canonical_index_bytes,
)
from firecube.core.errors import ClaimConflictError, ManifestError, ResolvedIndexConflictError
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.architecture]

_PRODUCT = "prod1"


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


def _index(
    *,
    name: str = "matrix_index",
    groups: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "v1",
        "name": name,
        "groups": groups
        if groups is not None
        else {"data_1km": {"kind": "integer", "size": 3, "params": {}}},
    }


def _record(*, run_id: str = "run-1", index: dict[str, Any] | None = None) -> ResolvedIndexRecord:
    payload = _index() if index is None else index
    return ResolvedIndexRecord(
        recorded_at="2026-08-20T00:00:00+00:00",
        recorded_by_run_id=run_id,
        identity_hash=hashlib.sha256(canonical_index_bytes(payload)).hexdigest(),
        index=payload,
    )


def _current_json(tmp_path: Path, cm: ChunkManager) -> Path:
    control_root = cm.get_control_root(product=_PRODUCT)
    return Path(control_root.removeprefix("file://")) / INDEX_DIRNAME / INDEX_CURRENT_FILENAME


def test_row1_fresh_store_writes_current_and_mirrors_attrs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    mirror = Mock()

    def _mirror(_self: ChunkManager, product: str, record: ResolvedIndexRecord) -> None:
        mirror(product, record)

    monkeypatch.setattr(ChunkManager, "_mirror_resolved_index_attrs", _mirror)

    stored, outcome = cm.ensure_resolved_index(product=_PRODUCT, record=declared)

    assert outcome == "created"
    assert stored == declared
    assert _current_json(tmp_path, cm).is_file()
    mirror.assert_called_once_with(_PRODUCT, declared)


def test_row2_file_and_attrs_match_returns_existing_without_rewrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    first, _ = cm.ensure_resolved_index(product=_PRODUCT, record=declared)
    repo_fs = cm.repo._fs
    assert repo_fs is not None
    write_spy = Mock(wraps=repo_fs.atomic_writer.write_atomic)
    monkeypatch.setattr(repo_fs.atomic_writer, "write_atomic", write_spy)
    monkeypatch.setattr(
        ChunkManager,
        "read_resolved_index_attrs_hash",
        lambda self, *, product: declared.identity_hash,
    )

    stored, outcome = cm.ensure_resolved_index(product=_PRODUCT, record=_record(run_id="run-2"))

    assert outcome == "matched_existing"
    assert stored == first
    current_writes = [
        call
        for call in write_spy.call_args_list
        if f"/{INDEX_DIRNAME}/{INDEX_CURRENT_FILENAME}" in str(call.args[0])
    ]
    assert current_writes == []


def test_row3_file_exists_attrs_missing_remirrors_and_returns_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    first, _ = cm.ensure_resolved_index(product=_PRODUCT, record=declared)
    mirror = Mock()

    def _mirror(_self: ChunkManager, product: str, record: ResolvedIndexRecord) -> None:
        mirror(product, record)

    monkeypatch.setattr(ChunkManager, "_mirror_resolved_index_attrs", _mirror)
    monkeypatch.setattr(
        ChunkManager, "read_resolved_index_attrs_hash", lambda self, *, product: None
    )

    stored, outcome = cm.ensure_resolved_index(product=_PRODUCT, record=_record(run_id="run-2"))

    assert outcome == "created"
    assert stored == first
    mirror.assert_called_once_with(_PRODUCT, first)


def test_row4_attrs_exist_without_file_raises_manifest_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    monkeypatch.setattr(
        ChunkManager,
        "read_resolved_index_attrs_hash",
        lambda self, *, product: declared.identity_hash,
    )

    with pytest.raises(ManifestError, match=r"root attrs.*no control-plane record"):
        cm.ensure_resolved_index(product=_PRODUCT, record=declared)


def test_row5_incompatible_record_raises_conflict_with_field_diff(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    stored = _record(
        index=_index(
            name="fci_index",
            groups={
                "data_1km": {"kind": "regular_time", "size": 10, "params": {"cadence_s": 300}},
                "data_2km": {"kind": "integer", "size": 5, "params": {}},
            },
        )
    )
    incoming = _record(
        run_id="run-2",
        index=_index(
            name="fci_index",
            groups={
                "data_1km": {"kind": "regular_time", "size": 12, "params": {"cadence_s": 600}},
                "data_500m": {"kind": "integer", "size": 20, "params": {}},
            },
        ),
    )
    cm.ensure_resolved_index(product=_PRODUCT, record=stored)

    with pytest.raises(ResolvedIndexConflictError) as excinfo:
        cm.ensure_resolved_index(product=_PRODUCT, record=incoming)

    message = str(excinfo.value)
    assert "data_2km" in message
    assert "data_500m" in message
    assert "cadence_s" in message
    assert stored.identity_hash[:16] in message
    assert incoming.identity_hash[:16] in message
    assert "2026-08-20" not in message


def test_precedence_row5_conflict_emits_wal_before_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    stored = _record()
    incoming = _record(
        run_id="run-2",
        index=_index(groups={"data_1km": {"kind": "integer", "size": 4, "params": {}}}),
    )
    events: list[IndexEnsuredEvent] = []

    def _record_event(self: ChunkManager, event: IndexEnsuredEvent) -> None:
        _ = self
        events.append(event)

    monkeypatch.setattr(ChunkManager, "record_index_ensured_event", _record_event)
    cm.ensure_resolved_index(product=_PRODUCT, record=stored)

    with pytest.raises(ResolvedIndexConflictError):
        cm.ensure_resolved_index(product=_PRODUCT, record=incoming)

    assert len(events) == 1
    event = events[0]
    assert event.outcome == "conflict_refused"
    assert event.product == _PRODUCT
    assert event.run_id == "run-2"
    assert event.identity_hash == incoming.identity_hash


def test_ensure_retry_conflict_emits_wal_before_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    stored = _record()
    incoming = _record(
        run_id="run-2",
        index=_index(groups={"data_1km": {"kind": "integer", "size": 4, "params": {}}}),
    )
    events: list[IndexEnsuredEvent] = []

    def _claim_conflict(self: ChunkManager, *args: Any, **kwargs: Any) -> Any:
        _ = (self, args, kwargs)
        raise ClaimConflictError("held")

    def _stored_record(self: ChunkManager, *, product: str) -> ResolvedIndexRecord:
        _ = (self, product)
        return stored

    def _record_event(self: ChunkManager, event: IndexEnsuredEvent) -> None:
        _ = self
        events.append(event)

    monkeypatch.setattr(ChunkManager, "acquire_claim", _claim_conflict)
    monkeypatch.setattr(ChunkManager, "get_resolved_index", _stored_record)
    monkeypatch.setattr(ChunkManager, "record_index_ensured_event", _record_event)

    with pytest.raises(ResolvedIndexConflictError):
        cm.ensure_resolved_index(product=_PRODUCT, record=incoming)

    assert len(events) == 1
    event = events[0]
    assert event.outcome == "conflict_refused"
    assert event.product == _PRODUCT
    assert event.run_id == "run-2"
    assert event.identity_hash == incoming.identity_hash


def test_row6_foreign_schema_version_record_raises_manifest_error(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    current = _current_json(tmp_path, cm)
    current.parent.mkdir(parents=True, exist_ok=True)
    payload = json.loads(declared.to_json_bytes())
    payload["schema_version"] = "v2"
    current.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ManifestError, match="schema_version"):
        cm.ensure_resolved_index(product=_PRODUCT, record=declared)


def test_apply_resolved_index_precedence_never_raises_assertion_error() -> None:
    """Terminal fall-through in ``_apply_resolved_index_precedence`` must raise
    ``ManifestError`` rather than ``AssertionError``.

    The terminal branch is genuinely unreachable via any concrete
    (cp_record, attrs_hash, record) triple that satisfies the earlier
    guards, so it cannot be triggered from the outside. The precedence rules
    are exhaustive:

    * (None, None): row 1 — write and return "created".
    * cp_record.schema_version != "v1": row 6 — raise ManifestError.
    * cp/attrs both agree with record: row 2 — return "matched_existing".
    * cp agrees, attrs missing: row 3 — remirror and return "created".
    * (None, hash): row 4 — raise ManifestError.
    * cp disagrees with record: row 5 — raise ResolvedIndexConflictError.
    * cp agrees, attrs disagree: row 7 — raise ManifestError.

    A defensive terminal branch still exists to surface any future invariant
    break loudly, but it must be a ``ManifestError`` (a documented control-plane
    exception operators can filter and log) rather than an ``AssertionError``
    (which is swallowed under ``python -O`` and reads as a programming bug to
    operators). This source-check test guards against a silent revert.
    """
    import inspect as _inspect

    from firecube.core.controlplane.manager import ChunkManager

    source = _inspect.getsource(ChunkManager._apply_resolved_index_precedence)
    assert "raise AssertionError" not in source, (
        "ChunkManager._apply_resolved_index_precedence must not raise AssertionError; "
        "use ManifestError so operators receive a documented control-plane error."
    )
    assert "raise ManifestError" in source, (
        "ChunkManager._apply_resolved_index_precedence terminal fall-through must "
        "still surface via ManifestError."
    )
