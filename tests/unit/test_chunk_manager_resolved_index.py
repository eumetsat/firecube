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

"""Unit tests for ChunkManager resolved-index persistence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from firecube.core import errors
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    ResolvedIndexRecord,
    WriteDomain,
    canonical_index_bytes,
)
from firecube.core.errors import ManifestError
from tests.helpers.storage import make_test_binding


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


def _index(group: str = "g1", size: int = 3) -> dict[str, object]:
    return {
        "groups": {
            group: {
                "axes": {"time": {"kind": "integer", "size": size}},
                "items": [
                    {"key": "a", "coordinates": {"time": 0}},
                    {"key": "b", "coordinates": {"time": 1}},
                ],
            }
        }
    }


def _record(
    *, run_id: str = "run-1", index: dict[str, object] | None = None
) -> ResolvedIndexRecord:
    payload = _index() if index is None else index
    return ResolvedIndexRecord(
        recorded_at="2026-08-20T00:00:00+00:00",
        recorded_by_run_id=run_id,
        identity_hash=hashlib.sha256(canonical_index_bytes(payload)).hexdigest(),
        index=payload,
    )


def _control_root(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube"


def _resolved_index_current(tmp_path: Path, product: str) -> Path:
    return _control_root(tmp_path, product) / INDEX_DIRNAME / INDEX_CURRENT_FILENAME


def _claim_file(tmp_path: Path, product: str) -> Path:
    domain = WriteDomain(product=product, category="resolved_index", name="current")
    return _control_root(tmp_path, product) / "claims" / domain.claim_name


@pytest.mark.unit
def test_get_resolved_index_returns_none_for_nonexistent_product(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)

    assert cm.get_resolved_index(product="missing") is None


@pytest.mark.unit
def test_ensure_resolved_index_fresh_store_returns_created_and_writes_current_json(
    tmp_path: Path,
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()

    record, outcome = cm.ensure_resolved_index(product="prod1", record=declared)

    assert outcome == "created"
    assert record == declared
    current = _resolved_index_current(tmp_path, "prod1")
    assert current.is_file()
    assert ResolvedIndexRecord.from_json_bytes(current.read_bytes()) == declared


@pytest.mark.unit
def test_get_resolved_index_returns_record_after_ensure(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    cm.ensure_resolved_index(product="prod1", record=declared)

    assert cm.get_resolved_index(product="prod1") == declared


@pytest.mark.unit
def test_second_ensure_with_identical_record_matches_existing(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()

    first, first_outcome = cm.ensure_resolved_index(product="prod1", record=declared)
    second, second_outcome = cm.ensure_resolved_index(product="prod1", record=declared)

    assert first_outcome == "created"
    assert second_outcome == "matched_existing"
    assert second == first == declared


@pytest.mark.unit
def test_second_ensure_with_same_identity_but_new_metadata_returns_existing_record(
    tmp_path: Path,
) -> None:
    cm = _make_manager(tmp_path)
    first = _record(run_id="run-1")
    same_index_later = _record(run_id="run-2")
    assert same_index_later.identity_hash == first.identity_hash

    cm.ensure_resolved_index(product="prod1", record=first)
    second, outcome = cm.ensure_resolved_index(product="prod1", record=same_index_later)

    assert outcome == "matched_existing"
    assert second == first
    assert second.recorded_by_run_id == "run-1"


@pytest.mark.unit
def test_divergent_record_raises_resolved_index_conflict(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    cm.ensure_resolved_index(product="prod1", record=_record(index=_index(size=3)))
    divergent = _record(run_id="run-2", index=_index(size=4))

    with pytest.raises(errors.ResolvedIndexConflictError):
        cm.ensure_resolved_index(product="prod1", record=divergent)


@pytest.mark.unit
def test_corrupt_current_json_propagates_manifest_error(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    current = _resolved_index_current(tmp_path, "prod1")
    current.parent.mkdir(parents=True, exist_ok=True)
    current.write_bytes(b"not-json")

    with pytest.raises(ManifestError, match="resolved-index record is not valid JSON"):
        cm.get_resolved_index(product="prod1")


@pytest.mark.unit
def test_get_resolved_index_propagates_non_missing_read_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)

    class _FakePath:
        def join(self, *segments: str) -> _FakePath:
            _ = segments
            return self

    class _FailingFilesystem:
        def open(self, path: _FakePath, mode: str):
            _ = (path, mode)
            raise PermissionError("permission denied while reading resolved index")

    monkeypatch.setattr(cm.repo, "get_control_root_uri", lambda _product: "file:///control")
    monkeypatch.setattr(cm.repo, "_get_fs", lambda _uri: (_FailingFilesystem(), _FakePath()))

    with pytest.raises(PermissionError, match="permission denied"):
        cm.get_resolved_index(product="prod1")


@pytest.mark.unit
def test_claim_conflict_with_matching_current_json_converges(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    declared = _record(run_id="winner")
    current = _resolved_index_current(tmp_path, "prod1")
    current.parent.mkdir(parents=True, exist_ok=True)
    current.write_bytes(declared.to_json_bytes())
    claim_file = _claim_file(tmp_path, "prod1")
    claim_file.parent.mkdir(parents=True, exist_ok=True)
    claim_file.write_text(
        json.dumps(
            {
                "product": "prod1",
                "domain": "prod1:resolved_index:current",
                "owner_id": "winner",
                "claim_path": str(claim_file),
                "acquired_at": 9999999999.0,
                "last_heartbeat_at": 9999999999.0,
                "heartbeat_interval_s": 30,
                "stale_threshold_s": 120,
            }
        )
    )

    record, outcome = cm.ensure_resolved_index(
        product="prod1", record=declared, max_retries=1, initial_backoff_s=0.01
    )

    assert record == declared
    assert outcome == "matched_existing"
