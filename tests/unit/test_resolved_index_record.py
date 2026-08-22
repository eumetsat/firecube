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

"""Unit tests for ResolvedIndexRecord persistence and canonical hashing."""

from __future__ import annotations

import hashlib
import json

import pytest

from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    RESOLVED_INDEX_ATTR,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    ResolvedIndexRecord,
    canonical_index_bytes,
)
from firecube.core.errors import ManifestError


def _make_index() -> dict[str, object]:
    return {
        "schema_version": "v1",
        "name": "resolved_index",
        "groups": {
            "data": {"kind": "integer", "params": {"size": 10}, "size": 10},
            "time": {"kind": "regular", "params": {"cadence_s": 300, "mode": "exact"}, "size": 5},
        },
    }


def _make_record(
    *,
    index: dict[str, object] | None = None,
    recorded_at: str = "2026-01-01T00:00:00Z",
    recorded_by_run_id: str = "run-abc",
) -> ResolvedIndexRecord:
    idx = index or _make_index()
    return ResolvedIndexRecord(
        schema_version="v1",
        recorded_at=recorded_at,
        recorded_by_run_id=recorded_by_run_id,
        identity_hash=hashlib.sha256(canonical_index_bytes(idx)).hexdigest(),
        index=idx,
    )


def test_constants_have_expected_values() -> None:
    assert INDEX_DIRNAME == "index"
    assert INDEX_CURRENT_FILENAME == "current.json"
    assert RESOLVED_INDEX_ATTR == "firecube_resolved_index"
    assert RESOLVED_INDEX_IDENTITY_HASH_ATTR == "firecube_resolved_index_identity_hash"


def test_round_trip_preserves_fields() -> None:
    rec = _make_record()
    recovered = ResolvedIndexRecord.from_json_bytes(rec.to_json_bytes())
    assert recovered.schema_version == rec.schema_version
    assert recovered.recorded_at == rec.recorded_at
    assert recovered.recorded_by_run_id == rec.recorded_by_run_id
    assert recovered.identity_hash == rec.identity_hash
    assert recovered.index == rec.index


def test_same_index_yields_same_identity_hash_even_when_recorded_at_differs() -> None:
    index = _make_index()
    rec_a = _make_record(index=index, recorded_at="2026-01-01T00:00:00Z")
    rec_b = _make_record(index=index, recorded_at="2026-01-02T00:00:00Z")
    assert rec_a.identity_hash == rec_b.identity_hash


def test_to_json_bytes_is_deterministic() -> None:
    rec = _make_record()
    assert (
        rec.to_json_bytes()
        == (
            f'{{"identity_hash":"{rec.identity_hash}",'
            '"index":{"groups":{"data":{"kind":"integer","params":{"size":10},"size":10},'
            '"time":{"kind":"regular","params":{"cadence_s":300,"mode":"exact"},"size":5}},'
            '"name":"resolved_index","schema_version":"v1"},'
            '"recorded_at":"2026-01-01T00:00:00Z","recorded_by_run_id":"run-abc",'
            '"schema_version":"v1"}'
        ).encode()
    )


def test_canonical_index_bytes_is_deterministic_for_nested_key_order_and_unicode() -> None:
    index_a = {
        "name": "résumé",
        "schema_version": "v1",
        "groups": {"b": {"size": 2, "kind": "integer"}, "a": {"kind": "regular", "size": 1}},
    }
    index_b = {
        "groups": {"a": {"size": 1, "kind": "regular"}, "b": {"kind": "integer", "size": 2}},
        "schema_version": "v1",
        "name": "résumé",
    }
    assert canonical_index_bytes(index_a) == canonical_index_bytes(index_b)
    assert b"r\xc3\xa9sum\xc3\xa9" in canonical_index_bytes(index_a)


def test_from_json_bytes_rejects_identity_hash_mismatch() -> None:
    rec = _make_record()
    payload = json.loads(rec.to_json_bytes())
    payload["identity_hash"] = "0" * 64
    with pytest.raises(ManifestError, match="identity-hash mismatch"):
        ResolvedIndexRecord.from_json_bytes(json.dumps(payload).encode("utf-8"))


def test_from_json_bytes_rejects_wrong_schema_version() -> None:
    rec = _make_record()
    payload = json.loads(rec.to_json_bytes())
    payload["schema_version"] = "v2"
    with pytest.raises(ManifestError, match="schema_version"):
        ResolvedIndexRecord.from_json_bytes(json.dumps(payload).encode("utf-8"))


def test_from_json_bytes_rejects_missing_required_field() -> None:
    payload = json.loads(_make_record().to_json_bytes())
    payload.pop("index")
    with pytest.raises(ManifestError, match="missing required fields"):
        ResolvedIndexRecord.from_json_bytes(json.dumps(payload).encode("utf-8"))
