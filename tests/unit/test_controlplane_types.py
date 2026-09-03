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

"""Unit tests for content-addressed item manifest schema on ResolvedIndexRecord.

Covers:

- Byte parity: RegularTimeAxis / IntegerAxis records unchanged pre and post
  the schema addition (asymmetric items handling).
- Freeze detection: manifest items fold into identity_hash so adding or
  removing an item mutates the recorded value.
- Validation: ``validate_manifest_entries`` rejects duplicate identity_hashes,
  duplicate coordinate entries, and empty source_ref.
- Wire round-trip: with-manifest records survive to_json_bytes /
  from_json_bytes preserving item content.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from firecube.core.controlplane.types import (
    ItemManifestEntry,
    ResolvedIndexRecord,
    canonical_index_bytes,
    compute_resolved_index_identity_hash,
    validate_manifest_entries,
)
from firecube.core.errors import ManifestError


def _regular_index() -> dict[str, object]:
    return {
        "schema_version": "v1",
        "name": "resolved_index",
        "groups": {
            "data": {
                "kind": "regular_time",
                "size": 10,
                "params": {
                    "epoch": "2024-01-01T00:00:00Z",
                    "cadence_s": 600,
                    "mode": "exact",
                },
            },
        },
    }


def _integer_index() -> dict[str, object]:
    return {
        "schema_version": "v1",
        "name": "resolved_index",
        "groups": {
            "data": {"kind": "integer", "size": 5, "params": {}},
        },
    }


def _make_record(
    *,
    index: dict[str, object],
    items: tuple[ItemManifestEntry, ...] | None = None,
    recorded_at: str = "2026-01-01T00:00:00Z",
    recorded_by_run_id: str = "run-abc",
) -> ResolvedIndexRecord:
    identity_hash = compute_resolved_index_identity_hash(index, items)
    return ResolvedIndexRecord(
        schema_version="v1",
        recorded_at=recorded_at,
        recorded_by_run_id=recorded_by_run_id,
        identity_hash=identity_hash,
        index=index,
        items=items,
    )


def _sample_items() -> tuple[ItemManifestEntry, ...]:
    return (
        ItemManifestEntry(
            identity_hash="a" * 64,
            coordinate_value="2024-01-01T00:00:00Z",
            source_ref="s3://bucket/one.nc",
            source_ref_kind="uri",
        ),
        ItemManifestEntry(
            identity_hash="b" * 64,
            coordinate_value="2024-01-01T00:10:00Z",
            source_ref="s3://bucket/two.nc",
            source_ref_kind="uri",
        ),
    )


def _coordinate_order_items() -> tuple[ItemManifestEntry, ...]:
    return (
        ItemManifestEntry(
            identity_hash="z" * 64,
            coordinate_value="2024-01-01T00:10:00Z",
            source_ref="s3://bucket/later.nc",
            source_ref_kind="uri",
        ),
        ItemManifestEntry(
            identity_hash="a" * 64,
            coordinate_value="2024-01-01T00:00:00Z",
            source_ref="s3://bucket/earlier.nc",
            source_ref_kind="uri",
        ),
    )


def test_regular_axis_identity_hash_unchanged_after_schema() -> None:
    index = _regular_index()
    pre_manifest_hash = hashlib.sha256(canonical_index_bytes(index)).hexdigest()
    record = _make_record(index=index)
    assert record.items is None
    assert record.identity_hash == pre_manifest_hash


def test_integer_axis_identity_hash_unchanged_after_schema() -> None:
    index = _integer_index()
    pre_manifest_hash = hashlib.sha256(canonical_index_bytes(index)).hexdigest()
    record = _make_record(index=index)
    assert record.items is None
    assert record.identity_hash == pre_manifest_hash


def test_to_json_bytes_omits_items_key_when_none() -> None:
    record = _make_record(index=_regular_index())
    payload = json.loads(record.to_json_bytes())
    assert "items" not in payload


def test_manifest_items_affect_identity_hash() -> None:
    index = _regular_index()
    record_no_items = _make_record(index=index)
    record_with_items = _make_record(index=index, items=_sample_items())
    assert record_no_items.identity_hash != record_with_items.identity_hash


def test_manifest_item_content_changes_identity_hash() -> None:
    index = _regular_index()
    items_a = _sample_items()
    items_b = (
        items_a[0],
        ItemManifestEntry(
            identity_hash="c" * 64,
            coordinate_value="2024-01-01T00:20:00Z",
            source_ref="s3://bucket/three.nc",
            source_ref_kind="uri",
        ),
    )
    assert (
        _make_record(index=index, items=items_a).identity_hash
        != _make_record(index=index, items=items_b).identity_hash
    )


def test_manifest_item_order_does_not_affect_identity_hash() -> None:
    index = _regular_index()
    items = _sample_items()
    forward = _make_record(index=index, items=items)
    reversed_ = _make_record(index=index, items=items[::-1])
    assert forward.identity_hash == reversed_.identity_hash


def test_to_json_bytes_sorts_items_by_coordinate_value() -> None:
    record = _make_record(index=_regular_index(), items=_coordinate_order_items())
    payload = json.loads(record.to_json_bytes())

    assert [item["coordinate_value"] for item in payload["items"]] == [
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:10:00Z",
    ]


def test_validate_manifest_entries_accepts_valid_entries() -> None:
    validate_manifest_entries(list(_sample_items()))


def test_validate_manifest_entries_rejects_duplicate_identity_hashes() -> None:
    entries = [
        ItemManifestEntry(
            identity_hash="same" + "0" * 60,
            coordinate_value="2024-01-01T00:00:00Z",
            source_ref="s3://bucket/a.nc",
            source_ref_kind="uri",
        ),
        ItemManifestEntry(
            identity_hash="same" + "0" * 60,
            coordinate_value="2024-01-01T00:10:00Z",
            source_ref="s3://bucket/b.nc",
            source_ref_kind="uri",
        ),
    ]
    with pytest.raises(ValueError, match="duplicate identity_hash"):
        validate_manifest_entries(entries)


def test_validate_manifest_entries_rejects_duplicate_coordinates() -> None:
    entries = [
        ItemManifestEntry(
            identity_hash="a" * 64,
            coordinate_value="2024-01-01T00:00:00Z",
            source_ref="s3://bucket/a.nc",
            source_ref_kind="uri",
        ),
        ItemManifestEntry(
            identity_hash="b" * 64,
            coordinate_value="2024-01-01T00:00:00Z",
            source_ref="s3://bucket/b.nc",
            source_ref_kind="uri",
        ),
    ]
    with pytest.raises(ValueError, match="duplicate coordinate_value"):
        validate_manifest_entries(entries)


def test_validate_manifest_entries_rejects_empty_source_ref() -> None:
    entries = [
        ItemManifestEntry(
            identity_hash="a" * 64,
            coordinate_value="2024-01-01T00:00:00Z",
            source_ref="",
            source_ref_kind="uri",
        ),
    ]
    with pytest.raises(ValueError, match="source_ref must be non-empty"):
        validate_manifest_entries(entries)


def test_round_trip_preserves_manifest() -> None:
    record = _make_record(index=_regular_index(), items=_sample_items())
    recovered = ResolvedIndexRecord.from_json_bytes(record.to_json_bytes())
    assert recovered.identity_hash == record.identity_hash
    assert recovered.items is not None
    assert len(recovered.items) == len(_sample_items())
    recovered_hashes = {entry.identity_hash for entry in recovered.items}
    expected_hashes = {entry.identity_hash for entry in _sample_items()}
    assert recovered_hashes == expected_hashes


def test_round_trip_preserves_absence_of_manifest() -> None:
    record = _make_record(index=_regular_index())
    recovered = ResolvedIndexRecord.from_json_bytes(record.to_json_bytes())
    assert recovered.items is None


def test_from_json_bytes_rejects_tampered_manifest() -> None:
    record = _make_record(index=_regular_index(), items=_sample_items())
    payload = json.loads(record.to_json_bytes())
    assert isinstance(payload["items"], list) and payload["items"]
    payload["items"][0]["identity_hash"] = "d" * 64
    with pytest.raises(ManifestError, match="identity-hash mismatch"):
        ResolvedIndexRecord.from_json_bytes(json.dumps(payload).encode("utf-8"))


def test_from_json_bytes_rejects_invalid_source_ref_kind() -> None:
    record = _make_record(index=_regular_index(), items=_sample_items())
    payload = json.loads(record.to_json_bytes())
    payload["items"][0]["source_ref_kind"] = "bogus"
    with pytest.raises(ManifestError, match="source_ref_kind"):
        ResolvedIndexRecord.from_json_bytes(json.dumps(payload).encode("utf-8"))


def test_item_manifest_entry_is_reexported_from_public_apis() -> None:
    from firecube.core.api import ItemManifestEntry as CoreEntry
    from firecube.ingestor.api import ItemManifestEntry as IngestorEntry

    assert CoreEntry is ItemManifestEntry
    assert IngestorEntry is ItemManifestEntry


def test_validate_manifest_entries_is_reexported_from_public_apis() -> None:
    from firecube.core.api import validate_manifest_entries as core_fn
    from firecube.ingestor.api import validate_manifest_entries as ingestor_fn

    assert core_fn is validate_manifest_entries
    assert ingestor_fn is validate_manifest_entries
