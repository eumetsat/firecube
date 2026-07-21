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

"""Unit tests for SlotIndexModelRecord persistence and WAL/repo facades."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from firecube.core.controlplane._wal_writer import ManifestWalWriter
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    SlotIndexModelRecord,
)
from firecube.core.errors import ManifestError
from firecube.core.slot_index import SlotAxis, SlotIndexModel


def _make_model(name: str = "opera_v1") -> SlotIndexModel:
    return SlotIndexModel(
        name=name,
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


def _make_record(model: SlotIndexModel | None = None) -> SlotIndexModelRecord:
    m = model or _make_model()
    return SlotIndexModelRecord(
        model=m,
        identity_hash=m.identity_hash,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00Z",
        recorded_by_run_id="run-abc",
    )


def test_constants_have_expected_string_values() -> None:
    assert SLOT_INDEX_DIRNAME == "slot_index"
    assert SLOT_INDEX_CURRENT_FILENAME == "current.json"
    assert EVENT_SLOT_INDEX_MODEL_RECORDED == "slot_index_model_recorded"
    assert EVENT_SLOT_INDEX_MODEL_VERIFIED == "slot_index_model_verified"


def test_round_trip_preserves_fields() -> None:
    rec = _make_record()
    recovered = SlotIndexModelRecord.from_json_bytes(rec.to_json_bytes())
    assert recovered.identity_hash == rec.identity_hash
    assert recovered.schema_version == rec.schema_version
    assert recovered.recorded_at == rec.recorded_at
    assert recovered.recorded_by_run_id == rec.recorded_by_run_id
    assert recovered.model.name == rec.model.name
    assert recovered.model.epoch == rec.model.epoch
    assert recovered.model.identity_hash == rec.model.identity_hash
    assert set(recovered.model.groups) == set(rec.model.groups)
    for k, axis in rec.model.groups.items():
        assert recovered.model.groups[k].cadence_s == axis.cadence_s
        assert recovered.model.groups[k].mode == axis.mode


def test_round_trip_with_correct_identity_hash_succeeds() -> None:
    m = _make_model()
    rec = SlotIndexModelRecord(
        model=m,
        identity_hash=m.identity_hash,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00Z",
        recorded_by_run_id="r1",
    )
    recovered = SlotIndexModelRecord.from_json_bytes(rec.to_json_bytes())
    assert recovered.identity_hash == m.identity_hash


@pytest.mark.parametrize(
    "identity_hash",
    [
        pytest.param("0" * 63, id="too-short"),
        pytest.param("0" * 65, id="too-long"),
        pytest.param("g" * 64, id="non-hex"),
        pytest.param("A" * 64, id="uppercase"),
    ],
)
def test_constructor_rejects_invalid_identity_hash_shape(identity_hash: str) -> None:
    with pytest.raises(ValueError, match="64-character lowercase hex string"):
        SlotIndexModelRecord(
            model=_make_model(),
            identity_hash=identity_hash,
            schema_version="v1",
            recorded_at="2026-01-01T00:00:00Z",
            recorded_by_run_id="r1",
        )


def test_to_json_bytes_is_deterministic() -> None:
    rec = _make_record()
    assert rec.to_json_bytes() == (
        b'{"identity_hash":"bc441fbb3eb848c54db2c4673e75271d3b98dbb2b7b49081b521b7ef3923a894",'
        b'"model":{"epoch":"2026-01-01T00:00:00Z","groups":{"g1":{"cadence_s":300,"mode":"exact"}},'
        b'"name":"opera_v1","schema_version":"v1","time_unit":null},'
        b'"recorded_at":"2026-01-01T00:00:00Z","recorded_by_run_id":"run-abc",'
        b'"schema_version":"v1"}'
    )


def test_to_json_bytes_does_not_embed_canonical_hash_in_model() -> None:
    rec = _make_record()
    payload = json.loads(rec.to_json_bytes())
    assert "identity_hash" not in payload["model"]


def test_from_json_bytes_rejects_empty_bytes() -> None:
    with pytest.raises(ManifestError, match="not valid JSON"):
        SlotIndexModelRecord.from_json_bytes(b"")


def test_from_json_bytes_rejects_corrupt_json() -> None:
    with pytest.raises(ManifestError, match="not valid JSON"):
        SlotIndexModelRecord.from_json_bytes(b"corrupt")


def test_from_json_bytes_rejects_missing_fields() -> None:
    payload = b'{"model":{},"identity_hash":"x"}'
    with pytest.raises(ManifestError, match="missing required fields"):
        SlotIndexModelRecord.from_json_bytes(payload)


def test_from_json_bytes_rejects_invalid_model_structure() -> None:
    payload = json.dumps(
        {
            "schema_version": "v1",
            "recorded_at": "2026-01-01T00:00:00Z",
            "recorded_by_run_id": "r1",
            "identity_hash": "0" * 64,
            "model": {"name": "x"},
        }
    ).encode("utf-8")
    with pytest.raises(ManifestError, match="invalid model structure"):
        SlotIndexModelRecord.from_json_bytes(payload)


def test_from_json_bytes_rejects_identity_hash_mismatch() -> None:
    m = _make_model()
    tampered = SlotIndexModelRecord(
        model=m,
        identity_hash="0" * 64,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00Z",
        recorded_by_run_id="r1",
    )
    with pytest.raises(ManifestError, match="identity-hash mismatch") as excinfo:
        SlotIndexModelRecord.from_json_bytes(tampered.to_json_bytes())
    err = str(excinfo.value)
    assert "stored=" in err
    assert "recomputed=" in err
    assert m.identity_hash in err


def test_record_is_frozen() -> None:
    rec = _make_record()
    with pytest.raises((AttributeError, Exception)):
        rec.identity_hash = "tampered"  # type: ignore[misc]


def test_wal_writer_rejects_invalid_event_type() -> None:
    repo_stub = MagicMock()
    wal = ManifestWalWriter(repo_stub)
    with pytest.raises(ValueError, match="event_type"):
        wal.record_slot_index_model_event(
            product="prod",
            run_id="run-1",
            event_type="invalid_event",
            identity_hash="0" * 64,
            model_name="m",
        )


def test_wal_writer_appends_recorded_event(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_stub = MagicMock()
    wal = ManifestWalWriter(repo_stub)
    appended: list[dict[str, Any]] = []

    class _StubWriter:
        def append(
            self,
            event_type: str,
            record: dict[str, Any],
            *,
            meta: dict[str, Any],
            flush: bool,
        ) -> None:
            appended.append(
                {
                    "event_type": event_type,
                    "record": dict(record),
                    "meta": dict(meta),
                    "flush": flush,
                }
            )

    def fake_writer_factory(
        product: str, run_id: str, *, resume_existing: bool = False, **_kwargs: Any
    ) -> _StubWriter:
        assert product == "prod"
        assert run_id == "run-1"
        assert resume_existing is True
        return _StubWriter()

    monkeypatch.setattr(wal, "_writer", fake_writer_factory)
    wal.record_slot_index_model_event(
        product="prod",
        run_id="run-1",
        event_type=EVENT_SLOT_INDEX_MODEL_RECORDED,
        identity_hash="a" * 64,
        model_name="opera_v1",
        group="g1",
        meta={"k": "v"},
    )
    assert len(appended) == 1
    entry = appended[0]
    assert entry["event_type"] == EVENT_SLOT_INDEX_MODEL_RECORDED
    assert entry["record"] == {
        "identity_hash": "a" * 64,
        "model_name": "opera_v1",
        "group": "g1",
    }
    assert entry["meta"] == {"k": "v"}
    assert entry["flush"] is False


def test_wal_writer_omits_group_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_stub = MagicMock()
    wal = ManifestWalWriter(repo_stub)
    appended: list[dict[str, Any]] = []

    class _StubWriter:
        def append(
            self,
            event_type: str,
            record: dict[str, Any],
            *,
            meta: dict[str, Any],
            flush: bool,
        ) -> None:
            appended.append({"record": dict(record), "meta": dict(meta)})

    monkeypatch.setattr(
        wal,
        "_writer",
        lambda *_a, **_kw: _StubWriter(),
    )
    wal.record_slot_index_model_event(
        product="prod",
        run_id="run-1",
        event_type=EVENT_SLOT_INDEX_MODEL_VERIFIED,
        identity_hash="b" * 64,
        model_name="opera_v1",
    )
    assert appended[0]["record"] == {
        "identity_hash": "b" * 64,
        "model_name": "opera_v1",
    }
    assert appended[0]["meta"] == {}
