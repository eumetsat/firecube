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

from pathlib import Path
from unittest.mock import MagicMock

from firecube.core.controlplane._wal_writer import ManifestWalWriter
from firecube.core.storage.uri import StorageUri


def _make_writer(orphan_payload: dict[str, object]) -> ManifestWalWriter:
    repo = MagicMock()
    repo._wal_reader.read_run_entry.return_value = orphan_payload
    writer = ManifestWalWriter.__new__(ManifestWalWriter)
    writer._repo = repo
    return writer


def _make_uri(tmp_path: Path, name: str) -> StorageUri:
    return StorageUri.from_local_path(tmp_path / name)


def test_orphan_resume_meta_heals_slot_range_and_group(tmp_path: Path) -> None:
    run_id = "prod-host-abc__group=SEVIRI_L15__slot=526080-526128"
    writer = _make_writer({"run_id": run_id, "status": "orphaned", "events": 0})

    resume_meta = writer._resume_meta_for_run(
        product="prod",
        run_id=run_id,
        control_path=_make_uri(tmp_path, "control-path"),
        control_uri=_make_uri(tmp_path, "control-uri"),
    )

    assert resume_meta is not None
    assert resume_meta["slot_range"] == [526080, 526128]
    assert resume_meta["slot_group"] == "SEVIRI_L15"


def test_explicit_resume_meta_wins_over_parser(tmp_path: Path) -> None:
    run_id = "prod-host-abc__group=SEVIRI_L15__slot=526080-526128"
    writer = _make_writer(
        {
            "run_id": run_id,
            "status": "orphaned",
            "events": 0,
            "slot_range": [1, 2],
            "slot_group": "EXPLICIT",
        }
    )

    resume_meta = writer._resume_meta_for_run(
        product="prod",
        run_id=run_id,
        control_path=_make_uri(tmp_path, "control-path"),
        control_uri=_make_uri(tmp_path, "control-uri"),
    )

    assert resume_meta is not None
    assert resume_meta["slot_range"] == [1, 2]
    assert resume_meta["slot_group"] == "EXPLICIT"


def test_plain_run_id_does_not_heal_missing_slot_fields(tmp_path: Path) -> None:
    run_id = "plain-run-id"
    writer = _make_writer({"run_id": run_id, "status": "orphaned", "events": 0})

    resume_meta = writer._resume_meta_for_run(
        product="prod",
        run_id=run_id,
        control_path=_make_uri(tmp_path, "control-path"),
        control_uri=_make_uri(tmp_path, "control-uri"),
    )

    assert resume_meta is not None
    assert "slot_range" not in resume_meta
    assert "slot_group" not in resume_meta
