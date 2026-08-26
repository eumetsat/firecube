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

import logging
from pathlib import Path

import pytest

from firecube.core.controlplane._wal_reader import WalReader
from firecube.core.filesystem import FsspecFilesystem
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _reader(tmp_root: Path, product: str = "product.zarr") -> WalReader:
    binding = make_test_binding(tmp_root, product=product)
    return WalReader(
        fs=FsspecFilesystem(binding),
        resolver=lambda _product: (binding.identity.product_uri, binding.identity.product_uri),
        log=logging.getLogger(__name__),
        run_stale_threshold_s=3600,
    )


def _run_dir(tmp_root: Path, product: str, run_id: str) -> Path:
    return tmp_root / product / ".firecube" / "runs" / run_id


def test_read_run_entry_missing_dir_returns_none(tmp_path: Path) -> None:
    tmp_root = tmp_path
    reader = _reader(tmp_root)
    run_dir = StorageUri.from_local_path(_run_dir(tmp_root, "product.zarr", "missing"))

    entry = reader.read_run_entry(
        product="product.zarr",
        run_dir=run_dir,
        run_uri=run_dir.to_str(),
        run_id="missing",
    )

    assert entry is None


def test_read_run_entry_orphan_dir_returns_orphan_entry(tmp_path: Path) -> None:
    tmp_root = tmp_path
    reader = _reader(tmp_root)
    run_dir_path = _run_dir(tmp_root, "product.zarr", "orphan")
    run_dir_path.mkdir(parents=True)
    (run_dir_path / "events-0001.jsonl").write_text('{"event_type": "noop"}\n', encoding="utf-8")
    run_dir = StorageUri.from_local_path(run_dir_path)

    entry = reader.read_run_entry(
        product="product.zarr",
        run_dir=run_dir,
        run_uri=run_dir.to_str(),
        run_id="orphan",
    )

    assert entry is not None
    assert entry["status"] == "orphaned"
    assert entry["error"] == "missing_run_meta"
    assert entry["parts"] == 1


def test_read_run_entry_malformed_json_behavior(tmp_path: Path) -> None:
    tmp_root = tmp_path
    reader = _reader(tmp_root)
    run_dir_path = _run_dir(tmp_root, "product.zarr", "malformed")
    run_dir_path.mkdir(parents=True)
    (run_dir_path / "run.json").write_text("{NOT VALID JSON", encoding="utf-8")
    (run_dir_path / "events-0001.jsonl").write_text('{"event_type": "noop"}\n', encoding="utf-8")
    run_dir = StorageUri.from_local_path(run_dir_path)

    entry = reader.read_run_entry(
        product="product.zarr",
        run_dir=run_dir,
        run_uri=run_dir.to_str(),
        run_id="malformed",
    )

    assert entry is not None
    assert entry["status"] == "orphaned"
    assert entry["error"] == "unreadable_run_meta"
    assert entry["parts"] == 1


def test_read_run_entry_pending_empty_meta_no_segments_returns_none(tmp_path: Path) -> None:
    """A zero-byte run.json with no WAL segments is a peer's first meta write
    still in flight, not corruption: skip it exactly as if the file did not
    exist yet. Regression for the 12-pod startup race where a resume guard
    crashed with ControlPlaneCorruptionError ("Expecting value: ... char 0")
    while another pod was between open("w") and json.dump."""
    tmp_root = tmp_path
    reader = _reader(tmp_root)
    run_dir_path = _run_dir(tmp_root, "product.zarr", "pending")
    run_dir_path.mkdir(parents=True)
    (run_dir_path / "run.json").write_text("", encoding="utf-8")
    run_dir = StorageUri.from_local_path(run_dir_path)

    entry = reader.read_run_entry(
        product="product.zarr",
        run_dir=run_dir,
        run_uri=run_dir.to_str(),
        run_id="pending",
    )

    assert entry is None


def test_read_run_entry_garbage_meta_no_segments_still_raises(tmp_path: Path) -> None:
    """Non-empty unparseable run.json without segments stays a hard error:
    the pending-meta tolerance is scoped to zero-byte files only, so genuine
    corruption still surfaces."""
    tmp_root = tmp_path
    reader = _reader(tmp_root)
    run_dir_path = _run_dir(tmp_root, "product.zarr", "garbage")
    run_dir_path.mkdir(parents=True)
    (run_dir_path / "run.json").write_text("{NOT VALID JSON", encoding="utf-8")
    run_dir = StorageUri.from_local_path(run_dir_path)

    with pytest.raises(Exception, match="Expecting"):
        reader.read_run_entry(
            product="product.zarr",
            run_dir=run_dir,
            run_uri=run_dir.to_str(),
            run_id="garbage",
        )
