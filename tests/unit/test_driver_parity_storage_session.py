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

"""Driver-parity tests for StorageSession (T4.2).

These tests prove the no-fsspec-bypass invariant for the primary
``StorageSession`` operations: ``upload_tree`` (with parallel workers) and
``control_plane`` (ChunkManager construction with the injected
``session.fs()`` filesystem). Copying is covered through
``firecube.core.storage.transfer.copy_file``.

Each test exercises the real production code path under the fsspec driver
and asserts that ``firecube.core.filesystem.ops._open_fsspec_url`` is never
invoked. Under the obstore driver, the same assertion would prevent a
silent downgrade to fsspec, satisfying the AGENTS.md "one driver
everywhere" rule.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from firecube.core.storage.transfer import copy_file
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import assert_no_fsspec_bypass, make_test_session

pytestmark = pytest.mark.unit


def test_copy_file_no_bypass(tmp_path: Path) -> None:
    """``storage_transfer.copy_file`` must route through ``session.fs()`` only."""
    src_path = tmp_path / "src.bin"
    src_path.write_bytes(b"hello-firecube")
    dst_path = tmp_path / "dst.bin"

    session = make_test_session(tmp_path)
    src_uri = StorageUri.from_local_path(src_path)
    dst_uri = StorageUri.from_local_path(dst_path)

    with assert_no_fsspec_bypass():
        copy_file(src_uri, dst_uri, source_session=session, target_session=session)

    assert dst_path.exists()
    assert dst_path.read_bytes() == src_path.read_bytes()


def test_upload_tree_no_bypass(tmp_path: Path) -> None:
    """``session.upload_tree`` (parallel) must not touch the legacy adapter."""
    src_root = tmp_path / "src.zarr"
    src_root.mkdir()
    payloads = {
        "zarr.json": b"{}",
        "data/a.bin": b"alpha",
        "data/b.bin": b"beta",
    }
    for rel, data in payloads.items():
        target = src_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    dst_root = tmp_path / "dst.zarr"
    session = make_test_session(tmp_path)
    src_uri = StorageUri.from_local_path(src_root)
    dst_uri = StorageUri.from_local_path(dst_root)

    with assert_no_fsspec_bypass():
        session.upload_tree(src_uri, dst_uri, parallel_workers=4)

    for rel, data in payloads.items():
        copied = dst_root / rel
        assert copied.exists(), f"Missing uploaded file: {rel}"
        assert copied.read_bytes() == data, f"Content mismatch for: {rel}"


def test_control_plane_no_bypass(tmp_path: Path) -> None:
    """``session.control_plane().list_runs`` uses the injected filesystem only."""
    session = make_test_session(tmp_path)
    cm = session.control_plane()
    try:
        with assert_no_fsspec_bypass():
            runs = cm.list_runs(product="p")
        assert runs == []
    finally:
        cm.close()
