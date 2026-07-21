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
from typing import Any

from firecube.core.storage.uri import StorageUri


def test_counting_filesystem_ls_counts_once(
    counting_local_fs: tuple[Any, Any],
    tmp_path: Path,
) -> None:
    counting_fs, _ = counting_local_fs
    for idx in range(3):
        (tmp_path / f"file-{idx}.txt").write_text(f"payload-{idx}", encoding="utf-8")

    listing = counting_fs.ls(StorageUri.from_local_path(tmp_path))

    assert len(listing) == 3
    assert counting_fs.counts["ls"] == 1


def test_counting_filesystem_round_trips_bytes(
    counting_local_fs: tuple[Any, Any],
    tmp_path: Path,
) -> None:
    counting_fs, _ = counting_local_fs
    uri = StorageUri.from_local_path(tmp_path / "payload.bin")
    payload = b"counting-filesystem-pass-through"

    with counting_fs.open(uri, "wb") as handle:
        handle.write(payload)

    assert counting_fs.read_bytes(uri) == payload
    assert counting_fs.exists(uri)
    assert counting_fs.counts["open"] == 1
    assert counting_fs.counts["exists"] == 1


def test_counting_filesystem_reset_clears_counters(
    counting_local_fs: tuple[Any, Any],
    tmp_path: Path,
) -> None:
    counting_fs, _ = counting_local_fs
    uri = StorageUri.from_local_path(tmp_path / "payload.bin")

    counting_fs.exists(uri)
    counting_fs.reset()

    assert counting_fs.counts == {"ls": 0, "exists": 0, "open": 0, "rm": 0, "write_atomic": 0}
