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

"""Tests for validate_group_with_fs() budget controls."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.storage import make_local_session


def _make_chunked_store(base: Path, n_chunks: int) -> tuple[Path, str]:
    """Create a local Zarr V3 store with n_chunks chunk files."""
    arr = base / "G" / "val"
    arr.mkdir(parents=True, exist_ok=True)
    meta = {
        "node_type": "array",
        "shape": [n_chunks, 3],
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1, 3]}},
        "dimension_names": ["timestamp", "x"],
        "data_type": "float32",
        "fill_value": None,
        "chunk_key_encoding": {"name": "default", "separator": "/"},
        "codecs": [],
    }
    (arr / "zarr.json").write_text(json.dumps(meta))
    chunk_dir = arr / "c"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_chunks):
        d = chunk_dir / str(i)
        d.mkdir(exist_ok=True)
        (d / "0").write_bytes(b"x")
    return base, "G/val"


def _validate(store: Path, group: str, **kwargs):
    """Build a typed-fs session for the local store and call validate_group_with_fs."""
    from firecube.core.zarr.validation import validate_group_with_fs

    session = make_local_session(str(store))
    return validate_group_with_fs(session.fs(), session.product.product_uri, group, **kwargs)


def test_max_chunks_limits_processing(tmp_path):
    store, group = _make_chunked_store(tmp_path / "store", 50)

    report = _validate(store, group, max_chunks=10, on_timeout="warn")
    assert report.budget_exceeded is True
    assert report.chunks_processed <= 10


def test_on_timeout_fail_raises(tmp_path):
    store, group = _make_chunked_store(tmp_path / "store", 50)

    with pytest.raises(TimeoutError):
        _validate(store, group, max_chunks=5, on_timeout="fail")


def test_no_budget_processes_all(tmp_path):
    store, group = _make_chunked_store(tmp_path / "store", 20)

    report = _validate(store, group)
    assert report.budget_exceeded is False
    assert report.chunks_processed == 0  # budget not active, counter not tracked
