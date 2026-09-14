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
from typing import cast

import pytest

from firecube.core.filesystem.protocol import StorageFilesystem
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
    kwargs.setdefault("time_dim_name", "timestamp")
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


class _FakeFs:
    pass


class _RaisingRoot:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def __getitem__(self, key):
        raise self._exc


def test_open_zarr_root_from_fs_returns_none_and_warns_on_expected_error(monkeypatch, caplog):
    import zarr

    from firecube.core.storage.uri import StorageUri
    from firecube.core.zarr import validation

    def _raise(**kwargs):
        raise FileNotFoundError("no such array")

    monkeypatch.setattr(zarr, "open_group", _raise)
    uri = StorageUri.parse("file:///tmp/nonexistent-a11.zarr")
    fs = cast(StorageFilesystem, _FakeFs())

    with caplog.at_level("WARNING", logger="firecube.core.zarr.validation"):
        result = validation._open_zarr_root_from_fs(fs, uri)

    assert result is None
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings, "expected a WARNING log record"
    joined = " ".join(r.getMessage() for r in warnings)
    assert "no such array" in joined
    assert any(r.exc_info is not None for r in warnings)


def test_open_zarr_root_from_fs_propagates_unexpected(monkeypatch):
    import zarr

    from firecube.core.storage.uri import StorageUri
    from firecube.core.zarr import validation

    def _raise(**kwargs):
        raise RuntimeError("weird")

    monkeypatch.setattr(zarr, "open_group", _raise)
    uri = StorageUri.parse("file:///tmp/nonexistent-a11.zarr")
    fs = cast(StorageFilesystem, _FakeFs())

    with pytest.raises(RuntimeError, match="weird"):
        validation._open_zarr_root_from_fs(fs, uri)


def test_zarr_array_at_returns_none_and_warns_on_keyerror(caplog):
    from firecube.core.zarr import validation

    root = _RaisingRoot(KeyError("missing arr"))

    with caplog.at_level("WARNING", logger="firecube.core.zarr.validation"):
        result = validation._zarr_array_at(root, "arr")

    assert result is None
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings
    joined = " ".join(r.getMessage() for r in warnings)
    assert "missing arr" in joined
    assert "'arr'" in joined


def test_zarr_array_at_propagates_unexpected():
    from firecube.core.zarr import validation

    root = _RaisingRoot(RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        validation._zarr_array_at(root, "arr")


def test_zarr_array_at_returns_none_when_root_is_none():
    from firecube.core.zarr import validation

    assert validation._zarr_array_at(None, "arr") is None


def test_candidate_time_dim_accepts_matching_explicit_state_dimension():
    from firecube.core.zarr import validation

    arrays = [
        validation._ArrayInfo(
            path="G/firecube_timestamp_state",
            dim_names=["acquisition_time"],
            shape=[3],
            chunk_shape=[3],
        )
    ]

    result = validation._candidate_time_dim(arrays, time_dim_name="acquisition_time")

    assert result == "acquisition_time"


def test_candidate_time_dim_rejects_contradicting_explicit_state_dimension():
    from firecube.core.zarr import validation

    arrays = [
        validation._ArrayInfo(
            path="G/firecube_timestamp_state",
            dim_names=["timestamp"],
            shape=[3],
            chunk_shape=[3],
        )
    ]

    with pytest.raises(ValueError, match=r"time.*timestamp"):
        validation._candidate_time_dim(arrays, time_dim_name="time")


def test_candidate_time_dim_logs_when_explicit_name_absent(caplog):
    from firecube.core.zarr import validation

    arrays = [
        validation._ArrayInfo(
            path="G/firecube_timestamp_state",
            dim_names=["timestamp"],
            shape=[3],
            chunk_shape=[3],
        )
    ]

    with caplog.at_level("INFO", logger="firecube.core.zarr.validation"):
        result = validation._candidate_time_dim(arrays)

    assert result == "timestamp"
    assert [
        record
        for record in caplog.records
        if "No explicit time dimension name provided" in record.getMessage()
    ]
