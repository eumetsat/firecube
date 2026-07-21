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

"""Tests for staged metadata seeding."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from firecube.core.storage.session import StorageSession
from firecube.ingestor.runtime.zarr.staged_metadata import (
    StagedMetadataError,
    _is_coordinate_chunk,
    seed_staged_store_metadata,
)
from tests.helpers.storage import assert_no_fsspec_bypass, make_local_session


def _make_zarr_store(base: Path, group: str, array: str, shape: list[int]) -> None:
    """Create a minimal Zarr V3 store with zarr.json at group/array."""
    arr_path = base / group / array
    arr_path.mkdir(parents=True, exist_ok=True)
    meta = {
        "node_type": "array",
        "shape": shape,
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": [min(shape[0], 40), *shape[1:]]},
        },
        "dimension_names": ["timestamp"] + [f"d{i}" for i in range(len(shape) - 1)],
        "data_type": "float32",
        "fill_value": None,
        "chunk_key_encoding": {"name": "default", "separator": "/"},
        "codecs": [],
    }
    (arr_path / "zarr.json").write_text(json.dumps(meta))


def _deny_write_bytes_under(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make Path.write_bytes fail for deterministic staged-write failure tests."""
    original_write_bytes = Path.write_bytes

    def guarded_write_bytes(self: Path, data: bytes) -> int:
        try:
            self.relative_to(path)
        except ValueError:
            return original_write_bytes(self, data)
        raise PermissionError(f"denied: {self}")

    monkeypatch.setattr(Path, "write_bytes", guarded_write_bytes)


def test_seed_copies_zarr_json(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "val", [100, 3])

    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    assert result["G"]["seeded"] is True
    assert result["G"]["files"] >= 1
    seeded_meta = json.loads((temp / "G" / "val" / "zarr.json").read_text())
    assert seeded_meta["shape"] == [100, 3]


def test_seed_fresh_ingest_is_noop(tmp_path):
    final = tmp_path / "nonexistent.zarr"
    temp = tmp_path / "temp.zarr"

    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    assert result["G"]["seeded"] is False
    assert not temp.exists() or not (temp / "G").exists()


def test_seed_only_copies_zarr_json(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "val", [50, 3])
    # Add a chunk file that should NOT be copied
    chunk_dir = final / "G" / "val" / "c"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    (chunk_dir / "0").write_bytes(b"chunk_data")

    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    # zarr.json should be copied
    assert (temp / "G" / "val" / "zarr.json").exists()
    # chunk file should NOT be copied
    assert not (temp / "G" / "val" / "c" / "0").exists()


def test_seed_staged_store_metadata_requires_session(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"

    with pytest.raises(TypeError, match="session"):
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
        )  # pyright: ignore[reportCallIssue]


def test_seed_staged_store_metadata_rejects_storage_config(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    session = make_local_session(str(final))

    with pytest.raises(TypeError, match="storage_config"):
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            storage_config={"driver": "fsspec"},  # pyright: ignore[reportCallIssue]
        )


def test_seed_staged_store_metadata_rejects_storage_options(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    session = make_local_session(str(final))

    with pytest.raises(TypeError, match="storage_options"):
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            storage_options={"key": "value"},  # pyright: ignore[reportCallIssue]
        )


def test_seed_staged_store_metadata_strict_wraps_unexpected_exception(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    broken_session = MagicMock(spec=StorageSession)
    broken_session.fs.side_effect = RuntimeError("boom")

    with pytest.raises(StagedMetadataError, match="boom"):
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=broken_session,
            strict=True,
        )


def test_seed_strict_raises_on_existing_target_failure(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """When final target exists but write to temp fails, strict mode must raise."""
    from firecube.ingestor.runtime.zarr.staged_metadata import (
        StagedMetadataError,
        seed_staged_store_metadata,
    )

    final = tmp_path / "final.zarr"
    arr = final / "G" / "val"
    arr.mkdir(parents=True)
    (arr / "zarr.json").write_text(json.dumps({"node_type": "array", "shape": [100, 3]}))

    temp = tmp_path / "temp.zarr"
    temp.mkdir()
    _deny_write_bytes_under(temp, monkeypatch)

    session = make_local_session(str(final))
    with pytest.raises(StagedMetadataError), assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            strict=True,
            session=session,
        )


def test_seed_strict_false_on_nonexistent_target(tmp_path):
    """When final target does not exist, strict=True still just skips (fresh ingest)."""
    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    final = tmp_path / "nonexistent.zarr"
    temp = tmp_path / "temp.zarr"

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            strict=True,
            session=session,
        )

    assert result["G"]["seeded"] is False


def test_seed_partial_failure_raises_in_strict_mode(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Partial failure (1 of 2 groups fails) still raises in strict mode."""
    from firecube.ingestor.runtime.zarr.staged_metadata import (
        StagedMetadataError,
        seed_staged_store_metadata,
    )

    final = tmp_path / "final.zarr"
    for grp in ["G1", "G2"]:
        arr = final / grp / "val"
        arr.mkdir(parents=True)
        (arr / "zarr.json").write_text(json.dumps({"node_type": "array", "shape": [50, 3]}))

    temp = tmp_path / "temp.zarr"
    temp.mkdir()
    (temp / "G1").mkdir()
    g2 = temp / "G2"
    g2.mkdir()
    _deny_write_bytes_under(g2, monkeypatch)

    session = make_local_session(str(final))
    with pytest.raises(StagedMetadataError), assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G1", "G2"],
            strict=True,
            session=session,
        )


def test_is_coordinate_chunk_matches_timestamp_chunk_v3():
    assert _is_coordinate_chunk("data/timestamp/c/0", ["timestamp"]) is True


def test_is_coordinate_chunk_matches_nested_chunk_path():
    assert _is_coordinate_chunk("data/timestamp/c/0/0", ["timestamp"]) is True


def test_is_coordinate_chunk_rejects_zarr_json():
    assert _is_coordinate_chunk("data/timestamp/zarr.json", ["timestamp"]) is False


def test_is_coordinate_chunk_rejects_substring_collision():
    assert _is_coordinate_chunk("data/timestamp_bounds/c/0", ["timestamp"]) is False


def test_is_coordinate_chunk_rejects_data_array_chunk():
    assert _is_coordinate_chunk("data/val/c/0", ["timestamp"]) is False


def test_is_coordinate_chunk_returns_false_for_empty_coords():
    assert _is_coordinate_chunk("data/timestamp/c/0", []) is False


def test_is_coordinate_chunk_returns_false_for_none_coords():
    assert _is_coordinate_chunk("data/timestamp/c/0", None) is False


def test_is_coordinate_chunk_handles_trailing_slash():
    assert _is_coordinate_chunk("data/timestamp/c/0/", ["timestamp"]) is True


def test_is_coordinate_chunk_matches_alternate_coord_name():
    assert _is_coordinate_chunk("data/time/c/0", ["time"]) is True


def test_seed_copies_coord_chunks_when_coordinate_arrays_provided(tmp_path):
    """coordinate_arrays=['timestamp'] copies timestamp/c/* chunks but NOT val chunks."""
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "timestamp", [2])
    ts_chunk_dir = final / "G" / "timestamp" / "c"
    ts_chunk_dir.mkdir(parents=True, exist_ok=True)
    (ts_chunk_dir / "0").write_bytes(b"timestamp_chunk_payload")
    _make_zarr_store(final, "G", "val", [2])
    val_chunk_dir = final / "G" / "val" / "c"
    val_chunk_dir.mkdir(parents=True, exist_ok=True)
    (val_chunk_dir / "0").write_bytes(b"val_chunk_payload")

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            coordinate_arrays=["timestamp"],
        )

    assert result["G"]["seeded"] is True
    ts_chunk_root = temp / "G" / "timestamp" / "c"
    assert ts_chunk_root.exists(), "timestamp chunk root not copied"
    assert any(ts_chunk_root.rglob("*")), "no timestamp chunk files copied"
    val_chunk_root = temp / "G" / "val" / "c"
    assert not val_chunk_root.exists() or not any(val_chunk_root.rglob("*")), (
        "val (data) chunks were wrongly copied"
    )


def test_seed_skips_coord_chunks_when_coordinate_arrays_is_none(tmp_path):
    """coordinate_arrays=None preserves exact legacy metadata-only behavior."""
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "timestamp", [2])
    ts_chunk_dir = final / "G" / "timestamp" / "c"
    ts_chunk_dir.mkdir(parents=True, exist_ok=True)
    (ts_chunk_dir / "0").write_bytes(b"timestamp_chunk_payload")

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    assert result["G"]["seeded"] is True
    ts_chunk_root = temp / "G" / "timestamp" / "c"
    assert not ts_chunk_root.exists() or not any(ts_chunk_root.rglob("*")), (
        "coord chunks copied despite coordinate_arrays=None"
    )


def test_seed_skips_coord_chunks_when_coordinate_arrays_is_empty_list(tmp_path):
    """coordinate_arrays=[] is semantically identical to None."""
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "timestamp", [2])
    ts_chunk_dir = final / "G" / "timestamp" / "c"
    ts_chunk_dir.mkdir(parents=True, exist_ok=True)
    (ts_chunk_dir / "0").write_bytes(b"timestamp_chunk_payload")

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            coordinate_arrays=[],
        )

    assert result["G"]["seeded"] is True
    ts_chunk_root = temp / "G" / "timestamp" / "c"
    assert not ts_chunk_root.exists() or not any(ts_chunk_root.rglob("*")), (
        "coord chunks copied despite coordinate_arrays=[]"
    )


def test_seed_does_not_overwrite_existing_workspace_coord_chunk(tmp_path):
    """If a workspace coord chunk already exists, seeding must NOT overwrite it."""
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "timestamp", [2])
    ts_chunk_dir = final / "G" / "timestamp" / "c"
    ts_chunk_dir.mkdir(parents=True, exist_ok=True)
    (ts_chunk_dir / "0").write_bytes(b"timestamp_chunk_payload")

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            coordinate_arrays=["timestamp"],
        )

    ts_chunk_root = temp / "G" / "timestamp" / "c"
    chunk_files = [p for p in ts_chunk_root.rglob("*") if p.is_file()]
    assert chunk_files, "first seed should produce a chunk file"
    chunk_path = chunk_files[0]
    sentinel = b"\x00" * chunk_path.stat().st_size
    chunk_path.write_bytes(sentinel)

    with assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            coordinate_arrays=["timestamp"],
        )

    assert chunk_path.read_bytes() == sentinel, (
        "second seed overwrote an existing workspace coord chunk"
    )


def test_seed_tolerates_group_lacking_named_coord_array(tmp_path):
    """A group that lacks the named coord array must not raise; silent no-op for those chunks."""
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_store(final, "G", "val", [2])

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
            coordinate_arrays=["timestamp"],
        )

    assert "G" in result
    ts_chunk_root = temp / "G" / "timestamp" / "c"
    assert not ts_chunk_root.exists() or not any(ts_chunk_root.rglob("*"))
