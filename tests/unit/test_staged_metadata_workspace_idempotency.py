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

"""Workspace idempotency for staged metadata seeding."""

from __future__ import annotations

import json
from pathlib import Path

from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from tests.helpers.storage import assert_no_fsspec_bypass, make_local_session


def _make_zarr_array_meta(base: Path, group: str, array: str, shape: list[int]) -> None:
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


def test_pre_existing_workspace_zarr_json_is_preserved(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_array_meta(final, "G", "val", [100, 3])

    ws_arr = temp / "G" / "val"
    ws_arr.mkdir(parents=True)
    interrupted_meta = {
        "node_type": "array",
        "shape": [50, 3],
        "custom_marker": "interrupted",
    }
    (ws_arr / "zarr.json").write_text(json.dumps(interrupted_meta))

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            strict=True,
            session=session,
        )

    assert result["G"]["seeded"] is True
    assert result["G"]["files"] >= 1

    preserved = json.loads((ws_arr / "zarr.json").read_text())
    assert preserved.get("custom_marker") == "interrupted"
    assert preserved["shape"] == [50, 3]


def test_clean_workspace_zarr_json_is_seeded(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_array_meta(final, "G", "val", [100, 3])

    assert not (temp / "G" / "val" / "zarr.json").exists()

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            strict=True,
            session=session,
        )

    assert result["G"]["seeded"] is True
    assert result["G"]["files"] >= 1

    seeded = json.loads((temp / "G" / "val" / "zarr.json").read_text())
    assert seeded["shape"] == [100, 3]
    assert "custom_marker" not in seeded


def test_partial_pre_existing_workspace_seeding(tmp_path):
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _make_zarr_array_meta(final, "G", "val_a", [100, 3])
    _make_zarr_array_meta(final, "G", "val_b", [100, 3])

    ws_a = temp / "G" / "val_a"
    ws_a.mkdir(parents=True)
    interrupted_meta = {
        "node_type": "array",
        "shape": [50, 3],
        "custom_marker": "interrupted",
    }
    (ws_a / "zarr.json").write_text(json.dumps(interrupted_meta))

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            strict=True,
            session=session,
        )

    assert result["G"]["seeded"] is True
    assert result["G"]["files"] >= 2

    a = json.loads((ws_a / "zarr.json").read_text())
    assert a.get("custom_marker") == "interrupted"
    assert a["shape"] == [50, 3]

    b = json.loads((temp / "G" / "val_b" / "zarr.json").read_text())
    assert b["shape"] == [100, 3]
    assert "custom_marker" not in b
