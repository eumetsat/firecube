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

"""Unit tests for the resolved-index Zarr root-attr mirror.

``ChunkManager._mirror_resolved_index_attrs`` writes the two
reserved root attributes ``firecube_resolved_index`` and
``firecube_resolved_index_identity_hash`` inside the
``resolved_index:current`` write claim as part of
``ensure_resolved_index()``. The reserved-root-attrs guard rejects those
same names when supplied from external (user/plugin) code paths.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    RESOLVED_INDEX_ATTR,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    ResolvedIndexRecord,
    canonical_index_bytes,
)
from firecube.core.zarr._reserved_root_attrs import (
    RESERVED_ROOT_ATTRS,
    assert_root_attrs_safe,
)
from tests.helpers.storage import make_test_binding


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


def _index(group: str = "g1", size: int = 3) -> dict[str, object]:
    return {
        "groups": {
            group: {
                "axes": {"time": {"kind": "integer", "size": size}},
                "items": [
                    {"key": "a", "coordinates": {"time": 0}},
                    {"key": "b", "coordinates": {"time": 1}},
                ],
            }
        }
    }


def _record(
    *, run_id: str = "run-1", index: dict[str, object] | None = None
) -> ResolvedIndexRecord:
    payload = _index() if index is None else index
    return ResolvedIndexRecord(
        recorded_at="2026-08-20T00:00:00+00:00",
        recorded_by_run_id=run_id,
        identity_hash=hashlib.sha256(canonical_index_bytes(payload)).hexdigest(),
        index=payload,
    )


def _read_zarr_root_attrs(tmp_path: Path, product: str) -> dict[str, object]:
    store = LocalStore(str(tmp_path / product))
    root = zarr.open_group(store=store, mode="r", zarr_format=3)
    return dict(root.attrs)


@pytest.mark.unit
def test_ensure_resolved_index_mirrors_full_index_json_to_zarr_root_attr(
    tmp_path: Path,
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()

    cm.ensure_resolved_index(product="prod1", record=declared)

    root_attrs = _read_zarr_root_attrs(tmp_path, "prod1")
    assert RESOLVED_INDEX_ATTR in root_attrs
    assert root_attrs[RESOLVED_INDEX_ATTR] == canonical_index_bytes(declared.index).decode("utf-8")


@pytest.mark.unit
def test_ensure_resolved_index_mirrors_identity_hash_to_zarr_root_attr(
    tmp_path: Path,
) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()

    cm.ensure_resolved_index(product="prod1", record=declared)

    root_attrs = _read_zarr_root_attrs(tmp_path, "prod1")
    assert RESOLVED_INDEX_IDENTITY_HASH_ATTR in root_attrs
    assert root_attrs[RESOLVED_INDEX_IDENTITY_HASH_ATTR] == declared.identity_hash


@pytest.mark.unit
def test_read_resolved_index_attrs_hash_returns_written_hash(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    declared = _record()
    cm.ensure_resolved_index(product="prod1", record=declared)

    assert cm.read_resolved_index_attrs_hash(product="prod1") == declared.identity_hash


@pytest.mark.unit
def test_read_resolved_index_attrs_hash_returns_none_when_root_absent(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)

    assert cm.read_resolved_index_attrs_hash(product="never-created") is None


@pytest.mark.unit
def test_reserved_root_attrs_registry_includes_resolved_index_attrs() -> None:
    assert RESOLVED_INDEX_ATTR in RESERVED_ROOT_ATTRS
    assert RESOLVED_INDEX_IDENTITY_HASH_ATTR in RESERVED_ROOT_ATTRS


@pytest.mark.unit
def test_assert_root_attrs_safe_rejects_resolved_index_attr() -> None:
    with pytest.raises(ValueError, match=RESOLVED_INDEX_ATTR):
        assert_root_attrs_safe({RESOLVED_INDEX_ATTR: "{}"})


@pytest.mark.unit
def test_assert_root_attrs_safe_rejects_resolved_index_identity_hash_attr() -> None:
    with pytest.raises(ValueError, match=RESOLVED_INDEX_IDENTITY_HASH_ATTR):
        assert_root_attrs_safe({RESOLVED_INDEX_IDENTITY_HASH_ATTR: "a" * 64})
