# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deleting an omitted fill-only chunk succeeds without a missing-key error.

When write_empty_chunks=False, fill-value-only chunks are never materialised.
Attempting to delete them raises FileNotFoundError. The deletion engine must
treat that as a non-error (the data is gone) and still mark the span as
replaced, but must NOT count the phantom key toward ``deleted_keys`` — that
counter reports actual on-disk removals only. Real errors (PermissionError,
etc.) must still surface and block span replacement.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.types import ChunkInfo
from tests.helpers.storage import make_test_binding


def _make_span(product: str = "my_product") -> ChunkInfo:
    return ChunkInfo(
        key=f"span_{product}_b1_F120",
        product=product,
        chunk_type="span",
        size=0,
        timestamp=1.0,
        manifest_path=f"/tmp/{product}/.firecube/spans/span_b1_F120.json",
        meta={"group": ""},
        record={
            "span": {
                "arrays": ["data"],
                "time_index_ranges": [[0, 3]],
                "aligned": True,
            }
        },
    )


def _make_engine_with_mocked_repo(tmp_path):
    from firecube.core.controlplane.repo import ManifestRepository

    binding = make_test_binding(tmp_path)
    repo = ManifestRepository(binding=binding, workspace=tmp_path)

    repo.list_claims = MagicMock(return_value=[])
    repo.acquire_claim = MagicMock()
    repo.clear_claim = MagicMock()
    repo.record_maintenance_started = MagicMock()
    repo.record_maintenance_completed = MagicMock()
    repo.record_maintenance_failed = MagicMock()
    repo.mark_chunks_replaced = MagicMock()

    return DeletionEngine(repo)


@pytest.mark.unit
def test_missing_chunk_key_treated_as_absent(tmp_path):
    engine = _make_engine_with_mocked_repo(tmp_path)
    span = _make_span()

    mock_fs = MagicMock()
    mock_fs.rm.side_effect = FileNotFoundError("chunk not found")

    mock_base_uri = MagicMock()
    mock_base_uri.protocol = "file"
    mock_base_uri.path = str(tmp_path)
    mock_base_uri.to_str.return_value = f"file://{tmp_path}"

    engine.repo._get_fs = MagicMock(return_value=(mock_fs, mock_base_uri))

    grid = (["timestamp", "lat", "lon"], [4, 2, 3], [4, 2, 3], 0)

    with (
        patch(
            "firecube.core.controlplane.deletion.resolve_span_time_dims",
            return_value={span.key: "timestamp"},
        ),
        patch.object(engine, "_measure_span_alignment", return_value=(True, {"data": grid}, [])),
        patch.object(engine, "_acquire_maintenance_claims", return_value=[]),
    ):
        result = engine.delete_spans([span], force=True, update_manifest=True, update_state=False)

    assert result["errors"] == [], f"Expected no errors, got: {result['errors']}"
    assert result["deleted_spans"] == 1
    assert result["deleted_keys"] == 0
    engine.repo.mark_chunks_replaced.assert_called_once()  # type: ignore[attr-defined] # mock method


@pytest.mark.unit
def test_real_error_still_fails(tmp_path):
    engine = _make_engine_with_mocked_repo(tmp_path)
    span = _make_span()

    mock_fs = MagicMock()
    mock_fs.rm.side_effect = PermissionError("access denied")

    mock_base_uri = MagicMock()
    mock_base_uri.protocol = "file"
    mock_base_uri.path = str(tmp_path)
    mock_base_uri.to_str.return_value = f"file://{tmp_path}"

    engine.repo._get_fs = MagicMock(return_value=(mock_fs, mock_base_uri))

    grid = (["timestamp", "lat", "lon"], [4, 2, 3], [4, 2, 3], 0)

    with (
        patch(
            "firecube.core.controlplane.deletion.resolve_span_time_dims",
            return_value={span.key: "timestamp"},
        ),
        patch.object(engine, "_measure_span_alignment", return_value=(True, {"data": grid}, [])),
        patch.object(engine, "_acquire_maintenance_claims", return_value=[]),
    ):
        result = engine.delete_spans([span], force=True, update_manifest=True, update_state=False)

    assert result["errors"], "Expected errors for PermissionError, got none"
    assert any("access denied" in e for e in result["errors"])
    assert result["deleted_spans"] == 0
    engine.repo.mark_chunks_replaced.assert_not_called()  # type: ignore[attr-defined] # mock method
