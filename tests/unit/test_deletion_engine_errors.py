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

import pytest

from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import ChunkInfo
from firecube.core.errors import ManifestError
from tests.helpers.storage import make_test_binding


def _span_chunk(*, product: str) -> ChunkInfo:
    return ChunkInfo(
        key=f"span_{product}_b1_F120",
        product=product,
        chunk_type="span",
        size=0,
        timestamp=1.0,
        manifest_path=f"/tmp/{product}/.firecube/spans/span_b1_F120.json",
        record={"span": {"arrays": [], "time_index_ranges": []}},
    )


@pytest.mark.unit
def test_delete_spans_raises_for_mixed_products(temp_workspace):
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    engine = DeletionEngine(repo)

    spans = [_span_chunk(product="p1"), _span_chunk(product="p2")]
    with pytest.raises(ManifestError) as exc:
        engine.delete_spans(spans, dry_run=True)

    assert "single product" in str(exc.value)


@pytest.mark.unit
def test_delete_spans_raises_when_repository_filesystem_is_missing(temp_workspace):
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    def _missing_fs(_base_uri: str):
        raise ManifestError("filesystem is not available")

    repo._get_fs = _missing_fs  # type: ignore[method-assign]
    engine = DeletionEngine(repo)

    with pytest.raises(ManifestError) as exc:
        engine.delete_spans([_span_chunk(product="p1")], dry_run=True)

    assert "filesystem is not available" in str(exc.value)
