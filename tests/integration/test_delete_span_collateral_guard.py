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

"""Shared-chunk deletion requires explicit confirmation of affected spans.

When ``firecube chunks delete-span --force`` targets a span whose
``time_index_ranges`` are misaligned with the stored chunk grid, the
physical chunk keys it removes may also hold slot data belonging to OTHER
active spans. Without a collateral guard the neighbouring spans are
silently destroyed (see GIRAFE regression that motivated ).

These tests exercise the guard end-to-end against a real Zarr v3 store:
one physical chunk holds two consecutive misaligned spans, and the
deletion engine is asked to force-delete just one of them.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


PRODUCT = "product.zarr"
GROUP = "data"
ARRAY_PATH = f"{GROUP}/counts"
CHUNK_LEN = 4


def _build_store_and_manager(tmp_path: Path) -> ChunkManager:
    store_root = tmp_path / PRODUCT
    store_root.mkdir(parents=True, exist_ok=True)

    root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
    grp = root.require_group(GROUP)
    arr = grp.create_array(
        "counts",
        shape=(8, 2),
        chunks=(CHUNK_LEN, 2),
        dtype="f4",
        dimension_names=("timestamp", "x"),
        overwrite=True,
    )
    arr[:] = np.ones((8, 2), dtype=np.float32)

    binding = make_test_binding(tmp_path, product=PRODUCT)
    return ChunkManager(binding=binding, workspace=tmp_path)


def _record_span(
    manager: ChunkManager,
    *,
    run_id: str,
    batch_id: str,
    time_index_ranges: list[list[int]],
    time_min: str,
    time_max: str,
) -> None:
    output_path = str(manager.workspace / PRODUCT)
    manager.record_run_started(
        product=PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    manager.record_span(
        product=PRODUCT,
        run_id=run_id,
        batch_id=batch_id,
        group=GROUP,
        status="active",
        coverage=SpanCoverage(
            group=GROUP,
            arrays=[ARRAY_PATH],
            time_index_ranges=time_index_ranges,
            aligned=False,
            time_min=time_min,
            time_max=time_max,
        ),
        meta={
            "plugin": "test",
            "group": GROUP,
            "time_min": time_min,
            "time_max": time_max,
        },
    )
    manager.record_run_terminal(
        product=PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": "test"},
        status="complete",
    )


def _find_span_by_run(manager: ChunkManager, run_id: str):
    spans = manager.repo.list_chunks(product=PRODUCT, chunk_type="span")
    for span in spans:
        if isinstance(span.meta, dict) and span.meta.get("run_id") == run_id:
            return span
    raise AssertionError(f"span for run_id={run_id!r} not found")


def _seed_shared_chunk_spans(manager: ChunkManager) -> tuple:
    """Record two misaligned spans that share physical chunk 0.

    Span A covers timestamp indices ``[0, 1]`` (half of chunk 0), span B
    covers ``[2, 3]`` (other half of chunk 0). Force-deleting one destroys
    physical chunk 0, which also holds the other span's slot data.
    """
    _record_span(
        manager,
        run_id="run-a",
        batch_id="batch-a",
        time_index_ranges=[[0, 1]],
        time_min="2024-01-01T00:00:00",
        time_max="2024-01-01T01:00:00",
    )
    _record_span(
        manager,
        run_id="run-b",
        batch_id="batch-b",
        time_index_ranges=[[2, 3]],
        time_min="2024-01-01T02:00:00",
        time_max="2024-01-01T03:00:00",
    )
    return _find_span_by_run(manager, "run-a"), _find_span_by_run(manager, "run-b")


class TestCollateralGuard:
    def test_collateral_guard_blocks_shared_chunk_deletion(self, tmp_path: Path) -> None:
        manager = _build_store_and_manager(tmp_path)
        span_a, span_b = _seed_shared_chunk_spans(manager)

        result = manager.delete_spans(
            [span_a],
            dry_run=False,
            force=True,
            yes_i_really_mean_it=False,
        )

        errors = result.get("errors") or []
        assert errors, (
            "Force-delete against a span that shares a physical chunk with "
            "another active span must refuse when yes_i_really_mean_it=False; "
            f"got no errors. result={result!r}"
        )
        combined = "\n".join(errors)
        assert span_b.key in combined, (
            "The refusal message must list the collateral span key so the "
            f"operator knows what would be destroyed; errors={errors!r}"
        )
        assert "yes_i_really_mean_it" in combined, (
            "The refusal message must tell the operator how to acknowledge "
            f"the collateral damage; errors={errors!r}"
        )
        assert result.get("deleted_keys", 0) == 0, (
            "No chunk keys must be removed while the guard is refusing; "
            f"deleted_keys={result.get('deleted_keys')!r}"
        )
        assert result.get("deleted_spans", 0) == 0

    def test_yes_i_really_mean_it_bypasses_guard(self, tmp_path: Path) -> None:
        manager = _build_store_and_manager(tmp_path)
        span_a, span_b = _seed_shared_chunk_spans(manager)

        result = manager.delete_spans(
            [span_a],
            dry_run=False,
            force=True,
            yes_i_really_mean_it=True,
        )

        assert not result.get("errors"), (
            "Ack flag must let the deletion proceed without new errors; "
            f"errors={result.get('errors')!r}"
        )
        assert result.get("deleted_keys", 0) > 0, (
            "Ack flag must let the force-delete write to storage; "
            f"deleted_keys={result.get('deleted_keys')!r}"
        )
        assert result.get("deleted_spans", 0) == 1

        collateral = result.get("collateral_spans") or []
        assert span_b.key in collateral, (
            "Collateral spans that were destroyed by shared-chunk removal "
            f"must be reported in result['collateral_spans']; got {collateral!r}"
        )

        remaining = manager.repo.list_chunks(product=PRODUCT, chunk_type="span")
        active_keys = {s.key for s in remaining if s.is_active}
        assert span_a.key not in active_keys, (
            f"Target span {span_a.key!r} must be marked replaced; active={active_keys!r}"
        )
        assert span_b.key not in active_keys, (
            "Collateral span must also be marked replaced when its data was "
            f"destroyed by the shared-chunk removal; active={active_keys!r}"
        )

    def test_dry_run_reports_collateral_as_warning(self, tmp_path: Path) -> None:
        manager = _build_store_and_manager(tmp_path)
        span_a, span_b = _seed_shared_chunk_spans(manager)

        result = manager.delete_spans(
            [span_a],
            dry_run=True,
            force=True,
            yes_i_really_mean_it=False,
        )

        assert not result.get("errors"), (
            "Dry-run collateral must be reported as a warning, not an error; "
            f"errors={result.get('errors')!r}"
        )
        warnings_out = result.get("warnings") or []
        combined = "\n".join(warnings_out)
        assert span_b.key in combined, (
            "Dry-run warnings must name the collateral span so the operator "
            f"can preview the destruction; warnings={warnings_out!r}"
        )
        assert "will also destroy" in combined, (
            "Dry-run warning wording must match the delete.md contract "
            f"('will also destroy'); warnings={warnings_out!r}"
        )
