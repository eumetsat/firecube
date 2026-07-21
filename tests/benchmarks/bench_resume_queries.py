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

import json
import time
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.storage.session import StorageSession  # pyright: ignore[reportMissingImports]
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_local_session, make_test_binding, make_test_session


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


def _session(uri: str) -> StorageSession:
    path = Path(uri)
    return make_test_session(path.parent, product=path.name)


def _populate_spans(manager: ChunkManager, product: str, n_runs: int, n_groups: int = 5) -> None:
    for i in range(n_runs):
        rid = f"r{i:04d}"
        month = (i % 12) + 1
        tmin = f"2024-{month:02d}-01T00:00:00"
        tmax = f"2024-{month:02d}-28T23:59:59"
        grp = f"G{i % n_groups}"
        manager.record_run_started(
            product=product,
            run_id=rid,
            output_path=str(manager.workspace / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "bench"},
        )
        manager.record_span(
            product=product,
            run_id=rid,
            batch_id="b1",
            group=grp,
            status="active",
            coverage=SpanCoverage(
                group=grp,
                arrays=[f"{grp}/val"],
                time_index_ranges=[[i * 10, (i + 1) * 10 - 1]],
                time_min=tmin,
                time_max=tmax,
            ),
            meta={"plugin": "bench", "group": grp, "time_min": tmin, "time_max": tmax},
        )
        manager.record_run_terminal(
            product=product,
            run_id=rid,
            output_path=str(manager.workspace / product),
            output_format="zarr",
            size=1,
            meta={"plugin": "bench"},
            status="complete",
        )


THRESHOLD_RATIO = 2.0  # filtered must be <= 2x baseline
THRESHOLD_ABS_S = 5.0  # absolute max per query


@pytest.fixture(scope="module")
def populated_manager(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("bench")
    m = _make_manager(tmp)
    _populate_spans(m, "P", n_runs=100)
    return m


def test_list_chunks_time_filter_overhead(populated_manager):
    m = populated_manager
    t0 = time.perf_counter()
    m.list_chunks(product="P", chunk_type="span")
    baseline = time.perf_counter() - t0

    t0 = time.perf_counter()
    m.list_chunks(product="P", chunk_type="span", time_min_after="2024-06-01T00:00:00")
    filtered = time.perf_counter() - t0

    ratio = filtered / max(baseline, 0.001)
    assert ratio <= THRESHOLD_RATIO, (
        f"time_min_after {ratio:.1f}x slower than baseline (threshold {THRESHOLD_RATIO}x)"
    )
    assert filtered <= THRESHOLD_ABS_S, (
        f"time_min_after took {filtered:.3f}s (threshold {THRESHOLD_ABS_S}s)"
    )


def test_list_chunks_time_overlaps_overhead(populated_manager):
    m = populated_manager
    t0 = time.perf_counter()
    m.list_chunks(product="P", chunk_type="span")
    baseline = time.perf_counter() - t0

    t0 = time.perf_counter()
    m.list_chunks(
        product="P", chunk_type="span", time_overlaps=("2024-03-01T00:00:00", "2024-09-01T00:00:00")
    )
    filtered = time.perf_counter() - t0

    ratio = filtered / max(baseline, 0.001)
    assert ratio <= THRESHOLD_RATIO, (
        f"time_overlaps {ratio:.1f}x slower (threshold {THRESHOLD_RATIO}x)"
    )
    assert filtered <= THRESHOLD_ABS_S


def test_list_runs_filter_overhead(populated_manager):
    m = populated_manager
    t0 = time.perf_counter()
    m.list_runs(product="P")
    baseline = time.perf_counter() - t0

    t0 = time.perf_counter()
    m.list_runs(product="P", status="complete")
    filtered = time.perf_counter() - t0

    ratio = filtered / max(baseline, 0.001)
    assert ratio <= 1.5, f"list_runs status filter {ratio:.1f}x slower (threshold 1.5x)"


def test_time_coverage_summary_overhead(populated_manager):
    m = populated_manager
    t0 = time.perf_counter()
    m.list_chunks(product="P", chunk_type="span")
    baseline = time.perf_counter() - t0

    t0 = time.perf_counter()
    m.time_coverage_summary(product="P")
    summary_t = time.perf_counter() - t0

    ratio = summary_t / max(baseline, 0.001)
    assert ratio <= 3.0, (
        f"time_coverage_summary {ratio:.1f}x slower than list_chunks (threshold 3x)"
    )
    assert summary_t <= THRESHOLD_ABS_S


def test_staged_metadata_seeding_overhead(tmp_path):
    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    for i in range(5):
        for j in range(3):
            arr = final / f"G{i}" / f"arr{j}"
            arr.mkdir(parents=True, exist_ok=True)
            meta = {"node_type": "array", "shape": [100, 3], "data_type": "float32"}
            (arr / "zarr.json").write_text(json.dumps(meta))

    t0 = time.perf_counter()
    seed_staged_store_metadata(
        temp_store_uri=str(temp),
        final_target_uri=str(final),
        groups=[f"G{i}" for i in range(5)],
        session=make_local_session(str(temp)),
    )
    elapsed = time.perf_counter() - t0
    assert elapsed <= 2.0, (
        f"staged metadata seeding took {elapsed:.3f}s (threshold 2.0s for 15 zarr.json files)"
    )


def test_local_storage_write_overhead(tmp_path):
    import shutil

    source = tmp_path / "source.zarr"
    _target = tmp_path / "target.zarr"
    for i in range(5):
        arr = source / f"G{i}" / "val"
        arr.mkdir(parents=True, exist_ok=True)
        (arr / "zarr.json").write_text(json.dumps({"node_type": "array", "shape": [40, 3]}))
        chunk_dir = arr / "c"
        chunk_dir.mkdir()
        for j in range(20):
            d = chunk_dir / str(j)
            d.mkdir()
            (d / "0").write_bytes(b"x" * 1024)

    target_existing = tmp_path / "target_existing.zarr"
    shutil.copytree(str(source), str(target_existing))
    for i in range(5):
        zarr_j = target_existing / f"G{i}" / "val" / "zarr.json"
        zarr_j.write_text(json.dumps({"node_type": "array", "shape": [100, 3]}))

    t0 = time.perf_counter()
    _session(str(target_existing)).upload_tree(
        StorageUri.parse(str(source)), StorageUri.parse(str(target_existing))
    )
    elapsed = time.perf_counter() - t0
    assert elapsed <= 5.0, (
        f"StorageSession.upload_tree() with merge took {elapsed:.3f}s (threshold 5.0s)"
    )
