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

from firecube.core.controlplane import ChunkManager
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, RegularTimeAxis
from firecube.core.zarr.planning import coord_chunk_sizes_by_group
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.errors import ConfigurationError
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.gate]

_GROUP = "data"
_COORD = "time"
_PRODUCT = "planner-window.zarr"


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group=_GROUP,
            arrays=[
                ZarrArraySpec(
                    name=_COORD,
                    shape=(512,),
                    dtype="datetime64[ns]",
                    chunks=(256,),
                )
            ],
        )
    ]


def _resolved_index() -> object:
    spec = IndexSpec(
        name="planner-window-v1",
        groups={
            _GROUP: RegularTimeAxis(
                coordinate=_COORD,
                epoch="2026-01-01T00:00:00Z",
                cadence_s=60,
                mode="exact",
                slot_count=512,
            )
        },
    )
    return resolve_index_spec(spec, time_dim_name=_COORD)


def _manager(tmp_path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=_PRODUCT))


def _record_started_run(
    manager: ChunkManager,
    *,
    run_id: str,
    slot_range: tuple[int, int],
) -> None:
    manager.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=f"file:///tmp/{_PRODUCT}",
        output_format="zarr",
        size=0,
        meta={"plugin": "planner-window-test"},
        slot_range=slot_range,
        slot_group=_GROUP,
    )


def _assert_window(
    manager: ChunkManager,
    window: tuple[int, int],
    *,
    current_run_id: str = "new-run",
) -> None:
    """Probe the live claim path: check the window, then roll the run back."""
    windows = {_GROUP: window}
    handle = manager.claim_coord_materialization_window(
        product=_PRODUCT,
        run_id=current_run_id,
        output_path=f"file:///tmp/{_PRODUCT}",
        output_format="zarr",
        windows_by_group=windows,
        coord_chunk_sizes=coord_chunk_sizes_by_group(_schema(), _resolved_index(), windows),
        slot_group=_GROUP,
        meta={"plugin": "planner-window-test"},
    )
    handle.release()
    manager.record_run_failed(
        product=_PRODUCT,
        run_id=current_run_id,
        output_path=f"file:///tmp/{_PRODUCT}",
        output_format="zarr",
        size=0,
        meta={"plugin": "planner-window-test"},
        error="window probe rollback",
    )


def test_sub_chunk_window_with_concurrent_conflict_refuses(tmp_path) -> None:
    manager = _manager(tmp_path)
    try:
        _record_started_run(manager, run_id="active-run", slot_range=(0, 10))

        with pytest.raises(ConfigurationError) as exc_info:
            _assert_window(manager, (20, 30))

        message = str(exc_info.value)
        assert "window [20, 30)" in message
        assert "coordinate chunk(s) [0]" in message
        assert "active-run" in message
        assert "state: started" in message
        assert "chunks runs abandon" in message
    finally:
        manager.close()


def test_aligned_windows_on_adjacent_chunks_succeed(tmp_path) -> None:
    manager = _manager(tmp_path)
    try:
        _record_started_run(manager, run_id="active-run", slot_range=(0, 256))

        _assert_window(manager, (256, 512))
    finally:
        manager.close()


def test_aligned_windows_on_adjacent_chunks_succeed_reverse_direction(tmp_path) -> None:
    """The adjacent-chunk guarantee is symmetric.

    The forward leg parks the active run in chunk 0 and probes chunk 1;
    this leg parks the active run in chunk 1 (``slot_range=(256, 512)``)
    and probes ``(0, 256)``. Both directions passing means the chunk files
    each run writes are disjoint by construction.
    """
    manager = _manager(tmp_path)
    try:
        _record_started_run(manager, run_id="active-run", slot_range=(256, 512))

        _assert_window(manager, (0, 256))
    finally:
        manager.close()


def test_window_straddling_chunk_boundary_refuses(tmp_path) -> None:
    """A window that spans two coord chunks is rejected when either is owned.

    ``window=(250, 500)`` crosses the chunk-0 / chunk-1 boundary at slot
    ``256``. An active run holding any slot in chunk 0 makes the straddling
    window unsafe: the planner must name coord chunk 0 (the chunk the
    active run owns and the proposed window also touches) so operators can
    map the failure straight back to the offending chunk.
    """
    manager = _manager(tmp_path)
    try:
        _record_started_run(manager, run_id="chunk0-run", slot_range=(0, 10))

        with pytest.raises(ConfigurationError) as exc_info:
            _assert_window(manager, (250, 500))

        message = str(exc_info.value)
        assert "window [250, 500)" in message
        assert "coordinate chunk(s) [0]" in message
        assert "chunk0-run" in message
        assert "state: started" in message
        assert "chunks runs abandon" in message
    finally:
        manager.close()


def test_sub_chunk_window_with_no_conflict_succeeds(tmp_path) -> None:
    manager = _manager(tmp_path)
    try:
        _record_started_run(manager, run_id="active-run", slot_range=(256, 266))

        _assert_window(manager, (20, 30))
    finally:
        manager.close()


def test_preallocate_full_window_with_concurrent_conflict_refuses(tmp_path) -> None:
    manager = _manager(tmp_path)
    try:
        _record_started_run(manager, run_id="ingest-run", slot_range=(300, 310))

        with pytest.raises(ConfigurationError) as exc_info:
            _assert_window(manager, (0, 512), current_run_id="preallocate")

        message = str(exc_info.value)
        assert "window [0, 512)" in message
        assert "coordinate chunk(s) [1]" in message
        assert "ingest-run" in message
    finally:
        manager.close()
