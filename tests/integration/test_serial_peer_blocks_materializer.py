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

"""Gate: a live serial ingest run blocks the windowed coord materializer.

The reverse leg (a ranged run trying to start while a serial run is live)
is already refused by ``ResumeGuard._check_non_terminal_runs``. The forward
leg is refused inside ``ChunkManager.claim_coord_materialization_window``:
any non-terminal peer with ``slot_range is None`` is treated as holding the
full slot extent, so every proposed materialization window conflicts with
it regardless of coord-chunk geometry.

The ranged-peer chunk-overlap semantics used by concurrent range writers
must not regress: a ranged peer whose owned coord chunks are disjoint from
the requested window still lets the claim succeed.
"""

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
_PRODUCT = "serial-first.zarr"
_SERIAL_RUN_ID = "serial-ingest-run"
_RANGED_RUN_ID = "ranged-peer-run"


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
        name="serial-first-v1",
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


def _record_serial_run(manager: ChunkManager, *, run_id: str) -> None:
    manager.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=f"file:///tmp/{_PRODUCT}",
        output_format="zarr",
        size=0,
        meta={"plugin": "serial-first-test"},
        slot_range=None,
        slot_group=None,
    )


def _record_ranged_run(
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
        size=slot_range[1] - slot_range[0],
        meta={"plugin": "serial-first-test"},
        slot_range=slot_range,
        slot_group=_GROUP,
    )


def _claim_window(
    manager: ChunkManager,
    window: tuple[int, int],
    *,
    current_run_id: str = "materializer-run",
) -> None:
    windows = {_GROUP: window}
    handle = manager.claim_coord_materialization_window(
        product=_PRODUCT,
        run_id=current_run_id,
        output_path=f"file:///tmp/{_PRODUCT}",
        output_format="zarr",
        windows_by_group=windows,
        coord_chunk_sizes=coord_chunk_sizes_by_group(_schema(), _resolved_index(), windows),
        slot_group=_GROUP,
        meta={"plugin": "serial-first-test"},
    )
    handle.release()
    manager.record_run_failed(
        product=_PRODUCT,
        run_id=current_run_id,
        output_path=f"file:///tmp/{_PRODUCT}",
        output_format="zarr",
        size=0,
        meta={"plugin": "serial-first-test"},
        error="window probe rollback",
    )


def test_serial_ingest_first_blocks_windowed_materializer(tmp_path) -> None:
    """A live serial peer must reject every proposed window as full-extent conflict."""
    manager = _manager(tmp_path)
    try:
        _record_serial_run(manager, run_id=_SERIAL_RUN_ID)

        with pytest.raises(ConfigurationError) as exc_info:
            _claim_window(manager, (256, 512))

        message = str(exc_info.value)
        assert "conflicting serial ingest run" in message, message
        assert _SERIAL_RUN_ID in message, message
        assert "holds full extent" in message, message
        assert "chunks runs abandon" in message, message
        assert "coordinate chunk(s)" not in message, (
            "serial-peer message must not borrow the ranged-peer chunk-overlap phrasing; "
            f"got: {message!r}"
        )
    finally:
        manager.close()


def test_ranged_peer_chunk_disjoint_window_still_succeeds(tmp_path) -> None:
    """Regression: a ranged peer disjoint from the proposed window keeps working."""
    manager = _manager(tmp_path)
    try:
        _record_ranged_run(manager, run_id=_RANGED_RUN_ID, slot_range=(0, 256))

        _claim_window(manager, (256, 512))
    finally:
        manager.close()
