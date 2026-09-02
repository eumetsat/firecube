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

"""Engine pod ingest proceeds alongside a non-terminal peer preallocate run.

Pod ingest with a slot window must not be rejected merely because a peer
preallocate run's ``slot_range`` shares a coordinate chunk with the
requested window:

* Fresh observed shells carry
  ``firecube_coord_managed`` at creation, so pod ingest against such a
  cube takes the verify-or-error write path
  (``RegionZarrWriter.write_timestamp``): cross-pod coord-chunk writes
  are race-free by construction, and every materialized timestamp in the
  ingested window survives with zero loss.
* Genuinely pre-marker legacy cubes (created before the managed-marker regime) fall back to the
  create-or-grow per-slot write path. Their parallel-pod fan-out race is
  characterized RED-by-design in
  ``test_multiprocess_race_managed_coord.py::test_shared_coord_chunk_multiprocess_race_loses_writes``
  (mark ``race``, excluded from the default lane); the corresponding
  fresh-managed GREEN gate lives in
  ``test_preallocate_parallel_zero_loss.py::test_p12_parallel_writers_on_fresh_managed_shell_zero_loss``.
* The only real cross-slot coord-chunk race is between two ``preallocate``
  runs, and the preallocate side serializes those through the atomic
  ``claim_coord_materialization_window`` (covered by
  ``test_two_concurrent_preallocates_on_overlapping_coord_chunk_must_not_both_succeed``).

Three scenarios, each with a non-terminal peer preallocate seeded in the WAL:

* Scenario A — ``firecube_coord_managed`` coord + peer window sharing coord
  chunk 0 with the engine window → ingest exits 0 and the window's data is
  written and recorded.
* Scenario B — fresh managed cube (the current default) + peer window sharing
  coord chunk 0 → verify-or-error pods preserve every materialized
  timestamp in the window with zero loss to ``NaT`` and zero drift; the
  data array is written and the run is recorded. The genuinely
  pre-marker legacy path this replaces is characterized in the race lane
  (see references above); Scenario B does not exercise it here.
* Scenario C — ``firecube_coord_managed`` + DISJOINT peer/engine coord
  chunks → ingest exits 0 and the run is recorded.

Peer/engine ``slot_range`` values are **slot-disjoint** but
**chunk-overlapping** for A and B. ``ResumeGuard.enforce`` raises
``RangeOverlapError`` on any half-open ``slot_range`` overlap between the
new run and an existing non-terminal peer run; slot-disjoint windows pass
it. With coord chunk size 256, peer ``[0, 100)`` and engine ``[100, 200)``
are slot-disjoint (half-open ranges touch but do not overlap) yet both land
in coord chunk 0 (``chunk_axis_range(*, 256) == {0}``) — exactly the shape
a coord-chunk-level rejection would spuriously refuse.

The peer is seeded via ``ChunkManager.record_run_started`` with a real
``slot_range`` so the engine sees a real WAL peer — no stubs, no monkey
patches on ``list_runs`` or on the plugin.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import direct_zarr_capable_test_plugin as _plugin_module
import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.ingestor.registry import loader as _loader
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.gate]

# ``direct_zarr_capable_test_plugin`` declares an exact RegularTimeAxis with
# slot_count=1000 on coordinate "timestamp" and NO coord ZarrArraySpec (the
# no-spec dense fallback path stamps ATTR_PREALLOCATED and default fill_value
# ``NaT[ns]`` on the coord array during preallocate, so no fill_value drift
# is possible on subsequent ingest). The dense resolver picks coord
# chunk_size=256, so coord chunk 0 covers slots [0, 256) and coord chunk 1
# covers slots [256, 512). The plugin's ``data`` array uses time chunk_size
# 100, so every slot window must align on a multiple of 100 (or reach the
# axis end 1000). Peer [0, 100) and engine [100, 200) are slot-disjoint
# (half-open ranges touch but do not overlap: 100 < 100 is False), yet
# both land in coord chunk 0, and both boundaries align on 100. Engine
# [300, 400) lands in coord chunk 1 (disjoint from the peer's coord
# chunk 0) and aligns on 100.
_PLUGIN = "direct_zarr_capable_test_plugin"
_PRODUCT = "direct_zarr_capable_test_product"
_GROUP = "data"
_COORD = "timestamp"
_PEER_RUN_ID = "peer-preallocate-run"
_PEER_SLOT_RANGE: tuple[int, int] = (0, 100)
_ENGINE_WINDOW_CHUNK_OVERLAP: tuple[int, int] = (100, 200)
_ENGINE_WINDOW_CHUNK_DISJOINT: tuple[int, int] = (300, 400)


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _target_path(tmp_path: Path) -> Path:
    return tmp_path / _PRODUCT


def _preallocate(tmp_path: Path) -> Path:
    """Materialize the dense coord + static arrays via the real preallocate CLI."""
    target = _target_path(tmp_path)
    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            _PLUGIN,
            "--target",
            f"file://{target}",
            "--product-name",
            _PRODUCT,
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--option",
            "no_progress=true",
        ],
    )
    assert result.exit_code == 0, (
        f"preallocate must succeed to set up the coord array under test; output:\n{result.output}"
    )
    return target


def _open_coord_rw(target: Path) -> Any:
    root = zarr.open_group(store=str(target), mode="a", zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD}"])


def _set_coord_state_managed(target: Path) -> None:
    """Rewrite coord attrs to ``CoordLifecycleState.COORD_MANAGED``.

    Preallocate on ``mode="exact"`` stamps ``ATTR_PREALLOCATED``; swap it to
    ``ATTR_COORD_MANAGED=True`` so the pod path takes the engine-managed
    verify-or-error branch on write, matching the design decision that pod
    ingest against a coord-managed cube must not be rejected on peer overlap.
    """
    coord = _open_coord_rw(target)
    if ATTR_PREALLOCATED in coord.attrs:
        del coord.attrs[ATTR_PREALLOCATED]
    coord.attrs[ATTR_COORD_MANAGED] = True


def _seed_peer_run(tmp_path: Path, target: Path, slot_range: tuple[int, int]) -> None:
    """Record a non-terminal peer preallocate run in the product WAL."""
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    wal = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        wal.record_run_started(
            product=_PRODUCT,
            run_id=_PEER_RUN_ID,
            output_path=str(target),
            output_format="zarr",
            size=slot_range[1] - slot_range[0],
            meta={"kind": "preallocate_peer"},
            slot_range=slot_range,
            slot_group=None,
        )
    finally:
        wal.close()


def _cleanup_peer_run(tmp_path: Path, target: Path, slot_range: tuple[int, int]) -> None:
    """Mark the seeded peer run as abandoned so it does not leak between tests."""
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    wal = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        wal.record_run_terminal(
            product=_PRODUCT,
            run_id=_PEER_RUN_ID,
            output_path=str(target),
            output_format="zarr",
            size=slot_range[1] - slot_range[0],
            meta={"kind": "preallocate_peer"},
            status="abandoned",
            slot_range=slot_range,
        )
    finally:
        wal.close()


def _list_runs(tmp_path: Path) -> list[Any]:
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    wal = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        return list(wal.list_runs(product=_PRODUCT))
    finally:
        wal.close()


def _assert_engine_run_recorded(tmp_path: Path, slot_range: tuple[int, int]) -> None:
    """The ingest must record a completed run with its slot window in the WAL."""
    runs = [r for r in _list_runs(tmp_path) if r.run_id != _PEER_RUN_ID]
    completed = [r for r in runs if r.status == "complete" and r.slot_range == slot_range]
    assert completed, (
        f"expected a completed engine run with slot_range={slot_range}; recorded runs: "
        + repr([(r.run_id, r.status, r.slot_range) for r in runs])
    )


def _assert_data_written(target: Path, slot_range: tuple[int, int]) -> None:
    """Every slot in the ingested window must hold the plugin's written row.

    ``direct_zarr_capable_test_plugin`` writes ``np.full((10,), float(item))``
    at the slot resolved for item ``item``, so row ``i`` of the ``data``
    array must equal ``float(i)`` across the whole window — not the fill
    value an unwritten shell would carry.
    """
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    data = cast(Any, root[f"{_GROUP}/data"])
    written = np.asarray(data[slot_range[0] : slot_range[1]])
    expected = np.repeat(
        np.arange(slot_range[0], slot_range[1], dtype=np.float32)[:, np.newaxis],
        written.shape[1],
        axis=1,
    )
    assert np.array_equal(written, expected), (
        f"slots [{slot_range[0]}, {slot_range[1]}) must hold the ingested values "
        f"(row i == float(i)); got {written!r}"
    )


def _assert_timestamps_intact(target: Path, slot_range: tuple[int, int]) -> None:
    """Every slot in the window must retain its materialized grid timestamp.

    ``_preallocate`` runs the real CLI, which for the plugin's grid-valued
    ``RegularTimeAxis(epoch=2024-01-01T00:00:00Z, cadence_s=1)`` prefills
    the coord array with ``epoch + slot * 1s`` and stamps a marker. Under
    the ``firecube_coord_managed`` marker (Scenario B), pod
    ``write_timestamp`` calls take the verify-or-error branch: matching
    values return as no-ops, mismatches raise ``SchemaDriftError``. So
    the window's slots must still hold their exact prefilled timestamps
    with zero loss to ``NaT`` and zero drift. This is the concrete
    ``0 loss on timestamps`` invariant that the fresh-managed contract
    provides to the peer-run scenario.
    """
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    coord = cast(Any, root[f"{_GROUP}/{_COORD}"])
    observed = np.asarray(coord[slot_range[0] : slot_range[1]]).astype("datetime64[ns]")
    epoch = np.datetime64("2024-01-01T00:00:00", "ns")
    cadence = np.timedelta64(1_000_000_000, "ns")
    expected = epoch + np.arange(slot_range[0], slot_range[1], dtype=np.int64) * cadence
    lost = int(np.sum(np.isnat(observed)))
    assert lost == 0, (
        f"expected all {slot_range[1] - slot_range[0]} timestamps in "
        f"slots [{slot_range[0]}, {slot_range[1]}) to survive under "
        f"firecube_coord_managed; {lost} slot(s) decayed to NaT. "
        f"observed={observed!r}"
    )
    assert np.array_equal(observed, expected), (
        f"timestamps in slots [{slot_range[0]}, {slot_range[1]}) drifted from "
        f"the materialized grid values. observed={observed!r}; expected={expected!r}"
    )


def _ingest_args(
    tmp_path: Path,
    target: Path,
    *,
    slot_start: int,
    slot_end: int,
) -> list[str]:
    input_dir = tmp_path / "input"
    input_dir.mkdir(exist_ok=True)
    return [
        "ingest",
        _PLUGIN,
        "--input-data",
        str(input_dir),
        "--target",
        f"file://{target}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--slot-start",
        str(slot_start),
        "--slot-end",
        str(slot_end),
        "--option",
        "no_progress=true",
    ]


def test_scenario_a_coord_managed_chunk_overlapping_peer_ingest_must_exit_zero(
    tmp_path: Path,
) -> None:
    """Scenario A: coord-managed marker + chunk-overlapping peer window → exit 0.

    Pods on a coord-managed cube take the verify-or-error write path, so
    there is no cross-slot coord-chunk race. Even though the peer's
    non-terminal ``slot_range=(0, 100)`` and the engine window ``[100, 200)``
    both intersect coord chunk 0, the engine filters the plugin's items to
    the requested slot window, produces batches, writes ``data`` values at
    slots 100..199, records the run, and exits 0.
    """
    target = _preallocate(tmp_path)
    _set_coord_state_managed(target)
    _seed_peer_run(tmp_path, target, _PEER_SLOT_RANGE)
    try:
        result = CliRunner().invoke(
            cli,
            _ingest_args(
                tmp_path,
                target,
                slot_start=_ENGINE_WINDOW_CHUNK_OVERLAP[0],
                slot_end=_ENGINE_WINDOW_CHUNK_OVERLAP[1],
            ),
        )
        exception_repr = repr(result.exception) if result.exception is not None else ""
        assert result.exit_code == 0, (
            "engine ingest must exit 0 under a chunk-overlapping peer preallocate "
            "on a coord-managed cube: pod writes are verify-or-error, so the peer "
            "slot_range=(0, 100) sharing coord chunk 0 with window [100, 200) is "
            "not a write conflict. "
            f"exception={exception_repr!r}\noutput:\n{result.output}"
        )
        _assert_data_written(target, _ENGINE_WINDOW_CHUNK_OVERLAP)
        _assert_engine_run_recorded(tmp_path, _ENGINE_WINDOW_CHUNK_OVERLAP)
    finally:
        _cleanup_peer_run(tmp_path, target, _PEER_SLOT_RANGE)


def test_scenario_b_fresh_managed_chunk_overlapping_peer_preserves_all_timestamps(
    tmp_path: Path,
) -> None:
    """Scenario B: fresh managed cube + chunk-overlapping peer → 0 timestamp loss.

    Fresh observed shells are stamped with
    ``firecube_coord_managed`` at creation; the marker-less pre-marker
    path only survives on genuinely pre-marker cubes. Under the marker,
    ``RegionZarrWriter.write_timestamp`` takes the verify-or-error branch:
    matching values return as no-ops and mismatches raise
    ``SchemaDriftError``. So the shared coord chunk 0 between the peer's
    ``slot_range=(0, 100)`` and the engine window ``[100, 200)`` cannot
    lose timestamps to a chunk-file race, and every materialized slot in
    the window survives with zero loss to ``NaT`` and zero drift.

    The genuinely-legacy parallel-pod fan-out race that this replaces is
    preserved as RED-by-design in
    ``test_multiprocess_race_managed_coord.py::test_shared_coord_chunk_multiprocess_race_loses_writes``
    (mark ``race``, excluded from the default lane); the fresh-managed
    GREEN gate for parallel writers on a shared coord chunk lives in
    ``test_preallocate_parallel_zero_loss.py::test_p12_parallel_writers_on_fresh_managed_shell_zero_loss``.
    """
    target = _preallocate(tmp_path)
    _set_coord_state_managed(target)
    _seed_peer_run(tmp_path, target, _PEER_SLOT_RANGE)
    try:
        result = CliRunner().invoke(
            cli,
            _ingest_args(
                tmp_path,
                target,
                slot_start=_ENGINE_WINDOW_CHUNK_OVERLAP[0],
                slot_end=_ENGINE_WINDOW_CHUNK_OVERLAP[1],
            ),
        )
        exception_repr = repr(result.exception) if result.exception is not None else ""
        assert result.exit_code == 0, (
            "engine ingest must exit 0 on a fresh managed cube with a "
            "chunk-overlapping peer WAL: verify-or-error pod writes on the "
            "coord-managed coord cannot race on shared coord chunks, so the "
            "peer slot_range=(0, 100) sharing coord chunk 0 with window "
            "[100, 200) is not a write conflict. "
            f"exception={exception_repr!r}\noutput:\n{result.output}"
        )
        _assert_timestamps_intact(target, _ENGINE_WINDOW_CHUNK_OVERLAP)
        _assert_data_written(target, _ENGINE_WINDOW_CHUNK_OVERLAP)
        _assert_engine_run_recorded(tmp_path, _ENGINE_WINDOW_CHUNK_OVERLAP)
    finally:
        _cleanup_peer_run(tmp_path, target, _PEER_SLOT_RANGE)


def test_scenario_c_coord_managed_chunk_disjoint_peer_ingest_must_exit_zero(
    tmp_path: Path,
) -> None:
    """Scenario C: coord-managed marker + DISJOINT peer coord chunk → exit 0.

    The peer at ``[0, 100)`` covers coord chunk 0; the engine ingests
    ``[300, 400)``, which covers coord chunk 1. The chunk sets are
    disjoint, so no coord-chunk interaction with the peer exists at all,
    and the ingest must record its run and exit 0.
    """
    target = _preallocate(tmp_path)
    _set_coord_state_managed(target)
    _seed_peer_run(tmp_path, target, _PEER_SLOT_RANGE)
    try:
        result = CliRunner().invoke(
            cli,
            _ingest_args(
                tmp_path,
                target,
                slot_start=_ENGINE_WINDOW_CHUNK_DISJOINT[0],
                slot_end=_ENGINE_WINDOW_CHUNK_DISJOINT[1],
            ),
        )
        exception_repr = repr(result.exception) if result.exception is not None else ""
        assert result.exit_code == 0, (
            "engine ingest must exit 0 on a disjoint peer window: there is no "
            "coord-chunk overlap between peer [0, 100) (chunk 0) and window "
            "[300, 400) (chunk 1). "
            f"exception={exception_repr!r}\noutput:\n{result.output}"
        )
        _assert_engine_run_recorded(tmp_path, _ENGINE_WINDOW_CHUNK_DISJOINT)
    finally:
        _cleanup_peer_run(tmp_path, target, _PEER_SLOT_RANGE)
