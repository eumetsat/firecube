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

"""Multi-process 12-writer race regression + promoted gate on a shared coord chunk.

Two tests share a common 12-writer, one-shared-chunk, ``multiprocessing.Pool``
harness. They differ only in how the target coord array is prepared before the
writers race:

* ``test_shared_coord_chunk_multiprocess_race_loses_writes`` (mark: ``race``)
  is the RED regression and stays RED. It creates the coord array
  with NO markers, so ``RegionZarrWriter.write_timestamp`` takes the legacy
  create-or-grow write path. Concurrent on-disk chunk-file rewrites overwrite
  each other and 10-11 of the 12 timestamps silently disappear per trial. It
  is preserved as executable documentation that the legacy path IS racy and
  is excluded from the default lane via ``@pytest.mark.race``.
* ``test_race_eliminated_under_coord_managed`` (mark: ``gate``) is the
  promoted gate. Setup pre-materializes slots ``[0..11]`` and
  stamps ``firecube_coord_managed`` on the coord array — exactly what
  ``firecube zarr preallocate`` produces for engine-managed coordinates.
  Under the marker, ``write_timestamp`` takes the verify-or-error branch, so
  each of the twelve workers sees its intended value already stored and
  returns without writing. No chunk-file rewrites collide, all 12 slots stay
  intact, and the test is GREEN. Any single lost or drifted slot fails the
  gate.

Both tests use real OS processes via ``multiprocessing.Pool(processes=12)`` —
never threads. Threads cannot reproduce the chunk-file overwrite pattern that
the on-disk store exhibits under real process concurrency.

The RED race test's trial count is driven by the ``--trials`` pytest option
(see the top-level ``conftest.py``); the GREEN gate runs a small fixed number
of trials. A ``multiprocessing.Manager().Barrier`` synchronizes the
twelve workers so they enter ``write_timestamp`` at the same instant, forcing
simultaneous chunk access. No ``time.sleep`` is used for synchronization.
"""

from __future__ import annotations

import multiprocessing as mp
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_GROUP = "data"
_TIME_COORD_NAME = "timestamp"
_SLOT_COUNT = 4320
_CHUNK_LEN = 256
_WRITER_COUNT = 12
_BARRIER_TIMEOUT_S = 30.0

_GATE_TRIALS = 3

_BASE_NS = np.datetime64("2024-01-01T00:00:00", "ns")
_TIMESTAMPS_NS: list[np.datetime64] = [
    _BASE_NS + np.timedelta64(i * 137, "s") for i in range(_WRITER_COUNT)
]


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "trial" in metafunc.fixturenames:
        if metafunc.definition.get_closest_marker("race") is not None:
            raw = metafunc.config.getoption("--trials")
            trials = int(raw) if raw is not None else 30
        else:
            trials = _GATE_TRIALS
        metafunc.parametrize("trial", range(trials))


_worker_barrier: Any = None


def _init_worker(barrier: Any) -> None:
    global _worker_barrier
    _worker_barrier = barrier


def _writer_worker(payload: tuple[str, int, str]) -> None:
    """Open the store from scratch and race one ``write_timestamp`` call.

    Each worker constructs its own ``RegionZarrWriter`` — no shared writer
    state — then blocks on the shared Barrier so all twelve workers reach
    ``write_timestamp`` at the same instant. The barrier eliminates
    scheduler-driven serialization so the underlying chunk-file race (legacy
    setup) or verify-or-error no-op (coord-managed setup) is the only
    behaviour left to observe.
    """
    store_path, slot_index, ts_iso = payload
    writer = RegionZarrWriter(store_uri=store_path, time_coord_name=_TIME_COORD_NAME)
    ts_val = np.datetime64(ts_iso, "ns")
    if _worker_barrier is not None:
        _worker_barrier.wait(timeout=_BARRIER_TIMEOUT_S)
    writer.write_timestamp(_GROUP, slot_index, ts_val)


def _prepare_unsealed_coord(store_path: Path) -> None:
    """Create the shared unsealed time coord array with dense chunks and NO markers.

    Slots ``[0..11]`` all live in the first ``(256,)`` chunk. The array is
    fill-valued at ``NaT[ns]`` and carries no ``firecube_preallocated`` or
    ``firecube_coord_managed`` attribute, so ``write_timestamp`` cannot take
    the verify-or-error branch and instead executes the racing legacy write.
    """
    store = LocalStore(str(store_path))
    root = zarr.open_group(store=store, mode="w", zarr_format=3)
    group = root.require_group(_GROUP)
    group.create_array(
        name=_TIME_COORD_NAME,
        shape=(_SLOT_COUNT,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(_CHUNK_LEN,),
        dimension_names=[_TIME_COORD_NAME],
    )


def _prepare_coord_managed(store_path: Path) -> None:
    """Create the shared coord array, materialize slots [0..11], stamp COORD_MANAGED.

    Mirrors the on-disk state ``firecube zarr preallocate`` leaves behind for
    an engine-managed coord: values written into the target window, then the
    ``firecube_coord_managed`` marker attr stamped. Under this state
    ``write_timestamp`` takes the verify-or-error branch: workers that agree
    with the stored value return as no-ops and only divergent writes raise.
    """
    store = LocalStore(str(store_path))
    root = zarr.open_group(store=store, mode="w", zarr_format=3)
    group = root.require_group(_GROUP)
    arr = group.create_array(
        name=_TIME_COORD_NAME,
        shape=(_SLOT_COUNT,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(_CHUNK_LEN,),
        dimension_names=[_TIME_COORD_NAME],
    )
    arr[:_WRITER_COUNT] = np.asarray(_TIMESTAMPS_NS, dtype="datetime64[ns]")
    arr.attrs[ATTR_COORD_MANAGED] = True


def _read_slot_values(store_path: Path) -> np.ndarray:
    store = LocalStore(str(store_path))
    root = zarr.open_group(store=store, mode="r", zarr_format=3)
    arr = cast("zarr.Array", root[f"{_GROUP}/{_TIME_COORD_NAME}"])
    return np.asarray(arr[:_WRITER_COUNT])


def _run_race(store_path: Path) -> np.ndarray:
    payloads = [(str(store_path), i, str(_TIMESTAMPS_NS[i])) for i in range(_WRITER_COUNT)]

    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    try:
        barrier = manager.Barrier(_WRITER_COUNT)
        with ctx.Pool(
            processes=_WRITER_COUNT,
            initializer=_init_worker,
            initargs=(barrier,),
        ) as pool:
            pool.map(_writer_worker, payloads, chunksize=1)
    finally:
        manager.shutdown()

    return _read_slot_values(store_path)


@pytest.mark.race
def test_shared_coord_chunk_multiprocess_race_loses_writes(
    tmp_path_factory: pytest.TempPathFactory, trial: int
) -> None:
    """RED: twelve processes racing on one shared coord chunk lose 10-11 writes.

    Preserved as executable documentation that the legacy (marker-absent)
    coord write path IS racy. The setup deliberately omits every sealing
    marker, so ``write_timestamp`` takes the create-or-grow branch and each
    process performs an unsynchronized read-modify-write of the shared chunk
    file. The verify-or-error gate that closes this race is the sibling test
    ``test_race_eliminated_under_coord_managed``; this one must stay RED.
    """
    tmp = tmp_path_factory.mktemp(f"race-{trial}")
    store_path = tmp / "product.zarr"
    _prepare_unsealed_coord(store_path)

    observed = _run_race(store_path)

    expected_ns = np.asarray(_TIMESTAMPS_NS, dtype="datetime64[ns]")
    observed_ns = observed.astype("datetime64[ns]")
    matches = int(np.sum(expected_ns == observed_ns))
    lost = _WRITER_COUNT - matches
    assert lost == 0, (
        f"Trial {trial}: shared-chunk multi-process race lost {lost} of "
        f"{_WRITER_COUNT} timestamps. Observed slot values: "
        f"{observed_ns.tolist()!r}; expected: {expected_ns.tolist()!r}. "
        "This RED test documents the legacy-path race; the coord-managed "
        "gate closes it (see test_race_eliminated_under_coord_managed)."
    )


@pytest.mark.gate
def test_race_eliminated_under_coord_managed(
    tmp_path_factory: pytest.TempPathFactory, trial: int
) -> None:
    """GREEN gate: 12 processes on a COORD_MANAGED coord all safely no-op.

    Setup materializes slots ``[0..11]`` and stamps the
    ``firecube_coord_managed`` marker — exactly the on-disk state
    ``firecube zarr preallocate`` produces for engine-managed coords. Under
    the marker, ``RegionZarrWriter.write_timestamp`` takes the
    verify-or-error branch: workers whose incoming value matches the stored
    value return without writing, and only divergent writes raise
    ``SchemaDriftError``. All twelve workers here pass matching values, so
    no chunk-file rewrites happen, no rewrites collide, and every slot
    survives.

    Assertions cover both required invariants:

    * ``12/12 timestamps present`` — no slot has decayed to ``NaT``.
    * ``12/12 slot values match`` — no slot drifted from its materialized
      value.

    A single lost or drifted slot in any trial fails the gate. The gate runs
    a small fixed number of trials; the ``--trials`` CLI option only drives
    the RED race regression.
    """
    tmp = tmp_path_factory.mktemp(f"gate-{trial}")
    store_path = tmp / "product.zarr"
    _prepare_coord_managed(store_path)

    observed = _run_race(store_path)

    expected_ns = np.asarray(_TIMESTAMPS_NS, dtype="datetime64[ns]")
    observed_ns = observed.astype("datetime64[ns]")
    present = int(np.sum(~np.isnat(observed_ns)))
    matches = int(np.sum(expected_ns == observed_ns))
    assert present == _WRITER_COUNT, (
        f"Trial {trial}: only {present}/{_WRITER_COUNT} timestamps present after "
        f"coord-managed no-op race. Observed: {observed_ns.tolist()!r}. "
        "The verify-or-error branch must never let a materialized slot decay to NaT."
    )
    assert matches == _WRITER_COUNT, (
        f"Trial {trial}: {matches}/{_WRITER_COUNT} slot values match. "
        f"Observed: {observed_ns.tolist()!r}; expected: {expected_ns.tolist()!r}. "
        "Under firecube_coord_managed, write_timestamp must be a strict no-op on "
        "matching values; a drifted slot means the marker branch failed to guard "
        "the coord."
    )
