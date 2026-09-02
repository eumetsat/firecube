# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import multiprocessing as mp
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr

from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_GROUP = "data"
_COORD = "time"
_SLOTS = 256
_WRITERS = 12
_BASE = np.datetime64("2024-01-01T00:00:07", "ns")
_VALUES = [_BASE + np.timedelta64(i * 600, "s") for i in range(_WRITERS)]
_BARRIER_TIMEOUT_S = 30.0

_worker_barrier: Any = None


def _init_worker(barrier: Any) -> None:
    global _worker_barrier
    _worker_barrier = barrier


def _writer_worker(payload: tuple[str, int, str]) -> None:
    store_path, slot_index, value = payload
    writer = RegionZarrWriter(store_uri=store_path, time_coord_name=_COORD)
    if _worker_barrier is not None:
        _worker_barrier.wait(timeout=_BARRIER_TIMEOUT_S)
    writer.write_timestamp(_GROUP, slot_index, np.datetime64(value, "ns"))


def _prepare_fresh_managed_shell(target: Path) -> None:
    root = zarr.open_group(store=str(target), mode="w", zarr_format=3)
    group = root.create_group(_GROUP)
    arr = group.create_array(
        _COORD,
        shape=(_SLOTS,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(_SLOTS,),
        dimension_names=[_COORD],
    )
    arr.attrs[ATTR_COORD_MANAGED] = True
    arr[:_WRITERS] = np.asarray(_VALUES, dtype="datetime64[ns]")


def _read_values(target: Path) -> np.ndarray:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    arr = cast(Any, root[f"{_GROUP}/{_COORD}"])
    return np.asarray(arr[:_WRITERS]).astype("datetime64[ns]")


@pytest.mark.gate
def test_p12_parallel_writers_on_fresh_managed_shell_zero_loss(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    _prepare_fresh_managed_shell(target)

    ctx = mp.get_context("spawn")
    manager = ctx.Manager()
    try:
        barrier = manager.Barrier(_WRITERS)
        payloads = [(str(target), i, str(_VALUES[i])) for i in range(_WRITERS)]
        with ctx.Pool(processes=_WRITERS, initializer=_init_worker, initargs=(barrier,)) as pool:
            pool.map(_writer_worker, payloads, chunksize=1)
    finally:
        manager.shutdown()

    observed = _read_values(target)
    expected = np.asarray(_VALUES, dtype="datetime64[ns]")
    assert int(np.sum(~np.isnat(observed))) == _WRITERS
    assert np.array_equal(observed, expected)
