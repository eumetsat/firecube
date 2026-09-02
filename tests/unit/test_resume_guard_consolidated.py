from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import zarr

from firecube.core.controlplane import ChunkManager
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from tests.helpers.storage import make_test_binding


def _make_ctx(**options: object) -> MagicMock:
    ctx = MagicMock()
    ctx.force_reingest = False
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_guard(manager: ChunkManager) -> ResumeGuard:
    return ResumeGuard(
        plugin_name="test_product",
        chunk_manager=manager,
        log=logging.getLogger("test_resume_guard_consolidated"),
        slice_meta_keys=(),
    )


@pytest.mark.unit
def test_consolidated_cube_blocks_ingest(tmp_path: Path) -> None:
    product = "sealed.zarr"
    timestamp_iso = "2026-08-27T12:00:00+00:00"
    manager = ChunkManager(binding=make_test_binding(tmp_path, product=product))

    try:
        manager.record_time_coord_consolidation(("F024",), timestamp_iso)
        guard = _make_guard(manager)

        with pytest.raises(ResumeConflictError) as exc_info:
            guard.enforce(ctx=_make_ctx(), product=product, slot_range=(0, 1), slot_group="F024")
    finally:
        manager.close()

    assert str(exc_info.value) == (
        f"Cube file://{tmp_path / product}/F024 is sealed (consolidated at {timestamp_iso}). "
        "Further ingest is blocked."
    )


@pytest.mark.unit
def test_preallocated_allows_ingest(tmp_path: Path) -> None:
    product = "preallocated.zarr"
    root = zarr.open_group(store=str(tmp_path / product), mode="w", zarr_format=3)
    group = root.require_group("F024")
    coord = group.create_array("time", shape=(1,), chunks=(1,), dtype="datetime64[s]")
    coord.attrs[ATTR_PREALLOCATED] = True
    manager = ChunkManager(binding=make_test_binding(tmp_path, product=product))

    try:
        _make_guard(manager).enforce(
            ctx=_make_ctx(resume_existing=False, validate_zarr=False),
            product=product,
            slot_range=(0, 1),
            slot_group="F024",
        )
    finally:
        manager.close()
