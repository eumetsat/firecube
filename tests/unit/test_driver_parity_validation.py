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

"""Driver-parity tests for zarr validation, scrub, and resume guard (T4.2).

Each test seeds a real Zarr V3 store under ``tmp_path``, exercises the
production code path through the typed-fs seam, and asserts that the
legacy ``_open_fsspec_url`` adapter is never invoked. This generalizes
the T2.4 vertical slice across the validation/scrub/resume_guard subsystem.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr

from firecube.core.zarr.scrub import run_scrub
from firecube.core.zarr.validation import validate_group_with_fs
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from tests.helpers.storage import assert_no_fsspec_bypass, make_test_session

pytestmark = pytest.mark.unit


def _seed_zarr_group(target: Path, group: str = "G") -> None:
    ds = xr.Dataset(
        {"val": (["timestamp", "x"], np.arange(15, dtype=np.float32).reshape(5, 3))},
        coords={"timestamp": np.arange(5), "x": np.arange(3)},
    )
    ds.to_zarr(str(target), group=group, mode="w", zarr_format=3)


def test_validate_group_with_fs_no_bypass(tmp_path: Path) -> None:
    target = tmp_path / "product.zarr"
    _seed_zarr_group(target, "G")

    session = make_test_session(tmp_path, product="product.zarr")

    with assert_no_fsspec_bypass():
        report = validate_group_with_fs(session.fs(), session.product.product_uri, "G/val")

    assert report.group == "G/val"
    assert report.shape == [5, 3]


def test_resume_guard_no_bypass(tmp_path: Path) -> None:
    target = tmp_path / "product.zarr"
    _seed_zarr_group(target, "G")

    session = make_test_session(tmp_path, product="product.zarr")

    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [MagicMock(meta={"plugin": "test"})]

    storage_ctx = MagicMock()
    storage_ctx.output = session
    ctx = MagicMock()
    ctx.storage = storage_ctx
    ctx.force_reingest = False
    ctx.option.side_effect = lambda name, default=None: {
        "validate_zarr": True,
        "resume_existing": True,
    }.get(name, default)

    guard = ResumeGuard(
        plugin_name="test",
        chunk_manager=chunk_manager,
        log=MagicMock(),
        slice_meta_keys=(),
    )

    with assert_no_fsspec_bypass():
        guard.enforce(
            ctx=ctx,
            product=session.product.product_name,
            validation_group="G/val",
        )


def test_scrub_no_bypass(tmp_path: Path) -> None:
    target = tmp_path / "product.zarr"
    _seed_zarr_group(target, "G")

    session = make_test_session(tmp_path, product="product.zarr")

    cm_seed = session.control_plane()
    try:
        cm_seed.record_run_started(
            product=session.product.product_name,
            run_id="parity-scrub-run",
            output_path=session.product.product_uri.to_str(),
            output_format="zarr",
            size=0,
            meta={},
        )
        cm_seed.record_run_terminal(
            product=session.product.product_name,
            run_id="parity-scrub-run",
            output_path=session.product.product_uri.to_str(),
            output_format="zarr",
            size=0,
            meta={},
            status="complete",
        )
    finally:
        cm_seed.close()

    with assert_no_fsspec_bypass():
        result = run_scrub(session, "G/val")

    assert result.product == session.product.product_name
    assert result.group == "G/val"
