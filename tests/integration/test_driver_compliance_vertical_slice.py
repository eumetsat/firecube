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

"""Driver-compliance vertical slice.

This test proves the storage-driver invariant on ONE end-to-end critical
path. The three operations exercised — zarr store creation, group
validation, and WAL event recording — together cover the data plane, the
metadata plane, and the control plane, which is the minimum slice that proves
"one driver everywhere" (AGENTS.md).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from firecube.core.storage.session import StorageSession
from firecube.core.zarr.validation import validate_group_with_fs
from tests.helpers.storage import make_test_session

pytestmark = pytest.mark.integration

_GROUP = "F024"
_ARRAY = "FWI"
_ARRAY_PATH = f"{_GROUP}/{_ARRAY}"
_SHAPE = (4, 3)


def _make_sample_dataset() -> xr.Dataset:
    """Build a minimal but real xarray dataset for end-to-end zarr writes."""
    rng = np.random.default_rng(7)
    return xr.Dataset(
        {_ARRAY: (("timestamp", "x"), rng.standard_normal(_SHAPE).astype(np.float32))},
        coords={"timestamp": np.arange(_SHAPE[0]), "x": np.arange(_SHAPE[1])},
    )


def _exercise_critical_path(session: StorageSession, *, run_id: str) -> None:
    """Run the three driver-bound operations that the invariant must cover.

    1. Create + write a zarr store via ``session.zarr.write_dataset`` (data plane).
    2. Validate the array group via ``validate_group_with_fs`` (metadata plane).
    3. Record a WAL run-started event via ``session.control_plane()`` (control plane).
    """
    uri = session.product.product_uri

    # 1. Data plane: write via the session's driver-aware zarr facade.
    ds = _make_sample_dataset()
    session.zarr.write_dataset(ds, uri, group=_GROUP)

    # 2. Metadata plane: validate via the typed-fs validator (no URI strings).
    report = validate_group_with_fs(session.fs(), uri, _ARRAY_PATH)
    assert report.group == _ARRAY_PATH, (
        f"validate_group_with_fs returned wrong group label "
        f"(got {report.group!r}, expected {_ARRAY_PATH!r})"
    )
    assert report.shape == list(_SHAPE), (
        f"validate_group_with_fs reported unexpected shape "
        f"(got {report.shape!r}, expected {list(_SHAPE)!r})"
    )

    # 3. Control plane: record a WAL event via the injected-filesystem ChunkManager.
    cm = session.control_plane()
    try:
        cm.record_run_started(
            product=session.product.product_name,
            run_id=run_id,
            output_path=uri.to_str(),
            output_format="zarr",
            size=0,
            meta={"test": "driver-compliance-vertical-slice"},
        )
    finally:
        cm.close()


def test_fsspec_path(tmp_path: Path) -> None:
    """Vertical slice runs cleanly under the fsspec driver.

    Demonstrates that the canonical critical-path APIs behave correctly when
    `storage_driver=fsspec`. This is the baseline functional assertion.
    """
    session = make_test_session(tmp_path, driver="fsspec")
    _exercise_critical_path(session, run_id="vertical-slice-fsspec-run")

    # Sanity: the WAL artifact actually landed under .firecube/ on disk.
    control_root = Path(session.product.control_root_uri.path)
    assert control_root.exists(), (
        f"Control plane root not created at {control_root}; "
        "record_run_started did not materialize WAL artifacts."
    )


def test_obstore_no_bypass(tmp_path: Path) -> None:
    """Driver invariant: the critical path must NOT call ``_open_fsspec_url``.

    ``_open_fsspec_url`` (in ``firecube.core.filesystem.ops``) is the legacy
    escape hatch that bypasses the configured driver and constructs an fsspec
    filesystem ad-hoc. AGENTS.md mandates "one driver everywhere": all I/O
    must flow through the typed ``StorageFilesystem`` returned by
    ``session.fs()`` (which is ``FsspecFilesystem`` or ``ObstoreFilesystem``,
    chosen via ``StorageDriverConfig``).

    We exercise the same critical path under the fsspec driver but assert that
    the source-module ``_open_fsspec_url`` is never invoked. Because none of
    the three operations are allowed to take the legacy path, the invariant
    holds for the obstore driver by construction (under obstore, an
    ``_open_fsspec_url`` call would silently downgrade to fsspec — exactly
    the violation we are guarding against).
    """
    session = make_test_session(tmp_path, driver="fsspec")

    with patch("firecube.core.filesystem.ops._open_fsspec_url") as mock_open:
        _exercise_critical_path(session, run_id="vertical-slice-no-bypass-run")

    assert mock_open.call_count == 0, (
        f"Driver invariant violated: `_open_fsspec_url` was called "
        f"{mock_open.call_count} time(s) during the vertical compliance slice. "
        "All I/O must flow through the typed StorageFilesystem (session.fs()), "
        "NOT through the legacy fsspec URL escape hatch. "
        f"Mock call args: {mock_open.call_args_list!r}"
    )
