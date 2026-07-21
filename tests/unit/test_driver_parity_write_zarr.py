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

"""Driver-parity tests for ``write_dataset_to_zarr``.

The canonical contract is ``zarr_store: ZarrStoreHandle`` only. The legacy
``store=`` keyword is not accepted by the function signature or by internal
callers.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr
from tests.helpers.storage import assert_no_fsspec_bypass

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_APPEND_SERVICES = (
    _REPO_ROOT / "src" / "firecube" / "ingestor" / "runtime" / "zarr" / "append_services.py"
)


def _make_minimal_dataset() -> xr.Dataset:
    timestamps = pd.date_range("2024-01-01", periods=3, freq="h")
    data = np.arange(3 * 2, dtype=np.float32).reshape(3, 2)
    return xr.Dataset(
        {"FWI": (("timestamp", "x"), data)},
        coords={"timestamp": timestamps, "x": np.arange(2)},
    )


def test_write_dataset_to_zarr_requires_zarr_store() -> None:
    """Calling ``write_dataset_to_zarr`` without ``zarr_store=`` must raise TypeError.

    ``zarr_store`` is a REQUIRED keyword-only argument with no default.
    Python's signature enforcement therefore raises
    ``TypeError("... missing 1 required keyword-only argument: 'zarr_store'")``.
    """
    ds = _make_minimal_dataset()
    with pytest.raises(
        TypeError,
        match=r"missing.*required.*keyword.*argument.*zarr_store",
    ):
        write_dataset_to_zarr(ds, group="G", mode="w")  # type: ignore[call-arg]


def test_write_dataset_to_zarr_no_store_param_accepted() -> None:
    """Calling ``write_dataset_to_zarr`` with ``store=`` must raise TypeError.

    The legacy ``store`` keyword is not part of the signature. Python's
    signature enforcement therefore raises ``TypeError("... got an unexpected
    keyword argument 'store'")``.
    """
    ds = _make_minimal_dataset()
    raw_store = Mock(name="raw_store")
    with pytest.raises(
        TypeError,
        match=r"unexpected keyword argument.*['\"]store['\"]",
    ):
        write_dataset_to_zarr(  # type: ignore[call-arg]
            ds,
            store=raw_store,  # pyright: ignore[reportCallIssue]
            group="G",
            mode="w",
        )


def test_write_dataset_to_zarr_obstore_no_fsspec_bypass(tmp_path: Path) -> None:
    """Under the obstore driver, ``write_dataset_to_zarr`` must not bypass to fsspec.

    The "one driver everywhere" invariant from AGENTS.md requires that when
    ``StorageDriverConfig(driver='obstore')`` is selected, the entire write
    path executes through obstore-backed primitives. Any silent fallback to
    ``firecube.core.filesystem.ops._open_fsspec_url`` would constitute a
    cross-driver downgrade and is forbidden.

    This test seeds an obstore-backed ``ZarrStoreHandle`` via
    ``create_zarr_store(driver='obstore')`` and asserts the legacy fsspec
    URL opener is never invoked during the write.
    """
    storage_config = StorageConfig(storage_type="local", storage_driver="obstore")
    target = tmp_path / "obstore_out.zarr"
    handle = create_zarr_store(uri=str(target), storage_config=storage_config, mode="w")

    ds = _make_minimal_dataset()

    with assert_no_fsspec_bypass():
        write_dataset_to_zarr(ds, zarr_store=handle, group="G", mode="w")

    assert (target / "G" / "FWI" / "zarr.json").exists()


def test_write_dataset_to_zarr_internal_callers_no_store() -> None:
    """No internal caller in append_services.py may pass ``store=`` to write_fn.

    AST-scans ``src/firecube/ingestor/runtime/zarr/append_services.py`` for any
    call to ``write_fn`` or ``write_dataset_to_zarr`` with a ``store=``
    keyword argument. The post-migration contract is that internal callers
    forward only ``zarr_store=ZarrStoreHandle``.

    """
    assert _APPEND_SERVICES.exists(), f"missing target file: {_APPEND_SERVICES}"
    source = _APPEND_SERVICES.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(_APPEND_SERVICES))

    target_callees = {"write_fn", "write_dataset_to_zarr", "_write_fn"}
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee_name: str | None = None
        func: Any = node.func
        if isinstance(func, ast.Name):
            callee_name = func.id
        elif isinstance(func, ast.Attribute):
            callee_name = func.attr
        if callee_name not in target_callees:
            continue
        offenders.extend(
            f"{_APPEND_SERVICES.name}:{node.lineno}: "
            f"{callee_name}(store=...) — migrate to zarr_store=ZarrStoreHandle"
            for kw in node.keywords
            if kw.arg == "store"
        )

    assert not offenders, (
        "Internal callers must not pass `store=` to write_dataset_to_zarr. "
        "Use `zarr_store=ZarrStoreHandle` instead. Offenders:\n" + "\n".join(offenders)
    )
