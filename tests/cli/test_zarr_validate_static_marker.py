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
from pathlib import Path
from typing import Any

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import FIRECUBE_STATIC_WRITTEN_ATTR
from tests.helpers.rows import table_row

pytestmark = pytest.mark.unit


def _invoke_validate(store: Path, group: str):
    return CliRunner().invoke(cli, ["zarr", "validate", "-t", store.as_uri(), "-g", group])


def _store_with_array(tmp_path: Path, name: str, *, dimension_names: tuple[str, ...]) -> Path:
    store = tmp_path / f"{name}.zarr"
    root = zarr.open_group(str(store), mode="w")
    root.create_array(name, shape=(4,), dtype="i4", chunks=(4,), dimension_names=dimension_names)
    return store


def _invoke_validate_with_time_dim(store: Path, group: str, time_dim: str = "timestamp"):
    return CliRunner().invoke(
        cli,
        ["zarr", "validate", "-t", store.as_uri(), "-g", group, "--time-dim", time_dim],
    )


_ROWS = [
    (
        "test_validate_reports_missing_static_marker",
        "lat",
        "lat",
        False,
        False,
        [{"array": "lat", "reason": "missing_or_false_static_marker"}],
    ),
    ("test_validate_passes_when_all_static_markers_present", "lat", "lat", True, True, []),
    ("test_validate_ignores_time_indexed_arrays", "temperature", "timestamp", False, False, []),
]


def test_validate_static_marker(subtests: Any, tmp_path: Path) -> None:
    for row_id, array, dim, marked, valid, failures in _ROWS:
        with table_row(subtests, row_id, tmp_path) as (workdir, _mp):
            store = _store_with_array(workdir, array, dimension_names=(dim,))
            if marked:
                zarr.open_group(str(store), mode="a")[array].attrs[FIRECUBE_STATIC_WRITTEN_ATTR] = (
                    True
                )

            result = _invoke_validate_with_time_dim(store, array)

            assert result.exit_code == (0 if valid else 1), result.output
            payload = json.loads(result.output)
            assert payload["is_valid"] is valid
            assert payload["static_marker_failures"] == failures
