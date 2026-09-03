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

"""Contracts for coordinate materialization during ``firecube zarr preallocate``.

Both ``IrregularTimeAxis`` (values != AUTO) and ``RegularTimeAxis``
(``slot_count`` set) groups persist their coordinate values densely into a
Zarr array at ``{group}/{axis.coordinate}`` and stamp
``firecube_preallocated=True``. ``IntegerAxis`` stays lazy-derived. Dry-run
must never touch the store.

Fixtures come from ``irregular_axis_test_plugin`` (installed by A6) and
``index_spec_test_plugin``. These tests exercise the real CLI wiring — no
mocks — so a regression in the preallocate loop surfaces here.

Dense-chunk contracts for ``RegularTimeAxis`` with an explicit ``(time,)``
coord ``ZarrArraySpec`` live in
``tests/integration/test_preallocate_regular_axis_dense.py``.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

irregular_plugin = pytest.importorskip(
    "irregular_axis_test_plugin",
    reason="irregular_axis_test_plugin fixture is installed by A6",
)
regular_plugin = pytest.importorskip(
    "index_spec_test_plugin",
    reason="index_spec_test_plugin fixture is installed by the CLI test setup",
)

_EXPECTED_SLOT_COUNT = 5
_BASE = np.datetime64("2026-01-01T00:00:00", "ns")
_STEP = np.timedelta64(600, "s").astype("timedelta64[ns]")
_EXPECTED_TIMESTAMPS = np.asarray(
    [_BASE + i * _STEP for i in range(_EXPECTED_SLOT_COUNT)],
    dtype="datetime64[ns]",
)


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("irregular_axis_test_plugin"))
    importlib.reload(importlib.import_module("index_spec_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(
    plugin: str,
    product: str,
    target_path: Path,
    *,
    dry_run: bool = False,
    extra_options: tuple[str, ...] = (),
) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target_path}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    for option in extra_options:
        args.extend(["--option", option])
    if dry_run:
        args.append("--dry-run")
    return args


def _root(target_path: Path) -> Any:
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


def test_irregular_preallocate_materializes_coord_array(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", target_path),
    )

    assert result.exit_code == 0, result.output
    root = _root(target_path)
    coord = cast(Any, root["data/timestamp"])
    assert coord.shape == (_EXPECTED_SLOT_COUNT,)
    assert coord.dtype == np.dtype("datetime64[ns]")
    values = np.asarray(coord[:])
    assert np.array_equal(values, _EXPECTED_TIMESTAMPS)
    assert coord.metadata.dimension_names == ("timestamp",)


def test_irregular_preallocate_is_idempotent_on_matching_coord_array(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    first = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", target_path),
    )
    second = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", target_path),
    )

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert "existing irregular coord array matches; no-op" in second.output
    values = np.asarray(cast(Any, _root(target_path)["data/timestamp"])[:])
    assert np.array_equal(values, _EXPECTED_TIMESTAMPS)


def test_regular_axis_preallocate_materializes_coords_via_no_spec_fallback(
    tmp_path: Path,
) -> None:
    from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED

    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args("index_spec_single", "index_spec_single", target_path),
    )

    assert result.exit_code == 0, result.output
    root = _root(target_path)
    coord = cast(Any, root["data/timestamp"])
    assert coord.shape == (12,)
    assert coord.dtype == np.dtype("datetime64[ns]")
    assert tuple(coord.chunks) == (12,)
    assert coord.attrs[ATTR_PREALLOCATED] is True
    epoch = np.datetime64("2024-01-01T00:00:00", "ns")
    step = np.timedelta64(300, "s").astype("timedelta64[ns]")
    expected = epoch + np.arange(12, dtype=np.int64) * step
    assert np.array_equal(np.asarray(coord[:]), expected)


def test_dry_run_does_not_write_any_zarr_metadata(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "irregular_axis_safe",
            "irregular_axis_safe",
            target_path,
            dry_run=True,
        ),
    )

    assert result.exit_code == 0, result.output
    assert not target_path.exists(), (
        f"dry-run must not create the target store; found {list(target_path.iterdir())}"
    )


def test_concrete_irregular_axis_materializes_same_values_as_auto(tmp_path: Path) -> None:
    auto_target = tmp_path / "auto.zarr"
    concrete_target = tmp_path / "concrete.zarr"

    auto_result = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", auto_target),
    )
    concrete_result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "irregular_axis_concrete",
            "irregular_axis_concrete",
            concrete_target,
        ),
    )

    assert auto_result.exit_code == 0, auto_result.output
    assert concrete_result.exit_code == 0, concrete_result.output
    auto_values = np.asarray(cast(Any, _root(auto_target)["data/timestamp"])[:])
    concrete_values = np.asarray(cast(Any, _root(concrete_target)["data/timestamp"])[:])
    assert np.array_equal(auto_values, concrete_values)
    assert np.array_equal(auto_values, _EXPECTED_TIMESTAMPS)


def test_coord_array_carries_only_expected_reserved_attrs(tmp_path: Path) -> None:
    from firecube.core.zarr._reserved_attrs import (
        FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
        RESERVED_ARRAY_ATTRS,
    )
    from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED

    target_path = tmp_path / "out.zarr"
    result = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", target_path),
    )

    assert result.exit_code == 0, result.output
    coord = cast(Any, _root(target_path)["data/timestamp"])
    user_attr_keys = set(coord.attrs)
    assert coord.attrs[ATTR_PREALLOCATED] is True
    disallowed = RESERVED_ARRAY_ATTRS - {
        "_ARRAY_DIMENSIONS",
        "_FillValue",
        ATTR_PREALLOCATED,
        FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
    }
    assert user_attr_keys.isdisjoint(disallowed), (
        f"coord attrs {user_attr_keys!r} intersect disallowed reserved names {disallowed!r}"
    )


def test_reverse_order_input_produces_sorted_coord_array(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "irregular_axis_reverse_order",
            "irregular_axis_reverse_order",
            target_path,
        ),
    )

    assert result.exit_code == 0, result.output
    values = np.asarray(cast(Any, _root(target_path)["data/timestamp"])[:])
    assert np.array_equal(values, _EXPECTED_TIMESTAMPS)
