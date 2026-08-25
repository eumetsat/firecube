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

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

PLUGIN = "direct_zarr_capable_test_plugin"
PRODUCT = "direct_zarr_capable_test_product"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module(PLUGIN))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target_path: Path, *options: str) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    for option in options:
        args.extend(["--option", option])
    return args


def _ingest_args(target_path: Path) -> list[str]:
    return [
        "ingest",
        PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "resume_existing=true",
        "--option",
        "no_progress=true",
    ]


def _root(target_path: Path) -> Any:
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


def test_declared_attrs_written(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(target_path))

    assert result.exit_code == 0, result.output
    root = _root(target_path)
    data = cast(Any, root["data/data"])
    lat = cast(Any, root["data/lat"])
    assert data.attrs["long_name"] == "test data"
    assert data.attrs["units"] == "K"
    assert lat.attrs["units"] == "degrees_north"
    assert lat.attrs["standard_name"] == "latitude"


def test_round_trip_preallocate_to_ingest(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    preallocate_result = CliRunner().invoke(cli, _preallocate_args(target_path))
    ingest_result = CliRunner().invoke(cli, _ingest_args(target_path))

    assert preallocate_result.exit_code == 0, preallocate_result.output
    assert ingest_result.exit_code == 0, ingest_result.output
    assert "SchemaDriftError" not in ingest_result.output


def test_reserved_attr_fails_loudly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import direct_zarr_capable_test_plugin as plugin_module

    def bad_zarr_schema(self: Any, ctx: Any) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype="float32",
                        attrs={"_ARRAY_DIMENSIONS": ["x"]},
                    )
                ],
            )
        ]

    monkeypatch.setattr(plugin_module.DirectZarrCapableTestIngestor, "zarr_schema", bad_zarr_schema)

    with pytest.raises(ValueError, match=r"Reserved attr '_ARRAY_DIMENSIONS'"):
        CliRunner().invoke(
            cli,
            _preallocate_args(tmp_path / "out.zarr"),
            catch_exceptions=False,
        )


def test_dimension_names_written(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _preallocate_args(target_path))

    assert result.exit_code == 0, result.output
    root = _root(target_path)
    data = cast(Any, root["data/data"])
    lat = cast(Any, root["data/lat"])
    assert data.metadata.dimension_names == ("timestamp", "x")
    assert lat.metadata.dimension_names == ("lat",)
