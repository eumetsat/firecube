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
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import AUTO, IndexSpec, IrregularTimeAxis, ItemInfo
from firecube.ingestor.api import DirectZarrIngestor, ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.registry.loader import register_ingestor

pytestmark = pytest.mark.integration

PLUGIN_NAME = "slots_auto_input_data"
PRODUCT_NAME = "slots_auto_input_data"


@register_ingestor(PLUGIN_NAME)
class _SlotsAutoInputDataIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME

    def index_spec(self, ctx):
        _ = ctx
        return IndexSpec(
            name="slots_auto_input_data_v1",
            groups={"data": IrregularTimeAxis(coordinate="timestamp", values=AUTO)},
        )

    def inspect_item(self, item: Any, ctx):
        _ = (item, ctx)
        return ItemInfo(coordinate=np.datetime64("2026-01-01T00:00:00", "ns"))

    def zarr_schema(self, ctx):
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(1,),
                        dtype="float32",
                        chunks=(1,),
                        expected_time_count=1,
                        time_indexed=True,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch, ctx):
        _ = (batch, ctx)
        return []


@pytest.fixture(autouse=True)
def restore_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _slots_args(target_dir: Path, input_data: str | None = None) -> list[str]:
    args = [
        "zarr",
        "slots",
        PLUGIN_NAME,
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]
    if input_data is not None:
        args.extend(["--input-data", input_data])
    return args


def test_slots_auto_requires_input_data_for_default_discovery(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"
    target_dir.mkdir()

    result = CliRunner().invoke(cli, _slots_args(target_dir))

    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert "Error:" in result.output
    assert "--input-data" in result.output
    assert "AUTO discovery" in result.output
    assert "discover_source_files" in result.output


def test_slots_auto_uses_input_data(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"
    target_dir.mkdir()
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "sample.nc").write_text("present", encoding="utf-8")

    result = CliRunner().invoke(cli, _slots_args(target_dir, input_dir.as_uri()))

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["product_name"] == PRODUCT_NAME
    assert payload["ranges"], result.output
