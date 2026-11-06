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

from typing import Any

import pytest

from firecube.cli._errors import render
from firecube.core.errors import ConfigurationError
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.templates.config import ZarrTemplateConfig
from firecube.ingestor.types.context import OutputPaths, PipelineResult
from tests.helpers.rows import table_row

REMOVED_MESSAGE = (
    "zarr_multi_res during ingest has been removed; "
    "run 'ZarrMultiresBuilder.build <target>' after ingestion instead."
)


def test_zarr_multi_res_list_raises_config_error() -> None:
    """zarr_multi_res=[1.0, 0.5] must raise an actionable migration error."""
    with pytest.raises(
        Exception,
        match="zarr_multi_res during ingest has been removed",
    ) as exc_info:
        ZarrTemplateConfig.from_options({"zarr_multi_res": [1.0, 0.5]})
    assert str(exc_info.value) == REMOVED_MESSAGE
    assert "run 'firecube zarr multires <target>' after" in render(exc_info.value)


def test_zarr_multi_res_rejected_by_presence_not_value(subtests, tmp_path) -> None:
    for row_id, value in [
        ("test_zarr_multi_res_empty_list_raises", []),
        ("test_zarr_multi_res_bool_raises", True),
    ]:
        with (
            table_row(subtests, row_id, tmp_path),
            pytest.raises(Exception, match="zarr_multi_res during ingest has been removed"),
        ):
            ZarrTemplateConfig.from_options({"zarr_multi_res": value})


def test_zarr_multi_res_absent_is_fine() -> None:
    """No zarr_multi_res key — normal, no error."""
    cfg = ZarrTemplateConfig.from_options({})
    assert cfg is not None


def test_append_multires_handler_not_importable() -> None:
    """Handler class must be gone."""
    with pytest.raises(ImportError):
        exec(
            "from firecube.ingestor.runtime.zarr.append_services import Append" + "MultiresHandler",
        )


def test_zarr_region_write_concurrency_default_is_one() -> None:
    cfg = ZarrTemplateConfig()
    assert cfg.zarr_region_write_concurrency == 1


def test_zarr_region_write_concurrency_rejects_below_one(subtests, tmp_path) -> None:
    for row_id, value in [
        ("test_zarr_region_write_concurrency_rejects_zero", 0),
        ("test_zarr_region_write_concurrency_rejects_negative", -1),
    ]:
        with (
            table_row(subtests, row_id, tmp_path),
            pytest.raises(ConfigurationError, match="zarr_region_write_concurrency must be >= 1"),
        ):
            ZarrTemplateConfig(zarr_region_write_concurrency=value)


def test_zarr_region_write_concurrency_accepts_positive() -> None:
    cfg = ZarrTemplateConfig(zarr_region_write_concurrency=4)
    assert cfg.zarr_region_write_concurrency == 4


@pytest.mark.contract
def test_zarr_unit_write_buffer_defaults_off_and_parses_true() -> None:
    assert ZarrTemplateConfig().zarr_unit_write_buffer is False
    assert (
        ZarrTemplateConfig.from_options({"zarr_unit_write_buffer": "true"}).zarr_unit_write_buffer
        is True
    )


@pytest.mark.contract
def test_zarr_unit_write_buffer_refused_with_region_concurrency_above_one() -> None:
    with pytest.raises(ConfigurationError, match="zarr_unit_write_buffer") as exc_info:
        ZarrTemplateConfig(zarr_unit_write_buffer=True, zarr_region_write_concurrency=2)
    assert "zarr_region_write_concurrency" in str(exc_info.value)


@pytest.mark.contract
def test_show_options_lists_zarr_region_write_concurrency_and_unit_write_buffer() -> None:
    class _ZarrConcurrencyProbe(BaseIngestor):
        PRODUCT_NAME = "zarr_concurrency_probe"
        name = "zarr_concurrency_probe"
        template_config_class = ZarrTemplateConfig

        def _process_batch(self, batch: Any, ctx: Any) -> PipelineResult:
            return PipelineResult(batch=batch, outputs=OutputPaths(primary=""), success=True)

        def _aggregate_metrics(self, ctx: Any, state: Any) -> dict[str, Any]:
            return {}

    desc = _ZarrConcurrencyProbe.describe_options()

    assert "Template Options" in desc
    assert "zarr_region_write_concurrency" in desc["Template Options"]
    assert "zarr_unit_write_buffer" in desc["Template Options"]
