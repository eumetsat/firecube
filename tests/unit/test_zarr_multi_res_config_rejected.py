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

import pytest

from firecube.ingestor.templates.config import ZarrTemplateConfig

REMOVED_MESSAGE = (
    "zarr_multi_res during ingest has been removed; "
    "run 'firecube zarr multires <target>' after ingestion instead."
)


def test_zarr_multi_res_list_raises_config_error() -> None:
    """zarr_multi_res=[1.0, 0.5] must raise an actionable migration error."""
    with pytest.raises(
        Exception,
        match="zarr_multi_res during ingest has been removed",
    ) as exc_info:
        ZarrTemplateConfig.from_options({"zarr_multi_res": [1.0, 0.5]})
    assert str(exc_info.value) == REMOVED_MESSAGE


def test_zarr_multi_res_empty_list_raises() -> None:
    """Empty list also rejected — presence-based, not value-based."""
    with pytest.raises(Exception, match="zarr_multi_res during ingest has been removed"):
        ZarrTemplateConfig.from_options({"zarr_multi_res": []})


def test_zarr_multi_res_bool_raises() -> None:
    """Legacy bool form also rejected."""
    with pytest.raises(Exception, match="zarr_multi_res during ingest has been removed"):
        ZarrTemplateConfig.from_options({"zarr_multi_res": True})


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
