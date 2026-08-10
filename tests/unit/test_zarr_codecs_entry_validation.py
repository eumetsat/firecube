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

"""Structural entry validation for ZarrTemplateConfig.zarr_codecs."""

from __future__ import annotations

from typing import cast

import pytest

from firecube.ingestor.templates.config import ZarrTemplateConfig

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("input_value", "expected_error_substring"),
    [
        pytest.param(None, None, id="none"),
        pytest.param(
            [{"name": "blosc", "configuration": {"cname": "zstd"}}],
            None,
            id="single-valid-with-configuration",
        ),
        pytest.param(
            [{"name": "blosc"}],
            None,
            id="single-valid-no-configuration",
        ),
        pytest.param(
            [],
            "zarr_codecs must contain at least one",
            id="empty-list",
        ),
        pytest.param(
            [{}],
            "zarr_codecs[0].name is required",
            id="missing-name",
        ),
        pytest.param(
            [{"name": 123}],
            "zarr_codecs[0].name must be a string",
            id="name-not-string",
        ),
        pytest.param(
            [{"name": "blosc", "configuration": None}],
            "zarr_codecs[0].configuration must be an object",
            id="configuration-none",
        ),
        pytest.param(
            [{"name": "blosc", "configuration": {}, "extra_key": "x"}],
            "zarr_codecs[0] has unexpected keys",
            id="unexpected-key",
        ),
        pytest.param(
            "not-a-list",
            "zarr_codecs must be a list",
            id="not-a-list",
        ),
    ],
)
def test_zarr_codecs_entry_validation(
    input_value: object,
    expected_error_substring: str | None,
) -> None:
    if expected_error_substring is None:
        codecs = cast(list[dict] | None, input_value)
        cfg = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=codecs)
        assert cfg.zarr_codecs == codecs
        return

    with pytest.raises(ValueError) as excinfo:
        ZarrTemplateConfig(zarr_compression=True, zarr_codecs=cast(list[dict] | None, input_value))
    assert expected_error_substring in str(excinfo.value)


def test_multi_element_pipeline_accepted() -> None:
    codecs = [
        {"name": "bytes", "configuration": {}},
        {"name": "zstd", "configuration": {}},
    ]

    cfg = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=codecs)

    assert cfg.zarr_codecs == codecs


def test_out_of_order_pipeline_rejected() -> None:
    codecs = [
        {"name": "zstd", "configuration": {}},
        {"name": "bytes", "configuration": {}},
    ]

    with pytest.raises(ValueError, match=r"[Cc]odec|order|pipeline"):
        ZarrTemplateConfig(zarr_compression=True, zarr_codecs=codecs)


def test_single_compressor_still_accepted() -> None:
    codecs = [{"name": "blosc", "configuration": {"cname": "zstd"}}]

    cfg = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=codecs)

    assert cfg.zarr_codecs == codecs
