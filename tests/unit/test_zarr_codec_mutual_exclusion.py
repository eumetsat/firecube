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

"""zarr_compression=True and zarr_codecs=[...] are mutually exclusive."""

from __future__ import annotations

import pytest

from firecube.ingestor.templates.config import ZarrTemplateConfig

pytestmark = pytest.mark.unit


def test_both_set_raises() -> None:
    with pytest.raises(ValueError, match="zarr_compression=True conflicts with zarr_codecs"):
        ZarrTemplateConfig(
            zarr_compression=True, zarr_codecs=[{"name": "blosc", "configuration": {}}]
        )


def test_error_names_both_fields() -> None:
    with pytest.raises(ValueError) as exc_info:
        ZarrTemplateConfig(
            zarr_compression=True, zarr_codecs=[{"name": "blosc", "configuration": {}}]
        )

    msg = str(exc_info.value)
    assert "zarr_compression" in msg
    assert "zarr_codecs" in msg


def test_false_and_codecs_accepted() -> None:
    entry = {"name": "blosc", "configuration": {}}
    cfg = ZarrTemplateConfig(zarr_compression=False, zarr_codecs=[entry])
    assert cfg.zarr_codecs == [entry]
    assert cfg.zarr_compression is False


def test_true_and_none_accepted() -> None:
    cfg = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=None)
    assert cfg.zarr_compression is True


def test_false_and_none_accepted() -> None:
    cfg = ZarrTemplateConfig(zarr_compression=False, zarr_codecs=None)
    assert cfg.zarr_compression is False
