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

"""The legacy ``zarr_chunk`` alias is deleted; ``zarr_chunk_shape`` is the
single chunking option.

``zarr_chunk: bool`` was a second knob for the same concern whose only
behavior was silently nulling an explicit ``zarr_chunk_shape`` — the Option
Aliases anti-pattern (plans/STYLE.md). STYLE.md requires the duplicate be
deleted, not deprecated: passing it must fail strict unknown-key rejection.
"""

from __future__ import annotations

from dataclasses import fields

import pytest

from firecube.ingestor.templates.config import ZarrTemplateConfig

pytestmark = pytest.mark.unit


def test_zarr_chunk_is_not_a_config_field() -> None:
    assert "zarr_chunk" not in {f.name for f in fields(ZarrTemplateConfig)}
    assert "zarr_chunk_shape" in {f.name for f in fields(ZarrTemplateConfig)}


def test_zarr_chunk_rejected_as_unknown_key() -> None:
    with pytest.raises(ValueError, match="zarr_chunk"):
        ZarrTemplateConfig.from_options({"zarr_chunk": False})


def test_explicit_chunk_shape_survives_construction() -> None:
    """The bug the alias caused: zarr_chunk=False silently nulled an explicit
    shape. With one option, an explicit shape is always honored."""
    cfg = ZarrTemplateConfig.from_options({"zarr_chunk_shape": {"time": 24}})
    assert cfg.zarr_chunk_shape == {"time": 24}
