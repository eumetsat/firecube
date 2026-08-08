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

"""The legacy ``zarr_compression`` string form is removed."""

from __future__ import annotations

from typing import Any, cast

import pytest

from firecube.ingestor.templates.config import ZarrTemplateConfig

pytestmark = pytest.mark.contract


def test_string_value_rejected_as_non_bool() -> None:
    with pytest.raises(ValueError, match="zarr_compression must be bool"):
        ZarrTemplateConfig(zarr_compression=cast(Any, "zstd"))


@pytest.mark.parametrize("value", [1, 0, 1.0, None])
def test_non_bool_values_raise(value: Any) -> None:
    with pytest.raises(ValueError, match="zarr_compression"):
        ZarrTemplateConfig(zarr_compression=value)


def test_from_options_string_rejected_as_non_bool() -> None:
    # from_options no longer has a zarr_compression string special-case.
    # coerce_cli_value raises ValueError for non-bool strings.
    with pytest.raises(ValueError, match=r"(must be bool|Invalid boolean)"):
        ZarrTemplateConfig.from_options({"zarr_compression": "zstd"})
