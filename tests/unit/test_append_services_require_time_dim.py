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

from typing import Any, cast

import pytest

from firecube.ingestor.runtime.zarr.append_services import (
    AppendCoverageBuilder,
    AppendTimestampState,
)


@pytest.mark.unit
def test_append_timestamp_state_requires_time_dim_name() -> None:
    constructor = cast(Any, AppendTimestampState)
    with pytest.raises(TypeError):
        constructor("firecube_timestamp_state")


@pytest.mark.unit
def test_append_coverage_builder_requires_time_dim_name() -> None:
    constructor = cast(Any, AppendCoverageBuilder)
    with pytest.raises(TypeError):
        constructor()
