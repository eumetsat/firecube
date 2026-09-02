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

"""Unit tests for ZarrArraySpec fill_value validation."""

from __future__ import annotations

import numpy as np
import pytest

from firecube.ingestor.api import ZarrArraySpec

pytestmark = pytest.mark.unit


def test_nat_string_fill_value_rejected_for_datetime_dtype() -> None:
    with pytest.raises(ValueError, match="fill_value='NaT' is a string"):
        ZarrArraySpec(
            name="coord",
            shape=(1000,),
            dtype="datetime64[ns]",
            chunks=(256,),
            fill_value="NaT",
        )


def test_nat_numpy_fill_value_accepted_for_datetime_dtype() -> None:
    spec = ZarrArraySpec(
        name="coord",
        shape=(1000,),
        dtype="datetime64[ns]",
        chunks=(256,),
        fill_value=np.datetime64("NaT", "ns"),
    )

    assert np.isnat(spec.fill_value)
    assert spec.fill_value.dtype == np.dtype("datetime64[ns]")
