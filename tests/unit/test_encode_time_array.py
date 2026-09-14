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

"""Tests for firecube.core.zarr.time_decode encoding."""

# pyright: reportMissingImports=false

import numpy as np
import pytest

from firecube.core.zarr.time_decode import decode_time_array, encode_time_array

pytestmark = pytest.mark.unit


def test_encode_time_array_round_trips_through_decode() -> None:
    values = np.array(
        ["2023-12-01T00:00:00", "2023-12-01T00:00:01"],
        dtype="datetime64[ns]",
    )
    attrs = {"units": "seconds since 1970-01-01", "calendar": "standard"}

    encoded, units, calendar = encode_time_array(values, attrs)
    decoded = decode_time_array(encoded, {"units": units, "calendar": calendar})

    assert np.array_equal(decoded.astype(values.dtype), values)


def test_encode_time_array_malformed_units_raise_valueerror() -> None:
    values = np.array(["2023-12-01"], dtype="datetime64[ns]")

    with pytest.raises(ValueError, match=r"units|since|invalid"):
        encode_time_array(values, {"units": "kelvin", "calendar": "standard"})


def test_encode_time_array_is_exported_from_core_api() -> None:
    from firecube.core.api import encode_time_array as exported

    assert exported is encode_time_array
