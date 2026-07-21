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

from tests.fixtures.cf_dataset_fixtures import (
    make_ambiguous_dataset,
    make_broken_dataset,
    make_cf_compliant_dataset,
    make_legacy_timestamp_dataset,
)


def test_cf_compliant_has_correct_attrs() -> None:
    ds = make_cf_compliant_dataset()

    assert ds.attrs["Conventions"] == "CF-1.8"
    assert " since " in ds["time"].attrs["units"]
    assert ds["time"].attrs["calendar"] == "standard"
    assert ds["temperature"].attrs["units"] == "K"
    assert ds["temperature"].attrs["standard_name"] == "air_temperature"


def test_broken_conventions() -> None:
    ds = make_broken_dataset("conventions")

    assert "Conventions" not in ds.attrs


def test_broken_time_units() -> None:
    ds = make_broken_dataset("time_units")

    assert "units" not in ds["time"].attrs


def test_broken_var_units() -> None:
    ds = make_broken_dataset("var_units")

    assert "units" not in ds["temperature"].attrs


def test_broken_var_names_only() -> None:
    ds = make_broken_dataset("var_names_only")

    assert ds["temperature"].attrs == {"units": "K", "long_name": "Air Temperature"}


def test_broken_reference() -> None:
    ds = make_broken_dataset("broken_reference")

    assert ds["temperature"].attrs["coordinates"] == "nonexistent_var"


def test_legacy_timestamp_dim() -> None:
    ds = make_legacy_timestamp_dataset()

    assert "timestamp" in ds.dims
    assert "time" not in ds.dims


def test_ambiguous_has_both_dims() -> None:
    ds = make_ambiguous_dataset()

    assert "time" in ds.dims
    assert "timestamp" in ds.dims


def test_broken_dataset_rejects_unknown_missing() -> None:
    with pytest.raises(ValueError, match="Unknown missing="):
        make_broken_dataset("unknown")
