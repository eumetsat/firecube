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

# pyright: reportMissingImports=false
from __future__ import annotations

import pytest

from firecube.core.zarr._reserved_attrs import RESERVED_ARRAY_ATTRS
from firecube.core.zarr._reserved_root_attrs import (
    RESERVED_ROOT_ATTRS,
    assert_root_attrs_safe,
)


def test_reserved_root_attrs_exact_frozenset() -> None:
    assert isinstance(RESERVED_ROOT_ATTRS, frozenset)
    assert {
        "firecube_slot_index_model",
        "firecube_slot_index_model_identity_hash",
    } == RESERVED_ROOT_ATTRS


def test_empty_attrs_are_allowed() -> None:
    assert assert_root_attrs_safe({}) is None


@pytest.mark.parametrize(
    ("attrs", "expected_name"),
    [
        ({"firecube_slot_index_model": "anything"}, "firecube_slot_index_model"),
        (
            {"firecube_slot_index_model_identity_hash": "a" * 64},
            "firecube_slot_index_model_identity_hash",
        ),
    ],
)
def test_reserved_names_raise_name_only(attrs: dict[str, str], expected_name: str) -> None:
    with pytest.raises(ValueError, match=expected_name):
        assert_root_attrs_safe(attrs)


def test_non_reserved_names_are_allowed() -> None:
    assert assert_root_attrs_safe({"user_attr": "value"}) is None


def test_mixed_attrs_raise_and_identify_offending_key() -> None:
    with pytest.raises(ValueError, match="firecube_slot_index_model"):
        assert_root_attrs_safe({"user_attr": "v", "firecube_slot_index_model": "x"})


def test_root_names_are_not_array_reserved_attrs() -> None:
    assert "firecube_slot_index_model" not in RESERVED_ARRAY_ATTRS
    assert "firecube_slot_index_model_identity_hash" not in RESERVED_ARRAY_ATTRS
