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

from firecube.core.api import ATTR_CONSOLIDATED_AT, ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr._reserved_attrs import (  # pyright: ignore[reportMissingImports]
    FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
    RESERVED_ARRAY_ATTRS,
    assert_attrs_safe,
)


@pytest.mark.parametrize("key", sorted(RESERVED_ARRAY_ATTRS))
def test_each_reserved_key_triggers_error(key: str) -> None:
    with pytest.raises(ValueError, match=key):
        assert_attrs_safe({key: "value"})


def test_non_reserved_keys_pass() -> None:
    assert_attrs_safe(
        {
            "units": "seconds since 1970-01-01",
            "calendar": "standard",
        }
    )


def test_empty_mapping_passes() -> None:
    assert_attrs_safe({})


@pytest.mark.parametrize(
    ("marker", "expected_value"),
    [
        (ATTR_PREALLOCATED, "firecube_preallocated"),
        (ATTR_CONSOLIDATED_AT, "firecube_consolidated_at"),
        (ATTR_COORD_MANAGED, "firecube_coord_managed"),
    ],
)
def test_sealing_markers_reserved(marker: str, expected_value: str) -> None:
    """Sealing-marker constants keep their persisted names, stay reserved, and are rejected."""
    assert marker == expected_value
    assert marker in RESERVED_ARRAY_ATTRS

    with pytest.raises(ValueError, match=marker):
        assert_attrs_safe({marker: True})


def test_group_identity_hash_attr_reserved() -> None:
    assert FIRECUBE_GROUP_IDENTITY_HASH_ATTR == "firecube_group_identity_hash"
    assert FIRECUBE_GROUP_IDENTITY_HASH_ATTR in RESERVED_ARRAY_ATTRS

    with pytest.raises(ValueError, match=FIRECUBE_GROUP_IDENTITY_HASH_ATTR):
        assert_attrs_safe({FIRECUBE_GROUP_IDENTITY_HASH_ATTR: "a" * 64})
