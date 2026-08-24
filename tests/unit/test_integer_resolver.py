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

"""Integer axis resolver contract tests."""

from __future__ import annotations

import numpy as np
import pytest

from firecube.core import index_resolve
from firecube.core.index_spec import IndexSpec, IntegerAxis


def _resolver(size: int = 8):
    return index_resolve.IntegerResolver(axis=IntegerAxis(slot_count=size))


def test_resolver_for_integer_axis_returns_integer_resolver() -> None:
    resolver = index_resolve._resolver_for(IntegerAxis(slot_count=8))

    assert isinstance(resolver, index_resolve.IntegerResolver)


def test_position_maps_integer_coordinate_to_same_slot() -> None:
    assert _resolver().position(5) == 5


def test_position_accepts_numpy_integer() -> None:
    assert _resolver().position(np.int64(7)) == 7


def test_position_rejects_negative_coordinate() -> None:
    with pytest.raises(IndexError, match=r"0.*8"):
        _resolver().position(-1)


def test_position_rejects_coordinate_at_size() -> None:
    with pytest.raises(IndexError, match="coordinate 8 out of range"):
        _resolver().position(8)


def test_position_rejects_bool_coordinate() -> None:
    with pytest.raises(TypeError, match="bool is explicitly rejected"):
        _resolver().position(True)


def test_position_rejects_non_integral_coordinate() -> None:
    with pytest.raises(TypeError, match="coordinate must be an integral type"):
        _resolver().position(1.5)


def test_coordinate_maps_integer_index_to_same_coordinate() -> None:
    assert _resolver().coordinate(3) == 3


def test_coordinate_rejects_negative_index() -> None:
    with pytest.raises(IndexError, match=r"0.*8"):
        _resolver().coordinate(-1)


def test_coordinate_rejects_bool_index() -> None:
    with pytest.raises(TypeError, match="bool is explicitly rejected"):
        _resolver().coordinate(False)


def test_resolved_index_integer_identity_hash_is_deterministic() -> None:
    resolved = index_resolve.ResolvedIndex(
        IndexSpec(name="x", groups={"data": IntegerAxis(slot_count=8)}),
        {"data": _resolver()},
    )

    assert resolved.identity_hash == resolved.identity_hash
