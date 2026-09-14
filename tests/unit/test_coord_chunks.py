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

from unittest.mock import MagicMock

import pytest

from firecube.core.zarr._coord_chunks import resolve_coord_chunks


@pytest.mark.parametrize(
    "n,expected",
    [
        (0, (1,)),
        (1, (1,)),
        (255, (255,)),
        (256, (256,)),
        (257, (256,)),
        (4320, (256,)),
    ],
)
def test_default_chunks_boundary_n(n: int, expected: tuple[int, ...]) -> None:
    assert resolve_coord_chunks(None, n) == expected


def test_spec_chunks_honored() -> None:
    spec = MagicMock()
    spec.chunks = (1024,)
    assert resolve_coord_chunks(spec, 100) == (1024,)


def test_spec_chunks_none_falls_back_to_default() -> None:
    spec = MagicMock()
    spec.chunks = None
    assert resolve_coord_chunks(spec, 100) == (100,)


def test_spec_none_is_default() -> None:
    assert resolve_coord_chunks(None, 100) == (100,)


def test_rank_mismatch_raises() -> None:
    spec = MagicMock()
    spec.chunks = (256, 128)
    with pytest.raises(ValueError, match="rank-1"):
        resolve_coord_chunks(spec, 100)
