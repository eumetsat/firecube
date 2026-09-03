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
