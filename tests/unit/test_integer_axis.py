from __future__ import annotations

import dataclasses

import numpy as np
import pytest

from firecube.core import index_spec


def test_integer_axis_accepts_positive_python_int() -> None:
    axis = index_spec.IntegerAxis(slot_count=8)

    assert axis.slot_count == 8
    assert type(axis.slot_count) is int


def test_integer_axis_accepts_one() -> None:
    axis = index_spec.IntegerAxis(slot_count=1)

    assert axis.slot_count == 1


def test_integer_axis_accepts_numpy_integer_and_normalizes_to_python_int() -> None:
    axis = index_spec.IntegerAxis(slot_count=np.int64(8))  # type: ignore[arg-type]

    assert axis.slot_count == 8
    assert type(axis.slot_count) is int


@pytest.mark.parametrize("bad_size", [0, -1])
def test_integer_axis_rejects_non_positive_sizes(bad_size: int) -> None:
    with pytest.raises(ValueError, match="slot_count"):
        index_spec.IntegerAxis(slot_count=bad_size)


@pytest.mark.parametrize("bad_size", [True, 1.5, "8"])
def test_integer_axis_rejects_non_integral_sizes(bad_size: object) -> None:
    with pytest.raises(TypeError, match="slot_count"):
        index_spec.IntegerAxis(slot_count=bad_size)  # type: ignore[arg-type]


def test_integer_axis_requires_size_argument() -> None:
    with pytest.raises(TypeError):
        index_spec.IntegerAxis()  # type: ignore[call-arg]


def test_item_info_accepts_coordinate_only() -> None:
    item = index_spec.ItemInfo(coordinate="2025-01-01T00:00:00Z")

    assert item.coordinate == "2025-01-01T00:00:00Z"


def test_item_info_rejects_key_keyword() -> None:
    with pytest.raises(TypeError, match="key"):
        index_spec.ItemInfo(coordinate="x", key="k")  # type: ignore[call-arg]


def test_item_info_rejects_group_keyword() -> None:
    with pytest.raises(TypeError, match="group"):
        index_spec.ItemInfo(coordinate="x", group="g")  # type: ignore[call-arg]


def test_item_info_has_single_field() -> None:
    assert {field.name for field in dataclasses.fields(index_spec.ItemInfo)} == {"coordinate"}
