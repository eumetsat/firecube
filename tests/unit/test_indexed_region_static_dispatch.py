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

import tempfile
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.errors import SchemaDriftError
from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy,
)
from firecube.ingestor.templates.direct_zarr import (
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)

pytestmark = pytest.mark.unit


def _make_strategy(store_uri: str) -> IndexedRegionStrategy:
    schema = [
        ZarrGroupSpec(
            group="g",
            arrays=[
                ZarrArraySpec(name="data", shape=(5, 4), dtype="float32"),
                ZarrArraySpec(name="lat", shape=(4,), dtype="float64", time_indexed=False),
            ],
        )
    ]
    return IndexedRegionStrategy(store_uri=store_uri, schema=schema)


def _preallocate(tmp: str) -> None:
    store = LocalStore(tmp)
    root = zarr.open_group(store=store, mode="a", zarr_format=3)
    grp = root.require_group("g")
    grp.create_array("data", shape=(5, 4), dtype="float32")
    grp.create_array("lat", shape=(4,), dtype="float64")


def _static_intent(data: np.ndarray) -> WriteIntent:
    return WriteIntent(group="g", array="lat", ts_index=0, data=data, kind="static")


def _read_lat(tmp: str) -> np.ndarray:
    root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
    return np.asarray(cast(Any, root["g/lat"])[:])


def test_static_intent_writes_correctly() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = _make_strategy(f"file://{tmp}")
        lat_data = np.array([10.0, 20.0, 30.0, 40.0])
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        np.testing.assert_array_equal(_read_lat(tmp), lat_data)


def test_static_intent_no_coverage() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = _make_strategy(f"file://{tmp}")
        lat_data = np.array([10.0, 20.0, 30.0, 40.0])
        result = strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        assert result["coverage"] == []


def test_static_intent_bypasses_slot_range() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = _make_strategy(f"file://{tmp}")
        lat_data = np.array([10.0, 20.0, 30.0, 40.0])
        # ts_index=0 is outside slot_range=[2, 4); a timed intent here would raise
        # WriteIntentRangeError, but static intents bypass slot-range validation.
        strategy.write_groups(
            group_to_intents={"g": [_static_intent(lat_data)]},
            slot_range=(2, 4),
        )
        np.testing.assert_array_equal(_read_lat(tmp), lat_data)


def test_static_resume_identical_data_succeeds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = _make_strategy(f"file://{tmp}")
        lat_data = np.array([10.0, 20.0, 30.0, 40.0])
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        np.testing.assert_array_equal(_read_lat(tmp), lat_data)


def test_static_resume_divergent_data_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = _make_strategy(f"file://{tmp}")
        lat_data_a = np.array([10.0, 20.0, 30.0, 40.0])
        lat_data_b = np.array([11.0, 22.0, 33.0, 44.0])
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data_a)]})
        with pytest.raises(SchemaDriftError, match="diverged"):
            strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data_b)]})


def _make_strategy_nan_fill(store_uri: str) -> IndexedRegionStrategy:
    schema = [
        ZarrGroupSpec(
            group="g",
            arrays=[
                ZarrArraySpec(name="data", shape=(5, 4), dtype="float32"),
                ZarrArraySpec(
                    name="lat",
                    shape=(4,),
                    dtype="float64",
                    fill_value=np.float32("nan"),
                    time_indexed=False,
                ),
            ],
        )
    ]
    return IndexedRegionStrategy(store_uri=store_uri, schema=schema)


def _preallocate_nan_fill(tmp: str) -> None:
    store = LocalStore(tmp)
    root = zarr.open_group(store=store, mode="a", zarr_format=3)
    grp = root.require_group("g")
    grp.create_array("data", shape=(5, 4), dtype="float32")
    grp.create_array("lat", shape=(4,), dtype="float64", fill_value=np.float32("nan"))


def test_static_intent_nan_fill_first_write_succeeds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate_nan_fill(tmp)
        strategy = _make_strategy_nan_fill(f"file://{tmp}")
        lat_data = np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float32)
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        np.testing.assert_array_equal(_read_lat(tmp), np.array([10.0, 20.0, 30.0, 40.0]))


def test_static_intent_nan_data_idempotent_resume() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate_nan_fill(tmp)
        strategy = _make_strategy_nan_fill(f"file://{tmp}")
        lat_data = np.array([10.0, np.nan, 30.0, 40.0], dtype=np.float32)
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        strategy.write_groups(group_to_intents={"g": [_static_intent(lat_data)]})
        assert np.array_equal(_read_lat(tmp), np.array([10.0, np.nan, 30.0, 40.0]), equal_nan=True)


def test_static_all_nan_data_then_divergent_resume_raises() -> None:
    """Landmine guard: legitimate all-NaN static data must NOT be treated as a
    fresh (never-written) array on resume.

    fill_value is NaN AND the committed data is all-NaN, so a contents-based
    freshness probe (``_array_is_all_fill``) would classify the array as fresh
    and silently overwrite it. The reserved ``firecube_static_written`` marker
    records the prior commit instead, so a divergent resume fails loudly.
    """
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate_nan_fill(tmp)
        strategy = _make_strategy_nan_fill(f"file://{tmp}")
        all_nan = np.array([np.nan, np.nan, np.nan, np.nan], dtype=np.float32)
        divergent = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
        strategy.write_groups(group_to_intents={"g": [_static_intent(all_nan)]})
        with pytest.raises(SchemaDriftError, match="diverged"):
            strategy.write_groups(group_to_intents={"g": [_static_intent(divergent)]})


def test_static_all_nan_data_idempotent_resume() -> None:
    """Replaying identical all-NaN static data on resume is a no-op, not drift."""
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate_nan_fill(tmp)
        strategy = _make_strategy_nan_fill(f"file://{tmp}")
        all_nan = np.array([np.nan, np.nan, np.nan, np.nan], dtype=np.float32)
        strategy.write_groups(group_to_intents={"g": [_static_intent(all_nan)]})
        strategy.write_groups(group_to_intents={"g": [_static_intent(all_nan)]})
        assert np.array_equal(_read_lat(tmp), all_nan, equal_nan=True)


def test_static_write_stamps_written_marker() -> None:
    """A committed static array carries the reserved written-marker attr."""
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = _make_strategy(f"file://{tmp}")
        strategy.write_groups(
            group_to_intents={"g": [_static_intent(np.array([1.0, 2.0, 3.0, 4.0]))]}
        )
        root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
        assert cast(Any, root["g/lat"]).attrs.get("firecube_static_written") is True


@pytest.mark.unit
def test_static_array_allocator_respects_time_indexed_even_when_expected_time_count_leaked() -> (
    None
):
    """Defence-in-depth regression: allocator must respect time_indexed even if spec
    is mutated post-construction (via dataclasses.replace, deserialization, etc.).

    Uses object.__setattr__ to bypass the frozen-dataclass construction validation and
    simulate a poisoned spec reaching indexed_region.py:220-228. The guard added by T7
    makes the safety explicit instead of relying on upstream augmentation.
    """

    with tempfile.TemporaryDirectory() as tmp:
        static = ZarrArraySpec(name="lat", shape=(4,), dtype="float64", time_indexed=False)
        object.__setattr__(static, "expected_time_count", 10)
        schema = [
            ZarrGroupSpec(
                group="g",
                arrays=[
                    ZarrArraySpec(name="data", shape=(5, 4), dtype="float32"),
                    static,
                ],
            )
        ]
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}", schema=schema)

        strategy.write_groups(group_to_intents={"g": []})

        root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
        assert cast(Any, root["g/lat"]).shape == (4,)


@pytest.mark.unit
def test_time_indexed_array_shape_still_substituted() -> None:
    """GREEN-only smoke: time-indexed substitution must still work after T7 guard is applied.

    Verifies that the explicit time_indexed guard does not break the legitimate substitution
    for time-indexed arrays. Uses legitimate construction (no object.__setattr__ bypass).
    """

    with tempfile.TemporaryDirectory() as tmp:
        schema = [
            ZarrGroupSpec(
                group="g",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        shape=(5, 4),
                        dtype="float32",
                        expected_time_count=10,
                    ),
                    ZarrArraySpec(name="lat", shape=(4,), dtype="float64", time_indexed=False),
                ],
            )
        ]
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}", schema=schema)

        strategy.write_groups(group_to_intents={"g": []})

        root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
        assert cast(Any, root["g/data"]).shape == (10, 4)
        assert cast(Any, root["g/lat"]).shape == (4,)


@pytest.mark.parametrize(
    "dtype, data",
    [
        pytest.param(np.int32, np.array([1, 2, 3, 4], dtype=np.int32), id="int32"),
        pytest.param(np.uint8, np.array([10, 20, 30, 40], dtype=np.uint8), id="uint8"),
        pytest.param(bool, np.array([True, False, True, False], dtype=bool), id="bool"),
        pytest.param(
            "datetime64[s]",
            np.array(
                ["2025-01-01", "2025-01-02", "NaT", "2025-01-04"],
                dtype="datetime64[s]",
            ),
            id="datetime64-with-NaT",
        ),
        pytest.param("U3", np.array(["aaa", "bbb", "ccc", "ddd"], dtype="U3"), id="str-U3"),
    ],
)
def test_static_replay_dtype_safety(tmp_path: Path, dtype: Any, data: np.ndarray) -> None:
    """Replay of an identical static array must succeed for all reasonable dtypes.

    Pre-fix, the ``U``-kind variant raises ``TypeError`` from
    ``np.array_equal(..., equal_nan=True)``; the helper handles it via the
    plain-equality fallback. The ``datetime64`` variant carries a NaT value
    to prove the helper preserves NaT-aware comparison (which the rejected
    ad-hoc ``{"f", "c"}`` gate would have broken).
    """
    tmp = str(tmp_path)
    store = LocalStore(tmp)
    root = zarr.open_group(store=store, mode="a", zarr_format=3)
    grp = root.require_group("g")
    grp.create_array("data", shape=(5, 4), dtype="float32")
    grp.create_array("lat", shape=(4,), dtype=dtype)

    schema = [
        ZarrGroupSpec(
            group="g",
            arrays=[
                ZarrArraySpec(name="data", shape=(5, 4), dtype="float32"),
                ZarrArraySpec(name="lat", shape=(4,), dtype=dtype, time_indexed=False),
            ],
        )
    ]
    strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}", schema=schema)
    intent = WriteIntent(group="g", array="lat", ts_index=0, data=data, kind="static")

    # First write
    strategy.write_groups(group_to_intents={"g": [intent]})
    # Replay with identical data — must NOT raise
    strategy.write_groups(group_to_intents={"g": [intent]})
