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

"""Generic group-level attributes for DirectZarr schemas (`ZarrGroupSpec.attrs`).

Firecube writes plugin-declared group attrs verbatim onto the group's zarr.json;
it does not interpret them (no convention is assumed). These tests use generic
attribute names on purpose — the core mechanism is convention-agnostic.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
import zarr

from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.templates.direct_zarr import (
    _compute_schema_hash,
    _setup_global_zarr_schema,
)

_ATTRS = {"title": "demo dataset", "owner": "team-x"}


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def _group_attrs(store_path, group: str) -> dict:
    grp = zarr.open_group(store=str(store_path), mode="r", zarr_format=3)[group]
    return dict(cast(Any, grp).attrs)


@pytest.mark.unit
def test_set_group_attrs_roundtrip(tmp_path) -> None:
    store_path = tmp_path / "g.zarr"
    writer = RegionZarrWriter(str(store_path))
    writer.ensure_group("data/x", shape=(2,), dtype="float64", fill_value=0.0)

    writer.set_group_attrs("data", _ATTRS)

    assert _group_attrs(store_path, "data") == _ATTRS


@pytest.mark.unit
def test_set_group_attrs_is_noop_for_empty(tmp_path) -> None:
    store_path = tmp_path / "g.zarr"
    writer = RegionZarrWriter(str(store_path))
    writer.ensure_group("data/x", shape=(2,), dtype="float64", fill_value=0.0)

    writer.set_group_attrs("data", None)
    writer.set_group_attrs("data", {})

    assert _group_attrs(store_path, "data") == {}


@pytest.mark.unit
def test_set_group_attrs_rejects_reserved_names(tmp_path) -> None:
    store_path = tmp_path / "g.zarr"
    writer = RegionZarrWriter(str(store_path))
    writer.ensure_group("data/x", shape=(2,), dtype="float64", fill_value=0.0)

    with pytest.raises(ValueError):
        writer.set_group_attrs("data", {"firecube_internal": True})


@pytest.mark.unit
def test_group_spec_attrs_rejects_non_mapping() -> None:
    with pytest.raises(ValueError):
        ZarrGroupSpec(group="data", attrs=cast(Any, "not-a-mapping"))


@pytest.mark.unit
def test_group_spec_attrs_defaults_to_none() -> None:
    assert ZarrGroupSpec(group="data").attrs is None


@pytest.mark.unit
def test_schema_hash_unchanged_when_no_group_attrs() -> None:
    """attrs=None and attrs={} must hash identically (backward-compatible: a
    schema declaring no group attrs keeps its pre-feature identity)."""
    arrays = [ZarrArraySpec(name="v", shape=(0, 2), dtype="float32", chunks=(1, 2))]
    expected = {"data": 4}
    h_none = _compute_schema_hash([ZarrGroupSpec(group="data", arrays=arrays)], expected)
    h_empty = _compute_schema_hash([ZarrGroupSpec(group="data", arrays=arrays, attrs={})], expected)
    assert h_none == h_empty


@pytest.mark.unit
def test_schema_hash_changes_with_group_attrs() -> None:
    arrays = [ZarrArraySpec(name="v", shape=(0, 2), dtype="float32", chunks=(1, 2))]
    expected = {"data": 4}
    h_none = _compute_schema_hash([ZarrGroupSpec(group="data", arrays=arrays)], expected)
    h_attrs = _compute_schema_hash(
        [ZarrGroupSpec(group="data", arrays=arrays, attrs={"title": "a"})], expected
    )
    h_other = _compute_schema_hash(
        [ZarrGroupSpec(group="data", arrays=arrays, attrs={"title": "b"})], expected
    )
    assert h_none != h_attrs
    assert h_attrs != h_other
    # Same attrs -> same hash (deterministic).
    h_attrs_again = _compute_schema_hash(
        [ZarrGroupSpec(group="data", arrays=arrays, attrs={"title": "a"})], expected
    )
    assert h_attrs == h_attrs_again


@pytest.mark.unit
def test_setup_global_zarr_schema_writes_group_attrs(tmp_path) -> None:
    store_path = tmp_path / "setup.zarr"
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[ZarrArraySpec(name="v", shape=(4, 2), dtype="float32", chunks=(1, 2))],
            attrs=_ATTRS,
        )
    ]

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=schema,
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    assert _group_attrs(store_path, "data") == _ATTRS
