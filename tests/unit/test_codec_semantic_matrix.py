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

"""4-cell ``zarr_compression`` x ``zarr_codecs`` semantic-matrix contract tests.

Locks the shared codec semantics across the two Zarr templates, exercised at
the writer/encoding boundary with real zarr stores:

* Cell 1 — ``False + None``    → uncompressed (no ``BytesBytesCodec``)
* Cell 2 — ``True + None``     → zarr default codec applied
* Cell 3 — ``True + [codecs]`` → declared codecs applied
* Cell 4 — ``False + [codecs]`` → rejected at ``ZarrTemplateConfig`` construction

Each cell is exercised twice: once for the GenericZarr path
(``write_dataset_to_zarr`` → ``_build_zarr_encoding``) and once for the
DirectZarr path (``derive_effective_codecs_for_spec`` →
``RegionZarrWriter.ensure_group``). Assertions read back ``arr.metadata.codecs``
and check codec presence by ABC — the exact default identity (e.g. ZstdCodec
level value) is intentionally not asserted so a zarr default bump does not
break this contract lane.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from zarr.abc.codec import BytesBytesCodec

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.write import (
    derive_effective_codecs_for_spec,
    write_dataset_to_zarr,
)
from firecube.ingestor.templates.config import ZarrTemplateConfig
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec

pytestmark = pytest.mark.unit


BLOSC_ENTRY: dict[str, Any] = {
    "name": "blosc",
    "configuration": {"cname": "zstd", "clevel": 5},
}


def _local_handle(path: str, mode: str = "w"):
    return create_zarr_store(
        uri=path,
        storage_config=StorageConfig(storage_type="local"),
        mode=mode,
    )


def _dataset() -> xr.Dataset:
    return xr.Dataset(
        {"var1": (["timestamp", "ny", "nx"], np.zeros((2, 4, 4), dtype="float32"))},
        coords={"timestamp": pd.date_range("2023-12-01", periods=2, freq="5min")},
    )


def _codecs_of(store_path: str, array_path: str) -> list[Any]:
    arr = zarr.open_array(store_path, path=array_path, mode="r")
    return list(cast(Any, arr.metadata).codecs)


def _codec_names(codecs: list[Any]) -> list[str]:
    names: list[str] = []
    for codec in codecs:
        to_dict = getattr(codec, "to_dict", None)
        if callable(to_dict):
            dumped = cast(dict[str, Any], to_dict())
            name = dumped.get("name")
            if isinstance(name, str):
                names.append(name)
                continue
        names.append(type(codec).__name__)
    return names


# ---------------------------------------------------------------------------
# GenericZarr path — write_dataset_to_zarr → _build_zarr_encoding
# ---------------------------------------------------------------------------


def test_generic_false_none_uncompressed(tmp_path) -> None:
    """Cell 1 (Generic): False + None → array carries no ``BytesBytesCodec``."""
    store = str(tmp_path / "gen-false-none.zarr")
    write_dataset_to_zarr(
        _dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=False,
        zarr_codecs=None,
    )
    codecs = _codecs_of(store, "G/var1")
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"False+None must be uncompressed, got codecs={_codec_names(codecs)!r}"
    )


def test_generic_true_none_zarr_default(tmp_path) -> None:
    """Cell 2 (Generic): True + None → zarr injects its own default (not Blosc)."""
    store = str(tmp_path / "gen-true-none.zarr")
    write_dataset_to_zarr(
        _dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=True,
        zarr_codecs=None,
    )
    codecs = _codecs_of(store, "G/var1")
    names = _codec_names(codecs)
    assert any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"True+None must apply zarr's default compressor, got {names!r}"
    )
    assert "blosc" not in names, (
        f"True+None must delegate to zarr default (currently zstd), not firecube-opinionated blosc; got {names!r}"
    )


def test_generic_true_declared_codecs(tmp_path) -> None:
    """Cell 3 (Generic): True + [blosc] → declared blosc codec lands on the array."""
    store = str(tmp_path / "gen-true-blosc.zarr")
    write_dataset_to_zarr(
        _dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=True,
        zarr_codecs=[BLOSC_ENTRY],
    )
    codecs = _codecs_of(store, "G/var1")
    names = _codec_names(codecs)
    assert "blosc" in names, f"True+[blosc] must apply declared blosc codec, got {names!r}"


def test_generic_false_declared_codecs_raises() -> None:
    """Cell 4 (Generic): False + [blosc] → rejected at config construction."""
    with pytest.raises(ValueError) as excinfo:
        ZarrTemplateConfig(
            zarr_compression=False,
            zarr_codecs=[BLOSC_ENTRY],
        )
    msg = str(excinfo.value)
    assert "zarr_compression" in msg and "zarr_codecs" in msg, (
        f"error must name both zarr_compression and zarr_codecs: {msg!r}"
    )


# ---------------------------------------------------------------------------
# DirectZarr path — derive_effective_codecs_for_spec → ensure_group
# ---------------------------------------------------------------------------


def _direct_write(tmp_path, template: ZarrTemplateConfig, spec: ZarrArraySpec, subdir: str) -> str:
    store = str(tmp_path / subdir)
    filters, serializer, compressors = derive_effective_codecs_for_spec(spec, template)
    writer = RegionZarrWriter(store)
    writer.ensure_group(
        f"G/{spec.name}",
        shape=spec.shape,
        dtype=spec.dtype,
        filters=filters,
        serializer=serializer,
        compressors=compressors,
    )
    return store


def test_directzarr_false_none_uncompressed(tmp_path) -> None:
    """Cell 1 (Direct): False + None → derivation yields ``(None, None, [])``, no compressor lands."""
    template = ZarrTemplateConfig(zarr_compression=False, zarr_codecs=None)
    spec = ZarrArraySpec(name="var1", shape=(4, 4), dtype="f4")

    store = _direct_write(tmp_path, template, spec, "dir-false-none.zarr")
    codecs = _codecs_of(store, "G/var1")
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"DirectZarr False+None must be uncompressed, got {_codec_names(codecs)!r}"
    )


def test_directzarr_true_none_zarr_default(tmp_path) -> None:
    """Cell 2 (Direct): True + None → derivation returns all-None so zarr default fires."""
    template = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=None)
    spec = ZarrArraySpec(name="var1", shape=(4, 4), dtype="f4")

    store = _direct_write(tmp_path, template, spec, "dir-true-none.zarr")
    codecs = _codecs_of(store, "G/var1")
    names = _codec_names(codecs)
    assert any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"DirectZarr True+None must apply zarr's default compressor, got {names!r}"
    )
    assert "blosc" not in names, (
        f"DirectZarr True+None must delegate to zarr default, not blosc; got {names!r}"
    )


def test_directzarr_true_declared_codecs(tmp_path) -> None:
    """Cell 3 (Direct): True + [blosc] → declared blosc codec lands on the array."""
    template = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=[BLOSC_ENTRY])
    spec = ZarrArraySpec(name="var1", shape=(4, 4), dtype="f4")

    store = _direct_write(tmp_path, template, spec, "dir-true-blosc.zarr")
    codecs = _codecs_of(store, "G/var1")
    names = _codec_names(codecs)
    assert "blosc" in names, (
        f"DirectZarr True+[blosc] must apply declared blosc codec, got {names!r}"
    )


def test_directzarr_false_declared_codecs_raises() -> None:
    """Cell 4 (Direct): False + [blosc] → rejected at config construction (shared validator)."""
    with pytest.raises(ValueError) as excinfo:
        ZarrTemplateConfig(
            zarr_compression=False,
            zarr_codecs=[BLOSC_ENTRY],
        )
    msg = str(excinfo.value)
    assert "zarr_compression" in msg and "zarr_codecs" in msg, (
        f"error must name both zarr_compression and zarr_codecs: {msg!r}"
    )
