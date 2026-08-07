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

"""End-to-end wiring tests for Phase 1 custom compression codecs (issue #25).

Verifies that :func:`write_dataset_to_zarr` correctly threads the
``zarr_codecs`` kwarg through :func:`_build_zarr_encoding` and
:func:`resolve_compressor` and writes the resolved codec pipeline into the
Zarr store. The five cases cover the full decision matrix locked by
``tests/unit/test_zarr_codec_api_assumptions.py``:

* Case 1: default preset (regression against pre-issue-#25 behavior).
* Case 2: explicit no compression (``compressors=[]`` shape).
* Case 3: explicit Blosc with custom clevel and typesize.
* Case 4: explicit native Zstd codec (non-Blosc path).
* Case 5: unknown codec surfaces the resolver error.
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
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr

pytestmark = pytest.mark.integration


def _local_handle(path: str, mode: str = "w"):
    return create_zarr_store(
        uri=path,
        storage_config=StorageConfig(storage_type="local"),
        mode=mode,
    )


def _make_dataset() -> xr.Dataset:
    return xr.Dataset(
        {"var1": (["timestamp", "ny", "nx"], np.random.rand(2, 4, 4).astype("float32"))},
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


def test_case_1_default_preset_writes_blosc(tmp_path) -> None:
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        _make_dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=True,
        zarr_codecs=None,
    )
    codecs = _codecs_of(store, "G/var1")
    has_bytes_bytes = any(isinstance(codec, BytesBytesCodec) for codec in codecs)
    assert has_bytes_bytes, f"expected a compressor codec, got {_codec_names(codecs)!r}"
    assert "blosc" in _codec_names(codecs), (
        f"expected 'blosc' in codec pipeline, got {_codec_names(codecs)!r}"
    )


def test_case_2_no_compression_writes_empty_compressors(tmp_path) -> None:
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        _make_dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=False,
        zarr_codecs=None,
    )
    codecs = _codecs_of(store, "G/var1")
    assert not any(isinstance(codec, BytesBytesCodec) for codec in codecs), (
        f"expected no compressor, got {_codec_names(codecs)!r}"
    )


def test_case_3_explicit_blosc_zstd_clevel9_typesize8(tmp_path) -> None:
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        _make_dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=False,
        zarr_codecs=[
            {
                "name": "blosc",
                "configuration": {"cname": "zstd", "clevel": 9, "typesize": 8},
            }
        ],
    )
    codecs = _codecs_of(store, "G/var1")
    blosc_codecs = [
        codec
        for codec in codecs
        if isinstance(codec, BytesBytesCodec) and "blosc" in _codec_names([codec])
    ]
    assert blosc_codecs, f"expected a blosc codec, got {_codec_names(codecs)!r}"
    dumped = cast(Any, blosc_codecs[0]).to_dict()
    assert dumped["configuration"]["clevel"] == 9, dumped
    assert dumped["configuration"]["cname"] == "zstd", dumped


def test_case_4_explicit_native_zstd(tmp_path) -> None:
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        _make_dataset(),
        zarr_store=_local_handle(store),
        group="G",
        compression=False,
        zarr_codecs=[{"name": "zstd", "configuration": {"level": 7}}],
    )
    codecs = _codecs_of(store, "G/var1")
    assert "zstd" in _codec_names(codecs), (
        f"expected 'zstd' in codec pipeline, got {_codec_names(codecs)!r}"
    )
    assert not any("blosc" in name for name in _codec_names(codecs)), (
        f"unexpected 'blosc' in pipeline, got {_codec_names(codecs)!r}"
    )


def test_case_5_unknown_codec_raises_resolver_error(tmp_path) -> None:
    store = str(tmp_path / "test.zarr")
    with pytest.raises(ValueError, match="not a registered zarr codec"):
        write_dataset_to_zarr(
            _make_dataset(),
            zarr_store=_local_handle(store),
            group="G",
            compression=False,
            zarr_codecs=[{"name": "__no_such__", "configuration": {}}],
        )
