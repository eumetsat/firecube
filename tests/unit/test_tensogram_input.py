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

"""Tests for .tgm input file discovery and xarray backend."""

from __future__ import annotations

import numpy as np
import xarray as xr

from firecube.core.formats import KNOWN_EXTENSIONS, discover_input_files


class TestDiscoverTgmFiles:
    def test_finds_tgm_files_with_explicit_suffix(self, tmp_path):
        (tmp_path / "product_a.tgm").write_bytes(b"dummy")
        (tmp_path / "product_b.tgm").write_bytes(b"dummy")
        (tmp_path / "product.zarr").mkdir()

        found = discover_input_files(str(tmp_path), include_suffixes=[".tgm"])
        names = [f.split("/")[-1] for f in found]
        assert sorted(n for n in names if n.endswith(".tgm")) == [
            "product_a.tgm",
            "product_b.tgm",
        ]

    def test_does_not_include_tgm_by_default(self, tmp_path):
        (tmp_path / "product.tgm").write_bytes(b"dummy")
        (tmp_path / "product.nc").write_bytes(b"dummy")

        found = discover_input_files(str(tmp_path))
        names = [f.split("/")[-1] for f in found]
        assert not any(n.endswith(".tgm") for n in names)
        assert "product.nc" in names

    def test_tgm_in_known_extensions(self):
        assert ".tgm" in KNOWN_EXTENSIONS

    def test_mixed_suffixes_include_tgm_and_nc(self, tmp_path):
        (tmp_path / "a.tgm").write_bytes(b"dummy")
        (tmp_path / "b.nc").write_bytes(b"dummy")
        (tmp_path / "c.h5").write_bytes(b"dummy")

        found = discover_input_files(str(tmp_path), include_suffixes=[".tgm", ".nc"])
        names = {f.split("/")[-1] for f in found}
        assert names == {"a.tgm", "b.nc"}


class TestTensogramXarrayBackend:
    def test_opens_firecube_produced_tgm(self, tmp_path):
        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "test.zarr")
        tgm = str(tmp_path / "test.tgm")

        ds = xr.Dataset(
            {"var1": (["t"], np.array([1.0, 2.0, 3.0], dtype="float32"))},
            coords={"t": [0, 1, 2]},
        )
        ds.to_zarr(src)
        zarr_to_tgm(src, tgm)

        loaded = xr.open_dataset(tgm, engine="tensogram")
        assert "var1" in loaded.data_vars
        assert loaded["var1"].shape == (3,)
        loaded.close()
