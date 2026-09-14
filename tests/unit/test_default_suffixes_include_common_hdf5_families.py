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

from pathlib import Path

from firecube.core.formats.discovery import discover_input_files


def test_discovery_returns_hdf5_files(tmp_path: Path) -> None:
    (tmp_path / "precip_20240101.hdf").touch()
    (tmp_path / "other.nc").touch()
    (tmp_path / "ignored.txt").touch()

    results = discover_input_files(tmp_path, sniff_hdf5=False)
    names = {Path(p).name for p in results}

    assert "precip_20240101.hdf" in names
    assert "other.nc" in names
    assert "ignored.txt" not in names


def test_discovery_returns_he5_files(tmp_path: Path) -> None:
    (tmp_path / "data.he5").touch()
    (tmp_path / "data.nc4").touch()

    results = discover_input_files(tmp_path, sniff_hdf5=False)
    names = {Path(p).name for p in results}

    assert "data.he5" in names
    assert "data.nc4" in names
