# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests: discover_input_files handles file:// sources and suffix-less HDF5."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import h5py
import pytest

from firecube.core.formats.discovery import discover_input_files


@pytest.fixture
def hdf5_dir(tmp_path: Path) -> Path:
    """Create a directory with one suffix-less HDF5 file and one non-HDF5 file."""
    h5_file = tmp_path / "data"
    with h5py.File(h5_file, "w") as handle:
        handle.create_dataset("x", data=[1, 2, 3])

    (tmp_path / "not_hdf5.bin").write_bytes(b"\x00\x01\x02\x03")
    return tmp_path


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_uri_factory",
    [
        pytest.param(
            lambda directory: f"file://{directory}",
            id="file_uri",
        ),
        pytest.param(
            str,
            id="bare_path",
        ),
    ],
)
def test_file_uri_discovers_suffix_less_hdf5(
    hdf5_dir: Path,
    source_uri_factory: Callable[[Path], str],
) -> None:
    """Discover suffix-less HDF5 files and return plain absolute paths."""
    source_uri = source_uri_factory(hdf5_dir)

    results = discover_input_files(source_uri, sniff_hdf5=True)

    assert results == [str(hdf5_dir / "data")]
    assert all(not result.startswith("file://") for result in results)
    assert Path(results[0]).is_absolute()
