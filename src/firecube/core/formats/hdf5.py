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

"""HDF5 helpers for ingestion plugins.

Reusable helpers for sniffing, reading, and materializing HDF5 inputs
(either standalone files or members embedded inside ZIP archives).
"""

from __future__ import annotations

import contextlib
import logging
import tempfile
from pathlib import Path
from typing import Any

from firecube.core.formats.zip import extract_hdf5_from_zip

log = logging.getLogger("firecube.core.formats")


def looks_like_hdf5(path: Path) -> bool:
    """Best-effort magic-number check for HDF5 files without extensions."""
    try:
        with path.open("rb") as fh:
            sig = fh.read(8)
        return sig == b"\x89HDF\r\n\x1a\n"
    except Exception:
        return False


def read_hdf5_array(
    hdf5_path: Path,
    *,
    variable: str,
    logger: logging.Logger | None = None,
) -> Any:
    """Read a named array from a local HDF5(-like) file, with xarray fallback.

    Args:
        hdf5_path: Path to the HDF5 file.
        variable: Name of the variable/dataset to read.
        logger: Optional logger for debug messages.

    Returns:
        Numpy array of the data (float32).

    Raises:
        KeyError: If the variable is not found.
        RuntimeError: If dependencies are missing or read fails.
    """
    logger = logger or log
    try:
        import numpy as np
    except Exception as exc:
        raise RuntimeError("numpy is required to read HDF5 inputs") from exc

    try:
        import h5py
    except Exception as exc:
        raise RuntimeError("h5py is required to read HDF5 inputs") from exc

    try:
        with h5py.File(hdf5_path, "r") as handle:
            if variable not in handle:
                raise KeyError(f"{variable} dataset not found in HDF5 file")
            return np.asarray(handle[variable], dtype=np.float32)
    except Exception as h5_exc:
        try:
            import xarray as xr

            ds = xr.open_dataset(hdf5_path, phony_dims="sort")
            if variable not in ds:
                raise KeyError(f"{variable} variable not found in dataset")
            arr = ds[variable].values.astype(np.float32, copy=False)
            ds.close()
            return arr
        except Exception as xr_exc:
            raise RuntimeError(
                f"Failed to read {variable} from {hdf5_path.name}: {h5_exc}"
            ) from xr_exc
        finally:
            with contextlib.suppress(Exception):
                logger.debug("Fell back to xarray open_dataset for %s", hdf5_path.name)


def materialize_hdf5_path(
    file_path: Path,
    *,
    extract_root: Path | None = None,
    logger: logging.Logger | None = None,
) -> tuple[Path, tempfile.TemporaryDirectory | None]:
    """Return a local HDF5 path for a source path (ZIP or direct HDF5-like file).

    If the input is a ZIP file, it extracts the content to a temporary directory.
    If it is already an HDF5-like file, it is returned as-is.

    Args:
        file_path: Source file path.
        extract_root: Optional root directory for temporary extraction.
        logger: Optional logger.

    Returns:
        Tuple of (path_to_hdf5_file, temporary_directory_object_or_None).
    """
    logger = logger or log
    if file_path.suffix.lower() != ".zip":
        return file_path, None

    tmp_dir: tempfile.TemporaryDirectory | None
    if extract_root is None:
        tmp_dir = tempfile.TemporaryDirectory()
    else:
        extract_root.mkdir(parents=True, exist_ok=True)
        tmp_dir = tempfile.TemporaryDirectory(dir=str(extract_root))

    try:
        extracted = extract_hdf5_from_zip(file_path, Path(tmp_dir.name), logger=logger)
        if extracted is None:
            raise FileNotFoundError(f"No HDF5 member found in archive {file_path.name}")
        return extracted, tmp_dir
    except Exception:
        with contextlib.suppress(Exception):
            tmp_dir.cleanup()
        raise
