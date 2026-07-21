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

"""ZIP archive helpers for ingestion plugins.

Reusable helpers for extracting and streaming HDF5 members from ZIP archives.
Plugin-agnostic; callers should depend on the ``firecube.core.formats`` public
re-exports rather than this submodule path.
"""

from __future__ import annotations

import concurrent.futures
import logging
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

log = logging.getLogger("firecube.core.formats")


def extract_hdf5_from_zip(
    zip_path: Path, dest: Path, *, logger: logging.Logger | None = None
) -> Path | None:
    """Extract the first HDF5-like member from a ZIP archive into dest.

    Selection:
      - Prefer members ending with .h5
      - Fallback: first non-directory member whose filename contains "HDF5"
    """
    logger = logger or log
    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".h5")]
            if not members:
                candidates = [
                    name
                    for name in zf.namelist()
                    if not name.endswith("/") and "HDF5" in Path(name).name.upper()
                ]
                if not candidates:
                    return None
                members = [candidates[0]]
            member = members[0]

            # Stream optimization: read to memory then write (often faster than extract()).
            try:
                hdf5_data = zf.read(member)
                extracted_path = dest / member
                extracted_path.parent.mkdir(parents=True, exist_ok=True)
                with extracted_path.open("wb") as handle:
                    handle.write(hdf5_data)
                logger.debug(
                    "Streamed %s bytes from %s/%s", f"{len(hdf5_data):,}", zip_path.name, member
                )
                return extracted_path
            except Exception as stream_exc:
                logger.debug(
                    "Stream extraction failed for %s (%s); falling back to ZipFile.extract()",
                    zip_path,
                    stream_exc,
                )
                extracted_path = Path(zf.extract(member, dest))
                return extracted_path
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP archive: %s", zip_path)
        return None


def stream_hdf5_from_zip(zip_path: Path, *, logger: logging.Logger | None = None) -> bytes | None:
    """Stream HDF5 content directly from ZIP to memory (no temp files)."""
    logger = logger or log
    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".h5")]
            if not members:
                candidates = [
                    name
                    for name in zf.namelist()
                    if not name.endswith("/") and "HDF5" in Path(name).name.upper()
                ]
                if not candidates:
                    return None
                members = [candidates[0]]
            member = members[0]
            logger.debug("Streaming HDF5 data from %s/%s", zip_path.name, member)
            return zf.read(member)
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP archive: %s", zip_path)
        return None


def extract_zip_files_parallel(
    zip_files: list[Path],
    *,
    tempdir_factory: Callable[[], tempfile.TemporaryDirectory],
    extract_fn: Callable[[Path, Path], Path | None] = extract_hdf5_from_zip,
    max_workers: int = 4,
    record_temp_dirs: bool = False,
    temp_dir_map: dict[str, tempfile.TemporaryDirectory] | None = None,
    logger: logging.Logger | None = None,
) -> tuple[list[Path], list[tempfile.TemporaryDirectory]]:
    """Extract ZIP files in parallel using ThreadPoolExecutor for I/O-bound operations.

    Returns:
      (resolved_paths, temp_dirs)

    If record_temp_dirs=True and temp_dir_map is provided, it will map each
    extracted file path (resolved) to the TemporaryDirectory that produced it.
    """
    logger = logger or log
    resolved: list[Path] = []
    temp_dirs: list[tempfile.TemporaryDirectory] = []

    if not zip_files:
        return resolved, temp_dirs

    def _maybe_record(path: Path, tmp: tempfile.TemporaryDirectory) -> None:
        if not record_temp_dirs or temp_dir_map is None:
            return
        try:
            temp_dir_map[str(path.resolve())] = tmp
        except Exception:
            temp_dir_map[str(path)] = tmp

    # For small numbers of files, don't bother with parallelism overhead
    if len(zip_files) <= 2:
        for zip_path in zip_files:
            tmp = tempdir_factory()
            extracted = extract_fn(zip_path, Path(tmp.name))
            if extracted:
                _maybe_record(extracted, tmp)
                temp_dirs.append(tmp)
                resolved.append(extracted)
            else:
                tmp.cleanup()
                logger.warning("No HDF5 file found inside archive %s", zip_path)
        return resolved, temp_dirs

    def extract_single_zip(
        zip_path: Path,
    ) -> tuple[Path | None, tempfile.TemporaryDirectory | None]:
        try:
            tmp = tempdir_factory()
            extracted = extract_fn(zip_path, Path(tmp.name))
            if extracted:
                return extracted, tmp
            tmp.cleanup()
            logger.warning("No HDF5 file found inside archive %s", zip_path)
            return None, None
        except Exception as exc:
            logger.error("Failed to extract ZIP file %s: %s", zip_path, exc)
            return None, None

    actual_workers = min(int(max_workers), len(zip_files))
    logger.debug(
        "Extracting %d ZIP files using %d parallel workers", len(zip_files), actual_workers
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
        future_to_zip = {
            executor.submit(extract_single_zip, zip_path): zip_path for zip_path in zip_files
        }
        for future in concurrent.futures.as_completed(future_to_zip):
            zip_path = future_to_zip[future]
            try:
                extracted_path, tmp_dir = future.result()
                if extracted_path and tmp_dir:
                    _maybe_record(extracted_path, tmp_dir)
                    resolved.append(extracted_path)
                    temp_dirs.append(tmp_dir)
            except Exception as exc:
                logger.error("ZIP extraction failed for %s: %s", zip_path, exc)

    logger.debug(
        "Parallel ZIP extraction completed: %d files extracted from %d archives",
        len(resolved),
        len(zip_files),
    )
    return resolved, temp_dirs
