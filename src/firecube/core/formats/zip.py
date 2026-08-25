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


def _ensure_safe_zip_member(name: str) -> None:
    """Reject ZIP member names that can escape the extraction root."""
    normalized = name.replace("\\", "/")
    if name.startswith(("/", "\\")) or normalized.startswith("/"):
        raise ValueError(f"Unsafe ZIP member path: {name}")
    if len(name) >= 2 and name[1] == ":":
        raise ValueError(f"Unsafe ZIP member path: {name}")
    if any(segment == ".." for segment in normalized.split("/")):
        raise ValueError(f"Unsafe ZIP member path: {name}")


def _select_hdf5_member(names: list[str], requested_member: str | None) -> str:
    """Pick the single HDF5 member, or validate the explicitly requested one."""
    if requested_member is not None:
        _ensure_safe_zip_member(requested_member)
        if requested_member not in names:
            raise ValueError(f"HDF5 member {requested_member!r} not found in archive")
        return requested_member

    members = [name for name in names if name.lower().endswith(".h5")]
    if not members:
        members = [
            name for name in names if not name.endswith("/") and "HDF5" in Path(name).name.upper()
        ]
    if not members:
        raise ValueError("No HDF5 member found in archive")
    if len(members) > 1:
        raise ValueError(
            "Multiple HDF5 members found; pass member explicitly: " + ", ".join(members)
        )
    return members[0]


def extract_hdf5_from_zip(
    zip_path: Path,
    dest: Path,
    *,
    member: str | None = None,
    logger: logging.Logger | None = None,
) -> Path | None:
    """Extract an HDF5-like member from a ZIP archive into ``dest``.

    Args:
        zip_path: Path to the ZIP archive.
        dest: Directory where the selected member is written.
        member: Optional explicit member name. Required when multiple HDF5
            candidates are present.
        logger: Optional logger for diagnostics.

    Returns:
        Path to the extracted HDF5 member, or ``None`` when ``zip_path`` is not a
        valid ZIP archive.

    Raises:
        ValueError: If member names are unsafe, no HDF5 candidate exists, the
            explicit member is absent, or multiple candidates require
            disambiguation.

    Examples:
        Extract a single HDF5 member:

        >>> extract_hdf5_from_zip(Path("product.zip"), Path("work"))

        Disambiguate archives that contain multiple HDF5 files:

        >>> extract_hdf5_from_zip(Path("product.zip"), Path("work"), member="data/a.h5")
    """
    logger = logger or log
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            for name in names:
                _ensure_safe_zip_member(name)
            selected_member = _select_hdf5_member(names, member)

            # Stream optimization: read to memory then write (often faster than extract()).
            try:
                hdf5_data = zf.read(selected_member)
                extracted_path = dest / selected_member
                extracted_path.parent.mkdir(parents=True, exist_ok=True)
                with extracted_path.open("wb") as handle:
                    handle.write(hdf5_data)
                logger.debug(
                    "Streamed %s bytes from %s/%s",
                    f"{len(hdf5_data):,}",
                    zip_path.name,
                    selected_member,
                )
                return extracted_path
            except Exception as stream_exc:
                logger.debug(
                    "Stream extraction failed for %s (%s); falling back to ZipFile.extract()",
                    zip_path,
                    stream_exc,
                )
                extracted_path = Path(zf.extract(selected_member, dest))
                return extracted_path
    except zipfile.BadZipFile:
        logger.warning("Invalid ZIP archive: %s", zip_path)
        return None


def stream_hdf5_from_zip(
    zip_path: Path, *, member: str | None = None, logger: logging.Logger | None = None
) -> bytes | None:
    """Stream HDF5 content directly from ZIP to memory.

    Args:
        zip_path: Path to the ZIP archive.
        member: Optional explicit member name. Required when multiple HDF5
            candidates are present.
        logger: Optional logger for diagnostics.

    Returns:
        The selected HDF5 member bytes, or ``None`` when ``zip_path`` is not a
        valid ZIP archive.

    Raises:
        ValueError: If member names are unsafe, no HDF5 candidate exists, the
            explicit member is absent, or multiple candidates require
            disambiguation.

    Examples:
        Stream a single HDF5 member without writing files:

        >>> stream_hdf5_from_zip(Path("product.zip"))

        Disambiguate archives that contain multiple HDF5 files:

        >>> stream_hdf5_from_zip(Path("product.zip"), member="data/a.h5")
    """
    logger = logger or log
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            for name in names:
                _ensure_safe_zip_member(name)
            selected_member = _select_hdf5_member(names, member)
            logger.debug("Streaming HDF5 data from %s/%s", zip_path.name, selected_member)
            return zf.read(selected_member)
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
    """Extract multiple ZIP archives concurrently into per-archive temp directories.

    Runs ``extract_fn`` for each archive on a thread pool. Archives that fail to
    extract or contain no matching member are logged and skipped; they do not
    abort the batch. Two or fewer archives are extracted serially.

    Args:
        zip_files: ZIP archive paths to extract.
        tempdir_factory: Zero-argument callable returning a fresh
            ``tempfile.TemporaryDirectory`` for each archive.
        extract_fn: Callable ``(zip_path, dest_dir) -> extracted_path | None``
            applied to each archive. Defaults to :func:`extract_hdf5_from_zip`.
        max_workers: Upper bound on concurrent extractions. Capped at
            ``len(zip_files)``.
        record_temp_dirs: When true and ``temp_dir_map`` is given, record which
            temporary directory produced each extracted file.
        temp_dir_map: Mapping filled with ``str(resolved_path) ->
            TemporaryDirectory`` entries when ``record_temp_dirs`` is true.
        logger: Optional logger for diagnostics.

    Returns:
        A ``(resolved_paths, temp_dirs)`` pair. ``resolved_paths`` holds one
        extracted file path per successful archive; ``temp_dirs`` holds the
        matching temporary directories, which the caller must keep alive while
        the extracted files are in use and clean up afterwards.

    Examples:
        Extract a batch of archives and clean up afterwards:

            >>> paths, tmp_dirs = extract_zip_files_parallel(
            ...     [Path("a.zip"), Path("b.zip")],
            ...     tempdir_factory=tempfile.TemporaryDirectory,
            ... )
            >>> for tmp in tmp_dirs:
            ...     tmp.cleanup()
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


def extract_all_from_zip(zip_path: Path, dest: Path) -> None:
    """Extract every member of ``zip_path`` into ``dest``, rejecting unsafe names.

    Args:
        zip_path: Path to the zip archive to extract.
        dest: Destination directory. Created if it does not exist.

    Returns:
        None: The function returns nothing.

    Raises:
        ValueError: If any member name would escape ``dest``.
        zipfile.BadZipFile: If ``zip_path`` is not a valid zip archive.

    Examples:
        Extract every member of a small archive:

            >>> from pathlib import Path
            >>> extract_all_from_zip(Path("archive.zip"), Path("dest_dir"))
    """
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            _ensure_safe_zip_member(name)
        zf.extractall(dest)
