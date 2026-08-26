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
import shutil
import zipfile
from collections.abc import Callable, Sequence
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


def _extract_one_zip(zip_path: Path, dest: Path) -> None:
    """Extract every member of ``zip_path`` into ``dest``, rejecting unsafe names."""
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            _ensure_safe_zip_member(name)
        zf.extractall(dest)


def extract_all_from_zips(
    zip_paths: Sequence[Path],
    dest_dir_for: Callable[[Path], Path],
    *,
    workers: int = 1,
) -> tuple[dict[Path, Path], dict[Path, str]]:
    """Extract every member of each ZIP archive, optionally in parallel.

    Destination directories are resolved by calling ``dest_dir_for`` once per
    archive, serially and in input order, before any extraction starts, so the
    callable needs no locking. Each archive is then fully extracted into its
    directory; member names that could escape it (``..`` segments, absolute
    paths, Windows drive prefixes) are rejected before anything is written.

    A failing archive never raises and never aborts the batch: its partially
    extracted directory is removed and the failure is reported in the result.
    Callers MUST check the returned failures mapping — an unsafe member name
    or a corrupt archive is reported there, not as an exception. ``workers=1``
    extracts serially; higher values extract concurrently with identical
    failure semantics. Extraction is disk-bound, so ``workers`` composes with,
    and is independent of, the engine's ``pipeline_workers`` option; a plugin
    running several pipeline workers multiplies the two, so keep ``workers``
    modest.

    Args:
        zip_paths: Archives to extract.
        dest_dir_for: Callable mapping each archive path to its destination
            directory. Called once per archive before extraction begins.
        workers: Upper bound on concurrent extractions, capped at the number
            of archives. Defaults to serial extraction. Ingestion plugins
            conventionally pass the engine's ``extract_workers`` option here
            so operators control it with ``--option extract_workers=N``.

    Returns:
        An ``(extracted, failures)`` pair: ``extracted`` maps each
        successfully extracted archive to its destination directory, and
        ``failures`` maps each failed archive to its error message. Every
        input path appears in exactly one of the two mappings.

    Raises:
        ValueError: If ``workers`` is less than 1.

    Examples:
        Extract a batch of archives next to each archive:

            >>> from pathlib import Path
            >>> extracted, failures = extract_all_from_zips(
            ...     [Path("a.zip"), Path("b.zip")],
            ...     lambda zip_path: zip_path.parent / zip_path.stem,
            ...     workers=4,
            ... )
            >>> if failures:
            ...     raise RuntimeError(f"failed archives: {failures}")
    """
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")

    paths = [Path(p) for p in zip_paths]
    dests = {path: Path(dest_dir_for(path)) for path in paths}
    extracted: dict[Path, Path] = {}
    failures: dict[Path, str] = {}

    def _extract(path: Path) -> None:
        dest = dests[path]
        try:
            _extract_one_zip(path, dest)
        except Exception as exc:
            shutil.rmtree(dest, ignore_errors=True)
            log.warning("ZIP extraction failed for %s: %s", path, exc)
            failures[path] = str(exc)
        else:
            extracted[path] = dest

    pool_size = min(int(workers), len(paths)) if paths else 0
    if pool_size <= 1:
        for path in paths:
            _extract(path)
        return extracted, failures

    with concurrent.futures.ThreadPoolExecutor(max_workers=pool_size) as pool:
        for future in [pool.submit(_extract, path) for path in paths]:
            future.result()
    return extracted, failures
