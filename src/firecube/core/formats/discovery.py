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

"""Input file discovery helpers for ingestion plugins.

Format-agnostic discovery utilities that walk a local path or remote URI
and return candidate input file URIs (ZIP, HDF5, NetCDF, ...).
"""

from __future__ import annotations

import contextlib
import fnmatch
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from firecube.core.formats.hdf5 import looks_like_hdf5
from firecube.core.uris import is_remote_target, parse_uri

# All file extensions recognised by the firecube ecosystem.
# The first three are the defaults for ``discover_input_files``; additional
# formats (e.g. ``.tgm``) must be requested explicitly via *include_suffixes*.
KNOWN_EXTENSIONS: frozenset[str] = frozenset({".zip", ".h5", ".nc", ".tgm"})


def _path_matches_any_glob(
    path: str, patterns: Iterable[str], *, candidates: Iterable[str]
) -> bool:
    """Return True when any candidate path matches any glob pattern."""
    normalized_candidates = {path, *candidates}
    return any(
        fnmatch.fnmatch(candidate, pattern)
        for pattern in patterns
        for candidate in normalized_candidates
    )


def _filter_discovered_paths(
    paths: Iterable[str],
    *,
    source_uri: str,
    include_suffixes: Sequence[str],
    preferred_globs: Iterable[str] | None,
    recursive: bool,
    sniff_hdf5: bool,
    exclude: Iterable[str] | None,
    fs: Any,
    root: str,
) -> list[str]:
    """Filter discovered filesystem paths down to candidate input files."""
    remote_source = is_remote_target(source_uri)
    suffixes = {s.lower() for s in include_suffixes}
    preferred_patterns = tuple(preferred_globs or ())
    excluded_patterns = tuple(exclude or ())
    source_is_dir = False
    with contextlib.suppress(Exception):
        source_is_dir = bool(fs.isdir(root))

    root_prefix = root.rstrip("/")
    resolved: list[str] = []

    for path in paths:
        normalized = str(path)
        path_for_match = parse_uri(normalized)["path"] if remote_source else normalized
        basename = Path(path_for_match).name
        relative = path_for_match

        if root_prefix and path_for_match.startswith(f"{root_prefix}/"):
            relative = path_for_match[len(root_prefix) + 1 :]
        elif path_for_match == root_prefix:
            relative = basename

        if not recursive and "/" in relative:
            continue

        glob_candidates = {basename, relative, normalized}

        if excluded_patterns and _path_matches_any_glob(
            normalized, excluded_patterns, candidates=glob_candidates
        ):
            continue

        suffix = Path(path_for_match).suffix.lower()
        include_by_suffix = suffix in suffixes
        include_by_sniff = False
        if sniff_hdf5 and (suffix == "" or suffix not in suffixes):
            include_by_sniff = looks_like_hdf5(Path(normalized))

        include_by_glob = False
        if preferred_patterns and source_is_dir:
            include_by_glob = _path_matches_any_glob(
                normalized, preferred_patterns, candidates=glob_candidates
            )

        if include_by_suffix or include_by_sniff or include_by_glob:
            resolved.append(normalized)

    dedup = dict.fromkeys(resolved)
    return sorted(dedup, key=lambda path: Path(path).name)


def discover_input_files(
    source: str | Path,
    *,
    storage_config: Any | None = None,
    include_suffixes: Sequence[str] = (".zip", ".h5", ".nc"),
    preferred_globs: Iterable[str] | None = None,
    recursive: bool = True,
    sniff_hdf5: bool = True,
    exclude: Iterable[str] | None = None,
) -> list[str]:
    """Discover input files from a local path or remote URI.

    Selection is intentionally conservative and format-agnostic:
      - Accept files matching `include_suffixes`
      - Optionally accept extensionless files that look like HDF5
      - Optionally add files matched by `preferred_globs` (glob patterns)

    Returns URI/path strings (for example ``/tmp/data/file.nc`` or
    ``s3://bucket/prefix/file.nc``). ``Path`` inputs are accepted for backward
    compatibility and are converted to strings internally.
    """
    from firecube.core.filesystem.ops import _open_fsspec_url
    from firecube.core.uris import is_remote_target, parse_uri

    source_uri = str(source)
    is_remote = is_remote_target(source_uri)

    try:
        fs, root = _open_fsspec_url(source_uri, storage_config=storage_config)
    except Exception as exc:
        raise ValueError(f"Cannot open source location {source_uri!r}: {exc}") from exc

    try:
        all_paths = [str(path) for path in fs.find(root)]
    except Exception as exc:
        raise ValueError(f"Cannot list source location {source_uri!r}: {exc}") from exc

    if is_remote:
        protocol = parse_uri(source_uri)["protocol"]
        all_paths = [path if "://" in path else f"{protocol}://{path}" for path in all_paths]

    return _filter_discovered_paths(
        all_paths,
        source_uri=source_uri,
        include_suffixes=include_suffixes,
        preferred_globs=preferred_globs,
        recursive=recursive,
        sniff_hdf5=sniff_hdf5 and not is_remote,
        exclude=exclude,
        fs=fs,
        root=root,
    )
