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

import fnmatch
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from firecube.core.filesystem.ops import open_source_filesystem
from firecube.core.formats._input_filters import split_input_filters
from firecube.core.formats.hdf5 import looks_like_hdf5
from firecube.core.uris import is_remote_target, parse_uri

# All file extensions recognised by the firecube ecosystem. See
# DEFAULT_INCLUDE_SUFFIXES for the defaults used by ``discover_input_files``;
# other formats must be requested explicitly via *include_suffixes*.
KNOWN_EXTENSIONS: frozenset[str] = frozenset({".zip", ".h5", ".nc", ".nc4", ".hdf", ".he5", ".tgm"})

DEFAULT_INCLUDE_SUFFIXES: tuple[str, ...] = (".zip", ".h5", ".nc", ".nc4", ".hdf", ".he5")


def _path_matches_any_glob(
    path: str, patterns: Iterable[str], *, candidates: Iterable[str]
) -> bool:
    """Return True when any candidate path matches any glob pattern."""
    normalized_candidates = {path, *candidates}
    return any(
        fnmatch.fnmatchcase(candidate, pattern)
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
    root: str,
) -> list[str]:
    """Filter discovered filesystem paths down to candidate input files."""
    remote_source = is_remote_target(source_uri)
    suffixes = {s.lower() for s in include_suffixes}
    preferred_patterns, negated_patterns = split_input_filters(preferred_globs)
    excluded_patterns = (*tuple(exclude or ()), *negated_patterns)

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
        if preferred_patterns:
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
    include_suffixes: Sequence[str] = DEFAULT_INCLUDE_SUFFIXES,
    preferred_globs: Iterable[str] | None = None,
    recursive: bool = True,
    sniff_hdf5: bool = True,
    exclude: Iterable[str] | None = None,
) -> list[str]:
    """Discover input files from a local path or remote URI.

    Selection is intentionally conservative and format-agnostic:

    - Accept files matching ``include_suffixes``.
    - Optionally accept local files with unselected suffixes (including no
      suffix) that look like HDF5.
    - Add files matched by positive ``preferred_globs`` entries. These add
      to the suffix selection; they do not replace it.
    - Entries starting with ``!`` exclude matches. They and ``exclude`` win
      over every inclusion rule, regardless of order, before content sniffing.
      For example, ``["!*", "*.csv"]`` selects nothing.

    Glob patterns in ``preferred_globs`` and ``exclude`` are matched against
    the file's base name, its path relative to ``source``, and its full
    path or URI, so both ``"*.nc4"`` and ``"subdir/*.nc4"`` are usable.
    Matching is case-sensitive on every platform, including for explicit
    file sources. Wildcards follow ``fnmatch`` grammar: ``*`` can cross
    directory separators, dotfiles are ordinary names, and ``**`` has no
    special meaning. A leading backslash before ``!`` escapes a literal
    positive filename: pass ``r"\\!measurement.nc"``. Strip exclusion markers
    once, so ``!!measurement.nc`` excludes the literal ``!measurement.nc``.
    Spaces within entries are preserved. Filters do not prune directory
    traversal or supply readers for newly selected file types.

    Args:
        source: Discovery root: a local path or a remote URI such as
            ``s3://bucket/prefix``.
        storage_config: Storage settings used to reach a remote ``source``.
        include_suffixes: File suffixes accepted, case-insensitively. Defaults to
            ``.zip``, ``.h5``, ``.nc``, ``.nc4``, ``.hdf``, and ``.he5``.
        preferred_globs: Filename filters; positive entries add matches and
            ``!`` entries exclude them. Empty strings and bare ``!`` are
            invalid. ``None`` or an empty iterable retains the suffix and
            content selection. Plugin discovery hooks can pass
            ``self.engine_config.input_filters`` here.
        recursive: Search below ``source``; when ``False``, only entries
            directly in ``source`` are returned.
        sniff_hdf5: Accept files whose content looks like HDF5 when their suffix
            is absent or not in ``include_suffixes``. Local sources only.
        exclude: Additional case-sensitive exclusion globs. These entries
            are used literally as globs; no leading-marker parsing is applied.

    Returns:
        URI/path strings (for example ``/tmp/data/file.nc`` or
        ``s3://bucket/prefix/file.nc``), sorted for deterministic batching.

    Raises:
        ValueError: If ``source`` cannot be opened or listed, or a filter
            is empty, a bare ``!``, or not a string.
    """
    source_uri = str(source)
    is_remote = is_remote_target(source_uri)

    try:
        fs, uri_obj = open_source_filesystem(source_uri, storage_config)
        root = uri_obj.to_str()
    except Exception as exc:
        raise ValueError(f"Cannot open source location {source_uri!r}: {exc}") from exc

    single_object_uris = None
    try:
        if is_remote:
            try:
                info = fs.info(uri_obj)
                if info and info.get("type") == "file":
                    single_object_uris = [uri_obj]
            except (FileNotFoundError, KeyError, AttributeError):
                pass

        all_paths_uris = single_object_uris if single_object_uris is not None else fs.find(uri_obj)
        all_paths = [path.to_str() for path in all_paths_uris]
    except Exception as exc:
        raise ValueError(f"Cannot list source location {source_uri!r}: {exc}") from exc

    if not is_remote:
        root = parse_uri(root)["path"] if "://" in root else root
        all_paths = [
            parse_uri(path)["path"] if path.startswith("file://") else path for path in all_paths
        ]

    return _filter_discovered_paths(
        all_paths,
        source_uri=source_uri,
        include_suffixes=include_suffixes,
        preferred_globs=preferred_globs,
        recursive=recursive,
        sniff_hdf5=sniff_hdf5 and not is_remote,
        exclude=exclude,
        root=root,
    )
