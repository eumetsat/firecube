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

"""URI/path helpers used across core, CLI, and plugins.

This module centralizes protocol detection and local path normalization so we
don't duplicate `fsspec.utils.infer_storage_options(...)` logic in multiple
places (storage, filesystem, CLI).
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

from fsspec.utils import infer_storage_options

if TYPE_CHECKING:
    from firecube.core.storage.uri import StorageUri

_log = logging.getLogger(__name__)
_URI_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+\-.]*[:]{0,1}[/]{1,2}")
_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^[a-zA-Z]:[/\\]")
_MALFORMED_URI_SCHEMELESS_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9+\-.]*)//")
_COMMON_URI_SCHEMES = {
    "abfs",
    "adl",
    "az",
    "file",
    "ftp",
    "gcs",
    "gs",
    "hdfs",
    "http",
    "https",
    "memory",
    "s3",
    "sftp",
}


def _looks_like_uri(target: str) -> bool:
    """Return True if target appears to be an intentional URI scheme (even if malformed)."""
    return bool(_URI_SCHEME_RE.match(target))


def _looks_like_windows_absolute_path(target: str) -> bool:
    """Return True for Windows drive-letter absolute paths on any host OS."""
    return bool(_WINDOWS_ABSOLUTE_PATH_RE.match(target))


def _is_malformed_uri_candidate(target: str, protocol: str, path: str) -> bool:
    """Return True when a URI-like target resolved only as a plain local path."""
    if protocol != "file" or path != target or _looks_like_windows_absolute_path(target):
        return False

    if target.startswith("://") or "://" in target:
        return True

    if re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*:/", target):
        return True

    schemeless_match = _MALFORMED_URI_SCHEMELESS_RE.match(target)
    return bool(schemeless_match and schemeless_match.group(1).lower() in _COMMON_URI_SCHEMES)


def parse_uri(target: str) -> dict[str, str]:
    """Parse a URI/path into a minimal fsspec-like structure.

    Returns a dict with:
      - protocol: "file", "s3", ...
      - path: protocol-specific path (e.g. "bucket/prefix/key" for s3, "/abs/path" for file)
    """
    target = str(target or "")
    if target.startswith("file://"):
        # Treat file:// as local. fsspec parsing will give us an absolute path if possible.
        try:
            parsed = infer_storage_options(target)
        except Exception:
            if _looks_like_uri(target):
                raise ValueError(f"Malformed URI: {target!r}") from None
            return {"protocol": "file", "path": target.replace("file://", "", 1)}
    else:
        try:
            parsed = infer_storage_options(target)
        except Exception:
            if _looks_like_uri(target):
                raise ValueError(f"Malformed URI: {target!r}") from None
            return {"protocol": "file", "path": target}

    protocol = parsed.get("protocol") or "file"
    if isinstance(protocol, (list, tuple)):
        protocol = protocol[0] if protocol else "file"
    path = str(parsed.get("path") or "")
    if _is_malformed_uri_candidate(target, str(protocol), path):
        raise ValueError(f"Malformed URI: {target!r}")
    _log.debug("parse_uri(%r) -> protocol=%s path=%s", target, protocol, path)
    return {"protocol": str(protocol), "path": path}


def infer_target_protocol(target: str) -> str:
    """Infer the fsspec protocol for a target string.

    Returns "file" for local paths and file:// URIs.
    """
    return parse_uri(target)["protocol"]


def is_remote_target(target: str) -> bool:
    """Return True when `target` is a non-local URI (e.g. s3://...)."""
    protocol = infer_target_protocol(target)
    return bool(protocol and protocol != "file")


def local_path_from_target(target: str) -> Path:
    """Resolve a target string into an absolute local path.

    Supports relative paths and file:// URIs.
    """
    target = str(target or "")
    if target.startswith("file://"):
        from firecube.core.storage.uri import StorageUri

        uri = StorageUri.parse(target)
        return Path(uri.path)
    p = Path(target)  # firecube: STORAGE-URI — intentional: this IS the local-path adapter
    return p if p.is_absolute() else (Path.cwd() / p)


def storage_uri_from_target(target: str) -> StorageUri:
    from firecube.core.storage.uri import StorageUri  # local import to avoid circular

    if is_remote_target(target) or str(target).startswith("file://"):
        return StorageUri.parse(target)
    return StorageUri.from_local_path(local_path_from_target(target))


def is_windows_absolute_path(target: str) -> bool:
    """Best-effort check for Windows drive-letter absolute paths."""
    target = str(target or "")
    return bool(os.name == "nt" and ":" in target)


def parse_target(target: StorageUri | str) -> StorageUri:
    """Parse a target string tolerant of bare local paths.

    Accepts:
      - StorageUri: returned as-is
      - file:///… URI: parsed strictly via StorageUri.parse
      - s3:// gs:// memory:// URIs: parsed strictly via StorageUri.parse
      - Bare absolute local path (/abs/path): coerced via StorageUri.from_local_path
      - Bare relative path: rejected (use file:/// or absolute path)

    This is the tolerant boundary helper for public APIs that accept
    user-supplied target strings (CLI args, plugin entry points,
    legacy-shaped function signatures). Internal code should use
    StorageUri.parse directly (strict).
    """
    from firecube.core.storage.uri import StorageUri  # local import to avoid circular

    if isinstance(target, StorageUri):
        return target
    s = str(target)
    if "://" in s:
        return StorageUri.parse(s)
    if s.startswith("/"):
        return StorageUri.from_local_path(s)
    raise ValueError(f"target must be an absolute path or URI: {target!r}")
