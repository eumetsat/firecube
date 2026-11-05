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

"""Read and check the `firecube-archive.json` marker of a ZIP archive.

The archive module and the chain restore both import this module. It imports neither.
"""

from __future__ import annotations

import json
import os
import re
import zipfile
from collections.abc import Mapping
from typing import Any, Final

from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME

MARKER_ENTRY = "firecube-archive.json"
"""Reserved root entry that describes the archive; it is never a product key."""

MODE_FULL: Final = "full"
MODE_DELTA: Final = "delta"

_MARKER_KEYS: Final = frozenset(
    {
        "version",
        "mode",
        "base",
        "base_uri",
        "base_digest",
        "inventory",
        "metadata_manifest",
        "deleted",
    }
)
_HEX_ID: Final = re.compile(r"[0-9a-f]{64}")
_DIGEST: Final = re.compile(r"sha256:[0-9a-f]{64}")
_CLAIMS_PREFIX: Final = f"{CONTROL_DIRNAME}/{CLAIMS_DIRNAME}/"


def read_zip_marker(path: str) -> Mapping[str, Any] | None:
    """Return the parsed ``firecube-archive.json`` marker of a zip archive, or None.

    Args:
        path: Local path or ``file://`` URI of the archive.

    Raises:
        FileNotFoundError: If ``path`` is not a file.
        ValueError: If the marker entry cannot be read or is not a JSON object.
    """
    archive_path = path.removeprefix("file://")
    if not os.path.isfile(archive_path):
        raise FileNotFoundError(f"Archive not found: {path}")
    with zipfile.ZipFile(archive_path) as zf:
        if MARKER_ENTRY not in zf.namelist():
            return None
        try:
            raw = zf.read(MARKER_ENTRY)
        except zipfile.BadZipFile as exc:
            raise ValueError(
                f"Entry {MARKER_ENTRY!r} in archive {path} cannot be read: {exc}"
            ) from exc
    try:
        marker = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"Entry {MARKER_ENTRY!r} in archive {path} is not valid JSON") from exc
    if not isinstance(marker, dict):
        raise ValueError(f"Entry {MARKER_ENTRY!r} in archive {path} is not a JSON object")
    return marker


def deleted_key_problem(key: object) -> str | None:
    """Return why `key` cannot be a deleted key of a delta marker, or `None` when it can.

    A chain restore refuses a marker that lists such a key, so a release never writes one.
    """
    if not isinstance(key, str):
        return "is not a string"
    if not key:
        return "is empty"
    if key.startswith("/"):
        return "is absolute"
    if "\\" in key:
        return "holds a backslash"
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in key):
        return "holds a control character"
    if any(segment in ("", ".", "..") for segment in key.split("/")):
        return "has an empty, '.' or '..' segment"
    if key.startswith(_CLAIMS_PREFIX):
        return "names a claim file"
    if key == MARKER_ENTRY:
        return "names the archive marker"
    return None


def marker_problem(marker: Mapping[str, Any]) -> str | None:
    """Return why `marker` is not a valid archive marker, or `None` when it is.

    A chain restore and `validate_zip` both apply this check.
    """
    if set(marker) != _MARKER_KEYS:
        missing = sorted(_MARKER_KEYS.difference(marker))
        extra = sorted(set(marker).difference(_MARKER_KEYS))
        return f"its keys differ from the eight a marker holds (missing {missing}, extra {extra})"
    version, mode = marker["version"], marker["mode"]
    if not isinstance(version, str) or not version:
        return "its version is not a non-empty string"
    if mode not in (MODE_FULL, MODE_DELTA):
        return f"its mode is {mode!r}, not 'full' or 'delta'"
    for name in ("inventory", "metadata_manifest"):
        value = marker[name]
        if not isinstance(value, str) or not _HEX_ID.fullmatch(value):
            return f"its {name} is not a 64-digit lowercase hex id"
    deleted = marker["deleted"]
    if not isinstance(deleted, list):
        return "its deleted entry is not a list"
    if mode == MODE_FULL:
        if any(marker[name] is not None for name in ("base", "base_uri", "base_digest")):
            return "it is full but names a base"
        if deleted:
            return "it is full but lists deleted keys"
        return None
    base, base_uri, base_digest = marker["base"], marker["base_uri"], marker["base_digest"]
    if not isinstance(base, str) or not base:
        return "it is a delta whose base is not a non-empty string"
    if not isinstance(base_uri, str) or not base_uri:
        return "it is a delta whose base_uri is not a non-empty string"
    if not isinstance(base_digest, str) or not _DIGEST.fullmatch(base_digest):
        return "it is a delta whose base_digest is not a sha256:<hex> digest"
    for key in deleted:
        if (problem := deleted_key_problem(key)) is not None:
            return f"its deleted key {key!r} {problem}"
    if len(set(deleted)) != len(deleted):
        return "it lists a deleted key twice"
    return None
