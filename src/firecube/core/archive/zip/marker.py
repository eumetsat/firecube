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

The archive module and the backup chain restore both import this module. It imports neither.
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
MODE_INCREMENTAL: Final = "incremental"

_PREVIOUS_KEYS: Final = ("previous_release", "previous_backup_uri", "previous_backup_digest")
_MARKER_KEYS: Final = frozenset(
    {
        "version",
        "mode",
        "previous_release",
        "previous_backup_uri",
        "previous_backup_digest",
        "inventory",
        "metadata_manifest",
        "deleted_keys",
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
    """Return why `key` cannot be a deleted key of an incremental backup, or `None` when it can.

    A backup chain restore refuses a marker that lists such a key, so a release never writes one.
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

    A backup chain restore and `validate_zip` both apply this check.
    """
    if set(marker) != _MARKER_KEYS:
        missing = sorted(_MARKER_KEYS.difference(marker))
        extra = sorted(set(marker).difference(_MARKER_KEYS))
        return f"its keys differ from the eight a marker holds (missing {missing}, extra {extra})"
    version, mode = marker["version"], marker["mode"]
    if not isinstance(version, str) or not version:
        return "its version is not a non-empty string"
    if mode not in (MODE_FULL, MODE_INCREMENTAL):
        return f"its mode is {mode!r}, not {MODE_FULL!r} or {MODE_INCREMENTAL!r}"
    for name in ("inventory", "metadata_manifest"):
        value = marker[name]
        if not isinstance(value, str) or not _HEX_ID.fullmatch(value):
            return f"its {name} is not a 64-digit lowercase hex id"
    deleted_keys = marker["deleted_keys"]
    if not isinstance(deleted_keys, list):
        return "its deleted_keys entry is not a list"
    if mode == MODE_FULL:
        if any(marker[name] is not None for name in _PREVIOUS_KEYS):
            return "it is a full backup but names a previous release"
        if deleted_keys:
            return "it is a full backup but lists deleted keys"
        return None
    previous_release = marker["previous_release"]
    previous_backup_uri = marker["previous_backup_uri"]
    previous_backup_digest = marker["previous_backup_digest"]
    if not isinstance(previous_release, str) or not previous_release:
        return "it is an incremental backup whose previous_release is not a non-empty string"
    if not isinstance(previous_backup_uri, str) or not previous_backup_uri:
        return "it is an incremental backup whose previous_backup_uri is not a non-empty string"
    if not isinstance(previous_backup_digest, str) or not _DIGEST.fullmatch(previous_backup_digest):
        return (
            "it is an incremental backup whose previous_backup_digest is not a sha256:<hex> digest"
        )
    for key in deleted_keys:
        if (problem := deleted_key_problem(key)) is not None:
            return f"its deleted key {key!r} {problem}"
    if len(set(deleted_keys)) != len(deleted_keys):
        return "it lists a deleted key twice"
    return None
