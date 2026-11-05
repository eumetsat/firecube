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

"""Create-only and sha256-verified file helpers of the content-addressed stores under .firecube/.

The metadata snapshot store, the release records and the inventory manifests
share these helpers. A file named by the sha256 hex of its bytes is created
once and read back only after its bytes are checked against that name.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import re
from typing import Final, cast

from firecube.core.errors import ControlPlaneCorruptionError, ManifestError
from firecube.core.filesystem.protocol import AtomicPublish, StorageFilesystemFull
from firecube.core.storage.uri import StorageUri

log = logging.getLogger(__name__)

HEX_ID: Final = re.compile(r"[0-9a-f]{64}")
"""A content id: the sha256 hex of a file's bytes, 64 lowercase hex digits."""


def sha256_hex(data: bytes) -> str:
    """Return the sha256 hex of *data*."""
    return hashlib.sha256(data).hexdigest()


def require_id(value: str, *, what: str) -> str:
    """Return *value* when it is a content id; raise `ManifestError` naming *what* otherwise."""
    if not isinstance(value, str) or not HEX_ID.fullmatch(value):
        raise ManifestError(f"{value!r} is not a {what} id: expected 64 lowercase hex digits")
    return value


def makedirs(fs: StorageFilesystemFull, uri: StorageUri) -> None:
    """Create the directory *uri*; a failure is logged, and the write that follows reports it."""
    try:
        fs.makedirs(uri, exist_ok=True)
    except Exception:
        # The write that follows reports a real failure.
        log.debug("makedirs failed for %s (non-fatal)", uri)


def create(fs: StorageFilesystemFull, uri: StorageUri, data: bytes) -> None:
    """Create *uri* with *data* once; an existing object holds the same content-addressed bytes."""
    makedirs(fs, uri.parent())
    with contextlib.suppress(FileExistsError):
        fs.atomic_writer.write_atomic(uri, data)


def publish(fs: StorageFilesystemFull, uri: StorageUri, data: bytes) -> None:
    """Replace *uri* so a concurrent reader sees the old or the new bytes whole."""
    makedirs(fs, uri.parent())
    if AtomicPublish not in fs.capabilities():
        fs.atomic_writer.replace_atomic(uri, data)
        return

    def write(target: StorageUri) -> None:
        with fs.open(target, "wb") as handle:
            handle.write(data)

    cast(AtomicPublish, fs).publish_atomic(uri, write)


def read_verified(fs: StorageFilesystemFull, uri: StorageUri, expected_hex: str) -> bytes:
    """Read *uri* and refuse bytes whose sha256 is not *expected_hex*."""
    data = fs.read_bytes(uri)
    actual = sha256_hex(data)
    if actual != expected_hex:
        raise ControlPlaneCorruptionError(
            f"{uri.to_str()} holds bytes with sha256 {actual}, not {expected_hex}"
        )
    return data
