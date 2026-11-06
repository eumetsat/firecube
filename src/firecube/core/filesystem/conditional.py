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

"""Conditional writes of small local files, shared by both storage drivers.

The ``ConditionalWrite`` and ``CompareDelete`` capabilities of a local (``file``)
target delegate here, as ``AtomicPublish`` delegates to ``publication``.

Validator: the local validator of a file is ``"sha256:"`` followed by the
lowercase hex SHA-256 digest of its bytes. Equal bytes give equal validators,
as an S3 ETag of a single-part PUT does.

Guard: every compare runs under the guard of the file's directory. The guard is
the permanent file ``.firecube-claims.lock`` in that directory, held with
``fcntl.flock(LOCK_EX)``, inside a process-wide ``threading.Lock`` keyed by the
guard's resolved path. No code here removes the guard file, and both drivers
leave it out of ``ls`` and ``find``.

Writes: a create or replace writes a reserved publication temporary beside the
file, fsyncs it, and links or renames it onto the file. A reader therefore sees
the old or the new bytes in full. ``read_snapshot`` takes no guard: one read of
one published file gives bytes and validator of one generation.

Limits: the guard excludes only writers that take it. A writer that writes the
file in place or removes it without the guard is not excluded. A flock on NFS is
not trusted, so an NFS target is not supported.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import stat
import threading
from collections.abc import Iterator

from firecube.core.filesystem import publication
from firecube.core.filesystem.protocol import PreconditionFailed
from firecube.core.storage.uri import StorageUri

GUARD_FILE_NAME = ".firecube-claims.lock"
"""Name of the permanent guard file in each directory that holds conditionally written files."""

_VALIDATOR_PREFIX = "sha256:"

_table_lock = threading.Lock()
_guard_locks: dict[str, threading.Lock] = {}


def is_guard_file(path: str) -> bool:
    """Return True when the last segment of ``path`` names the guard file."""
    return path.rstrip("/").rsplit("/", 1)[-1] == GUARD_FILE_NAME


def validator_of(data: bytes) -> str:
    """Return the local validator of ``data``."""
    return _VALIDATOR_PREFIX + hashlib.sha256(data).hexdigest()


def read_snapshot(uri: StorageUri) -> tuple[bytes, str]:
    """Read the local file of ``uri`` once and return its bytes and their validator.

    Raises:
        FileNotFoundError: No file exists at ``uri``.
    """
    with open(_local_path(uri), "rb") as handle:
        data = handle.read()
    return data, validator_of(data)


def create_if_absent(uri: StorageUri, data: bytes) -> str:
    """Create the local file of ``uri`` with ``data`` when none exists, and return its validator.

    The call creates the parent directory when it is missing.

    Raises:
        FileExistsError: A file already exists at ``uri``.
    """
    path = _local_path(uri)
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    with _hold_guard(directory, uri):
        tmp = _write_temporary(directory, data, mode_from=None)
        try:
            # A link fails when the name exists, so a writer outside the guard
            # that created the file meanwhile is not overwritten.
            os.link(tmp, path)
        except FileExistsError as exc:
            raise FileExistsError(uri.to_str()) from exc
        finally:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
    return validator_of(data)


def compare_replace(uri: StorageUri, validator: str, data: bytes) -> str:
    """Replace the local file of ``uri`` with ``data`` while its validator is ``validator``.

    Returns the validator of ``data``. The call never creates a file.
    The new file keeps the permission bits of the old one.

    Raises:
        PreconditionFailed: The file is missing or its validator differs.
    """
    path = _local_path(uri)
    directory = os.path.dirname(path) or "."
    with _hold_guard(directory, uri):
        current_mode = _require_current(uri, validator)
        tmp = _write_temporary(directory, data, mode_from=current_mode)
        try:
            os.replace(tmp, path)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
    return validator_of(data)


def compare_delete(uri: StorageUri, validator: str) -> None:
    """Delete the local file of ``uri`` while its validator is ``validator``.

    Raises:
        PreconditionFailed: The file is missing or its validator differs.
    """
    path = _local_path(uri)
    with _hold_guard(os.path.dirname(path) or ".", uri):
        _require_current(uri, validator)
        try:
            os.unlink(path)
        except FileNotFoundError as exc:
            raise PreconditionFailed(uri.to_str(), "the file vanished under the guard") from exc


def _local_path(uri: StorageUri) -> str:
    """Return the path of a ``file`` URI; raise ``ValueError`` for any other protocol."""
    if uri.protocol != "file":
        raise ValueError(f"local conditional writes need a file URI, got {uri.to_str()}")
    return uri.path


@contextlib.contextmanager
def _hold_guard(directory: str, uri: StorageUri) -> Iterator[None]:
    """Hold the process lock and the flock of the guard of ``directory``.

    Raises:
        PreconditionFailed: ``directory`` does not exist, so no file of ``uri`` exists.
    """
    guard_path = os.path.join(os.path.realpath(directory), GUARD_FILE_NAME)
    with _process_lock(guard_path):
        try:
            fd = os.open(guard_path, os.O_RDONLY | os.O_CREAT | os.O_CLOEXEC, 0o644)
        except FileNotFoundError as exc:
            raise PreconditionFailed(uri.to_str(), "its directory does not exist") from exc
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            # Closing the only descriptor of this open file releases the flock.
            os.close(fd)


def _process_lock(guard_path: str) -> threading.Lock:
    """Return the process-wide lock of ``guard_path``; entries stay for the process lifetime."""
    with _table_lock:
        lock = _guard_locks.get(guard_path)
        if lock is None:
            lock = _guard_locks[guard_path] = threading.Lock()
        return lock


def _require_current(uri: StorageUri, validator: str) -> int:
    """Return the permission bits of the file of ``uri`` when its validator is ``validator``.

    Raises:
        PreconditionFailed: The file is missing or its validator differs.
    """
    try:
        with open(uri.path, "rb") as handle:
            mode = stat.S_IMODE(os.fstat(handle.fileno()).st_mode)
            current = handle.read()
    except FileNotFoundError as exc:
        raise PreconditionFailed(uri.to_str(), "the file does not exist") from exc
    if validator_of(current) != validator:
        raise PreconditionFailed(uri.to_str(), "the file changed since the validator was read")
    return mode


def _write_temporary(directory: str, data: bytes, *, mode_from: int | None) -> str:
    """Write ``data`` to a new reserved publication temporary in ``directory``, fsynced.

    The temporary is created exclusive with mode 0600, or with ``mode_from`` when given.
    """
    tmp = os.path.join(directory, publication.publication_temp_name())
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_CLOEXEC, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            if mode_from is not None:
                os.fchmod(handle.fileno(), mode_from)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    return tmp
