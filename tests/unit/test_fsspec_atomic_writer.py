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

"""Unit tests for fsspec atomic create-if-not-exists conflict normalization.

The :class:`AtomicWriter` contract (``protocol.py``) requires ``write_atomic`` to
raise ``FileExistsError`` on an exclusive-create conflict, regardless of backend.
Local fs signals the conflict with ``errno EEXIST``; s3fs implements ``"xb"`` as a
conditional ``PutObject`` and signals a lost race with an HTTP 412
``PreconditionFailed`` re-wrapped as ``OSError(EINVAL)`` — a different errno that
the naive check used to miss, leaking a raw ``OSError`` into the control-plane
claim layer and crashing concurrent pods. These tests pin the normalization.
"""

from __future__ import annotations

import errno
from typing import Any

import pytest

from firecube.core.filesystem.fsspec_backend import (
    FsspecAtomicWriter,
    _is_precondition_failed,
)
from firecube.core.storage.uri import StorageUri

_URI = StorageUri.parse("s3://bucket/.firecube/claims/slot_index_model__current.json")


class _FakeClientError(Exception):
    """Minimal stand-in for botocore.exceptions.ClientError (duck-typed by .response)."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__(f"{code} ({status})")
        self.response = {
            "Error": {"Code": code, "Message": "Precondition Failed"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


def _s3fs_precondition_oserror() -> OSError:
    """Mirror the real s3fs surface: OSError(EINVAL) caused by a 412 ClientError."""
    exc = OSError(errno.EINVAL, "None")
    exc.__cause__ = _FakeClientError("PreconditionFailed", 412)
    return exc


class _RaisingFs:
    """Fake fsspec fs whose exclusive-create open raises a preset exception."""

    def __init__(self, to_raise: BaseException) -> None:
        self._to_raise = to_raise
        self.opened_modes: list[str] = []

    def open(self, path: str, mode: str = "rb") -> Any:
        self.opened_modes.append(mode)
        raise self._to_raise


class _CapturingFs:
    """Fake fsspec fs that records a successful exclusive-create write."""

    def __init__(self) -> None:
        self.written: bytes | None = None

    def open(self, path: str, mode: str = "rb") -> Any:
        fs = self

        class _Handle:
            def __enter__(self) -> _Handle:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def write(self, data: bytes) -> None:
                fs.written = data

        assert mode == "xb"
        return _Handle()


@pytest.mark.unit
def test_s3fs_precondition_failed_maps_to_file_exists() -> None:
    """The wrapped s3fs 412 (OSError EINVAL + ClientError cause) -> FileExistsError."""
    writer = FsspecAtomicWriter(_RaisingFs(_s3fs_precondition_oserror()))
    with pytest.raises(FileExistsError):
        writer.write_atomic(_URI, b"{}")


@pytest.mark.unit
def test_direct_client_error_precondition_maps_to_file_exists() -> None:
    """A bare botocore-style ClientError(412) with no OSError wrapper still maps."""
    writer = FsspecAtomicWriter(_RaisingFs(_FakeClientError("PreconditionFailed", 412)))
    with pytest.raises(FileExistsError):
        writer.write_atomic(_URI, b"{}")


@pytest.mark.unit
def test_local_eexist_maps_to_file_exists() -> None:
    """Regression: local-fs errno EEXIST still normalizes to FileExistsError."""
    writer = FsspecAtomicWriter(_RaisingFs(OSError(errno.EEXIST, "File exists")))
    with pytest.raises(FileExistsError):
        writer.write_atomic(_URI, b"{}")


@pytest.mark.unit
def test_native_file_exists_error_propagates() -> None:
    """A backend that already raises FileExistsError passes through unchanged."""
    writer = FsspecAtomicWriter(_RaisingFs(FileExistsError("exists")))
    with pytest.raises(FileExistsError):
        writer.write_atomic(_URI, b"{}")


@pytest.mark.unit
def test_unrelated_error_is_not_swallowed() -> None:
    """A non-conflict error (e.g. permission denied) must NOT become FileExistsError."""
    writer = FsspecAtomicWriter(_RaisingFs(OSError(errno.EACCES, "Permission denied")))
    with pytest.raises(OSError) as excinfo:
        writer.write_atomic(_URI, b"{}")
    assert not isinstance(excinfo.value, FileExistsError)
    assert excinfo.value.errno == errno.EACCES


@pytest.mark.unit
def test_non_precondition_client_error_propagates() -> None:
    """A different S3 status (e.g. 500) is a real error, not a conflict."""
    writer = FsspecAtomicWriter(_RaisingFs(_FakeClientError("InternalError", 500)))
    with pytest.raises(_FakeClientError):
        writer.write_atomic(_URI, b"{}")


@pytest.mark.unit
def test_happy_path_writes_payload() -> None:
    """No conflict: the payload is written via exclusive-create mode."""
    fs = _CapturingFs()
    FsspecAtomicWriter(fs).write_atomic(_URI, b'{"owner":"pod-1"}')
    assert fs.written == b'{"owner":"pod-1"}'


@pytest.mark.unit
def test_is_precondition_failed_detects_via_status_code() -> None:
    """Detection also fires on HTTP 412 even if the error Code differs."""
    err = _FakeClientError("SomeOtherLabel", 412)
    assert _is_precondition_failed(err) is True


@pytest.mark.unit
def test_is_precondition_failed_false_for_plain_oserror() -> None:
    """A plain EINVAL with no botocore cause is not a precondition failure."""
    assert _is_precondition_failed(OSError(errno.EINVAL, "None")) is False


# ---------------------------------------------------------------------------
# Local filesystem: content-atomicity (no zero-length window) via temp + link.
#
# Plain ``open(path, "xb")`` on local disk is atomic only for the *name*: the
# file is created empty and filled incrementally, so a concurrent reader can
# observe a 0-byte file mid-write (surfacing as a JSON "char 0" ManifestError
# in ``ChunkManager.ensure_slot_index_model`` losers that read outside the
# claim). These tests pin the temp-file + ``os.link`` publish that closes the
# window while preserving the create-if-not-exists ``FileExistsError`` contract.
# ---------------------------------------------------------------------------


def _local_writer() -> FsspecAtomicWriter:
    import fsspec

    return FsspecAtomicWriter(fsspec.filesystem("file"))


def _local_uri(path: Any) -> StorageUri:
    return StorageUri.from_local_path(path)


@pytest.mark.unit
def test_local_write_atomic_persists_full_payload(tmp_path: Any) -> None:
    """Happy path on a real LocalFileSystem writes the complete payload."""
    target = tmp_path / "slot_index" / "current.json"
    target.parent.mkdir(parents=True)
    payload = b'{"identity_hash":"abc","model":"v1"}'

    _local_writer().write_atomic(_local_uri(target), payload)

    assert target.read_bytes() == payload


@pytest.mark.unit
def test_local_write_atomic_conflict_raises_file_exists(tmp_path: Any) -> None:
    """An existing target makes the link-based publish raise FileExistsError."""
    target = tmp_path / "current.json"
    target.write_bytes(b'{"owner":"winner"}')

    with pytest.raises(FileExistsError):
        _local_writer().write_atomic(_local_uri(target), b'{"owner":"loser"}')

    # The loser must NOT clobber the winner's record.
    assert target.read_bytes() == b'{"owner":"winner"}'


@pytest.mark.unit
def test_local_write_atomic_leaves_no_temp_files(tmp_path: Any) -> None:
    """Neither success nor conflict may leak the sibling temp file."""
    target = tmp_path / "current.json"
    writer = _local_writer()

    writer.write_atomic(_local_uri(target), b"{}")
    with pytest.raises(FileExistsError):
        writer.write_atomic(_local_uri(target), b"{}")

    leftovers = [p.name for p in tmp_path.iterdir() if p.name != "current.json"]
    assert leftovers == [], f"temp file leaked: {leftovers!r}"
