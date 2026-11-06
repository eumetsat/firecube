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

The `AtomicWriter` contract (``protocol.py``) requires ``write_atomic`` to
raise ``FileExistsError`` on an exclusive-create conflict, regardless of backend.
Local fs signals the conflict with ``errno EEXIST``; s3fs implements ``"xb"`` as a
conditional ``PutObject`` and signals a lost race with an HTTP 412
``PreconditionFailed`` re-wrapped as ``OSError(EINVAL)`` — a different errno that
the naive check used to miss, leaking a raw ``OSError`` into the control-plane
claim layer and crashing concurrent pods. These tests pin the normalization.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from firecube.core.filesystem.fsspec_backend import (
    FsspecAtomicWriter,
    _is_precondition_failed,
)
from firecube.core.storage.uri import StorageUri
from tests.helpers.rows import table_row

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
def test_exclusive_create_conflict_maps_to_file_exists(subtests: Any, tmp_path: Path) -> None:
    """Every backend's exclusive-create conflict surfaces as FileExistsError."""
    rows: tuple[tuple[str, Callable[[], BaseException]], ...] = (
        # the wrapped s3fs 412 (OSError EINVAL + ClientError cause)
        ("test_s3fs_precondition_failed_maps_to_file_exists", _s3fs_precondition_oserror),
        # a bare botocore-style ClientError(412) with no OSError wrapper
        (
            "test_direct_client_error_precondition_maps_to_file_exists",
            lambda: _FakeClientError("PreconditionFailed", 412),
        ),
        # regression: local-fs errno EEXIST
        ("test_local_eexist_maps_to_file_exists", lambda: OSError(errno.EEXIST, "File exists")),
        # a backend that already raises FileExistsError passes through unchanged
        ("test_native_file_exists_error_propagates", lambda: FileExistsError("exists")),
    )
    for row_id, conflict in rows:
        with table_row(subtests, row_id, tmp_path):
            writer = FsspecAtomicWriter(_RaisingFs(conflict()))
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


@pytest.mark.unit
def test_local_replace_atomic_creates_when_missing(tmp_path: Any) -> None:
    target = tmp_path / "run.json"

    _local_writer().replace_atomic(_local_uri(target), b'{"status":"started"}')

    assert target.read_bytes() == b'{"status":"started"}'


@pytest.mark.unit
def test_local_replace_atomic_overwrites_existing(tmp_path: Any) -> None:
    """Unlike write_atomic, replace_atomic must succeed over an existing file."""
    target = tmp_path / "run.json"
    writer = _local_writer()

    writer.replace_atomic(_local_uri(target), b'{"status":"started"}')
    writer.replace_atomic(_local_uri(target), b'{"status":"completed"}')

    assert target.read_bytes() == b'{"status":"completed"}'


@pytest.mark.unit
def test_local_replace_atomic_leaves_no_temp_files(tmp_path: Any) -> None:
    target = tmp_path / "run.json"
    writer = _local_writer()

    writer.replace_atomic(_local_uri(target), b"{}")
    writer.replace_atomic(_local_uri(target), b'{"n":2}')

    leftovers = [p.name for p in tmp_path.iterdir() if p.name != "run.json"]
    assert leftovers == [], f"temp file leaked: {leftovers!r}"


class _CapturingReplaceFs:
    """Fake remote fs recording the buffered whole-body write replace_atomic emits."""

    protocol = "s3"

    def __init__(self) -> None:
        self.written: bytes | None = None
        self.mode: str | None = None

    def open(self, path: str, mode: str = "rb") -> Any:
        fs = self
        fs.mode = mode

        class _Handle:
            def __enter__(self) -> _Handle:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def write(self, data: bytes) -> None:
                fs.written = data

        return _Handle()


@pytest.mark.unit
def test_remote_replace_atomic_is_single_buffered_put() -> None:
    """On object stores the write must be one whole-body PUT ('wb'), never
    an exclusive create ('xb') that would fail on an existing object."""
    fs = _CapturingReplaceFs()
    writer = FsspecAtomicWriter(fs)

    writer.replace_atomic(_URI, b'{"status":"started"}')

    assert fs.mode == "wb"
    assert fs.written == b'{"status":"started"}'


def _payload(n: int) -> bytes:
    """A distinct, self-describing JSON body large enough to span a partial write."""
    return json.dumps({"generation": n, "filler": "x" * 4096}).encode("utf-8")


def _observe_while_writing(
    target: Path,
    write: Callable[[bytes], None],
    *,
    generations: int,
) -> list[bytes]:
    """Rewrite *target* *generations* times while a reader thread samples it.

    Returns every non-missing sample the reader observed. The reader only ever
    sees bytes on disk, so any truncated, empty, or half-written state the
    writer exposes lands in the returned list.
    """
    stop = threading.Event()
    seen: list[bytes] = []

    def reader() -> None:
        while not stop.is_set():
            with contextlib.suppress(FileNotFoundError):
                seen.append(target.read_bytes())

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    try:
        for n in range(generations):
            write(_payload(n))
    finally:
        stop.set()
        thread.join(timeout=10)
    return seen


def _corrupt_samples(seen: list[bytes]) -> list[bytes]:
    """Samples that are not a complete payload a writer actually published."""
    bad = []
    for sample in seen:
        try:
            decoded = json.loads(sample)
        except (ValueError, UnicodeDecodeError):
            bad.append(sample)
            continue
        if sample != _payload(int(decoded["generation"])):
            bad.append(sample)
    return bad


@pytest.mark.unit
def test_local_replace_atomic_never_exposes_a_partial_read(tmp_path: Any) -> None:
    """A concurrent reader observes whole generations only — the core contract.

    ``AtomicWriter.replace_atomic`` promises a reader sees either the previous
    content or the new content in full, never a truncated or empty file. This
    is the invariant the control plane depends on: peer pods list runs and
    parse ``run.json`` while a live run rewrites it, and a torn read surfaces
    as ``ControlPlaneCorruptionError``.
    """
    target = tmp_path / "run.json"
    writer = _local_writer()
    uri = _local_uri(target)

    seen = _observe_while_writing(
        target, lambda data: writer.replace_atomic(uri, data), generations=200
    )

    assert seen, "reader never sampled the file; the race harness did not run"
    assert _corrupt_samples(seen) == [], (
        f"replace_atomic exposed {len(_corrupt_samples(seen))} torn read(s) "
        f"out of {len(seen)} samples"
    )
    assert target.read_bytes() == _payload(199)


@pytest.mark.unit
def test_partial_read_harness_catches_a_truncating_writer(tmp_path: Any) -> None:
    """The harness above must actually be able to see a torn read.

    Guards `test_local_replace_atomic_never_exposes_a_partial_read` against
    passing vacuously: the same reader, pointed at the plain ``open(path, "w")``
    that ``replace_atomic`` replaced, must catch the 0-byte window between
    truncate and write.
    """
    target = tmp_path / "run.json"

    def truncating_write(data: bytes) -> None:
        with open(target, "wb") as fh:
            fh.flush()  # truncated to 0 bytes and visible to any reader
            time.sleep(0.001)
            fh.write(data)

    seen = _observe_while_writing(target, truncating_write, generations=20)

    assert _corrupt_samples(seen), (
        "harness saw no torn read from a truncating writer, so it cannot prove "
        "replace_atomic is atomic"
    )


@contextlib.contextmanager
def _writer_paused(
    monkeypatch: pytest.MonkeyPatch,
    seam: str,
    target: Path,
    method: str,
) -> Iterator[None]:
    """Run ``FsspecAtomicWriter.<method>(target)`` in a thread, paused at *seam*.

    ``created`` pauses right after the first file the writer creates in the
    destination directory (the in-flight temporary, still empty). ``published``
    pauses just before the writer links or renames onto *target* (temporary
    complete). Both seams are implementation-neutral: they do not depend on how
    the temporary is named or made. Only the writer thread is paused.
    """
    reached, release = threading.Event(), threading.Event()
    directory = str(target.parent)
    real_open, real_link, real_replace = os.open, os.link, os.replace

    def pause() -> None:
        if threading.current_thread().name != "paused-writer" or reached.is_set():
            return
        reached.set()
        assert release.wait(timeout=10), "test never released the paused writer"

    def open_(path: Any, *args: Any, **kwargs: Any) -> int:
        fd = real_open(path, *args, **kwargs)
        if os.path.dirname(os.fspath(path)) == directory:
            pause()
        return fd

    def link(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
        if os.fspath(dst) == str(target):
            pause()
        real_link(src, dst, *args, **kwargs)

    def replace(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
        if os.fspath(dst) == str(target):
            pause()
        real_replace(src, dst, *args, **kwargs)

    if seam == "created":
        monkeypatch.setattr(os, "open", open_)
    else:
        monkeypatch.setattr(os, "link", link)
        monkeypatch.setattr(os, "replace", replace)

    writer = _local_writer()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            getattr(writer, method)(_local_uri(target), b'{"owner":"peer"}')
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, name="paused-writer", daemon=True)
    thread.start()
    try:
        assert reached.wait(timeout=10), f"writer never reached the {seam!r} seam"
        yield
    finally:
        release.set()
        thread.join(timeout=10)
    assert errors == []


@pytest.mark.integration
@pytest.mark.parametrize("seam", ["created", "published"])
@pytest.mark.parametrize("method", ["write_atomic", "replace_atomic"])
def test_in_flight_atomic_write_is_not_listed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str, seam: str
) -> None:
    """A listing taken while a peer writes returns real files only.

    The in-flight temporary is empty right after it is created. A claim listing
    that opens it raises ``ControlPlaneCorruptionError`` ("Corrupt claim file"),
    so ``ls`` and ``find`` must never return it, whatever the writer names it.
    """
    from firecube.core.filesystem import FsspecFilesystem
    from tests.helpers.storage import make_test_binding

    directory = tmp_path / "product.zarr" / ".firecube" / "claims"
    directory.mkdir(parents=True)
    (directory / "existing.json").write_text("{}", encoding="utf-8")
    target = directory / "current.json"
    expected = ["existing.json"]
    if method == "replace_atomic":
        target.write_text("{}", encoding="utf-8")
        expected = ["current.json", "existing.json"]
    fs = FsspecFilesystem(make_test_binding(tmp_path))
    dir_uri = StorageUri.from_local_path(directory)

    with _writer_paused(monkeypatch, seam, target, method):
        listed = sorted(Path(str(entry)).name for entry in fs.ls(dir_uri))
        found = sorted(Path(uri.path).name for uri in fs.find(dir_uri))

    assert listed == expected
    assert found == expected


@pytest.mark.integration
def test_list_claims_survives_an_in_flight_claims_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``list_claims`` returns the held claims while a peer is mid-write in ``claims/``."""
    from firecube.core.controlplane import ChunkManager
    from firecube.core.controlplane.types import WriteDomain
    from tests.helpers.storage import make_test_binding

    product = "product.zarr"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product), workspace=workspace
    )
    domain = WriteDomain(product=product, category="zarr_region", name="held")
    peer = tmp_path / product / ".firecube" / "claims" / "peer.json"
    owners: list[str] = []
    try:
        with (
            manager.acquire_claim(product=product, domain=domain, owner_id="holder"),
            _writer_paused(monkeypatch, "created", peer, "write_atomic"),
        ):
            owners = [claim.owner_id for claim in manager.list_claims(product=product)]
    finally:
        manager.close()

    assert owners == ["holder"]
