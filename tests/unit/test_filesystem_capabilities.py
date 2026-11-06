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

import fcntl
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path
from typing import IO, Any, Protocol, cast

import pytest

from firecube.core.filesystem.conditional import GUARD_FILE_NAME
from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
from firecube.core.filesystem.protocol import (
    AtomicPublish,
    AtomicWriter,
    CompareDelete,
    ConditionalWrite,
    Multipart,
    MultipartUploader,
    PreconditionFailed,
    RangedRead,
    Signer,
    StorageFilesystem,
    StorageFilesystemFull,
    unquoted_etag,
)
from firecube.core.storage.uri import StorageUri
from tests.helpers.rows import table_row
from tests.helpers.storage import make_test_binding

ROOT = Path(__file__).parents[2]


@pytest.mark.unit
def test_fsspec_capabilities_includes_multipart(tmp_path):
    wrapper = FsspecFilesystem.from_binding(make_test_binding(tmp_path))

    assert Multipart in wrapper.capabilities()


@pytest.mark.unit
def test_obstore_capabilities_includes_ranged_read(tmp_path):
    fs = ObstoreFilesystem.from_local(str(tmp_path))

    expected = {Multipart, RangedRead, AtomicPublish, ConditionalWrite, CompareDelete}
    assert fs.capabilities() == expected


@pytest.mark.unit
def test_capabilities_is_set_of_protocol_classes(tmp_path):
    wrapper = FsspecFilesystem.from_binding(make_test_binding(tmp_path))
    capabilities = wrapper.capabilities()

    assert isinstance(capabilities, set)
    assert capabilities
    assert all(isinstance(capability, type) for capability in capabilities)
    assert all(issubclass(capability, Protocol) for capability in capabilities)


@pytest.mark.unit
def test_storage_filesystem_protocol_isinstance_check(tmp_path):
    fsspec_wrapper = FsspecFilesystem.from_binding(make_test_binding(tmp_path))

    assert isinstance(fsspec_wrapper, StorageFilesystem)


@pytest.mark.unit
def test_storage_filesystem_full_protocol_isinstance_check(tmp_path):
    fsspec_wrapper = FsspecFilesystem.from_binding(make_test_binding(tmp_path))

    assert isinstance(fsspec_wrapper, StorageFilesystemFull)


@pytest.mark.unit
def test_capability_protocols_are_runtime_checkable(tmp_path):
    fsspec_wrapper = FsspecFilesystem.from_binding(make_test_binding(tmp_path))
    obstore_fs = ObstoreFilesystem.from_local(str(tmp_path / "obstore"))

    assert isinstance(obstore_fs, Multipart)
    assert isinstance(obstore_fs, RangedRead)
    assert isinstance(fsspec_wrapper, Multipart)
    assert not isinstance(fsspec_wrapper, RangedRead)
    assert not isinstance(fsspec_wrapper, Signer)
    assert not isinstance(obstore_fs, Signer)


@pytest.mark.unit
def test_obstore_read_range_returns_requested_slice(tmp_path):
    fs = ObstoreFilesystem.from_local(str(tmp_path))

    uri = StorageUri.from_local_path(tmp_path).join("range.txt")
    with fs.open(uri, "wb") as handle:
        handle.write(b"abcdef")

    assert fs.read_range(uri, 1, 4) == b"bcd"


@pytest.mark.unit
def test_direct_construction_is_fully_wired(subtests, tmp_path):
    """Direct construction (bypassing from_binding / create_filesystem) must
    still yield a fully usable object.

    The factory is the sanctioned entry point, but ``__init__`` is the seam that
    guarantees a backend can never exist in a half-built state where
    ``capabilities()`` advertises Multipart while the uploader is missing. The
    two backends stay symmetric so neither can drift back to None-defaulted writers.
    """
    for row_id, backend, driver in [
        ("fsspec_direct_construction_is_fully_wired", FsspecFilesystem, "fsspec"),
        ("obstore_direct_construction_is_fully_wired", ObstoreFilesystem, "obstore"),
    ]:
        with table_row(subtests, row_id, tmp_path) as (workdir, _):
            fs = backend(make_test_binding(workdir, driver=driver))

            assert isinstance(fs.atomic_writer, AtomicWriter)
            assert isinstance(fs.multipart_uploader, MultipartUploader)

            uri = StorageUri.from_local_path(workdir).join("claim.txt")
            fs.atomic_writer.write_atomic(uri, b"claimed")
            assert fs.exists(uri)
            with pytest.raises(FileExistsError):
                fs.atomic_writer.write_atomic(uri, b"again")


@pytest.fixture(params=["fsspec", "obstore"])
def cond(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[Any, StorageUri]:
    backend = FsspecFilesystem if request.param == "fsspec" else ObstoreFilesystem
    fs = backend(make_test_binding(tmp_path, driver=request.param))
    return fs, StorageUri.from_local_path(tmp_path / "claims")


@pytest.mark.integration
def test_compare_delete_is_reported_on_local_bindings_only(cond):
    fs, base = cond
    assert ConditionalWrite in fs.capabilities()
    assert (CompareDelete in fs.capabilities()) is (base.protocol == "file")


_ETAGS = [('W/"x"', 'W/"x"'), ('"abc-2"', "abc-2"), ("", ""), ('"', '"'), ("abc", "abc")]
_ETAGS += [('""x""', '"x"')]


@pytest.mark.unit
@pytest.mark.parametrize(("raw", "want"), _ETAGS)
def test_unquoted_etag_strips_one_outer_pair_only(raw, want):
    assert unquoted_etag(raw) == want


@pytest.mark.integration
@pytest.mark.concurrency
def test_create_if_absent_has_one_winner_among_threads(cond):
    fs, base = cond
    barrier = threading.Barrier(16)

    def race(i: int) -> str:
        barrier.wait()
        with suppress(FileExistsError):
            fs.create_if_absent(base.join("race.json"), b"%d" % i)
            return "won"
        return "lost"

    with ThreadPoolExecutor(16) as pool:
        outcomes = sorted(pool.map(race, range(16)))
    assert outcomes == ["lost"] * 15 + ["won"]


_RACER = """
import sys
from pathlib import Path
from firecube.core.filesystem import FsspecFilesystem as F, ObstoreFilesystem as O
from firecube.core.filesystem.protocol import PreconditionFailed
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding
driver, root, validator, me = sys.argv[1:]
fs = (F if driver == "fsspec" else O)(make_test_binding(Path(root), driver=driver))
uri = StorageUri.from_local_path(Path(root) / "claims" / "race.json")
print("ready", flush=True)
sys.stdin.readline()
try:
    body = me.encode()
    print("won", fs.compare_replace(uri, validator, body) if validator else fs.create_if_absent(uri, body))
except (FileExistsError, PreconditionFailed):
    print("lost")
"""


@pytest.mark.integration
@pytest.mark.concurrency
@pytest.mark.parametrize("seeded", [False, True], ids=["create", "replace"])
def test_conditional_write_has_one_winner_among_processes(cond, tmp_path, seeded):
    fs, base = cond
    uri = base.join("race.json")
    validator = fs.create_if_absent(uri, b"seed") if seeded else ""
    driver = "fsspec" if isinstance(fs, FsspecFilesystem) else "obstore"
    argv = [sys.executable, "-c", _RACER, driver, str(tmp_path), validator]
    pipe = {"cwd": ROOT, "stdin": subprocess.PIPE, "stdout": subprocess.PIPE, "text": True}
    procs = [subprocess.Popen([*argv, me], **pipe) for me in "ab"]
    Path(base.path).mkdir(parents=True, exist_ok=True)
    guard = os.open(Path(base.path) / GUARD_FILE_NAME, os.O_RDONLY | os.O_CREAT, 0o644)
    fcntl.flock(guard, fcntl.LOCK_EX)
    try:
        assert [cast("IO[str]", p.stdout).readline().strip() for p in procs] == ["ready"] * 2
        for p in procs:
            print("go", file=p.stdin, flush=True)
        time.sleep(2)
        assert [p.poll() for p in procs] == [None, None], "a child crossed the held guard"
    finally:
        os.close(guard)
    outs = [p.communicate(timeout=90)[0].split() for p in procs]
    assert sorted(out[0] for out in outs) == ["lost", "won"]
    me, won = next((m, out[1]) for m, out in zip("ab", outs, strict=True) if out[0] == "won")
    assert fs.read_snapshot(uri) == (me.encode(), won)


@pytest.mark.integration
def test_stale_validator_is_refused_by_replace_and_delete(cond):
    fs, base = cond
    uri = base.join("claim.json")
    stale = fs.create_if_absent(uri, b"one")
    current = fs.compare_replace(uri, stale, b"two")
    with pytest.raises(PreconditionFailed):
        fs.compare_replace(uri, stale, b"three")
    refused = PreconditionFailed if CompareDelete in fs.capabilities() else NotImplementedError
    with pytest.raises(refused):
        fs.compare_delete(uri, stale)
    assert fs.read_snapshot(uri) == (b"two", current)


@pytest.mark.integration
def test_compare_replace_never_creates_a_missing_object(cond):
    fs, base = cond
    uri = base.join("absent.json")
    validator = fs.create_if_absent(uri, b"one")
    fs.rm(uri)
    with pytest.raises(PreconditionFailed):
        fs.compare_replace(uri, validator, b"two")
    assert not fs.exists(uri)


@pytest.mark.integration
@pytest.mark.concurrency
def test_read_snapshot_never_pairs_body_with_another_generation(cond):
    fs, base = cond
    uri = base.join("moving.json")
    validators = {b"0": fs.create_if_absent(uri, b"0")}
    done = threading.Event()
    with ThreadPoolExecutor(1) as pool:
        reads = pool.submit(lambda: [fs.read_snapshot(uri) for _ in iter(done.is_set, True)])
        try:
            for i in range(1, 60):
                previous = validators[b"%d" % (i - 1)]
                validators[b"%d" % i] = fs.compare_replace(uri, previous, b"%d" % i)
        finally:
            done.set()
    assert reads.result(timeout=60)
    assert all(validators[body] == validator for body, validator in reads.result())


@pytest.mark.integration
def test_guard_file_is_never_listed_and_never_removed(cond):
    fs, base = cond
    uri = base.join("claim.json")
    guard = Path(base.path) / GUARD_FILE_NAME
    validator = fs.create_if_absent(uri, b"a")
    inode = os.stat(guard).st_ino
    validator = fs.compare_replace(uri, validator, b"b")
    assert os.stat(guard).st_ino == inode
    fs.compare_delete(uri, validator)
    assert os.stat(guard).st_ino == inode
    fs.create_if_absent(base.join("other.json"), b"c")
    assert [Path(str(u)).name for u in fs.ls(base)] == ["other.json"]
    assert [Path(u.path).name for u in fs.find(base)] == ["other.json"]
