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

from typing import Protocol

import pytest

from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
from firecube.core.filesystem.protocol import (
    AtomicWriter,
    Multipart,
    MultipartUploader,
    RangedRead,
    Signer,
    StorageFilesystem,
    StorageFilesystemFull,
)
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding


@pytest.mark.unit
def test_fsspec_capabilities_includes_multipart(tmp_path):
    wrapper = FsspecFilesystem.from_binding(make_test_binding(tmp_path))

    assert Multipart in wrapper.capabilities()


@pytest.mark.unit
def test_obstore_capabilities_includes_ranged_read(tmp_path):
    fs = ObstoreFilesystem.from_local(str(tmp_path))

    assert fs.capabilities() == {Multipart, RangedRead}


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
def test_fsspec_direct_construction_is_fully_wired(tmp_path):
    """Direct construction (bypassing from_binding / create_filesystem) must
    still yield a fully usable object.

    The factory is the sanctioned entry point, but ``__init__`` is the seam that
    guarantees a backend can never exist in a half-built state where
    ``capabilities()`` advertises Multipart while the uploader is missing. This
    pins that invariant so a future regression in default wiring is caught.
    """
    fs = FsspecFilesystem(make_test_binding(tmp_path))

    assert isinstance(fs.atomic_writer, AtomicWriter)
    assert isinstance(fs.multipart_uploader, MultipartUploader)

    uri = StorageUri.from_local_path(tmp_path).join("claim.txt")
    fs.atomic_writer.write_atomic(uri, b"claimed")
    assert fs.exists(uri)
    with pytest.raises(FileExistsError):
        fs.atomic_writer.write_atomic(uri, b"again")


@pytest.mark.unit
def test_obstore_direct_construction_is_fully_wired(tmp_path):
    """Cross-backend twin of the fsspec direct-construction guard: keeps the two
    backends symmetric so neither can drift back to None-defaulted writers.
    """
    fs = ObstoreFilesystem(make_test_binding(tmp_path, driver="obstore"))

    assert isinstance(fs.atomic_writer, AtomicWriter)
    assert isinstance(fs.multipart_uploader, MultipartUploader)

    uri = StorageUri.from_local_path(tmp_path).join("claim.txt")
    fs.atomic_writer.write_atomic(uri, b"claimed")
    assert fs.exists(uri)
    with pytest.raises(FileExistsError):
        fs.atomic_writer.write_atomic(uri, b"again")
