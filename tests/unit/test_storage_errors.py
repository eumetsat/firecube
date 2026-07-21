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

from __future__ import annotations

import importlib

import pytest

from firecube.core.errors import StorageError
from tests.helpers.storage import make_test_session

storage_session_module = importlib.import_module("firecube.core.storage.session")
StorageUri = importlib.import_module("firecube.core.storage.uri").StorageUri


def test_local_session_upload_raises_typed_error(monkeypatch, temp_workspace):
    class _BrokenFs:
        def open(self, *args, **kwargs):
            _ = (args, kwargs)
            raise OSError("write failed")

    def _fake_create_filesystem(_binding):
        return _BrokenFs()

    monkeypatch.setattr(storage_session_module, "create_filesystem", _fake_create_filesystem)

    source_file = temp_workspace / "sample.bin"
    source_file.write_bytes(b"abc")

    session = make_test_session(temp_workspace)

    with pytest.raises(StorageError):
        session.upload_tree(
            StorageUri.from_local_path(source_file),
            StorageUri.from_local_path(temp_workspace / "output.zarr"),
            parallel_workers=1,
        )


@pytest.mark.unit
def test_s3_session_upload_raises_typed_error(monkeypatch, temp_workspace):
    class _BrokenFs:
        def open(self, *args, **kwargs):
            _ = (args, kwargs)
            raise OSError("write failed")

    def _fake_create_filesystem(_binding):
        return _BrokenFs()

    monkeypatch.setattr(storage_session_module, "create_filesystem", _fake_create_filesystem)

    source_file = temp_workspace / "sample.bin"
    source_file.write_bytes(b"abc")

    session = make_test_session(temp_workspace, protocol="s3", authority="bucket")

    with pytest.raises(StorageError):
        session.upload_tree(
            StorageUri.from_local_path(source_file),
            StorageUri.parse("s3://bucket/product"),
            parallel_workers=1,
        )
