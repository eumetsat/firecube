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

from typing import Any, get_type_hints

import pytest

from firecube.core.filesystem.protocol import StorageFilesystem
from firecube.core.storage.uri import StorageUri


class _MockStorageFilesystem:
    def exists(self, uri: StorageUri) -> bool:
        return True

    def open(self, uri: StorageUri, mode: str = "rb") -> Any:
        return object()

    def read_bytes(self, uri: StorageUri) -> bytes:
        return b""

    def find(self, uri: StorageUri) -> list[StorageUri]:
        return [uri]

    def isdir(self, uri: StorageUri) -> bool:
        return False

    def rm(self, uri: StorageUri, recursive: bool = False) -> None:
        return None

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        return None

    def info(self, uri: StorageUri) -> dict[str, Any]:
        return {"uri": uri.to_str()}

    def capabilities(self) -> set[type]:
        return set()


@pytest.mark.unit
def test_protocol_is_runtime_checkable() -> None:
    mock = _MockStorageFilesystem()

    assert isinstance(mock, StorageFilesystem)


@pytest.mark.unit
def test_protocol_methods_use_storage_uri_annotations() -> None:
    hints = get_type_hints(StorageFilesystem.exists)
    assert hints["uri"] is StorageUri

    hints = get_type_hints(StorageFilesystem.open)
    assert hints["uri"] is StorageUri

    hints = get_type_hints(StorageFilesystem.find)
    assert hints["uri"] is StorageUri
    assert hints["return"] == list[StorageUri]

    hints = get_type_hints(StorageFilesystem.isdir)
    assert hints["uri"] is StorageUri

    hints = get_type_hints(StorageFilesystem.rm)
    assert hints["uri"] is StorageUri

    hints = get_type_hints(StorageFilesystem.put)
    assert hints["src_uri"] is StorageUri
    assert hints["dst_uri"] is StorageUri

    hints = get_type_hints(StorageFilesystem.info)
    assert hints["uri"] is StorageUri


@pytest.mark.unit
def test_protocol_capabilities_signature_remains_set_of_types() -> None:
    hints = get_type_hints(StorageFilesystem.capabilities)
    assert hints["return"] == set[type]
