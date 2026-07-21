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

"""Unit tests for obstore atomic create-if-not-exists writes."""

# pyright: reportMissingImports=false

import pytest
from obstore.store import MemoryStore

from firecube.core.filesystem.obstore_backend import ObstoreAtomicWriter
from firecube.core.storage.uri import StorageUri


def test_obstore_atomic_writer_uses_put_mode_create() -> None:
    """write_atomic must create atomically without an exists+put sequence."""
    store = MemoryStore()
    writer = ObstoreAtomicWriter(store)
    uri = StorageUri.parse("memory:///test-bucket/path/to/claim.json")

    writer.write_atomic(uri, b'{"owner": "thread-1"}')

    with pytest.raises(FileExistsError):
        writer.write_atomic(uri, b'{"owner": "thread-2"}')

    assert bytes(store.get(uri.path).bytes()) == b'{"owner": "thread-1"}'
