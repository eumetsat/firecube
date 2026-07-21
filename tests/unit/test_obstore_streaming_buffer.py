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

from typing import Any

from firecube.core.filesystem.obstore_backend import StreamingObstoreWriteBuffer


class _StoreSpy:
    def __init__(self) -> None:
        self.put_calls: list[tuple[str, bytes, dict[str, Any]]] = []

    def put(self, path: str, data: bytes, **kwargs: Any) -> None:
        self.put_calls.append((path, data, kwargs))


def test_streaming_obstore_write_buffer_accumulates_and_flushes_on_close() -> None:
    store = _StoreSpy()
    buffer = StreamingObstoreWriteBuffer(store, "metadata/zarr.json")

    assert buffer.writable()
    assert not buffer.readable()
    assert not buffer.seekable()
    assert buffer.write(b'{"zarr_format":') == len(b'{"zarr_format":')
    assert buffer.write(b"3}") == len(b"3}")

    buffer.close()
    buffer.close()

    assert store.put_calls == [("metadata/zarr.json", b'{"zarr_format":3}', {})]


def test_streaming_obstore_write_buffer_flushes_context_manager_on_exit() -> None:
    store = _StoreSpy()

    with StreamingObstoreWriteBuffer(store, "attrs.json") as buffer:
        buffer.write(b"{}")

    assert store.put_calls == [("attrs.json", b"{}", {})]
