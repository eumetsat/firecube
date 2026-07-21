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

"""The existing-cube dim-compat check must read metadata single-shot, not via
a conditional cached fetch.

Parallel slot-range pods run `verify_dim_compatibility` at startup while other
pods are concurrently creating the same group's `zarr.json`. On s3fs,
`open().read()` takes a range-cached fetch that adds an `If-Match` precondition
and raises a 412 PreconditionFailed when the ETag changes mid-read (surfacing as
`OSError(EINVAL)` and crashing inside s3fs). `_read_json` must instead use the
driver's single-shot `read_bytes` (a plain GET, no precondition). This pins that
contract: a filesystem whose `open()` is poisoned must still be read via
`read_bytes`.
"""

from __future__ import annotations

import pytest

from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.zarr.existing_cube_check import _read_json


class _OpenForbiddenFs:
    """Fake fs whose conditional `open()` path is unusable (simulates the s3fs
    412 crash); only the single-shot `read_bytes` works."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.read_bytes_calls: list[StorageUri] = []

    def open(self, *_args: object, **_kwargs: object):
        raise AssertionError("dim-compat read must use read_bytes(), not open()")

    def read_bytes(self, uri: StorageUri) -> bytes:
        self.read_bytes_calls.append(uri)
        return self._payload


@pytest.mark.unit
def test_read_json_uses_read_bytes_not_open() -> None:
    fs = _OpenForbiddenFs(b'{"zarr_format": 3, "node_type": "array"}')
    uri = StorageUri.parse("s3://bucket/product.zarr/SEVIRI_L15/data/zarr.json")

    result = _read_json(fs, uri)

    assert result == {"zarr_format": 3, "node_type": "array"}
    assert fs.read_bytes_calls == [uri]
