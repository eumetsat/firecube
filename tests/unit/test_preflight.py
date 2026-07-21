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

from types import SimpleNamespace
from typing import Any, cast

import pytest

from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.preflight import StoragePreflightError, preflight


class DummySession:
    _skip_preflight: bool = False

    def __init__(self, uri: str) -> None:
        product_uri = StorageUri.parse(uri)
        self.product = SimpleNamespace(product_uri=product_uri)
        self.exists_called = False

    def exists(self, uri):
        self.exists_called = True
        self.last_uri = uri
        return True


def test_preflight_success() -> None:
    session = DummySession("s3://bucket/product.zarr")

    preflight(cast(Any, session))

    assert session.exists_called is True
    assert session.last_uri.to_str() == "s3://bucket"


def test_preflight_failure() -> None:
    session = DummySession("s3://bucket/product.zarr")

    def _raise(uri):
        raise PermissionError("denied")

    session.exists = _raise

    with pytest.raises(StoragePreflightError, match=r"product\.zarr") as exc_info:
        preflight(cast(Any, session))

    assert "denied" in str(exc_info.value)


def test_preflight_skip_flag() -> None:
    session = DummySession("s3://bucket/product.zarr")
    session._skip_preflight = True

    preflight(cast(Any, session))

    assert session.exists_called is False
