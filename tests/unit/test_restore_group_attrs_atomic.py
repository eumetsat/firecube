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

from typing import Any, cast

import pytest

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.ingestor.runtime.zarr import write as zarr_write

pytestmark = pytest.mark.unit


class _StoreHandle:
    def zarr_kwargs(self) -> dict[str, object]:
        return {"store": object()}


class _Root:
    def __init__(self, group: object) -> None:
        self._group = group

    def __getitem__(self, name: str) -> object:
        assert name == "G"
        return self._group


class _AttrsWithPut:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def clear(self) -> None:
        self.calls.append(("clear", None))
        raise KeyboardInterrupt("old clear/update path has a crash window")

    def put(self, attrs: dict[str, Any]) -> None:
        self.calls.append(("put", dict(attrs)))

    def update(self, attrs: dict[str, Any]) -> None:
        self.calls.append(("update", dict(attrs)))


class _Group:
    def __init__(self, attrs: object) -> None:
        self.attrs = attrs


def test_restore_group_attrs_uses_put_without_clear_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attrs = _AttrsWithPut()
    monkeypatch.setattr(zarr_write.zarr, "open_group", lambda **_: _Root(_Group(attrs)))

    zarr_write._restore_group_attrs(
        zarr_store=cast(ZarrStoreHandle, _StoreHandle()),
        group="G",
        attrs={"title": "first"},
        zarr_format=3,
    )

    assert attrs.calls == [("put", {"title": "first"})]
