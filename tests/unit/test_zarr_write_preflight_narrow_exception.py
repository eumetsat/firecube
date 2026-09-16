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
from firecube.ingestor.runtime.zarr.write import _open_preflight_compare_group

pytestmark = pytest.mark.unit


class _StoreHandle:
    target_uri = "file:///nonexistent.zarr"

    def zarr_kwargs(self) -> dict[str, object]:
        return {"store": object()}


def test_preflight_compare_propagates_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_permission_error(**_: Any) -> None:
        raise PermissionError("denied")

    monkeypatch.setattr(zarr_write.zarr, "open_group", _raise_permission_error)

    with pytest.raises(PermissionError, match="denied"):
        _open_preflight_compare_group(
            preflight_compare_zarr_store=cast(ZarrStoreHandle, _StoreHandle()),
            group="G",
            zarr_format=3,
        )


def test_preflight_compare_returns_none_for_missing_final_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_file_not_found(**_: Any) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(zarr_write.zarr, "open_group", _raise_file_not_found)

    assert (
        _open_preflight_compare_group(
            preflight_compare_zarr_store=cast(ZarrStoreHandle, _StoreHandle()),
            group="G",
            zarr_format=3,
        )
        is None
    )
