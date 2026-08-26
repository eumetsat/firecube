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

"""Tests for ``run_dir_for``."""

from __future__ import annotations

from typing import cast

import pytest

from firecube.core.controlplane._paths import run_dir_for
from firecube.core.controlplane.types import RUNS_DIRNAME
from firecube.core.storage.uri import StorageUri

pytestmark = pytest.mark.unit


def test_run_dir_for_happy_path(tmp_path) -> None:
    product = "product.zarr"
    control_path = StorageUri.from_local_path(tmp_path / product / ".firecube")
    control_uri = StorageUri.from_local_path(tmp_path / product / ".firecube")

    def resolver(requested_product: str) -> tuple[StorageUri, StorageUri]:
        assert requested_product == product
        return control_path, control_uri

    run_dir, run_uri = run_dir_for(resolver, product, "run-123")

    expected = control_path.join(RUNS_DIRNAME).join("run-123")
    assert run_dir == expected
    assert run_uri == expected.to_str()


def test_run_dir_for_zero_io() -> None:
    """Derivation touches the resolver and path joins only — never the store.

    Asserts the derived paths, not the sequence of ``join`` calls that built
    them: how many segments each join takes is an implementation detail.
    """

    class SpyStorageUri:
        def __init__(self, label: str) -> None:
            self.label = label

        def join(self, *segments: str) -> SpyStorageUri:
            return SpyStorageUri("/".join((self.label, *segments)))

        def to_str(self) -> str:
            return self.label

        def exists(self) -> None:  # pragma: no cover - defensive sentinel
            raise AssertionError("run_dir_for must not check filesystem existence")

        def ls(self) -> None:  # pragma: no cover - defensive sentinel
            raise AssertionError("run_dir_for must not list filesystem contents")

        def info(self) -> None:  # pragma: no cover - defensive sentinel
            raise AssertionError("run_dir_for must not inspect filesystem metadata")

    resolver_calls: list[str] = []

    def resolver(product: str) -> tuple[StorageUri, StorageUri]:
        resolver_calls.append(product)
        return cast(StorageUri, SpyStorageUri("control-path")), cast(
            StorageUri, SpyStorageUri("control-uri")
        )

    run_dir, run_uri = run_dir_for(resolver, "product.zarr", "run-123")

    assert resolver_calls == ["product.zarr"]
    spy_run_dir = cast(SpyStorageUri, run_dir)
    assert isinstance(spy_run_dir, SpyStorageUri)
    assert spy_run_dir.label == f"control-path/{RUNS_DIRNAME}/run-123"
    assert run_uri == f"control-uri/{RUNS_DIRNAME}/run-123"


@pytest.mark.parametrize("run_id", ["../escape", "./escape", "a//b", "/escape", ".."])
def test_run_dir_for_unsafe_run_id(run_id: str) -> None:
    def resolver(_product: str) -> tuple[StorageUri, StorageUri]:
        uri = StorageUri.from_local_path("/tmp/product/.firecube")
        return uri, uri

    with pytest.raises(ValueError, match="run_id"):
        run_dir_for(resolver, "product.zarr", run_id)
