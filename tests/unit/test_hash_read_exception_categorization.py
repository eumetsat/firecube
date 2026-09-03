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

"""Unit tests for hash-read exception categorization."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
)
from firecube.core.slot_index import SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


class _MissingAttrMap:
    def __getitem__(self, key: str) -> str:
        raise KeyError(key)


class _Root:
    def __init__(self, attrs: object) -> None:
        self.attrs = attrs


@pytest.fixture(
    params=[
        ("read_resolved_index_attrs_hash", RESOLVED_INDEX_IDENTITY_HASH_ATTR),
        ("read_slot_index_attrs_hash", SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR),
    ]
)
def hash_reader(request: pytest.FixtureRequest) -> tuple[str, str]:
    return request.param


@pytest.mark.parametrize("root_exc", [FileNotFoundError("missing")], ids=["file-not-found"])
def test_hash_read_missing_store_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hash_reader: tuple[str, str],
    root_exc: BaseException,
) -> None:
    method_name, _ = hash_reader
    cm = _make_manager(tmp_path)
    monkeypatch.setattr(
        ChunkManager, "get_product_root", lambda self, product: str(tmp_path / product)
    )

    def fake_open_group(**kwargs: object) -> object:
        _ = kwargs
        raise root_exc

    monkeypatch.setattr("zarr.open_group", fake_open_group)

    assert getattr(cm, method_name)(product="prod1") is None


def test_hash_read_missing_attr_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hash_reader: tuple[str, str],
) -> None:
    method_name, _attr_name = hash_reader
    cm = _make_manager(tmp_path)
    monkeypatch.setattr(
        ChunkManager, "get_product_root", lambda self, product: str(tmp_path / product)
    )
    monkeypatch.setattr("zarr.open_group", lambda **kwargs: _Root(_MissingAttrMap()))

    assert getattr(cm, method_name)(product="prod1") is None


@pytest.mark.parametrize(
    ("root_exc", "expected_exc"),
    [
        (PermissionError("denied"), PermissionError),
        (OSError("io"), OSError),
        (TimeoutError("timed out"), TimeoutError),
        (json.JSONDecodeError("bad json", "{", 1), json.JSONDecodeError),
        (ValueError("parse error"), ValueError),
    ],
    ids=["permission", "oserror", "timeout", "json-decode", "value-error"],
)
def test_hash_read_transient_errors_log_and_propagate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    hash_reader: tuple[str, str],
    root_exc: BaseException,
    expected_exc: type[BaseException],
) -> None:
    method_name, _ = hash_reader
    cm = _make_manager(tmp_path)
    monkeypatch.setattr(
        ChunkManager, "get_product_root", lambda self, product: str(tmp_path / product)
    )

    def fake_open_group(**kwargs: object) -> object:
        _ = kwargs
        raise root_exc

    monkeypatch.setattr("zarr.open_group", fake_open_group)

    with caplog.at_level(logging.WARNING), pytest.raises(expected_exc):
        getattr(cm, method_name)(product="prod1")

    assert any("Failed to read" in record.message for record in caplog.records)
