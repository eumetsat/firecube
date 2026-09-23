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

"""Exception classifier for ``_open_zarr_root_for_read``.

Pins the contract that the helper returns ``None`` for genuinely absent
stores and propagates every other exception with ``__cause__`` preserved.
Silently swallowing a ``PermissionError``, ``TimeoutError`` or generic
``OSError`` (credential/throttling/DNS faults on the remote-path branch)
used to mask real failures as the caller's misleading
``ConfigurationError('... must be preallocated ...')``; the classifier tuple
``_ZARR_NOT_FOUND_ERRORS`` narrows the catch to the missing-store family.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import zarr

from firecube.ingestor.templates.direct_zarr import (
    _ZARR_NOT_FOUND_ERRORS,
    _open_zarr_root_for_read,
)

pytestmark = pytest.mark.unit


def test_missing_local_store_returns_none(tmp_path: Path) -> None:
    absent = tmp_path / "does_not_exist.zarr"
    store_uri = f"file://{absent}"

    result = _open_zarr_root_for_read(store_uri, storage_config=None)

    assert result is None


def test_permission_error_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    absent = tmp_path / "would_be.zarr"
    store_uri = f"file://{absent}"

    def _raise_permission(*_args: object, **_kwargs: object) -> object:
        raise PermissionError("simulated 403 / EACCES from underlying store")

    monkeypatch.setattr(zarr, "open_group", _raise_permission)

    with pytest.raises(PermissionError, match="simulated 403 / EACCES"):
        _open_zarr_root_for_read(store_uri, storage_config=None)


def test_timeout_error_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    absent = tmp_path / "would_be.zarr"
    store_uri = f"file://{absent}"

    def _raise_timeout(*_args: object, **_kwargs: object) -> object:
        raise TimeoutError("simulated endpoint hang")

    monkeypatch.setattr(zarr, "open_group", _raise_timeout)

    with pytest.raises(TimeoutError, match="simulated endpoint hang"):
        _open_zarr_root_for_read(store_uri, storage_config=None)


def test_generic_os_error_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    absent = tmp_path / "would_be.zarr"
    store_uri = f"file://{absent}"

    def _raise_dns_style_oserror(*_args: object, **_kwargs: object) -> object:
        raise OSError("simulated DNS failure / getaddrinfo ENOENT")

    monkeypatch.setattr(zarr, "open_group", _raise_dns_style_oserror)

    with pytest.raises(OSError, match="simulated DNS failure") as excinfo:
        _open_zarr_root_for_read(store_uri, storage_config=None)
    assert not isinstance(excinfo.value, FileNotFoundError)


def test_value_error_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    absent = tmp_path / "would_be.zarr"
    store_uri = f"file://{absent}"

    def _raise_value_error(*_args: object, **_kwargs: object) -> object:
        raise ValueError("simulated malformed metadata")

    monkeypatch.setattr(zarr, "open_group", _raise_value_error)

    with pytest.raises(ValueError, match="simulated malformed metadata"):
        _open_zarr_root_for_read(store_uri, storage_config=None)


_GROUP_NOT_FOUND = getattr(__import__("zarr.errors", fromlist=["_"]), "GroupNotFoundError", None)


@pytest.mark.skipif(
    _GROUP_NOT_FOUND is None,
    reason="installed zarr does not expose zarr.errors.GroupNotFoundError",
)
def test_zarr_group_not_found_returns_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    group_not_found_cls = _GROUP_NOT_FOUND
    assert group_not_found_cls is not None
    assert group_not_found_cls in _ZARR_NOT_FOUND_ERRORS

    absent = tmp_path / "would_be.zarr"
    store_uri = f"file://{absent}"

    def _raise_group_not_found(*_args: object, **_kwargs: object) -> object:
        raise group_not_found_cls("simulated missing group")

    monkeypatch.setattr(zarr, "open_group", _raise_group_not_found)

    assert _open_zarr_root_for_read(store_uri, storage_config=None) is None
