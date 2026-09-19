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

import logging
from collections.abc import Callable
from typing import Any, Literal

import pytest

from firecube.core.config import StorageConfig
from firecube.core.filesystem.fsspec_backend import _fsspec_kwargs_from_binding
from firecube.core.filesystem.obstore_backend import _aws_config_from_driver
from firecube.core.filesystem.ops import _s3_fs_kwargs_from_storage_config, fs_kwargs_for_uri
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

Scheme = Literal["file", "s3"]
FsspecSeam = Literal["binding", "legacy"]


def _storage_config(
    *,
    scheme: Scheme = "s3",
    driver: Literal["fsspec", "obstore"] = "fsspec",
    anonymous: bool = False,
    with_credentials: bool = False,
) -> StorageConfig:
    return StorageConfig(
        storage_type="s3" if scheme == "s3" else "local",
        access_key="access-key" if with_credentials else None,
        secret_key="secret-key" if with_credentials else None,
        storage_driver=driver,
        anonymous=anonymous,
    )


def _binding_from_storage_config(
    storage_config: StorageConfig, *, scheme: Scheme
) -> StorageBinding:
    uri = StorageUri.parse(
        "s3://bucket/prefix/" if scheme == "s3" else "file:///tmp/firecube-test-product.zarr"
    )
    identity = ProductIdentity.from_uri(uri, "zarr", product_name="test")
    driver = StorageDriverConfig.from_storage_config(storage_config)
    return StorageBinding(identity=identity, driver=driver)


def _fsspec_kwargs(
    storage_config: StorageConfig, *, scheme: Scheme, seam: FsspecSeam
) -> dict[str, Any]:
    if seam == "binding":
        return _fsspec_kwargs_from_binding(
            _binding_from_storage_config(storage_config, scheme=scheme)
        )
    return _s3_fs_kwargs_from_storage_config(storage_config)


@pytest.mark.parametrize("seam", ["binding", "legacy"])
def test_fsspec_anon_false_s3_omits_anon_kwarg(seam: FsspecSeam) -> None:
    storage_config = _storage_config(scheme="s3", anonymous=False)

    kwargs = _fsspec_kwargs(storage_config, scheme="s3", seam=seam)

    assert "anon" not in kwargs


@pytest.mark.parametrize("seam", ["binding", "legacy"])
def test_fsspec_binding_anon_true_s3_sets_anon_kwarg(seam: FsspecSeam) -> None:
    storage_config = _storage_config(scheme="s3", anonymous=True)

    kwargs = _fsspec_kwargs(storage_config, scheme="s3", seam=seam)

    assert kwargs["anon"] is True


@pytest.mark.parametrize(
    ("seam", "build_kwargs"),
    [
        (
            "fsspec-binding",
            lambda: _fsspec_kwargs(
                _storage_config(scheme="file", anonymous=True), scheme="file", seam="binding"
            ),
        ),
        (
            "fsspec-legacy",
            lambda: fs_kwargs_for_uri(
                "file:///tmp/firecube-test-product.zarr",
                storage_config=_storage_config(scheme="file", anonymous=True),
            ),
        ),
        (
            "obstore",
            lambda: _obstore_config(
                _storage_config(scheme="file", driver="obstore", anonymous=True)
            ),
        ),
    ],
)
def test_local_anon_true_is_noop(
    seam: str,
    build_kwargs: Callable[[], dict[str, Any]],
) -> None:
    kwargs = build_kwargs()

    assert "anon" not in kwargs, seam
    assert "skip_signature" not in kwargs, seam


def _obstore_config(storage_config: StorageConfig) -> dict[str, Any]:
    driver = StorageDriverConfig.from_storage_config(storage_config)
    return _aws_config_from_driver(driver, is_s3=storage_config.storage_type == "s3")


def test_legacy_fsspec_kwargs_follow_uri_not_storage_type() -> None:
    storage_config = StorageConfig(
        storage_type="local",
        endpoint_url="https://s3.example.com",
        anonymous=True,
    )

    kwargs = fs_kwargs_for_uri("s3://bucket/x", storage_config)

    assert kwargs["anon"] is True
    assert kwargs["client_kwargs"]["endpoint_url"] == "https://s3.example.com"


def test_obstore_driver_anon_false_s3_omits_skip_signature() -> None:
    storage_config = _storage_config(scheme="s3", driver="obstore", anonymous=False)

    config = _obstore_config(storage_config)

    assert "skip_signature" not in config


def test_obstore_driver_anon_true_s3_sets_skip_signature() -> None:
    storage_config = _storage_config(scheme="s3", driver="obstore", anonymous=True)

    config = _obstore_config(storage_config)

    assert config["skip_signature"] is True


@pytest.mark.parametrize(
    ("seam", "build_kwargs"),
    [
        (
            "fsspec-binding",
            lambda: _fsspec_kwargs(
                _storage_config(scheme="s3", anonymous=True, with_credentials=True),
                scheme="s3",
                seam="binding",
            ),
        ),
        (
            "fsspec-legacy",
            lambda: _fsspec_kwargs(
                _storage_config(scheme="s3", anonymous=True, with_credentials=True),
                scheme="s3",
                seam="legacy",
            ),
        ),
        (
            "obstore",
            lambda: _obstore_config(
                _storage_config(
                    scheme="s3", driver="obstore", anonymous=True, with_credentials=True
                )
            ),
        ),
    ],
)
def test_anon_plus_credentials_warns_and_wins(
    caplog: pytest.LogCaptureFixture,
    seam: str,
    build_kwargs: Callable[[], dict[str, Any]],
) -> None:
    with caplog.at_level(logging.WARNING):
        kwargs = build_kwargs()

    assert any(
        "anonymous" in record.getMessage().lower() and "credential" in record.getMessage().lower()
        for record in caplog.records
    )
    if seam.startswith("fsspec"):
        assert kwargs["anon"] is True
    else:
        assert kwargs["skip_signature"] is True
