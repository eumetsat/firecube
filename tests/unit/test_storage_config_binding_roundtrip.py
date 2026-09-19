# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for storage_config_from_binding round-trip field preservation."""

from __future__ import annotations

import pytest

from firecube.core.config import StorageConfig
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import storage_config_from_binding

try:
    from firecube.core.uris import StorageUri
except ImportError:
    from firecube.core.storage.uri import StorageUri


def _make_binding(
    storage_config: StorageConfig, uri_str: str = "s3://bucket/prefix/"
) -> StorageBinding:
    uri = StorageUri.parse(uri_str)
    identity = ProductIdentity.from_uri(uri, "zarr", product_name="roundtrip_test")
    driver = StorageDriverConfig.from_storage_config(storage_config)
    return StorageBinding(identity=identity, driver=driver)


@pytest.mark.unit
@pytest.mark.parametrize(
    "anonymous",
    [True, False, None],
    ids=["anon_true", "anon_false", "anon_none"],
)
def test_roundtrip_preserves_anonymous(anonymous):
    """storage_config_from_binding must preserve anonymous (Finding 1 fix)."""
    cfg = StorageConfig(storage_type="s3", anonymous=anonymous)
    binding = _make_binding(cfg)
    rebuilt = storage_config_from_binding(binding)
    assert rebuilt.anonymous == anonymous, (
        f"anonymous={anonymous!r} was not preserved: got {rebuilt.anonymous!r}"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "endpoint_url",
    [None, "https://custom.endpoint.example.com"],
    ids=["no_endpoint", "custom_endpoint"],
)
def test_roundtrip_preserves_endpoint_url(endpoint_url):
    """storage_config_from_binding must preserve endpoint_url."""
    cfg = StorageConfig(storage_type="s3", endpoint_url=endpoint_url)
    binding = _make_binding(cfg)
    rebuilt = storage_config_from_binding(binding)
    assert rebuilt.endpoint_url == endpoint_url, (
        f"endpoint_url={endpoint_url!r} was not preserved: got {rebuilt.endpoint_url!r}"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "path_style",
    [True, False],
    ids=["path_style_true", "path_style_false"],
)
def test_roundtrip_preserves_path_style(path_style):
    """storage_config_from_binding must preserve path_style."""
    cfg = StorageConfig(storage_type="s3", path_style=path_style)
    binding = _make_binding(cfg)
    rebuilt = storage_config_from_binding(binding)
    assert rebuilt.path_style == path_style, (
        f"path_style={path_style!r} was not preserved: got {rebuilt.path_style!r}"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "storage_driver",
    ["fsspec", "obstore"],
    ids=["fsspec_driver", "obstore_driver"],
)
def test_roundtrip_preserves_storage_driver(storage_driver):
    """storage_config_from_binding must preserve storage_driver."""
    cfg = StorageConfig(storage_type="s3", storage_driver=storage_driver)
    binding = _make_binding(cfg)
    rebuilt = storage_config_from_binding(binding)
    assert rebuilt.storage_driver == storage_driver, (
        f"storage_driver={storage_driver!r} was not preserved: got {rebuilt.storage_driver!r}"
    )


@pytest.mark.unit
def test_roundtrip_anonymous_with_local_storage_type():
    """Finding 2a fix: anonymous must survive even when storage_type='local'.

    StorageDriverConfig.from_storage_config no longer gates anonymous on storage_type.
    """
    cfg = StorageConfig(storage_type="local", anonymous=True)
    uri = StorageUri.parse("file:///tmp/test-roundtrip/")
    identity = ProductIdentity.from_uri(uri, "zarr", product_name="local_roundtrip")
    driver = StorageDriverConfig.from_storage_config(cfg)
    binding = StorageBinding(identity=identity, driver=driver)
    rebuilt = storage_config_from_binding(binding)
    assert rebuilt.anonymous is True, (
        f"anonymous was dropped for local storage_type: got {rebuilt.anonymous!r}"
    )
