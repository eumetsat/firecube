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

"""Storage driver plumbing tests (wave-6 / T22 migration off S3Storage).

Driver selection lives on ``StorageDriverConfig.driver`` and is exposed via
``StorageSession.driver``.
"""

from __future__ import annotations

import pytest

from firecube.core.config import StorageConfig
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_session


def _session(uri: str, driver_config: StorageDriverConfig) -> StorageSession:
    product_uri = StorageUri.parse(uri)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="test_product"),
            driver=driver_config,
        )
    )


@pytest.mark.unit
class TestStorageDriverConfigDriver:
    def test_default_driver_via_storage_config_bridge_is_fsspec(self):
        sc = StorageConfig(storage_type="local")
        dc = StorageDriverConfig.from_storage_config(sc)
        assert dc.driver == "fsspec"

    def test_fsspec_driver_explicit(self):
        assert StorageDriverConfig(driver="fsspec").driver == "fsspec"

    def test_obstore_driver_explicit(self):
        assert StorageDriverConfig(driver="obstore").driver == "obstore"

    def test_invalid_driver_rejected(self):
        with pytest.raises(ValueError, match="driver must be one of"):
            StorageDriverConfig(driver="invalid")  # type: ignore[arg-type]


@pytest.mark.unit
class TestStorageSessionDriver:
    def test_default_session_driver_is_fsspec(self, tmp_path):
        session = make_test_session(tmp_path, driver="fsspec")
        assert session.driver.driver == "fsspec"

    def test_session_accepts_obstore_driver(self, tmp_path):
        session = make_test_session(tmp_path, driver="obstore")
        assert session.driver.driver == "obstore"

    def test_session_storage_config_bridge_propagates_driver(self):
        from firecube.core.storage.session import storage_config_from_binding

        session = _session(
            "s3://bucket/test_product.zarr",
            StorageDriverConfig(
                driver="obstore",
                endpoint_url="http://localhost:9000",
                region="us-east-1",
            ),
        )
        sc = storage_config_from_binding(session._binding)
        assert sc.storage_driver == "obstore"

    def test_session_driver_is_read_only(self, tmp_path):
        session = make_test_session(tmp_path, driver="fsspec")
        with pytest.raises(AttributeError):
            session.driver = "obstore"  # type: ignore[misc]
