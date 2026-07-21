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

"""Unit tests for ZarrStoreFactory: create_zarr_store."""

# pyright: reportAttributeAccessIssue=false

from pathlib import Path

import pytest
import zarr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import (
    ZarrStoreHandle,
    create_zarr_store,
)


@pytest.mark.unit
class TestCreateZarrStore:
    """Tests for create_zarr_store driver dispatch."""

    def test_obstore_returns_handle_for_obstore(self):
        """obstore driver returns a uniform handle with an object store."""
        from zarr.storage import ObjectStore

        sc = StorageConfig(storage_type="local", storage_driver="obstore")
        result = create_zarr_store(uri="/tmp/test.zarr", storage_config=sc)
        assert isinstance(result, ZarrStoreHandle)
        assert isinstance(result.store, ObjectStore)
        assert result.storage_options is None

    def test_fsspec_s3_handle_carries_storage_options(self):
        """S3 fsspec handles expose storage_options on the wrapper."""
        sc = StorageConfig(
            storage_type="s3",
            endpoint_url="http://localhost:9000",
            access_key="key",
            secret_key="secret",
            region="us-east-1",
            storage_driver="fsspec",
        )
        sc.bucket = "test-bucket"  # type: ignore[attr-defined]
        result = create_zarr_store(uri="s3://test-bucket/test.zarr", storage_config=sc)
        assert isinstance(result, ZarrStoreHandle)
        assert result.store == "s3://test-bucket/test.zarr"
        assert isinstance(result.storage_options, dict)
        assert result.storage_options["key"] == "key"
        assert result.storage_options["secret"] == "secret"

    @pytest.mark.parametrize("driver", ["fsspec", "obstore"])
    def test_uniform_handle_both_drivers(self, tmp_path: Path, driver: str):
        """Callers can splat handle kwargs into zarr without branching."""
        target = tmp_path / f"{driver}.zarr"
        sc = StorageConfig(storage_type="local", storage_driver=driver)
        handle = create_zarr_store(uri=str(target), storage_config=sc, mode="w")

        root = zarr.open_group(**handle.zarr_kwargs(), mode="w", zarr_format=3)
        root.attrs["driver"] = driver

        reopened = zarr.open_group(**handle.zarr_kwargs(), mode="a", zarr_format=3)
        assert reopened.attrs["driver"] == driver

    def test_zarr_kwargs_omits_none_storage_options(self):
        """Handle kwargs omit storage_options when not needed."""
        handle = ZarrStoreHandle(
            store="/tmp/test.zarr", storage_options=None, target_uri="/tmp/test.zarr"
        )
        assert handle.zarr_kwargs() == {"store": "/tmp/test.zarr"}


@pytest.mark.unit
class TestCreateZarrStoreStorageOptions:
    """Tests for storage_options on the returned handle."""

    def test_fsspec_local_returns_none(self):
        """Local fsspec config has no storage_options."""
        sc = StorageConfig(storage_type="local", storage_driver="fsspec")
        result = create_zarr_store(uri="/tmp/test.zarr", storage_config=sc)
        assert result.storage_options is None

    def test_obstore_returns_none(self):
        """obstore driver always returns None (no fsspec storage_options needed)."""
        sc = StorageConfig(storage_type="local", storage_driver="obstore")
        result = create_zarr_store(uri="/tmp/test.zarr", storage_config=sc)
        assert result.storage_options is None

    def test_obstore_s3_returns_none(self):
        """obstore driver returns None even for S3 configs."""
        sc = StorageConfig(
            storage_type="s3",
            endpoint_url="http://localhost:9000",
            access_key="key",
            secret_key="secret",
            region="us-east-1",
            storage_driver="obstore",
        )
        sc.bucket = "test-bucket"  # type: ignore[attr-defined]
        result = create_zarr_store(uri="s3://test-bucket/test.zarr", storage_config=sc)
        assert result.storage_options is None
