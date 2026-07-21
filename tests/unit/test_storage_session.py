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

import importlib
from pathlib import Path
from typing import Any, cast

import duckdb
import numpy as np
import pytest
import xarray as xr
from zarr.storage import LocalStore

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.filesystem import StorageFilesystem
from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.uri import StorageUri

StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig
storage_session_module = importlib.import_module("firecube.core.storage.session")
StorageSession = storage_session_module.StorageSession
_DUCKDB_OBSTORE_REMOTE_MESSAGE = storage_session_module._DUCKDB_OBSTORE_REMOTE_MESSAGE


def _binding(uri: str, *, driver: str = "fsspec") -> StorageBinding:
    product_uri = StorageUri.from_local_path(uri) if uri.startswith("/") else StorageUri.parse(uri)
    return StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="test_product.zarr"),
        driver=StorageDriverConfig(driver=driver),
    )


def test_driver_property_delegates() -> None:
    session = StorageSession(_binding("/tmp/test_product.zarr", driver="obstore"))

    assert session.driver.driver == "obstore"
    assert not hasattr(session, "_driver")


def test_driver_property_no_setter() -> None:
    session = StorageSession(_binding("/tmp/test_product.zarr", driver="obstore"))

    with pytest.raises(AttributeError):
        session.driver = "fsspec"  # type: ignore[misc]


def test_storage_config_from_binding_storage_type_for_s3() -> None:
    """``storage_config_from_binding`` reports ``storage_type='s3'`` for remote URIs.

    Bucket identity now lives on ``binding.identity.product_uri.authority``;
    the helper-returned ``StorageConfig`` no longer carries a ``bucket``
    field (that was the deleted ``_StorageConfigView`` bridge's job).
    """
    storage_config_from_binding = storage_session_module.storage_config_from_binding
    uri = StorageUri.parse("s3://bucket/data/product.zarr")
    cri = uri.join(".firecube")
    identity = ProductIdentity(
        product_uri=uri,
        control_root_uri=cri,
        product_name="product",
        format="zarr",
    )
    binding = StorageBinding(identity=identity, driver=StorageDriverConfig(driver="fsspec"))

    sc = storage_config_from_binding(binding)

    assert sc.storage_type == "s3"
    assert binding.identity.product_uri.authority == "bucket"


def test_storage_config_from_binding_storage_type_for_file() -> None:
    storage_config_from_binding = storage_session_module.storage_config_from_binding
    uri = StorageUri.parse("file:///tmp/product.zarr")
    cri = uri.join(".firecube")
    identity = ProductIdentity(
        product_uri=uri,
        control_root_uri=cri,
        product_name="product",
        format="zarr",
    )
    binding = StorageBinding(identity=identity, driver=StorageDriverConfig(driver="fsspec"))

    sc = storage_config_from_binding(binding)

    assert sc.storage_type == "local"


def test_product_property_returns_binding_identity() -> None:
    binding = _binding("/tmp/test_product.zarr")
    session = StorageSession(binding)

    assert session.product is binding.identity


def test_uri_namespace_removed() -> None:
    session = StorageSession(_binding("/tmp/test_product.zarr"))

    assert not hasattr(session, "uri")


def test_fs_returns_storage_filesystem_protocol(tmp_path) -> None:
    session = StorageSession(_binding(str(tmp_path / "test_product.zarr")))

    assert isinstance(session.fs(), StorageFilesystem)


def test_control_plane_takes_no_args() -> None:
    session = StorageSession(_binding("/tmp/test_product.zarr"))

    with pytest.raises(TypeError):
        session.control_plane(product="x")  # type: ignore[call-arg]


def test_control_plane_returns_chunk_manager_scoped() -> None:
    session = StorageSession(_binding("/tmp/test_product.zarr"))

    cm = session.control_plane()

    assert isinstance(cm, ChunkManager)
    assert cast(Any, cm).product_name == "test_product.zarr"
    assert cm.base_uri == StorageUri.from_local_path("/tmp").to_str()
    cm.close()


def test_duckdb_apply_obstore_remote_raises() -> None:
    session = StorageSession(_binding("s3://bucket/test_product.zarr", driver="obstore"))
    con = duckdb.connect(":memory:")

    with pytest.raises(RuntimeError, match=_DUCKDB_OBSTORE_REMOTE_MESSAGE):
        session.duckdb.apply(con)

    con.close()


def test_duckdb_apply_obstore_local_noop() -> None:
    session = StorageSession(_binding("/tmp/test_product.zarr", driver="obstore"))
    con = duckdb.connect(":memory:")

    assert session.duckdb.apply(con) is None

    con.close()


def test_duckdb_apply_fsspec_does_not_raise() -> None:
    session = StorageSession(_binding("s3://bucket/test_product.zarr"))
    con = duckdb.connect(":memory:")

    assert session.duckdb.apply(con) is None

    con.close()


def test_no_global_state(tmp_path) -> None:
    driver_config = StorageDriverConfig(driver="fsspec")

    binding = StorageBinding(
        identity=ProductIdentity.from_uri(
            StorageUri.from_local_path(tmp_path / "test_product.zarr"),
            "zarr",
            product_name="test_product",
        ),
        driver=driver_config,
    )
    session_a = StorageSession(binding)
    session_b = StorageSession(binding)

    assert session_a is not session_b
    assert session_a.fs() is not session_b.fs()


def _local_session_for_pickle(tmp_path: Path) -> StorageSession:
    """Helper for pickle-rejection tests — uses local-only config."""
    driver = StorageDriverConfig(driver="fsspec")
    product_uri = StorageUri.from_local_path(tmp_path / "product.zarr")
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity(
                product_name="product.zarr",
                product_uri=product_uri,
                control_root_uri=product_uri.join(".firecube"),
                format="zarr",
            ),
            driver=driver,
        )
    )


def test_storage_session_rejects_pickle(tmp_path):
    """T2 regression: StorageSession is process-local; pickling must raise TypeError."""
    import pickle as _pickle

    session = _local_session_for_pickle(tmp_path)
    with pytest.raises(TypeError, match="process-local"):
        _pickle.dumps(session)


def test_storage_session_rejects_reduce(tmp_path):
    """T2 regression: __reduce__ must raise TypeError."""
    session = _local_session_for_pickle(tmp_path)
    with pytest.raises(TypeError, match="process-local"):
        session.__reduce__()


def test_storage_session_rejects_reduce_ex(tmp_path):
    """T2 regression: __reduce_ex__ must raise TypeError (catches cloudpickle/dask)."""
    import pickle as _pickle

    session = _local_session_for_pickle(tmp_path)
    with pytest.raises(TypeError, match="process-local"):
        session.__reduce_ex__(_pickle.HIGHEST_PROTOCOL)


def test_zarr_create_store_returns_local_store(tmp_path: Path) -> None:
    uri = StorageUri.from_local_path(tmp_path / "product.zarr")
    session = StorageSession(_binding(uri.to_str()))

    handle = session.zarr.create_store(uri, mode="w")

    assert isinstance(handle, ZarrStoreHandle)
    assert isinstance(handle.store, LocalStore)


def test_zarr_write_and_open_dataset_round_trip(tmp_path: Path) -> None:
    uri = StorageUri.from_local_path(tmp_path / "roundtrip.zarr")
    session = StorageSession(_binding(uri.to_str()))
    original = xr.Dataset({"temp": (["t"], np.arange(10, dtype="float32"))})

    session.zarr.write_dataset(original, uri, group="G", mode="w", zarr_format=3)
    reopened = session.zarr.open_dataset(uri, group="G")

    assert np.array_equal(reopened["temp"].values, original["temp"].values)
