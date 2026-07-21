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

"""W4.5 — DuckDB driver integration suite.

Exercises ``StorageSession.duckdb.apply(...)`` against a real
``duckdb.connect(":memory:")`` connection. BT2b in
``test_architecture_invariants.py`` covers the same obstore-remote hard-error
contract via ``MagicMock``; this file complements it with real-DuckDB scenarios
plus fsspec-remote settings propagation and local parquet round-trips.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from firecube.core.credentials import Credentials
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri


def _make_session(
    target: str,
    *,
    driver: str,
    endpoint_url: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    region: str | None = None,
    path_style: bool = True,
) -> StorageSession:
    driver_config = StorageDriverConfig(
        driver=driver,  # type: ignore[arg-type]
        endpoint_url=endpoint_url,
        credentials=Credentials(access_key=access_key, secret_key=secret_key)
        if access_key is not None or secret_key is not None
        else None,
        region=region,
        path_style=path_style,
    )
    product_uri = (
        StorageUri.from_local_path(target) if "://" not in target else StorageUri.parse(target)
    )
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(product_uri, "parquet", product_name="test_product"),
            driver=driver_config,
        )
    )


def _write_local_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(
        f"COPY (SELECT 1 AS x, 'foo' AS y UNION ALL SELECT 2, 'bar') TO '{path}' (FORMAT 'parquet')"
    )


@pytest.mark.integration
def test_fsspec_remote_via_bridge() -> None:
    session = _make_session(
        "s3://test-bucket/data.parquet",
        driver="fsspec",
        endpoint_url="https://minio.example:9000",
        access_key="ACCESS",
        secret_key="SECRET",
        region="eu-central-1",
        path_style=False,
    )
    con = duckdb.connect(":memory:")
    try:
        assert session.duckdb.apply(con) is None

        endpoint = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert endpoint == ("minio.example:9000",)
    finally:
        con.close()


@pytest.mark.integration
def test_local_parquet_under_obstore(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    writer = duckdb.connect(":memory:")
    try:
        _write_local_parquet(writer, parquet_path)
    finally:
        writer.close()
    assert parquet_path.exists()

    session = _make_session(str(parquet_path), driver="obstore")
    con = duckdb.connect(":memory:")
    try:
        assert session.duckdb.apply(con) is None

        rows = con.execute(f"SELECT x, y FROM read_parquet('{parquet_path}') ORDER BY x").fetchall()
        assert rows == [(1, "foo"), (2, "bar")]
    finally:
        con.close()


@pytest.mark.integration
def test_obstore_remote_hard_error() -> None:
    """BT2b in ``test_architecture_invariants.py`` covers the same contract with a
    ``MagicMock`` connection; this cell adds real-DuckDB coverage so any future
    regression in the bridge surfaces in the parquet integration suite as well.
    """
    session = _make_session("s3://test-bucket/data.parquet", driver="obstore")
    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(RuntimeError) as excinfo:
            session.duckdb.apply(con)
        message = str(excinfo.value)
        assert "remote parquet" in message
        assert "storage_driver=obstore" in message
        assert "--storage-driver=fsspec" in message
    finally:
        con.close()


@pytest.mark.integration
def test_bridge_configures_s3_settings() -> None:
    session = _make_session(
        "s3://settings-bucket/data.parquet",
        driver="fsspec",
        endpoint_url="http://localhost:19000",
        access_key="MY_ACCESS",
        secret_key="MY_SECRET",
        region="us-test-1",
        path_style=True,
    )
    con = duckdb.connect(":memory:")
    try:
        session.duckdb.apply(con)

        def _setting(name: str) -> str:
            row = con.execute(f"SELECT current_setting('{name}')").fetchone()
            assert row is not None
            return str(row[0])

        assert _setting("s3_endpoint") == "localhost:19000"
        assert _setting("s3_region") == "us-test-1"
        assert _setting("s3_access_key_id") == "MY_ACCESS"
        assert _setting("s3_secret_access_key") == "MY_SECRET"
        assert _setting("s3_url_style") == "path"
        # http:// endpoint → bridge must disable s3_use_ssl.
        assert _setting("s3_use_ssl").lower() in {"false", "0"}
    finally:
        con.close()


@pytest.mark.integration
def test_bridge_configures_s3_https_endpoint_uses_ssl() -> None:
    session = _make_session(
        "s3://ssl-bucket/data.parquet",
        driver="fsspec",
        endpoint_url="https://s3.amazonaws.com",
        region="us-east-1",
    )
    con = duckdb.connect(":memory:")
    try:
        session.duckdb.apply(con)

        endpoint = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        use_ssl = con.execute("SELECT current_setting('s3_use_ssl')").fetchone()
        assert endpoint == ("s3.amazonaws.com",)
        assert use_ssl is not None
        assert str(use_ssl[0]).lower() in {"true", "1"}
    finally:
        con.close()


@pytest.mark.integration
def test_fsspec_local_parquet_is_noop(tmp_path: Path) -> None:
    parquet_path = tmp_path / "fsspec_local.parquet"
    writer = duckdb.connect(":memory:")
    try:
        _write_local_parquet(writer, parquet_path)
    finally:
        writer.close()

    session = _make_session(str(parquet_path), driver="fsspec")
    con = duckdb.connect(":memory:")
    try:
        assert session.duckdb.apply(con) is None

        rows = con.execute(f"SELECT x, y FROM read_parquet('{parquet_path}') ORDER BY x").fetchall()
        assert rows == [(1, "foo"), (2, "bar")]
    finally:
        con.close()


@pytest.mark.integration
def test_storage_uri_remote_detection_drives_bridge_branch() -> None:
    """Pins ``StorageUri.is_remote()`` as the local/remote selector.

    A regression that misclassifies ``s3://`` as local would silently downgrade
    the obstore guard into a no-op; this cell guards the W4.1/W4.2/W4.3 contract.
    """
    remote_uri = StorageUri.parse("s3://branch-detection/data.parquet")
    local_uri = StorageUri.parse("file:///tmp/branch-detection/data.parquet")
    assert remote_uri.is_remote() is True
    assert local_uri.is_remote() is False

    remote_session = _make_session(remote_uri.to_str(), driver="obstore")
    local_session = _make_session(local_uri.to_str(), driver="obstore")

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(RuntimeError, match="storage_driver=obstore"):
            remote_session.duckdb.apply(con)
        assert local_session.duckdb.apply(con) is None
    finally:
        con.close()
