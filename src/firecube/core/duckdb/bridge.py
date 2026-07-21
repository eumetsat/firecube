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

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from firecube.core.uris import is_remote_target

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

_DUCKDB_OBSTORE_REMOTE_MESSAGE = (
    "DuckDB remote parquet is not supported under storage_driver=obstore. "
    "Re-run this command with --storage-driver=fsspec."
)


def _quote_sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _remote_endpoint(endpoint_url: str) -> str:
    """Return the endpoint without its URL scheme (DuckDB's ``s3_endpoint`` wants host[:port])."""
    parsed = urlparse(endpoint_url)
    if parsed.netloc:
        return parsed.netloc + parsed.path
    return endpoint_url


def apply_duckdb_storage(
    con: Any,
    session: StorageSession,
    *,
    output_uri: str | None = None,
) -> None:
    product_uri = session.product.product_uri
    driver_config = session.driver
    if driver_config.driver == "obstore":
        source_is_remote = product_uri.is_remote()
        output_is_remote = is_remote_target(output_uri) if output_uri else False
        if source_is_remote or output_is_remote:
            raise RuntimeError(_DUCKDB_OBSTORE_REMOTE_MESSAGE)
        return None

    if not product_uri.is_remote():
        return None

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"SET s3_url_style={_quote_sql_string('path' if driver_config.path_style else 'vhost')}"
    )
    if driver_config.endpoint_url:
        con.execute(
            f"SET s3_endpoint={_quote_sql_string(_remote_endpoint(driver_config.endpoint_url))}"
        )
        con.execute(
            f"SET s3_use_ssl={'true' if driver_config.endpoint_url.startswith('https://') else 'false'}"
        )
    if driver_config.region:
        con.execute(f"SET s3_region={_quote_sql_string(driver_config.region)}")
    credentials = driver_config.credentials
    if credentials is not None and credentials.access_key:
        con.execute(f"SET s3_access_key_id={_quote_sql_string(credentials.access_key)}")
    if credentials is not None and credentials.secret_key:
        con.execute(f"SET s3_secret_access_key={_quote_sql_string(credentials.secret_key)}")
    if credentials is not None and credentials.session_token:
        con.execute(f"SET s3_session_token={_quote_sql_string(credentials.session_token)}")

    return None
