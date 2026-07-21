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

from firecube.core.duckdb.bridge import apply_duckdb_storage

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession


class DuckDBIO:
    def __init__(self, session: StorageSession) -> None:
        self.session = session

    def apply(self, con: Any, *, output_uri: str | None = None) -> None:
        return apply_duckdb_storage(con, self.session, output_uri=output_uri)
