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

"""DuckDB extension for Firecube Ingestors.

Provides DuckDbMixin for managing DuckDB connection lifecycle.
"""

from __future__ import annotations

import contextlib
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from firecube.ingestor.types.context import PluginContext
from firecube.ingestor.utils import duckdb_utils
from firecube.ingestor.utils.duckdb_utils import ensure_table

__all__ = ["DuckDbMixin", "ensure_table"]

if TYPE_CHECKING:
    import duckdb


class DuckDbMixin:
    """Mixin that manages a thread-local DuckDB connection for batch processing.

    Why thread-local?
        In parallel pipeline mode, each worker thread calls ``batch_setup`` /
        ``batch_teardown`` independently.  DuckDB connections are **not**
        thread-safe for concurrent writes; giving each thread its own in-memory
        connection avoids locking and eliminates the risk of file-level write
        conflicts when using a persistent ``*.duckdb`` file.

    Lifecycle per batch (driven by ``BaseIngestor``):
        ``batch_setup(ctx)``  → opens connection, applies settings, calls
                                ``prepare_duckdb_schema`` hook.
        ``batch_teardown(ctx)`` → closes and deletes the thread-local connection.

    MRO cooperation:
        Both ``batch_setup`` and ``batch_teardown`` call ``super()``
        cooperatively so that this mixin can be stacked with other mixins
        (e.g. ``ProductDataMixin``) without silently dropping their setup logic.

    Persistent mode (``duckdb_persist_batches=true``):
        Workers use an in-memory DB by default.  Persistence can be enabled
        for debugging or when accumulating rows across batches before a single
        final export.  Persistent connections use a file under the run's temp
        workspace; the main thread pre-initialises the schema in
        ``GenericZarrIngestor.on_pipeline_start`` to avoid races on first write.
    """

    @property
    def _db_local(self) -> threading.local:
        # Lazy-init the threading.local storage so that subclasses with
        # __slots__ or dataclass-style __init__ don't need to call super().__init__().
        if not hasattr(self, "_db_local_storage"):
            self._db_local_storage = threading.local()
        return self._db_local_storage

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if not hasattr(self._db_local, "con"):
            raise RuntimeError("DuckDB connection not initialized for this thread")
        return self._db_local.con

    @con.setter
    def con(self, value: duckdb.DuckDBPyConnection) -> None:
        self._db_local.con = value

    def setup_duckdb(
        self,
        workspace: Path | None = None,
        options: dict[str, Any] | None = None,
        in_memory: bool = True,
    ) -> None:
        """Initialize DuckDB connection with settings (THREAD-LOCAL)."""
        db_path = None
        if not in_memory and workspace:
            product_name = getattr(self, "PRODUCT_NAME", "product")
            filename = (options or {}).get("duckdb_filename", f"{product_name}.duckdb")
            db_path = workspace / filename

        self.con = duckdb_utils.open_duckdb(in_memory=in_memory, db_path=db_path)

        if options:
            duckdb_utils.apply_duckdb_settings(
                self.con,
                max_temp_directory_size=options.get("duckdb_max_temp_directory_size"),
                memory_limit=options.get("duckdb_memory_limit"),
                threads=options.get("duckdb_threads"),
                logger=getattr(self, "_log", None),
            )

    def teardown_duckdb(self) -> None:
        """Close DuckDB connection (THREAD-LOCAL)."""
        if hasattr(self._db_local, "con"):
            with contextlib.suppress(Exception):
                self.con.close()
            del self._db_local.con

    def batch_setup(self, ctx: PluginContext) -> None:
        # Memory vs persist decision:
        #   ctx.in_memory=True  → always use in-memory (test mode or explicit flag).
        #   ctx.in_memory=False → default to in-memory unless duckdb_persist_batches=true,
        #     which writes a per-worker file under the run's temp workspace.
        persist = ctx.option("duckdb_persist_batches", False)
        use_memory = True if ctx.in_memory else (not persist)
        workspace = getattr(self, "_workspace", None)

        self.setup_duckdb(
            workspace=workspace.temp_root if workspace else None,
            options=ctx.options,
            in_memory=use_memory,
        )

        # Ensure schema exists (whether fresh in-memory or existing persistent)
        try:
            self.prepare_duckdb_schema(self.con, ctx)
        except Exception as e:
            logger = getattr(self, "_log", None)
            if logger:
                logger.warning("Failed to prepare batch DuckDB schema: %s", e)
            raise

        # Cooperative MI
        # Optional by design: if no parent hook exists in the MRO, this is a no-op.
        super_setup = getattr(super(), "batch_setup", None)
        if callable(super_setup):
            super_setup(ctx)

    def batch_teardown(self, ctx: PluginContext) -> None:
        self.teardown_duckdb()
        # Optional by design: if no parent hook exists in the MRO, this is a no-op.
        super_teardown = getattr(super(), "batch_teardown", None)
        if callable(super_teardown):
            super_teardown(ctx)

    def prepare_duckdb_schema(self, con: duckdb.DuckDBPyConnection, ctx: PluginContext) -> None:
        """Optional hook to initialize tables in persistent DB mode."""
        pass
