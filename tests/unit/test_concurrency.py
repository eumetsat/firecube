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

import threading
import time
from unittest.mock import MagicMock

import pytest

from firecube.ingestor.extensions.duck import DuckDbMixin


@pytest.fixture
def ingestor_with_duckdb():
    class TestIngestor(DuckDbMixin):
        def __init__(self):
            super().__init__()
            self._log = MagicMock()

    return TestIngestor()


@pytest.mark.unit
def test_duckdb_thread_isolation(ingestor_with_duckdb):
    """Verify that each thread gets a unique DuckDB connection."""

    connections = {}
    lock = threading.Lock()

    def worker(thread_id):
        # Setup DB for this thread
        ingestor_with_duckdb.setup_duckdb()

        # Capture connection object identity
        # Note: We can't pickle connection, so we just store id/pointer or execute query
        try:
            # Verify we can execute
            res = ingestor_with_duckdb.con.execute("SELECT 1").fetchone()
            assert res[0] == 1

            # Store connection instance id
            with lock:
                connections[thread_id] = id(ingestor_with_duckdb.con)

            # Sleep to overlap with other threads
            time.sleep(0.1)

        finally:
            ingestor_with_duckdb.teardown_duckdb()

    threads = []
    for i in range(5):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Verify all threads had unique connections
    assert len(connections) == 5
    unique_ids = set(connections.values())
    assert len(unique_ids) == 5


@pytest.mark.unit
def test_duckdb_access_without_init(ingestor_with_duckdb):
    """Verify access triggers error if not initialized in thread."""

    error_raised = False

    def worker():
        nonlocal error_raised
        try:
            _ = ingestor_with_duckdb.con
        except RuntimeError:
            error_raised = True

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert error_raised
