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

"""Regression tests for create_filesystem() instrumentation wrapping (V2-fix).

V2 audit found that `create_filesystem()` returned plain adapters without
wrapping them with `InstrumentedFilesystem` when filesystem metrics were
active. This bypassed all metrics collection on the canonical I/O path. The
legacy `_open_fsspec_url()` helper had the wrap pattern at ops.py:213-214;
`create_filesystem()` should mirror it.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pytest

from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.filesystem.instrumentation import (
    InstrumentedFilesystem,
    collect_filesystem_metrics,
)
from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
from firecube.core.filesystem.ops import create_filesystem
from tests.helpers.storage import make_test_binding


@pytest.mark.unit
class TestCreateFilesystemInstrumentation:
    def test_wraps_fsspec_with_instrumentation_when_metrics_active(self, tmp_path: Path) -> None:
        binding = make_test_binding(tmp_path, driver="fsspec")

        with collect_filesystem_metrics():
            fs = create_filesystem(binding)

        assert isinstance(fs, InstrumentedFilesystem)

    def test_wraps_obstore_with_instrumentation_when_metrics_active(self, tmp_path: Path) -> None:
        binding = make_test_binding(tmp_path, driver="obstore")

        with collect_filesystem_metrics():
            fs = create_filesystem(binding)

        assert isinstance(fs, InstrumentedFilesystem)

    def test_returns_plain_fsspec_adapter_when_no_metrics(self, tmp_path: Path) -> None:
        binding = make_test_binding(tmp_path, driver="fsspec")

        fs = create_filesystem(binding)

        assert isinstance(fs, FsspecFilesystem)
        assert not isinstance(fs, InstrumentedFilesystem)

    def test_returns_plain_obstore_adapter_when_no_metrics(self, tmp_path: Path) -> None:
        binding = make_test_binding(tmp_path, driver="obstore")

        fs = create_filesystem(binding)

        assert isinstance(fs, ObstoreFilesystem)
        assert not isinstance(fs, InstrumentedFilesystem)

    def test_instrumented_filesystem_records_metrics_through_create_filesystem(
        self, tmp_path: Path
    ) -> None:
        binding = make_test_binding(tmp_path, driver="fsspec")
        storage_uri = import_module("firecube.core.storage.uri").StorageUri

        target = storage_uri.from_local_path(tmp_path / "product.zarr" / "sample.bin")

        with collect_filesystem_metrics() as metrics:
            fs = create_filesystem(binding)
            fs.makedirs(target.parent(), exist_ok=True)
            with fs.open(target, "wb") as handle:
                handle.write(b"hello-instrumented")
            assert fs.exists(target)
            with fs.open(target, "rb") as handle:
                _ = handle.read()

        summary = metrics.as_summary()
        assert summary["storage_client_requests"] > 0
        assert summary["storage_client_errors"] == 0
        assert summary["storage_client_bytes_written"] >= len(b"hello-instrumented")
        assert summary["storage_client_bytes_read"] >= len(b"hello-instrumented")

    def test_read_bytes_records_bytes_read(self, tmp_path: Path) -> None:
        binding = make_test_binding(tmp_path, driver="fsspec")
        storage_uri = import_module("firecube.core.storage.uri").StorageUri
        target = storage_uri.from_local_path(tmp_path / "product.zarr" / "zarr.json")

        with collect_filesystem_metrics() as metrics:
            fs = create_filesystem(binding)
            fs.makedirs(target.parent(), exist_ok=True)
            with fs.open(target, "wb") as handle:
                handle.write(b'{"zarr_format": 3}')
            payload = fs.read_bytes(target)

        assert payload == b'{"zarr_format": 3}'
        summary = metrics.as_summary()
        assert summary["storage_client_bytes_read"] >= len(b'{"zarr_format": 3}')

    def test_session_fs_picks_up_instrumentation_when_metrics_become_active_after_first_call(
        self, tmp_path: Path
    ) -> None:
        """Regression for V2-fix-followup: StorageSession.fs() must dynamically wrap with
        InstrumentedFilesystem when metrics become active AFTER the first fs() call.

        Bug: preflight (session.exists()) calls fs() before collect_filesystem_metrics()
        starts. Plain adapter is cached. Subsequent ops inside metrics context record 0.
        """
        storage_session = import_module("firecube.core.storage.session").StorageSession
        storage_uri = import_module("firecube.core.storage.uri").StorageUri

        binding = make_test_binding(tmp_path, driver="fsspec")
        session = storage_session(binding)

        # Preflight-style call BEFORE metrics context
        fs1 = session.fs()
        assert not isinstance(fs1, InstrumentedFilesystem)

        target = storage_uri.from_local_path(tmp_path / "product.zarr" / "test.bin")

        # Inside metrics context — fs() must dynamically wrap
        with collect_filesystem_metrics() as metrics:
            fs2 = session.fs()
            assert isinstance(fs2, InstrumentedFilesystem), (
                f"Expected InstrumentedFilesystem inside metrics context, got {type(fs2)}. "
                "Cached non-instrumented adapter being reused — V2-fix-followup regression."
            )

            fs2.makedirs(target.parent(), exist_ok=True)
            with fs2.open(target, "wb") as handle:
                handle.write(b"v2fix-followup")
            assert fs2.exists(target)

        summary = metrics.as_summary()
        assert summary["storage_client_requests"] > 0, summary
        assert summary["storage_client_bytes_written"] >= len(b"v2fix-followup"), summary

    def test_session_upload_tree_records_metrics_with_parallel_workers(
        self, tmp_path: Path
    ) -> None:
        """Regression for V2-fix-followup-2: StorageSession.upload_tree() with parallel_workers>1
        must propagate filesystem metrics context into worker threads.

        Bug: ThreadPoolExecutor workers call create_filesystem() in their own threads,
        but contextvars (used by active_filesystem_metrics) don't auto-propagate to
        executor threads. Workers see no metrics context and skip InstrumentedFilesystem
        wrapping, so requests/bytes go uncounted.
        """
        storage_session = import_module("firecube.core.storage.session").StorageSession
        storage_uri = import_module("firecube.core.storage.uri").StorageUri

        # Set up a source dir with multiple files (to trigger parallel path: len(files) > 2)
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        n_files = 4
        for i in range(n_files):
            (src_dir / f"file_{i}.bin").write_bytes(f"content-{i}".encode())

        binding = make_test_binding(tmp_path / "dst", driver="fsspec")
        session = storage_session(binding)

        src_uri = storage_uri.from_local_path(src_dir)
        dst_uri = storage_uri.from_local_path(tmp_path / "dst" / "uploaded")

        with collect_filesystem_metrics() as metrics:
            result = session.upload_tree(
                src_uri,
                dst_uri,
                parallel_workers=4,  # > 1 triggers the ThreadPoolExecutor path
            )

        assert result.files_written == n_files

        summary = metrics.as_summary()
        assert summary["storage_client_requests"] > 0, (
            f"upload_tree(parallel_workers=4) recorded zero metrics: {summary}. "
            "ThreadPoolExecutor workers are not seeing the metrics context — "
            "contextvars need explicit propagation via copy_context()."
        )
        assert summary["storage_client_bytes_written"] > 0, summary
