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

import os
import socket
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from firecube.core.discovery import SourceItem
from firecube.core.errors import FirecubeError
from firecube.ingestor.runtime.workspace import LocalSourceFile, WorkspaceManager


@pytest.mark.unit
def test_workspace_setup_does_not_mutate_global_temp_settings(temp_workspace):
    manager = WorkspaceManager(prefix="unit_ws")
    ctx = SimpleNamespace(options={"workspace": str(temp_workspace / "ws")})

    original_tempdir = tempfile.tempdir
    original_env_tmpdir = os.environ.get("TMPDIR")

    workspace_root = manager.setup(ctx)

    try:
        assert workspace_root == Path(ctx.options["workspace"])
        assert tempfile.tempdir == original_tempdir
        assert os.environ.get("TMPDIR") == original_env_tmpdir
    finally:
        manager.teardown(cleanup_dir=False)


@pytest.mark.integration
@pytest.mark.parametrize(
    "kind",
    ["file_uri", "source_item_path", "source_item_file_uri", "local_source_file_uri"],
)
def test_materialize_returns_the_file_a_local_href_names(tmp_path, kind):
    data = tmp_path / "small.nc"
    data.write_bytes(b"payload")
    href = f"file://{data.resolve()}"
    source = {
        "file_uri": href,
        "source_item_path": SourceItem(href=str(data.resolve()), scheme="stac"),
        "source_item_file_uri": SourceItem(href=href, scheme="stac"),
        "local_source_file_uri": LocalSourceFile(href),
    }[kind]
    manager = WorkspaceManager(prefix="unit_ws")
    manager._configure_temp_root(tmp_path / "ws")

    assert manager.materialize(source) == data.resolve()


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["gopher", "malformed_s3", "https"])
def test_materialize_refuses_a_scheme_it_cannot_open(tmp_path, kind):
    manager = WorkspaceManager(prefix="unit_ws")
    workspace = tmp_path / "ws"
    manager._configure_temp_root(workspace)

    if kind == "gopher":
        href = "gopher://x/y"
        with pytest.raises(FirecubeError) as caught:
            manager.materialize(href)
        assert "Firecube cannot open gopher:// sources" in str(caught.value)
        assert href in str(caught.value)
        assert not [p for p in workspace.rglob("*") if p.is_file()]
    elif kind == "malformed_s3":
        with pytest.raises(ValueError) as caught:
            manager.materialize("s3://bucket/key?x=1")
        assert "cannot open s3:// sources" not in str(caught.value)
    else:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        # fsspec's HTTP client raises aiohttp's ClientConnectorError (an OSError)
        # whose os_error is the refused connection.
        with pytest.raises(OSError) as caught:
            manager.materialize(f"https://127.0.0.1:{port}/a.nc")
        assert isinstance(getattr(caught.value, "os_error", None), ConnectionRefusedError)
        assert "cannot open https:// sources" not in str(caught.value)
        assert "unsupported protocol" not in str(caught.value)


@pytest.mark.unit
def test_temporary_directory_is_scoped_under_workspace_root(temp_workspace):
    manager = WorkspaceManager(prefix="unit_ws")
    ctx = SimpleNamespace(options={"workspace": str(temp_workspace / "ws")})
    workspace_root = manager.setup(ctx)

    try:
        with manager.temporary_directory() as tmp:
            assert Path(tmp).parent == workspace_root
    finally:
        manager.teardown(cleanup_dir=False)
