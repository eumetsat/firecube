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
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from firecube.ingestor.runtime.workspace import WorkspaceManager


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
