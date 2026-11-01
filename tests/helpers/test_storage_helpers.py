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

"""Self-tests for ``tests/helpers/storage.py``.

Regression guards prevent re-introducing previously fixed test-helper bugs —
notably the ``assert_no_fsspec_bypass`` aliased-import blind spot
(Finding 4, T5.1).
"""

from __future__ import annotations

import ast
import importlib
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.helpers.rows import table_row
from tests.helpers.storage import (
    _OPEN_FSSPEC_URL_PATCH_TARGETS,
    assert_no_fsspec_bypass,
)


def test_assert_no_fsspec_bypass_catches_source_calls() -> None:
    """Sanity: the source binding (``ops._open_fsspec_url``) is patched."""
    from firecube.core.filesystem import ops

    with (
        pytest.raises(AssertionError, match=r"firecube\.core\.filesystem\.ops"),
        assert_no_fsspec_bypass(),
    ):
        ops._open_fsspec_url("file:///tmp/test")  # pyright: ignore[reportAttributeAccessIssue]


def test_assert_no_fsspec_bypass_catches_aliased_imports() -> None:
    """Regression guard for Finding 4 (T5.1): module-level aliased imports.

    Before T5.1, calling ``validation._open_fsspec_url(...)`` slipped through
    because the helper only patched ``ops._open_fsspec_url`` — but
    ``validation.py:20`` does ``from ...ops import _open_fsspec_url`` which
    creates an independent module-local binding unaffected by patching the
    source.
    """
    from firecube.core.zarr import validation as validation_module

    with (
        pytest.raises(AssertionError, match=r"firecube\.core\.zarr\.validation"),
        assert_no_fsspec_bypass(),
    ):
        validation_module._open_fsspec_url("file:///tmp/test")  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize("target", _OPEN_FSSPEC_URL_PATCH_TARGETS)
def test_assert_no_fsspec_bypass_target_is_importable(target: str) -> None:
    """Every patch target must resolve — guards against stale targets after refactor."""
    module_path, _, attr = target.rpartition(".")
    module = __import__(module_path, fromlist=[attr])
    assert hasattr(module, attr), (
        f"Patch target {target!r} not resolvable: "
        f"either the source moved or _OPEN_FSSPEC_URL_PATCH_TARGETS is stale."
    )


def test_no_silent_offenders_when_clean() -> None:
    """Sanity: empty-block usage MUST NOT raise."""
    with assert_no_fsspec_bypass():
        pass


def test_assert_no_fsspec_bypass_restores_all_bindings_after_lazy_import() -> None:
    """Regression guard for the lazy-import teardown bug.

    When a dependent module is not yet loaded, ``assert_no_fsspec_bypass``
    must not let ``mock.patch`` capture an already-active mock as original.
    """
    saved_modules = {key: value for key, value in sys.modules.items() if key.startswith("firecube")}

    for key in saved_modules:
        del sys.modules[key]

    try:
        with assert_no_fsspec_bypass():
            pass

        from firecube.core.filesystem.ops import _open_fsspec_url as real_fn  # pyright: ignore[reportAttributeAccessIssue]

        for target in _OPEN_FSSPEC_URL_PATCH_TARGETS:
            module_path, attr = target.rsplit(".", 1)
            module = importlib.import_module(module_path)
            binding = getattr(module, attr, None)
            if binding is None:
                continue
            assert not isinstance(binding, MagicMock), (
                f"{target} was left as MagicMock after assert_no_fsspec_bypass exit: {binding!r}"
            )
            if attr == "_open_fsspec_url":
                assert binding is real_fn
    finally:
        for key in list(sys.modules):
            if key.startswith("firecube"):
                del sys.modules[key]
        sys.modules.update(saved_modules)


def test_patch_targets_cover_all_module_level_imports() -> None:
    """Sentinel: every module-level import of ``_open_fsspec_url`` MUST be in patch targets.

    Without this, adding a new ``from firecube.core.filesystem.ops import _open_fsspec_url``
    (or ``from firecube.core.api import open_fsspec_url``) in production code
    silently re-introduces the F4 blind spot.

    Function-local imports are intentionally NOT covered here — they resolve at
    call-time and are caught by the source patch.
    """
    src_root = Path(__file__).resolve().parents[2] / "src" / "firecube"
    assert src_root.is_dir(), f"src tree not found at {src_root}"

    discovered: set[str] = set()
    for py_file in src_root.rglob("*.py"):
        tree = ast.parse(py_file.read_text(), filename=str(py_file))
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            for alias in node.names:
                bound_name = alias.asname or alias.name
                qualifies = (
                    node.module == "firecube.core.filesystem.ops"
                    and alias.name == "_open_fsspec_url"
                ) or (node.module == "firecube.core.api" and alias.name == "open_fsspec_url")
                if not qualifies:
                    continue
                rel = py_file.relative_to(src_root.parent).with_suffix("")
                module_dotted = ".".join(rel.parts)
                discovered.add(f"{module_dotted}.{bound_name}")

    missing = discovered - set(_OPEN_FSSPEC_URL_PATCH_TARGETS)
    assert not missing, (
        f"_OPEN_FSSPEC_URL_PATCH_TARGETS is missing {len(missing)} import "
        f"site(s) discovered in src/: {sorted(missing)}. "
        f"Add them to tests/helpers/storage.py to prevent the F4 blind spot."
    )


class _SwallowingSubtests:
    """Stand-in for the ``subtests`` fixture: records ids, swallows failures."""

    def __init__(self) -> None:
        self.ids: list[str] = []
        self.failed: list[str] = []
        self.errors: dict[str, Exception] = {}

    @contextmanager
    def test(self, msg: str):
        self.ids.append(msg)
        try:
            yield
        except Exception as exc:
            self.failed.append(msg)
            self.errors[msg] = exc


def test_table_row_gives_each_row_fresh_state(subtests, tmp_path, monkeypatch) -> None:
    """Rows get their own directory and patches, and a failing row does not stop the next."""
    probe = "FIRECUBE_TABLE_ROW_PROBE"
    monkeypatch.delenv(probe, raising=False)
    dirs: list[Path] = []

    with subtests.test(msg="fresh_dirs_differ_per_row"):
        for row_id in ("row one", "row/two"):
            with table_row(subtests, row_id, tmp_path) as (workdir, mp):
                assert mp is None
                assert workdir.is_dir()
                assert list(workdir.iterdir()) == []
                (workdir / "marker").write_text(row_id)
                dirs.append(workdir)
        assert dirs[0] != dirs[1]
        assert all(d.parent == tmp_path for d in dirs)

    with subtests.test(msg="monkeypatch_is_undone_between_rows"):
        seen: list[str | None] = []
        for row_id in ("patch", "observe"):
            with table_row(subtests, row_id, tmp_path, monkeypatch) as (_, mp):
                seen.append(os.environ.get(probe))
                if row_id == "patch":
                    assert mp is not None
                    mp.setenv(probe, "set-in-row-1")
                    assert os.environ[probe] == "set-in-row-1"
        assert seen == [None, None]
        assert probe not in os.environ

    with subtests.test(msg="failing_row_does_not_stop_later_rows"):
        stub = _SwallowingSubtests()
        ran: list[str] = []
        for row_id in ("bad", "after"):
            with table_row(stub, row_id, tmp_path, monkeypatch) as (_, mp):
                assert mp is not None
                mp.setenv(probe, row_id)
                ran.append(row_id)
                assert row_id != "bad"
        assert stub.ids == ["bad", "after"]
        assert stub.failed == ["bad"]
        assert ran == ["bad", "after"]
        assert probe not in os.environ

    with subtests.test(msg="distinct_ids_with_same_slug_get_distinct_dirs"):
        long_id = "x" * 300
        ids = ["a/b", "a_b", "\u00e9\u00e8", "\u4e2d\u6587", long_id]
        made: list[Path] = []
        for row_id in ids:
            with table_row(subtests, row_id, tmp_path) as (workdir, _):
                assert workdir.is_dir()
                made.append(workdir)
        assert len({d.name for d in made}) == len(ids)
        assert all(len(d.name) <= 51 for d in made)

    with subtests.test(msg="repeated_row_id_fails_inside_its_own_row"):
        stub = _SwallowingSubtests()
        ran = []
        for row_id in ("dup", "dup", "other"):
            with table_row(stub, row_id, tmp_path) as (workdir, _):
                ran.append(row_id)
        assert ran == ["dup", "dup", "other"]
        assert stub.failed == ["dup"]
        assert isinstance(stub.errors["dup"], FileExistsError)
