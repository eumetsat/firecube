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
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

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

        from firecube.core.filesystem.ops import _open_fsspec_url as real_fn  # pyright: ignore[reportAttributeAccessIssue]  # noqa: I001

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
