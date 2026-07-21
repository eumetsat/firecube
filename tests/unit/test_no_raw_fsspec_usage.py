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

"""Architectural lint: detect storage-abstraction bypass patterns in src/firecube.

Each bypass pattern below has either:
- NO allowlist (truly forbidden everywhere), OR
- a per-pattern allowlist where every entry MUST include a permanent
  justification naming the module's role as the canonical seam implementation.

Temporary allowlist entries are not accepted here. Permanent allowlist additions
(for canonical seam implementations) are the only acceptable exception and must
be marked
``PERMANENT:`` with a one-line rationale.

Patterns covered (W1.7):
  1. ``<obj>.fsspec_storage_options()``                   (no allowlist)
  2. ``zarr.open_group(<positional>, ...)``               (per-file allowlist;
                                                           keyword-only calls
                                                           such as ``store=`` or
                                                           ``**handle.zarr_kwargs()``
                                                           pass everywhere — they
                                                           already route through
                                                           the storage abstraction)
  3. ``xr.open_zarr(..., storage_options=...)``           (per-file allowlist)
  4. ``<obj>.to_zarr(..., storage_options=...)``          (per-file allowlist)
  5. ``fsspec.{filesystem,core.url_to_fs,get_mapper,open,url_to_fs}(...)``
                                                          (allow ``core/filesystem/`` only)
  6. ``infer_storage_options(...)`` /
     ``fsspec.utils.infer_storage_options(...)``          (allow ``core/uris.py``)
  7. ``con.execute("SET s3_*")``                          (allow DuckDB bridge)
  8. ``Path(<uri-like-param>)``                           (heuristic; line allowlist
                                                           plus ``# firecube: STORAGE-URI``
                                                           opt-out on the same line)
  9. Direct ``S3Storage(...)`` / ``LocalStorage(...)``     (forbidden outside
                                                           storage facade)
"""

from __future__ import annotations

import ast
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

_ZARR_ALIASES = frozenset({"zarr", "_zarr"})
_XARRAY_ALIASES = frozenset({"xr", "xarray"})
_URI_PARAM_NAMES = frozenset({"uri", "target", "source", "store_uri", "product_uri"})
_NOQA_TOKEN = "firecube: STORAGE-URI"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src" / "firecube"

_ALLOWED_FSSPEC_FILESYSTEM = frozenset(
    {
        # PERMANENT: this module IS the fsspec adapter implementation.
        (_SRC_ROOT / "core" / "filesystem" / "ops.py").resolve(),
    }
)

_FSSPEC_PERMANENT_ALLOWLIST = frozenset(
    {
        # PERMANENT: source-side workspace materializer for arbitrary external inputs.
        "src/firecube/ingestor/runtime/workspace.py",
        # PERMANENT: source-side discovery enumerator for external input locations.
        "src/firecube/core/formats/discovery.py",
        # PERMANENT: legacy URI-string adapter seam for `read_chunk_grid(uri)`,
        # `discover_groups(uri)`, and `group_exists(uri)`; the typed-fs entry
        # point `validate_group_with_fs(fs, store_uri, group)` is preferred for
        # new callers.
        "src/firecube/core/zarr/validation.py",
        # PERMANENT: `_has_controlplane_root(source)` legacy fallback for remote
        # sources that have not been migrated to the session-aware path.
        "src/firecube/core/tensogram/converter.py",
    }
)

_ALLOWED_INFER_STORAGE_OPTIONS = frozenset(
    {
        # PERMANENT: this module IS the URI parser using fsspec internals.
        (_SRC_ROOT / "core" / "uris.py").resolve(),
    }
)

_ALLOWED_ZARR_OPEN_GROUP = frozenset(
    {
        # PERMANENT: implements ``session.zarr.open_group(...)`` canonical seam.
        (_SRC_ROOT / "core" / "storage" / "session.py").resolve(),
        # PERMANENT: region writer is the canonical low-level zarr mutation seam.
        (_SRC_ROOT / "core" / "zarr" / "region_writer.py").resolve(),
        # PERMANENT: timestamp state module owns zarr state-array mutation.
        (_SRC_ROOT / "core" / "zarr" / "state.py").resolve(),
        # PERMANENT: zarr.open_group(store=opened_store) is the canonical
        # fallback for opened-store inputs to group_exists; not a URI bypass.
        (_SRC_ROOT / "core" / "zarr" / "validation.py").resolve(),
        # PERMANENT: opened-store object passed positionally — not a URI bypass.
        (_SRC_ROOT / "ingestor" / "runtime" / "zarr" / "append.py").resolve(),
    }
)

_ALLOWED_XR_OPEN_ZARR_STORAGE_OPTIONS = frozenset(
    {
        # PERMANENT: canonical seam implementation in ``_ZarrNamespace.open_dataset``.
        (_SRC_ROOT / "core" / "storage" / "session.py").resolve(),
    }
)

_ALLOWED_TO_ZARR_STORAGE_OPTIONS = frozenset(
    {
        # PERMANENT: canonical seam implementation in ``_ZarrNamespace.write_dataset``.
        (_SRC_ROOT / "core" / "storage" / "session.py").resolve(),
    }
)

_STORE_PARAM_ALLOWLIST: frozenset[tuple[str, str]] = frozenset()

_STORE_URI_STORAGE_OPTIONS_ALLOWLIST: frozenset[tuple[str, str]] = frozenset()

_ALLOWED_SET_S3 = frozenset(
    {
        # PERMANENT: canonical DuckDB bridge implementation.
        (_SRC_ROOT / "core" / "duckdb" / "bridge.py").resolve(),
        # PERMANENT: shared DuckDB helpers module is the documented utility seam.
        (_SRC_ROOT / "ingestor" / "utils" / "duckdb_utils.py").resolve(),
    }
)

_ALLOWED_PATH_URI_LINES = frozenset(
    {
        # PERMANENT: ``core/uris.py`` IS the URI->Path adapter for local protocols.
        ((_SRC_ROOT / "core" / "uris.py").resolve(), 119),
        # PERMANENT: local fast-path inside the fsspec adapter implementation.
        ((_SRC_ROOT / "core" / "filesystem" / "ops.py").resolve(), 321),
        # PERMANENT: product resolver normalizes already-typed absolute targets.
        ((_SRC_ROOT / "core" / "product" / "resolver.py").resolve(), 47),
        # PERMANENT: tensogram runtime strategy only accepts local archive targets.
        ((_SRC_ROOT / "ingestor" / "runtime" / "tensogram" / "strategy.py").resolve(), 40),
        # PERMANENT: IngestContext materializer fallback handles local file sources.
        ((_SRC_ROOT / "ingestor" / "types" / "context.py").resolve(), 179),
    }
)

# PERMANENT allowlist: canonical owners of `output_base_uri` plumbing.
# These modules are the legitimate seam where the base URI flows through
# (StorageUri / ProductTarget construction, write planners, runtime base).
# Other modules must use ProductTarget.resolve(...) or pre-resolved URIs.
_PERMANENT_OUTPUT_BASE_URI_OWNERS = frozenset(
    {
        # PERMANENT: ProductTarget owns the base-uri/product-name composition.
        "src/firecube/core/product/target.py",
        # PERMANENT: multires planner anchors derived levels under the base.
        "src/firecube/core/zarr/multires.py",
        # PERMANENT: BaseIngestor coordinates write/output URI derivation.
        "src/firecube/ingestor/runtime/base.py",
        # PERMANENT: engine plumbs the base into the run context.
        "src/firecube/ingestor/runtime/engine.py",
        # PERMANENT: resume guard reads the base from the chunk manager.
        "src/firecube/ingestor/runtime/resume_guard.py",
    }
)

# PERMANENT allowlist: callers of `derive_target_uri(storage_config)`.
# `derive_target_uri` is the canonical helper that turns a `StorageConfig`
# into a base URI; these modules legitimately need the storage-derived
# URI as the entry point for downstream resolution.
_PERMANENT_DERIVE_TARGET_URI_USERS = frozenset(
    {
        # PERMANENT: chunks subcommand bootstraps a ChunkManager from storage config.
        "src/firecube/cli/chunks/_manager.py",
        # PERMANENT: zarr store factory derives the base for new stores.
        "src/firecube/core/filesystem/store_factory.py",
        # PERMANENT: runtime context builder resolves target URI from storage config.
        "src/firecube/core/runtime.py",
        # PERMANENT: tensogram converter targets a storage-config-derived archive URI.
        "src/firecube/core/tensogram/converter.py",
        # PERMANENT: tensogram restore needs base URI to resolve archive sources.
        "src/firecube/core/tensogram/restore.py",
        # PERMANENT: multires uses the helper for the level-0 anchor.
        "src/firecube/core/zarr/multires.py",
    }
)

# PERMANENT allowlist: callers of `create_filesystem(config)` outside
# `core/filesystem/`. The factory itself lives inside the boundary;
# `storage_session.py` is the only public seam that wires it up.
_PERMANENT_CREATE_FILESYSTEM_USERS = frozenset(
    {
        # PERMANENT: StorageSession is the canonical wiring point for the factory.
        "src/firecube/core/storage/session.py",
        # PERMANENT: controlplane _helpers.open_controlplane_fs_cached uses create_filesystem(binding) for driver-aware control-plane filesystem construction — legitimate internal adapter.
        "src/firecube/core/controlplane/_helpers.py",
        # PERMANENT: control-plane codec reconstructs the product-local control plane via the driver-aware factory.
        "src/firecube/core/tensogram/controlplane_codec.py",
        # PERMANENT: intake parquet-catalog discovery reconstructs a per-call read filesystem from a derived StorageBinding — legitimate read-side seam (T3.4).
        "src/firecube/core/intake.py",
        # PERMANENT: scrub builds a ChunkManager filesystem from the session StorageBinding for driver-correct deletion (T3.4).
        "src/firecube/core/zarr/scrub.py",
        # PERMANENT: pre-write group-aware existing-cube dim verification needs read-only fs access before the write-domain StorageSession is established (cf18-compliance)
        "src/firecube/ingestor/runtime/zarr/existing_cube_check.py",
    }
)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse(path: Path) -> tuple[ast.AST, list[str]] | None:
    text = _read_text(path)
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError:
        return None
    return tree, text.splitlines()


def _attr_value_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return func.value.id
    return None


def _attr_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _has_keyword(node: ast.Call, name: str) -> bool:
    return any(isinstance(kw, ast.keyword) and kw.arg == name for kw in node.keywords)


def _first_arg_string_starts_with(node: ast.Call, prefix: str) -> bool:
    if not node.args:
        return False
    arg = node.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value.startswith(prefix)
    if isinstance(arg, ast.JoinedStr) and arg.values:
        head = arg.values[0]
        if isinstance(head, ast.Constant) and isinstance(head.value, str):
            return head.value.startswith(prefix)
    return False


def _is_path_call(node: ast.Call) -> bool:
    return isinstance(node.func, ast.Name) and node.func.id == "Path"


def _function_uri_params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    args = fn.args
    names: set[str] = set()
    for collection in (args.args, args.posonlyargs, args.kwonlyargs):
        for arg in collection:
            if arg.arg in _URI_PARAM_NAMES:
                names.add(arg.arg)
    if args.vararg and args.vararg.arg in _URI_PARAM_NAMES:
        names.add(args.vararg.arg)
    if args.kwarg and args.kwarg.arg in _URI_PARAM_NAMES:
        names.add(args.kwarg.arg)
    return names


def _is_string_literal(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return True
    if isinstance(node, ast.JoinedStr):
        return all(
            isinstance(value, ast.Constant) and isinstance(value.value, str)
            for value in node.values
        )
    return False


def _qualname(class_stack: list[str], function_stack: list[str]) -> str:
    parts = [*class_stack, *function_stack]
    return ".".join(parts) if parts else "<module>"


def _call_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _store_kw_violations(paths: list[Path]) -> list[str]:
    offenders: list[str] = []
    for path in paths:
        if path.is_dir():
            files = sorted(path.rglob("*.py"))
        else:
            files = [path]
        for file_path in files:
            parsed = _parse(file_path)
            if parsed is None:
                continue
            tree, _ = parsed
            imported_aliases: set[str] = set()
            rel_path = file_path.relative_to(_REPO_ROOT).as_posix()

            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if alias.name == "write_dataset_to_zarr":
                            imported_aliases.add(alias.asname or alias.name)

            class Visitor(ast.NodeVisitor):
                def __init__(self, rel_path: str, imported_aliases: set[str]) -> None:
                    self._rel_path = rel_path
                    self._imported_aliases = imported_aliases
                    self.class_stack: list[str] = []
                    self.function_stack: list[str] = []

                def visit_ClassDef(self, node: ast.ClassDef) -> None:
                    self.class_stack.append(node.name)
                    self.generic_visit(node)
                    self.class_stack.pop()

                def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                    self.function_stack.append(node.name)
                    self.generic_visit(node)
                    self.function_stack.pop()

                def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
                    self.function_stack.append(node.name)
                    self.generic_visit(node)
                    self.function_stack.pop()

                def visit_Call(self, node: ast.Call) -> None:
                    qualname = _qualname(self.class_stack, self.function_stack[-1:])
                    kw_names = {kw.arg for kw in node.keywords if kw.arg is not None}

                    if (
                        _call_name(node) in ({"write_dataset_to_zarr"} | self._imported_aliases)
                        and "store" in kw_names
                        and (self._rel_path, qualname) not in _STORE_PARAM_ALLOWLIST
                    ):
                        offenders.append(
                            f"{self._rel_path}:{node.lineno}: `{qualname}` uses legacy `store=` with `write_dataset_to_zarr(...)`."
                        )

                    if (
                        "store_uri" in kw_names
                        and "storage_options" in kw_names
                        and (self._rel_path, qualname) not in _STORE_URI_STORAGE_OPTIONS_ALLOWLIST
                    ):
                        offenders.append(
                            f"{self._rel_path}:{node.lineno}: `{qualname}` uses both `store_uri=` and `storage_options=` in one call."
                        )

                    self.generic_visit(node)

            Visitor(rel_path, imported_aliases).visit(tree)

    return offenders


def _write_temp_python_file(source: str) -> Path:
    with tempfile.NamedTemporaryFile("w", suffix=".py", dir=_SRC_ROOT, delete=False) as handle:
        handle.write(source)
        handle.flush()
        return Path(handle.name)


def _resolve_line_violations(
    root: Path,
    *,
    token: str,
    allowed_paths: frozenset[str],
    excluded_prefixes: tuple[str, ...] = (),
) -> tuple[list[str], list[str]]:
    violations: list[str] = []
    unexpected: list[str] = []
    for path in sorted(root.rglob("*.py")):
        rel_path = path.relative_to(_REPO_ROOT).as_posix()
        if excluded_prefixes and any(rel_path.startswith(prefix) for prefix in excluded_prefixes):
            continue

        text = _read_text(path)
        for lineno, line in enumerate(text.splitlines(), start=1):
            if token not in line:
                continue
            message = f"{rel_path}:{lineno}: contains `{token}`"
            violations.append(message)
            if rel_path not in allowed_paths:
                unexpected.append(message)
    return violations, unexpected


def test_no_storage_bypass_patterns() -> None:
    """Aggregated lint: gate every known storage-abstraction bypass pattern.

    Adding a new violation should fail this test until either the call site is
    routed through the storage abstraction or explicitly justified as a
    permanent canonical seam.
    """
    root = Path(__file__).resolve().parents[2] / "src" / "firecube"
    assert root.exists(), f"Expected source root to exist: {root}"

    fsspec_misc_allowed_dir = (root / "core" / "filesystem").resolve()

    offenders: list[str] = []

    for path in sorted(root.rglob("*.py")):
        resolved = path.resolve()
        parsed = _parse(path)
        if parsed is None:
            continue
        tree, source_lines = parsed

        offenders.extend(
            f"{path}:{fn.lineno}: raw product URI string splitter reintroduced "
            f"— use `ProductTarget.resolve(...)` / `StorageUri` instead."
            for fn in ast.walk(tree)
            if isinstance(fn, ast.FunctionDef) and fn.name == "_split_product_source"
        )

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            # Pattern 1: ``<obj>.fsspec_storage_options()`` — no allowlist.
            if _attr_name(node) == "fsspec_storage_options":
                offenders.append(
                    f"{path}:{node.lineno}: contains "
                    f"`<obj>.fsspec_storage_options()` — should never appear; "
                    f"use `fs_kwargs_for_uri(uri, storage_config)` instead."
                )

            if isinstance(node.func, ast.Name) and node.func.id in {"S3Storage", "LocalStorage"}:
                offenders.append(
                    f"{path}:{node.lineno}: direct `{node.func.id}(...)` construction "
                    f"outside the storage facade — use `StorageSession` / "
                    f"`create_filesystem(config)` instead."
                )

            # Pattern 2: ``zarr.open_group(<positional>, ...)`` outside the allowlist.
            # Keyword-only calls (``store=``, ``**handle.zarr_kwargs()``) already
            # route through the storage abstraction and are allowed everywhere.
            if (
                _attr_name(node) == "open_group"
                and _attr_value_name(node) in _ZARR_ALIASES
                and node.args
                and resolved not in _ALLOWED_ZARR_OPEN_GROUP
            ) or (
                _attr_name(node) == "open_group"
                and _attr_value_name(node) in _ZARR_ALIASES
                and _has_keyword(node, "store")
                and any(
                    isinstance(kw, ast.keyword)
                    and kw.arg == "store"
                    and _is_string_literal(kw.value)
                    for kw in node.keywords
                )
                and resolved not in _ALLOWED_ZARR_OPEN_GROUP
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw `zarr.open_group(<uri>, ...)` "
                    f"outside allowlist — use `create_zarr_store(...)` + "
                    f"`zarr.open_group(**handle.zarr_kwargs(), ...)` (or "
                    f"`session.zarr.open_group(...)`) instead."
                )

            # Pattern 3: ``xr.open_zarr(..., storage_options=...)`` outside the allowlist.
            if (
                _attr_name(node) == "open_zarr"
                and _attr_value_name(node) in _XARRAY_ALIASES
                and _has_keyword(node, "storage_options")
                and resolved not in _ALLOWED_XR_OPEN_ZARR_STORAGE_OPTIONS
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw "
                    f"`xr.open_zarr(..., storage_options=...)` outside allowlist "
                    f"— use `session.zarr.open_dataset(...)` instead."
                )

            # Pattern 4: ``<obj>.to_zarr(..., storage_options=...)`` outside the allowlist.
            if (
                _attr_name(node) == "to_zarr"
                and _has_keyword(node, "storage_options")
                and resolved not in _ALLOWED_TO_ZARR_STORAGE_OPTIONS
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw "
                    f"`<dataset>.to_zarr(..., storage_options=...)` outside "
                    f"allowlist — use `session.zarr.write_dataset(...)` instead."
                )

            # Pattern 5a: ``fsspec.filesystem(...)`` outside ``core/filesystem/ops.py``.
            if (
                _attr_name(node) == "filesystem"
                and _attr_value_name(node) == "fsspec"
                and resolved not in _ALLOWED_FSSPEC_FILESYSTEM
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw `fsspec.filesystem(...)` outside "
                    f"`core/filesystem/` — use `create_filesystem(config)` instead."
                )

            # Pattern 5a': ``fsspec.core.url_to_fs(...)`` outside ``core/filesystem/ops.py``.
            if (
                _attr_name(node) == "url_to_fs"
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "core"
                and isinstance(node.func.value.value, ast.Name)
                and node.func.value.value.id == "fsspec"
                and resolved not in _ALLOWED_FSSPEC_FILESYSTEM
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw `fsspec.core.url_to_fs(...)` "
                    f"outside `core/filesystem/` — use `open_fs(uri, ...)` instead."
                )

            # Pattern 5b: ``fsspec.{get_mapper,open,url_to_fs}(...)`` outside ``core/filesystem/``.
            if _attr_value_name(node) == "fsspec" and _attr_name(node) in {
                "get_mapper",
                "open",
                "url_to_fs",
            }:
                in_fs_dir = fsspec_misc_allowed_dir in resolved.parents
                if not in_fs_dir:
                    offenders.append(
                        f"{path}:{node.lineno}: raw "
                        f"`fsspec.{_attr_name(node)}(...)` outside "
                        f"`core/filesystem/` — use the storage abstraction."
                    )

            # Pattern 6a: bare ``infer_storage_options(...)`` outside ``core/uris.py``.
            func = node.func
            if (
                isinstance(func, ast.Name)
                and func.id == "infer_storage_options"
                and resolved not in _ALLOWED_INFER_STORAGE_OPTIONS
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw `infer_storage_options(...)` "
                    f"outside `core/uris.py` — use `parse_uri(...)` instead."
                )

            # Pattern 6b: ``fsspec.utils.infer_storage_options(...)`` outside ``core/uris.py``.
            if (
                _attr_name(node) == "infer_storage_options"
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "utils"
                and isinstance(node.func.value.value, ast.Name)
                and node.func.value.value.id == "fsspec"
                and resolved not in _ALLOWED_INFER_STORAGE_OPTIONS
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw "
                    f"`fsspec.utils.infer_storage_options(...)` outside "
                    f"`core/uris.py` — use `parse_uri(...)` instead."
                )

            # Pattern 7: ``con.execute("SET s3_*")`` outside the DuckDB bridge.
            if (
                _attr_name(node) == "execute"
                and _first_arg_string_starts_with(node, "SET s3_")
                and resolved not in _ALLOWED_SET_S3
            ):
                offenders.append(
                    f"{path}:{node.lineno}: raw DuckDB `SET s3_*` outside the "
                    f"allowlisted bridge module — route through "
                    f"`session.duckdb.apply(con)` instead."
                )

        flagged: set[int] = set()
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            param_names = _function_uri_params(fn)
            if not param_names:
                continue
            for sub in ast.walk(fn):
                if not isinstance(sub, ast.Call) or not _is_path_call(sub):
                    continue
                if id(sub) in flagged:
                    continue
                if not sub.args:
                    continue
                first = sub.args[0]
                if not (isinstance(first, ast.Name) and first.id in param_names):
                    continue

                lineno = sub.lineno
                idx = lineno - 1
                if 0 <= idx < len(source_lines) and _NOQA_TOKEN in source_lines[idx]:
                    flagged.add(id(sub))
                    continue
                if (resolved, lineno) in _ALLOWED_PATH_URI_LINES:
                    flagged.add(id(sub))
                    continue

                flagged.add(id(sub))
                offenders.append(
                    f"{path}:{lineno}: heuristic "
                    f"`Path(<uri-param '{first.id}'>)` — add "
                    f"`# firecube: STORAGE-URI` if intentional, otherwise route "
                    f"through `session.uri.parse(...)` / typed `StorageUri`."
                )

    if offenders:
        offenders.sort()
        msg = "Found forbidden storage-bypass patterns:\n" + "\n".join(offenders)
        raise AssertionError(msg)


_FORBIDDEN_OPEN_FSSPEC_NAMES = frozenset({"_open_fsspec_url", "open_fsspec_url"})


def _collect_open_fsspec_url_offenders(paths: list[Path]) -> list[str]:
    """Detect forbidden ``_open_fsspec_url`` / ``open_fsspec_url`` usage.

    Both the canonical underscored name and the legacy public alias
    (``open_fsspec_url``, no underscore) are flagged. The same adapter
    boundary and ``_FSSPEC_PERMANENT_ALLOWLIST`` apply to both forms.
    """
    offenders: list[str] = []
    for path in paths:
        files = sorted(path.rglob("*.py")) if path.is_dir() else [path]
        for file_path in files:
            rel_path = file_path.relative_to(_REPO_ROOT).as_posix()
            if (
                rel_path.startswith("src/firecube/core/filesystem/")
                or rel_path == "src/firecube/core/storage/session.py"
            ):
                continue
            if rel_path in _FSSPEC_PERMANENT_ALLOWLIST:
                continue

            parsed = _parse(file_path)
            if parsed is None:
                continue

            tree, _ = parsed
            for node in ast.walk(tree):
                matched_name: str | None = None

                if isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if (
                            alias.name in _FORBIDDEN_OPEN_FSSPEC_NAMES
                            or alias.asname == "open_fsspec_url"
                        ):
                            matched_name = alias.asname or alias.name
                            break
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.endswith("._open_fsspec_url") or alias.name.endswith(
                            ".open_fsspec_url"
                        ):
                            matched_name = alias.name
                            break
                elif isinstance(node, ast.Call):
                    func = node.func
                    if isinstance(func, ast.Name) and func.id in _FORBIDDEN_OPEN_FSSPEC_NAMES:
                        matched_name = func.id
                    elif (
                        isinstance(func, ast.Attribute)
                        and func.attr in _FORBIDDEN_OPEN_FSSPEC_NAMES
                    ):
                        matched_name = func.attr

                if matched_name is not None:
                    offenders.append(
                        f"{rel_path}:{getattr(node, 'lineno', 1)}: `{matched_name}` usage is "
                        f"outside the approved adapter boundary; route through "
                        f"`core/filesystem/` or `core/storage/session.py`."
                    )
    return offenders


def test_open_fsspec_url_strict() -> None:
    """Strict lint (T4.5): `_open_fsspec_url`/`open_fsspec_url` is banned outside adapters.

    Adapter boundary: `src/firecube/core/filesystem/`, `src/firecube/core/storage/session.py`,
    plus the modules listed in `_FSSPEC_PERMANENT_ALLOWLIST` (each documented with
    a `PERMANENT:` rationale). Any other call site or import — under either the
    canonical underscored name or the legacy public alias — fails this test.
    """
    offenders = _collect_open_fsspec_url_offenders([_SRC_ROOT])
    if offenders:
        offenders.sort()
        pytest.fail(
            "Forbidden `_open_fsspec_url`/`open_fsspec_url` usage:\n" + "\n".join(offenders)
        )


def test_no_legacy_store_param_in_src() -> None:
    assert not _store_kw_violations([_SRC_ROOT])

    bad_file = _write_temp_python_file(
        "from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr\n"
        "write_dataset_to_zarr(ds, store=store_handle)\n"
    )
    try:
        offenders = _store_kw_violations([bad_file])
        assert offenders, "Expected legacy `store=` usage to be detected"
    finally:
        bad_file.unlink(missing_ok=True)

    good_file = _write_temp_python_file(
        "from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr\n"
        "write_dataset_to_zarr(ds, zarr_store=zarr_store)\n"
    )
    try:
        assert not _store_kw_violations([good_file])
    finally:
        good_file.unlink(missing_ok=True)


def test_no_store_uri_storage_options_cooccurrence() -> None:
    assert not _store_kw_violations([_SRC_ROOT])

    bad_file = _write_temp_python_file(
        "from firecube.ingestor.runtime.zarr.append import read_chunk_grid\n"
        "read_chunk_grid(store_uri=store_uri, storage_options=storage_options)\n"
    )
    try:
        offenders = _store_kw_violations([bad_file])
        assert offenders, "Expected `store_uri=` + `storage_options=` usage to be detected"
    finally:
        bad_file.unlink(missing_ok=True)

    good_file = _write_temp_python_file(
        "from firecube.ingestor.runtime.zarr.append import read_chunk_grid\n"
        "read_chunk_grid(store_uri=store_uri)\n"
    )
    try:
        assert not _store_kw_violations([good_file])
    finally:
        good_file.unlink(missing_ok=True)


def test_no_output_base_uri_references_outside_boundary_modules() -> None:
    root = _SRC_ROOT
    _, unexpected = _resolve_line_violations(
        root,
        token="output_base_uri",
        allowed_paths=_PERMANENT_OUTPUT_BASE_URI_OWNERS,
        excluded_prefixes=(
            "src/firecube/cli/",
            "src/firecube/core/product_resolver.py",
            "src/firecube/core/storage_uri.py",
        ),
    )
    assert not unexpected, "\n".join(["Unexpected output_base_uri references:", *unexpected])


def test_no_derive_target_uri_references() -> None:
    root = _SRC_ROOT
    _, unexpected = _resolve_line_violations(
        root,
        token="derive_target_uri",
        allowed_paths=_PERMANENT_DERIVE_TARGET_URI_USERS,
        excluded_prefixes=("src/firecube/core/config.py",),
    )
    assert not unexpected, "\n".join(["Unexpected derive_target_uri references:", *unexpected])


def test_no_raw_create_filesystem_calls_outside_filesystem_boundary() -> None:
    root = _SRC_ROOT
    _, unexpected = _resolve_line_violations(
        root,
        token="create_filesystem(",
        allowed_paths=_PERMANENT_CREATE_FILESYSTEM_USERS,
        excluded_prefixes=("src/firecube/core/filesystem/",),
    )
    assert not unexpected, "\n".join(["Unexpected create_filesystem references:", *unexpected])
