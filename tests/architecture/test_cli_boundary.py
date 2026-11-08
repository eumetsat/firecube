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

"""Architectural gate for the CLI boundary, with a tagged allowlist of today's debt.

Seven checks walk ``src/firecube`` with :mod:`ast`:

1. Click is imported only below ``src/firecube/cli/``.
2. ``firecube.cli`` is imported only below ``src/firecube/cli/``.
3. Runtime strings in ``core/`` and ``ingestor/`` (raised, logged, warned or
   inside an f-string) do not name CLI commands or flags.
4. ``cli/`` modules import no private ``firecube.core`` / ``firecube.ingestor``
   module (a path component with one leading underscore). Private *names* are
   covered by ``test_cli_no_private_cross_subsystem_imports.py``.
5. Every ``cli/`` module stays at or under ``CLI_MODULE_LINE_CEILING`` lines.
6. Only ``cli/_uri_policy.py`` parses URIs; other ``cli/`` calls are rows keyed by function.
7. ``cli/`` imports from ``firecube`` only ``firecube.cli``, ``firecube.operations``,
   ``firecube.core.api``, ``firecube.ingestor.api`` and ``firecube.core.errors``; imports
   under ``TYPE_CHECKING`` and inside functions count. Other imports are rows.

Existing debt is listed in the row tables below. Each row carries a tag,
``PERMANENT: <reason>`` or ``UNTIL <owner>: <reason>``. Rows only shrink by
edit: a row that matches nothing, or whose count, tokens or cap differ from
what the tree holds, fails its check and names the row to delete or correct.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src" / "firecube"
_CLI_ROOT = _SRC_ROOT / "cli"
CLI_MODULE_LINE_CEILING = 500

_TAG = re.compile(r"^(PERMANENT: \S.*|UNTIL [a-z0-9-]+: \S.*)$")
_CLI_FLAG = re.compile(r"--[a-z-]+")
_CLI_COMMAND = re.compile(r"firecube [a-z][a-z-]*")
_LOG_METHODS = frozenset(
    {"debug", "info", "warning", "warn", "error", "exception", "critical", "fatal", "log"}
)

# Row tables. Key -> tag.
_CLICK_ROWS: dict[str, str] = {}
_CLI_IMPORT_ROWS: dict[str, str] = {
    "src/firecube/__main__.py": "PERMANENT: python -m firecube is the CLI's own entry point",
}
# Text rows, one per line: "path below src/firecube | enclosing scope | sorted tokens".
_CLI_TEXT_PERMANENT = {
    "core/controlplane/manager.py | ChunkManager._apply_slot_model_precedence | firecube will": (
        "PERMANENT: prose, the phrase firecube will not adopt it names no command"
    ),
    "core/filesystem/_obstore_compat.py | require_obstore | --storage-driver": (
        "PERMANENT: dependency-install hint that names --storage-driver"
    ),
    "core/zarr/_reserved_root_attrs.py | assert_root_attrs_safe | firecube root": (
        "PERMANENT: prose, the phrase firecube root attribute(s) names no command"
    ),
}
_CLI_TEXT_ROWS: dict[tuple[str, str, tuple[str, ...]], str] = {
    ("src/firecube/" + p.strip(), sc.strip(), tuple(t.strip().split(", "))): tag
    for row, tag in _CLI_TEXT_PERMANENT.items()
    for p, sc, t in [row.split(" | ")]
}
# path -> tag; every CLI-text hit in the file is covered.
_CLI_TEXT_FILE_ROWS: dict[str, str] = {}
# (path, private module, import count) -> tag
_PRIVATE_IMPORT_ROWS: dict[tuple[str, str, int], str] = {}
# path -> (cap, tag)
_LINE_CEILING_ROWS: dict[str, tuple[int, str]] = {
    "src/firecube/cli/main.py": (963, "UNTIL unscheduled: module is over the ceiling"),
}
_URI_CALL = re.compile(
    r"(?:\w+\.)*(urlparse|urlsplit|parse_uri|infer_target_protocol|is_remote_target|StorageUri\.parse"
    r"|local_path_from_target|storage_uri_from_target|parse_target)\(|.*\.(removeprefix)\('file://'\)$"
)
_URI_PARSE_ROWS = {  # "path below src/firecube | qualified scope | callee xN" -> tag
    "cli/chunks/_manager.py | _storage_uri_from_base | StorageUri.parse x1": (
        "PERMANENT: parses the base URI of the [storage] config, not a CLI value"
    ),
}
_PUBLIC_SURFACE = frozenset(
    {
        "firecube.cli",
        "firecube.operations",
        "firecube.core.api",
        "firecube.ingestor.api",
        "firecube.core.errors",
    }
)
# One row per file and module: "path below src/firecube/cli | imported module | reason".
_SURFACE_PHASE_B = """
advise.py | firecube.core.cf | advise on the facade
advise.py | firecube.core.cf.validator | advise on the facade
archive/_inspect.py | firecube.core.archive.tensogram.inspection | inspect dispatch
archive/_inspect.py | firecube.core.archive.zip.archive | inspect dispatch
chunks/_delete.py | firecube.core.controlplane.types | chunks multi-store
chunks/_list.py | firecube.core.controlplane.types | chunks multi-store
chunks/_manager.py | firecube.core.config | chunks multi-store
chunks/_manager.py | firecube.core.product.identity | chunks multi-store
chunks/_manager.py | firecube.core.runtime | chunks multi-store
index.py | firecube.core.time.calendar | derived coordinates
index.py | firecube.core.zarr.time_decode | derived coordinates
main.py | firecube.core.config | ingest on the facade
main.py | firecube.core.observability | ingest on the facade
main.py | firecube.core.product.resolver | ingest on the facade
main.py | firecube.ingestor.registry.loader | ingest on the facade
main.py | firecube.ingestor.runtime.configure | ingest on the facade
parquet.py | firecube.core.filesystem | parquet on the facade
"""
_SURFACE_UNSCHEDULED = """
_ctx.py | firecube.core.config | config
_ctx.py | firecube.core.runtime | config
_errors.py | firecube.core.observability.logging | observability
_errors.py | firecube.ingestor.runtime.preflight | error classes
_input_filters.py | firecube.core.config | config
_input_filters.py | firecube.core.discovery | source admission
_input_sources.py | firecube.core.discovery | source admission
_typed_options.py | firecube.core.discovery | source admission
_typed_options.py | firecube.ingestor.config.coercion | plugin option tooling
_typed_options.py | firecube.ingestor.config.engine | plugin option tooling
_typed_options.py | firecube.ingestor.errors | error classes
_uri_policy.py | firecube.core.discovery | source admission
advise.py | firecube.core.observability | observability
archive/__init__.py | firecube.core.observability | observability
archive/_create.py | firecube.core.config | config
catalog.py | firecube.core.intake | catalog
catalog.py | firecube.core.observability | observability
catalog.py | firecube.ingestor.registry.loader | plugin registry
chunks/__init__.py | firecube.core.observability | observability
chunks/_claims.py | firecube.core.controlplane.types | claim rendering types
chunks/_runs.py | firecube.core.controlplane.types | abandon-sweep rendering
index.py | firecube.ingestor.registry.loader | plugin registry
index.py | firecube.ingestor.runtime.index_record | IndexRecordError
migrate/__init__.py | firecube.core.observability | observability
parquet.py | firecube.core.observability | observability
plugins/commands.py | firecube.core.observability | observability
plugins/commands.py | firecube.ingestor.devtools.scaffolding | plugin tooling
plugins/commands.py | firecube.ingestor.registry.metadata | plugin registry
plugins/introspect.py | firecube.ingestor.registry.metadata | plugin registry
plugins/mgmt.py | firecube.ingestor.registry.loader | plugin registry
plugins/registry.py | firecube.ingestor.config.engine | plugin option tooling
plugins/registry.py | firecube.ingestor.registry.loader | plugin registry
release/__init__.py | firecube.core.observability | observability
snapshot/__init__.py | firecube.core.observability | observability
snapshot/_render.py | firecube.core.controlplane.metadata_snapshots | render types
snapshot/_render.py | firecube.core.zarr.metadata_documents | render types
zarr/__init__.py | firecube.core.observability | observability
zarr/_common.py | firecube.ingestor.errors | error classes
zarr/_common.py | firecube.ingestor.runtime.configure | configure_ingestor
zarr/_multires.py | firecube.core.zarr.layers | default multires resolutions
zarr/_preallocate.py | firecube.ingestor.registry.loader | plugin registry
zarr/_preallocate.py | firecube.ingestor.runtime.preallocate | SlotWindowError
zarr/_preallocate.py | firecube.ingestor.runtime.producer | capture_producer
zarr/_preallocate.py | firecube.ingestor.templates.direct_zarr | DirectZarrIngestor type check
zarr/_slots.py | firecube.core.config | config
zarr/_slots.py | firecube.ingestor.registry.loader | plugin registry
zarr/_slots.py | firecube.ingestor.runtime.slot_plan | slot-plan errors, PLAN_RANGE_CEILING
zarr/_slots.py | firecube.ingestor.templates.direct_zarr | DirectZarrIngestor type check
zarr/_validate.py | firecube.core.zarr.validation | validation
"""
_SURFACE_ROWS: dict[str, str] = {
    f"{path} | {module}": f"UNTIL {owner}: {why}"
    for owner, table in (("phase-b", _SURFACE_PHASE_B), ("unscheduled", _SURFACE_UNSCHEDULED))
    for path, module, why in (row.split(" | ") for row in table.strip().splitlines())
}


def _rel(path: Path) -> str:
    return path.relative_to(_REPO_ROOT).as_posix()


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _py_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.py"))


def _below_cli(path: Path) -> bool:
    return _CLI_ROOT in path.parents


def _imported_modules(
    path: Path, tree: ast.Module, *, submodules: bool = True
) -> Iterator[tuple[int, str]]:
    """Yield ``(lineno, dotted module)`` for every import, relative ones resolved.

    With ``submodules``, ``from pkg import name`` also yields ``pkg.name`` when
    that is a module file or package under ``src/firecube``; other names are
    symbols, not modules.
    """
    package = list(path.relative_to(_SRC_ROOT.parent).with_suffix("").parts)[:-1]
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - (node.level - 1)]
                parts = base + (node.module.split(".") if node.module else [])
            else:
                parts = (node.module or "").split(".")
            module = ".".join(parts)
            yield node.lineno, module
            if submodules:
                for alias in node.names:
                    target = f"{module}.{alias.name}"
                    base = _SRC_ROOT.joinpath(*target.split(".")[1:])
                    if target.startswith("firecube.") and (
                        base.with_suffix(".py").is_file() or (base / "__init__.py").is_file()
                    ):
                        yield node.lineno, target


def _dynamic_import_targets(tree: ast.Module) -> Iterator[tuple[int, str]]:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        first = node.args[0]
        if (
            name in {"import_module", "__import__"}
            and isinstance(first, ast.Constant)
            and isinstance(first.value, str)
        ):
            yield node.lineno, first.value


def _fail(title: str, offenders: list[str], stale: list[str]) -> None:
    lines = [*sorted(offenders), *sorted(stale)]
    if lines:
        pytest.fail(f"{title}:\n" + "\n".join(lines), pytrace=False)


def _stale_keys(rows: dict, found: set, label: str) -> list[str]:
    return [
        f"stale row, delete it: {label} {key!r} matches nothing" for key in rows if key not in found
    ]


def test_no_click_below_cli() -> None:
    offenders: list[str] = []
    found: set[str] = set()
    for path in _py_files(_SRC_ROOT):
        if _below_cli(path):
            continue
        rel = _rel(path)
        tree = _parse(path)
        hits = [
            (node.lineno, f"import {alias.name}")
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
            if alias.name == "click" or alias.name.startswith("click.")
        ]
        hits += [
            (node.lineno, f"from {node.module} import ...")
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.level == 0
            and node.module
            and (node.module == "click" or node.module.startswith("click."))
        ]
        hits += [
            (lineno, f"dynamic import of {target!r}")
            for lineno, target in _dynamic_import_targets(tree)
            if target.startswith("click")
        ]
        if hits:
            found.add(rel)
        if hits and rel not in _CLICK_ROWS:
            offenders += [f"{rel}:{lineno}: Click outside cli/: {code}" for lineno, code in hits]
    _fail(
        "Click is imported outside src/firecube/cli/",
        offenders,
        _stale_keys(_CLICK_ROWS, found, "Click path"),
    )


def test_no_firecube_cli_import_below_cli() -> None:
    offenders: list[str] = []
    found: set[str] = set()

    def is_cli(module: str) -> bool:
        return module == "firecube.cli" or module.startswith("firecube.cli.")

    for path in _py_files(_SRC_ROOT):
        if _below_cli(path):
            continue
        rel = _rel(path)
        tree = _parse(path)
        hits = {(n, m) for n, m in _imported_modules(path, tree) if is_cli(m)}
        hits |= {(n, t) for n, t in _dynamic_import_targets(tree) if is_cli(t)}
        if hits:
            found.add(rel)
        if hits and rel not in _CLI_IMPORT_ROWS:
            offenders += [f"{rel}:{n}: imports {m} outside cli/" for n, m in sorted(hits)]
    _fail(
        "firecube.cli is imported outside src/firecube/cli/",
        offenders,
        _stale_keys(_CLI_IMPORT_ROWS, found, "firecube.cli path"),
    )


def _cli_text_sites(path: Path) -> Iterator[tuple[int, str, str, list[str]]]:
    """Yield ``(lineno, qualified scope, literal part, tokens)`` for CLI text in runtime strings."""
    tree = _parse(path)
    parents: dict[ast.AST, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node

    def is_docstring(const: ast.Constant) -> bool:
        stmt = parents.get(const)
        owner = parents.get(stmt) if stmt is not None else None
        return (
            isinstance(stmt, ast.Expr)
            and isinstance(
                owner, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
            )
            and owner.body[0] is stmt
        )

    def is_logging_call(node: ast.AST) -> bool:
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            return False
        if node.func.attr not in _LOG_METHODS:
            return False
        target = node.func.value
        name = target.id if isinstance(target, ast.Name) else getattr(target, "attr", "")
        name = name.lstrip("_").lower()
        return name in {"logging", "warnings"} or name.endswith(("log", "logger"))

    def scope_of(node: ast.AST) -> str:
        names: list[str] = []
        cur = parents.get(node)
        while cur is not None:
            if isinstance(cur, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                names.append(cur.name)
            cur = parents.get(cur)
        return ".".join(reversed(names)) or "<module>"

    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        tokens = _CLI_FLAG.findall(node.value) + _CLI_COMMAND.findall(node.value)
        if not tokens or is_docstring(node):
            continue
        cur = parents.get(node)
        while cur is not None and not isinstance(
            cur, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda
        ):
            if isinstance(cur, ast.Raise | ast.JoinedStr) or is_logging_call(cur):
                yield node.lineno, scope_of(node), node.value, tokens
                break
            cur = parents.get(cur)


def test_no_cli_text_in_runtime_strings() -> None:
    found: dict[tuple[str, str], list[str]] = {}
    first_site: dict[tuple[str, str], str] = {}
    file_hits: set[str] = set()
    for root in (_SRC_ROOT / "core", _SRC_ROOT / "ingestor"):
        for path in _py_files(root):
            rel = _rel(path)
            for lineno, scope, text, tokens in _cli_text_sites(path):
                file_hits.add(rel)
                found.setdefault((rel, scope), []).extend(tokens)
                first_site.setdefault(
                    (rel, scope), f"{rel}:{lineno}: CLI text in {scope}: {text[:80]!r}"
                )
    pinned = {(rel, scope, tokens) for rel, scope, tokens in _CLI_TEXT_ROWS}
    offenders = [
        site
        for key, site in first_site.items()
        if key[0] not in _CLI_TEXT_FILE_ROWS and (*key, tuple(sorted(found[key]))) not in pinned
    ]
    stale: list[str] = []
    for rel, scope, tokens in _CLI_TEXT_ROWS:
        actual = tuple(sorted(found.get((rel, scope), [])))
        if not actual:
            stale.append(f"stale row, delete it: CLI text {(rel, scope)!r} matches nothing")
        elif actual != tokens:
            stale.append(
                f"stale row, correct its tokens: {(rel, scope)!r} has {actual!r}, row says {tokens!r}"
            )
    stale += _stale_keys(_CLI_TEXT_FILE_ROWS, file_hits, "CLI text file")
    _fail("CLI command or flag names in core/ or ingestor/ runtime strings", offenders, stale)


def test_cli_imports_no_private_module() -> None:
    offenders: list[str] = []
    found: dict[tuple[str, str], int] = {}
    for path in _py_files(_CLI_ROOT):
        rel = _rel(path)
        tree = _parse(path)
        counts: dict[str, int] = {}
        # Modules only (aliases that name a module count): private names are another test's concern.
        for _lineno, module in _imported_modules(path, tree):
            parts = module.split(".")
            if parts[:2] in (["firecube", "core"], ["firecube", "ingestor"]) and any(
                p.startswith("_") and not p.startswith("__") for p in parts[2:]
            ):
                counts[module] = counts.get(module, 0) + 1
        for module, count in counts.items():
            found[(rel, module)] = count
            if (rel, module, count) not in _PRIVATE_IMPORT_ROWS:
                offenders.append(f"{rel}: imports private module {module} x{count}")
    stale: list[str] = []
    for rel, module, count in _PRIVATE_IMPORT_ROWS:
        actual = found.get((rel, module))
        if actual is None:
            stale.append(f"stale row, delete it: private import {(rel, module)!r} matches nothing")
        elif actual != count:
            stale.append(
                f"stale row, correct its count: {(rel, module)!r} has {actual}, row says {count}"
            )
    _fail("cli/ imports private firecube modules", offenders, stale)


def test_cli_modules_stay_under_line_ceiling() -> None:
    offenders: list[str] = []
    stale: list[str] = []
    seen: set[str] = set()
    for path in _py_files(_CLI_ROOT):
        rel = _rel(path)
        seen.add(rel)
        lines = len(path.read_text(encoding="utf-8").splitlines())
        row = _LINE_CEILING_ROWS.get(rel)
        if row is None:
            if lines > CLI_MODULE_LINE_CEILING:
                offenders.append(f"{rel}: {lines} lines, ceiling {CLI_MODULE_LINE_CEILING}")
        elif lines <= CLI_MODULE_LINE_CEILING:
            stale.append(f"stale row, delete it: {rel} has {lines} lines, at or under the ceiling")
        elif lines > row[0]:
            offenders.append(f"{rel}: {lines} lines, over its cap {row[0]}")
    stale += _stale_keys(_LINE_CEILING_ROWS, seen, "line ceiling path")
    _fail("cli/ modules over the line ceiling", offenders, stale)


def _uri_calls(node: ast.AST, scope: str = "<module>") -> Iterator[tuple[str, str]]:
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.Call) and (hit := _URI_CALL.match(ast.unparse(child))):
            yield scope, hit[1] or hit[2]
        inner = scope
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            inner = child.name if scope == "<module>" else f"{scope}.{child.name}"
        yield from _uri_calls(child, inner)


def test_cli_parses_uris_only_in_uri_policy() -> None:
    calls = [
        (path.relative_to(_SRC_ROOT).as_posix(), scope, name)
        for path in _py_files(_CLI_ROOT)
        if path != _CLI_ROOT / "_uri_policy.py"
        for scope, name in _uri_calls(_parse(path))
    ]
    found = {f"{w} | {s} | {n} x{calls.count((w, s, n))}" for w, s, n in calls}
    stale = [f"stale row, delete or correct it: {row!r}" for row in _URI_PARSE_ROWS.keys() - found]
    _fail("URI parsing outside cli/_uri_policy.py", sorted(found - _URI_PARSE_ROWS.keys()), stale)


def _is_exit_code_member(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "ExitCode"
    )


def _exit_argument_is_typed(path: Path, node: ast.expr) -> bool:
    """Return whether an exit argument is an `ExitCode` member or a form that stays typed."""
    if _is_exit_code_member(node):
        return True
    if isinstance(node, ast.IfExp):  # parquet.py chooses between two members
        return _is_exit_code_member(node.body) and _is_exit_code_member(node.orelse)
    # _errors.py passes its `code: ExitCode` parameter
    return path.name == "_errors.py" and isinstance(node, ast.Name) and node.id == "code"


def _exit_sites(path: Path) -> Iterator[tuple[int, str]]:
    for node in ast.walk(_parse(path)):
        if isinstance(node, ast.Name) and node.id in {"SystemExit", "exit", "quit"}:
            yield node.lineno, f"uses {node.id}"
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name == "exit":
            yield node.lineno, "calls .exit()"
        elif name == "Exit" and not (node.args and _exit_argument_is_typed(path, node.args[0])):
            yield node.lineno, "Exit() argument is not an ExitCode member"
        elif name == "exit_with_error" and not _is_exit_code_member(node.args[-1]):
            yield node.lineno, "exit_with_error() code is not an ExitCode member"


def test_cli_exits_only_through_exit_code() -> None:
    offenders = [
        f"{_rel(path)}:{line} {what}"
        for path in _py_files(_CLI_ROOT)
        for line, what in _exit_sites(path)
    ]
    _fail("cli/ must end a command with click.exceptions.Exit(ExitCode.X)", offenders, [])


def test_cli_imports_only_the_public_surface() -> None:
    found: set[str] = set()
    for path in _py_files(_CLI_ROOT):
        tree = _parse(path)
        targets = [t for _, t in _dynamic_import_targets(tree)]
        for node in ast.walk(tree):
            if isinstance(node, ast.Import | ast.ImportFrom):
                mods = [m for _, m in _imported_modules(path, ast.Module([node], []))]
                # `from pkg import a, b`: pkg itself counts when any name is a symbol
                symbol = any(f"{mods[0]}.{a.name}" not in mods for a in node.names)
                targets += mods if symbol else mods[1:]
        for module in targets:
            if module.split(".")[0] != "firecube" or module.startswith("firecube.cli."):
                continue
            if module not in _PUBLIC_SURFACE:
                found.add(f"{path.relative_to(_CLI_ROOT).as_posix()} | {module}")
    _fail(
        "cli/ imports firecube beyond the public surface (row key: path | module)",
        [
            f"not allowed, add a tagged row or use the facade: {key!r}"
            for key in found - _SURFACE_ROWS.keys()
        ],
        _stale_keys(_SURFACE_ROWS, found, "public-surface import"),
    )


def test_allowlist_tags_are_well_formed() -> None:
    tags = [
        *_SURFACE_ROWS.values(),
        *_CLICK_ROWS.values(),
        *_CLI_IMPORT_ROWS.values(),
        *_CLI_TEXT_ROWS.values(),
        *_CLI_TEXT_FILE_ROWS.values(),
        *_PRIVATE_IMPORT_ROWS.values(),
        *_URI_PARSE_ROWS.values(),
        *(tag for _cap, tag in _LINE_CEILING_ROWS.values()),
    ]
    bad = [f"malformed tag: {tag!r}" for tag in tags if not _TAG.match(tag)]
    _fail("allowlist tags must be 'PERMANENT: <reason>' or 'UNTIL <owner>: <reason>'", bad, [])
