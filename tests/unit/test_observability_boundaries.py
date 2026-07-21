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

"""Architectural lints that pin the observability boundary.

PERMANENT: allowlist entries are permanent exceptions only. Format:
``'src/path.py': PERMANENT: <reason>``. Empty allowlists are the correct
post-Wave-2 state.

Three boundary categories are enforced here:

1. **OpenTelemetry** — raw (``import opentelemetry...``) and dynamic
   (``importlib.import_module("opentelemetry...")``) imports must live inside
   ``src/firecube/core/observability/`` only. Everywhere else consumes the
   facade :mod:`firecube.core.observability`.
2. **Prometheus client** — raw and dynamic ``prometheus_client`` imports must
   live inside ``src/firecube/core/observability/telemetry/`` only. The
   Pushgateway sink is the single owner of the Prometheus client API; all
   metric emission elsewhere goes through the observability facade.
3. **Logging handler configuration** — ``logging.basicConfig``,
   ``logging.config.dictConfig``, ``addHandler``/``setLevel`` calls, and direct
   instantiation of ``StreamHandler``/``FileHandler``/``RotatingFileHandler``
   must live inside ``src/firecube/core/observability/`` only. Everywhere else
   uses :func:`logging.getLogger` and the resulting logger's ``info``,
   ``debug``, ``warning``, ``error``, and ``exception`` methods, which are
   read-only with respect to handler configuration.
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
_OBSERVABILITY_BOUNDARY_PREFIX = "src/firecube/core/observability/"
_TELEMETRY_BOUNDARY_PREFIX = "src/firecube/core/observability/telemetry/"

# PERMANENT: empty allowlist is the correct post-Wave-2 state.
_PERMANENT_ALLOWLIST_OTEL: frozenset[str] = frozenset()

# PERMANENT: no allowlist — dynamic OTel imports defeat the static lint.
_DYNAMIC_OTEL_IMPORT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"""importlib\.import_module\(["']opentelemetry"""),
    re.compile(r"""__import__\(["']opentelemetry"""),
)

# PERMANENT: empty allowlist — Prometheus client is confined to the Pushgateway
# sink under ``src/firecube/core/observability/telemetry/``.
_PERMANENT_ALLOWLIST_PROMETHEUS: frozenset[str] = frozenset()

# PERMANENT: no allowlist — dynamic Prometheus imports defeat the static lint.
_DYNAMIC_PROMETHEUS_IMPORT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"""importlib\.import_module\(["']prometheus_client"""),
    re.compile(r"""__import__\(["']prometheus_client"""),
)

# PERMANENT: empty allowlist — logging handler/level configuration is confined
# to ``src/firecube/core/observability/logging.py``.
_PERMANENT_ALLOWLIST_LOGGING_CONFIG: frozenset[str] = frozenset()

_FORBIDDEN_LOGGING_METHOD_ATTRS: frozenset[str] = frozenset(
    {"basicConfig", "dictConfig", "addHandler", "removeHandler", "setLevel"}
)

_FORBIDDEN_LOGGING_HANDLER_CLASSES: frozenset[str] = frozenset(
    {"StreamHandler", "FileHandler", "RotatingFileHandler", "TimedRotatingFileHandler"}
)


def _scan_file_for_module_imports(
    filepath: str | Path, module_root: str
) -> Iterator[tuple[int, str]]:
    """Yield ``(lineno, raw_import_string)`` for every ``import`` /
    ``from ... import`` whose top-level module equals ``module_root`` or starts
    with ``module_root + '.'``. Files with syntax errors yield nothing."""
    path = Path(filepath)
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return

    prefix = module_root + "."
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == module_root or alias.name.startswith(prefix):
                    suffix = f" as {alias.asname}" if alias.asname else ""
                    yield node.lineno, f"import {alias.name}{suffix}"
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == module_root or module.startswith(prefix):
                names = ", ".join(
                    f"{alias.name} as {alias.asname}" if alias.asname else alias.name
                    for alias in node.names
                )
                yield node.lineno, f"from {module} import {names}"


def test_no_raw_opentelemetry_imports() -> None:
    """Architectural lint: raw ``opentelemetry`` imports must live inside
    ``src/firecube/core/observability/`` only.

    The OpenTelemetry SDK is an implementation detail of the observability
    package; all other modules must depend on the public observability facade
    (``firecube.core.observability``) instead of importing OTel directly.
    """
    offenders: list[str] = []
    for path in sorted(_SRC_ROOT.rglob("*.py")):
        rel_path = path.relative_to(_REPO_ROOT).as_posix()
        if rel_path.startswith(_OBSERVABILITY_BOUNDARY_PREFIX):
            continue
        if rel_path in _PERMANENT_ALLOWLIST_OTEL:
            continue
        for lineno, raw in _scan_file_for_module_imports(path, "opentelemetry"):
            offenders.append(f"{rel_path}:{lineno}: forbidden raw opentelemetry import: {raw}")

    if offenders:
        offenders.sort()
        pytest.fail(
            "Forbidden raw `opentelemetry` imports outside "
            "`src/firecube/core/observability/`:\n" + "\n".join(offenders)
        )


def test_no_dynamic_opentelemetry_imports() -> None:
    """Architectural lint: dynamic ``opentelemetry`` imports are forbidden.

    ``importlib.import_module("opentelemetry...")`` and
    ``__import__("opentelemetry...")`` defeat the static-import boundary check
    above and are never an acceptable workaround. This test has NO allowlist
    and MUST pass immediately — Wave 1 confirmed zero existing violations.
    """
    offenders: list[str] = []
    for path in sorted(_SRC_ROOT.rglob("*.py")):
        rel_path = path.relative_to(_REPO_ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for pattern in _DYNAMIC_OTEL_IMPORT_PATTERNS:
                if pattern.search(line):
                    offenders.append(
                        f"{rel_path}:{lineno}: forbidden dynamic opentelemetry "
                        f"import: {line.strip()}"
                    )
                    break

    if offenders:
        offenders.sort()
        pytest.fail(
            "Forbidden dynamic `opentelemetry` imports in `src/firecube/`:\n" + "\n".join(offenders)
        )


def test_no_raw_prometheus_client_imports() -> None:
    """Architectural lint: raw ``prometheus_client`` imports must live inside
    ``src/firecube/core/observability/telemetry/`` only.

    Prometheus client types (``CollectorRegistry``, ``Counter``, ``Gauge``,
    ``push_to_gateway``, etc.) are an implementation detail of the Pushgateway
    sink. All other modules emit metrics through the observability facade
    instead of importing ``prometheus_client`` directly.
    """
    offenders: list[str] = []
    for path in sorted(_SRC_ROOT.rglob("*.py")):
        rel_path = path.relative_to(_REPO_ROOT).as_posix()
        if rel_path.startswith(_TELEMETRY_BOUNDARY_PREFIX):
            continue
        if rel_path in _PERMANENT_ALLOWLIST_PROMETHEUS:
            continue
        for lineno, raw in _scan_file_for_module_imports(path, "prometheus_client"):
            offenders.append(f"{rel_path}:{lineno}: forbidden raw prometheus_client import: {raw}")

    if offenders:
        offenders.sort()
        pytest.fail(
            "Forbidden raw `prometheus_client` imports outside "
            "`src/firecube/core/observability/telemetry/`:\n" + "\n".join(offenders)
        )


def test_no_dynamic_prometheus_client_imports() -> None:
    """Architectural lint: dynamic ``prometheus_client`` imports are forbidden.

    ``importlib.import_module("prometheus_client...")`` and
    ``__import__("prometheus_client...")`` defeat the static-import boundary
    check above and are never an acceptable workaround. This test has NO
    allowlist and MUST pass immediately.
    """
    offenders: list[str] = []
    for path in sorted(_SRC_ROOT.rglob("*.py")):
        rel_path = path.relative_to(_REPO_ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for pattern in _DYNAMIC_PROMETHEUS_IMPORT_PATTERNS:
                if pattern.search(line):
                    offenders.append(
                        f"{rel_path}:{lineno}: forbidden dynamic prometheus_client "
                        f"import: {line.strip()}"
                    )
                    break

    if offenders:
        offenders.sort()
        pytest.fail(
            "Forbidden dynamic `prometheus_client` imports in `src/firecube/`:\n"
            + "\n".join(offenders)
        )


def _scan_file_for_logging_config_mutations(
    filepath: str | Path,
) -> Iterator[tuple[int, str]]:
    """Yield ``(lineno, snippet)`` for every call site that mutates logging
    configuration in ``filepath``.

    Detection rules (AST-based):

    * Method-attribute calls whose ``.attr`` is in
      :data:`_FORBIDDEN_LOGGING_METHOD_ATTRS` — covers
      ``logging.basicConfig(...)``, ``logging.config.dictConfig(...)``,
      ``some_logger.addHandler(...)``, ``some_logger.setLevel(...)``,
      regardless of the receiver expression. ``logger.info(...)``,
      ``logger.debug(...)``, etc. are NOT flagged because their attribute
      names are not in the forbidden set.
    * Handler-class constructions — either ``ast.Name`` whose ``.id`` is in
      :data:`_FORBIDDEN_LOGGING_HANDLER_CLASSES` (after a direct
      ``from logging import StreamHandler``), or ``ast.Attribute`` whose
      ``.attr`` is in that set (e.g. ``logging.StreamHandler(...)``).

    Files with syntax errors yield nothing.
    """
    path = Path(filepath)
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute):
            if (
                func.attr in _FORBIDDEN_LOGGING_METHOD_ATTRS
                or func.attr in _FORBIDDEN_LOGGING_HANDLER_CLASSES
            ):
                yield node.lineno, f"<call>.{func.attr}(...)"
        elif isinstance(func, ast.Name) and func.id in _FORBIDDEN_LOGGING_HANDLER_CLASSES:
            yield node.lineno, f"{func.id}(...)"


def test_no_logging_handler_config_outside_observability() -> None:
    """Architectural lint: logging handler/level mutation must live inside
    ``src/firecube/core/observability/`` only.

    The observability package owns the JSON formatter, root-handler wiring,
    and level configuration via :func:`configure_logging`. Code elsewhere is
    restricted to read-only logger usage — ``logging.getLogger(__name__)`` and
    the returned logger's emit methods (``info``, ``debug``, ``warning``,
    ``error``, ``exception``) — which do not mutate handler configuration.

    Forbidden constructs (detected via AST):

    * ``logging.basicConfig(...)`` / ``logging.config.dictConfig(...)``
    * ``logger.addHandler(...)`` / ``logger.removeHandler(...)`` /
      ``logger.setLevel(...)``
    * Direct instantiation of ``StreamHandler``, ``FileHandler``,
      ``RotatingFileHandler``, or ``TimedRotatingFileHandler``.
    """
    offenders: list[str] = []
    for path in sorted(_SRC_ROOT.rglob("*.py")):
        rel_path = path.relative_to(_REPO_ROOT).as_posix()
        if rel_path.startswith(_OBSERVABILITY_BOUNDARY_PREFIX):
            continue
        if rel_path in _PERMANENT_ALLOWLIST_LOGGING_CONFIG:
            continue
        for lineno, snippet in _scan_file_for_logging_config_mutations(path):
            offenders.append(f"{rel_path}:{lineno}: forbidden logging configuration: {snippet}")

    if offenders:
        offenders.sort()
        pytest.fail(
            "Forbidden logging handler/level configuration outside "
            "`src/firecube/core/observability/`:\n" + "\n".join(offenders)
        )
