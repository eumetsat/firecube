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

"""Plugin registry and discovery for Firecube ingestors.

Core responsibilities:
  - Provide the @register_ingestor decorator for PipelinedIngestor subclasses.
  - Keep an in-memory mapping of available ingestors by name.
  - Discover built-in plugins under firecube.ingestor.*
  - Discover external plugins via entry points (group: 'firecube.plugins').

Non-goals:

"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable
from importlib.metadata import entry_points
from types import ModuleType
from typing import TypeVar

from firecube.ingestor.contracts.interfaces import Ingestor

AVAILABLE_INGESTORS: dict[str, type[Ingestor]] = {}
_LOADED: bool = False
_IngestorT = TypeVar("_IngestorT", bound=Ingestor)


def register_ingestor(name: str) -> Callable[[type[_IngestorT]], type[_IngestorT]]:
    """Register an ingestor class under a public plugin name.

    Applied as a class decorator. The decorated class is added to the
    registry consulted by ``discover_ingestors()`` and ``get_ingestor()``,
    and its ``name`` attribute is set to ``name``.

    Args:
        name: Public name the plugin is looked up by, e.g. the value passed
            to ``firecube ingest <name>``.

    Returns:
        A class decorator that registers the class and returns it unchanged.

    Raises:
        TypeError: If the decorated class does not satisfy the ``Ingestor``
            protocol. Raised when the decorator is applied, not at call time.

    Examples:
        Register a plugin under the name used by ``firecube ingest``:

            @register_ingestor("my_product")
            class MyProductIngestor(GenericZarrIngestor):
                PRODUCT_NAME = "my_product"

                def build_dataset(self, group, items, ctx):
                    ...
    """

    def decorator(cls: type[_IngestorT]) -> type[_IngestorT]:
        # Using runtime protocol check
        if not issubclass(cls, Ingestor):
            raise TypeError("Ingestor must satisfy the Ingestor protocol")

        AVAILABLE_INGESTORS[name] = cls
        cls.name = name  # pyright: ignore[reportAttributeAccessIssue]
        return cls

    return decorator


def get_ingestor(name: str) -> type[Ingestor]:
    """Retrieve a registered ingestor class by name.

    Args:
        name: The name used in @register_ingestor.

    Returns:
        The registered Ingestor class.

    Raises:
        KeyError: If the ingestor is not found.
    """

    try:
        return AVAILABLE_INGESTORS[name]
    except KeyError as exc:
        raise KeyError(f"Ingestor '{name}' not registered") from exc


def discover_ingestors() -> dict[str, type[Ingestor]]:
    """Import all plugin modules so that decorators can register them.

    Discovery strategy:
      1) If already loaded, return cached copy.
      2) Load entry points from group 'firecube.plugins'.
         This triggers module imports, ensuring @register_ingestor runs.
      3) If FIRECUBE_LOAD_BUILTIN_PLUGINS=1 is set, fallback to walking
         the `firecube.plugins` namespace.
    """
    global _LOADED
    if _LOADED:
        return AVAILABLE_INGESTORS.copy()

    import os

    # 1. Entry Points (Standard)
    group = "firecube.plugins"
    eps = entry_points(group=group)

    from firecube.ingestor.registry.version_compat import warn_if_incompatible

    for ep in eps:
        try:
            ep.load()
        except Exception as e:
            import logging

            logging.getLogger("firecube.registry").warning(
                f"Failed to load plugin entry point '{ep.name}': {e}"
            )
            continue

        registered = AVAILABLE_INGESTORS.get(ep.name)
        if registered is not None:
            warn_if_incompatible(ep.name, registered)

    # 2. Legacy Fallback (Dev-only)
    if os.environ.get("FIRECUBE_LOAD_BUILTIN_PLUGINS") == "1":
        try:
            import firecube.plugins as plugins_pkg  # pyright: ignore[reportMissingImports]
        except ImportError:
            plugins_pkg = None

        def _import_submodules(package: ModuleType, prefix: str) -> None:
            for mod_info in pkgutil.iter_modules(package.__path__):
                full_name = f"{prefix}.{mod_info.name}"
                try:
                    module = importlib.import_module(full_name)
                    if mod_info.ispkg:
                        _import_submodules(module, full_name)
                except Exception:
                    pass

        if plugins_pkg is not None:
            _import_submodules(plugins_pkg, plugins_pkg.__name__)

    _LOADED = True
    return AVAILABLE_INGESTORS.copy()


def reset_plugin_discovery_cache() -> None:
    """Invalidate the plugin discovery cache.

    Used by tests and plugin management CLI commands to force re-discovery.
    """
    global _LOADED
    AVAILABLE_INGESTORS.clear()
    _LOADED = False
