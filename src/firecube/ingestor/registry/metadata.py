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

"""
Shared helpers for surfacing plugin metadata to CLI and API layers.
"""

from __future__ import annotations

from dataclasses import dataclass

from firecube.ingestor.registry.loader import AVAILABLE_INGESTORS, discover_ingestors, get_ingestor


@dataclass
class PluginDescriptor:
    name: str
    module: str
    description: str | None
    products: dict[str, object]
    defaults: dict[str, object] | None
    required_options: dict[str, object] | None
    product_name: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "module": self.module,
            "description": self.description,
            "products": self.products,
            "defaults": self.defaults,
            "required_options": self.required_options,
            "product_name": self.product_name,
        }


def _ensure_loaded() -> None:
    discover_ingestors()


def list_plugins() -> list[str]:
    _ensure_loaded()
    return sorted(AVAILABLE_INGESTORS.keys())


def get_plugin_descriptor(plugin: str) -> PluginDescriptor:
    _ensure_loaded()
    cls = get_ingestor(plugin)
    description = (cls.__doc__ or "").strip() or None

    products: dict[str, object] = {}
    spec = getattr(cls, "spec", None)
    if spec:
        for attr in ("product", "variable", "file_glob"):
            value = getattr(spec, attr, None)
            if value:
                products[attr] = value

    defaults = None
    if hasattr(cls, "plugin_defaults"):
        defaults = cls.plugin_defaults()  # type: ignore[attr-defined]

    required = None
    if hasattr(cls, "required_options"):
        required = cls.required_options()  # type: ignore[attr-defined]

    resolved_product_name = getattr(cls, "PRODUCT_NAME", None)
    product_name = (
        resolved_product_name
        if isinstance(resolved_product_name, str) and resolved_product_name
        else None
    )

    return PluginDescriptor(
        name=plugin,
        module=cls.__module__,
        description=description,
        products=products,
        defaults=defaults,
        required_options=required,
        product_name=product_name,
    )


def list_plugin_descriptors() -> list[PluginDescriptor]:
    return [get_plugin_descriptor(name) for name in list_plugins()]
