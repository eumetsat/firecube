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

"""Validation logic for ingestor configuration contracts."""

from __future__ import annotations

from typing import Any


def validate_config_collisions(
    base: dict[str, Any], new: dict[str, Any], scope: str = "Configuration"
) -> None:
    """Check for key collisions between two configuration dictionaries.

    Args:
        base: The existing configuration dictionary.
        new: The new configuration dictionary to merge or check.
        scope: A descriptive name for the scope being validated (e.g., "Engine options").

    Raises:
        TypeError: If a key in `new` already exists in `base`.
    """
    intersection = set(base.keys()) & set(new.keys())
    if intersection:
        keys = ", ".join(sorted(str(k) for k in intersection))
        raise TypeError(f"Configuration key collision ({scope}): {keys}")


def declares_new_abstract_member(cls: type[Any]) -> bool:
    """Return True when a subclass explicitly introduces an abstract hook."""
    return any(getattr(member, "__isabstractmethod__", False) for member in cls.__dict__.values())


def validate_product_name_contract(cls: type[Any]) -> None:
    """Require PRODUCT_NAME on concrete plugin subclasses at class definition time."""
    product_name = getattr(cls, "PRODUCT_NAME", None)
    if declares_new_abstract_member(cls) or (isinstance(product_name, str) and product_name):
        return
    raise TypeError(
        f"{cls.__module__}.{cls.__name__} must declare PRODUCT_NAME: ClassVar[str] "
        f"(non-empty string). This is the canonical logical name used in .firecube/ "
        f"control-plane records, manifests, and telemetry. Add: PRODUCT_NAME = 'my_product_name'"
    )
