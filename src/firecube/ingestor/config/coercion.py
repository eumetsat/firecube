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

"""Shared CLI value coercion for config dataclasses.

Used by EngineConfig, TemplateConfig, and PluginConfig from_options() methods.
"""

from __future__ import annotations

import json
from typing import Any, get_args, get_origin


def _unwrap_optional(t: Any) -> tuple[Any, bool]:
    """Return (inner_type, is_optional).

    Unwraps Optional[X] / X | None to get the real type.
    Returns (t, False) if not optional.
    """
    # Handle both Union[X, None] (typing) and X | None (types.UnionType)
    args = get_args(t)

    # Check for X | None or Optional[X] or Union[X, None]
    if args and type(None) in args:
        non_none = tuple(a for a in args if a is not type(None))
        if len(non_none) == 1:
            return non_none[0], True
    return t, False


def coerce_cli_value(value: Any, target_type: Any, field_name: str) -> Any:
    """Coerce a CLI string value to the expected Python type.

    Handles: str→bool, str→int, str→float, str→JSON dict/list,
    Optional[X] unwrapping, bool|str union, None/null strings.
    Uses typing.get_args()/get_origin() for robust type inspection.

    Args:
        value: Raw value from CLI (typically str, or already-parsed JSON)
        target_type: The Python type annotation for this field
        field_name: Field name for error messages

    Returns:
        Coerced value

    Raises:
        ValueError: If coercion fails
    """
    if target_type is None:
        return value

    # Step 1: Unwrap Optional[X] / X | None
    inner_type, is_optional = _unwrap_optional(target_type)

    # Step 2: Handle None values for optional fields
    if is_optional and isinstance(value, str) and value.lower() in ("none", "null", ""):
        return None
    if is_optional and value is None:
        return None

    # From here, work with inner_type
    t = inner_type
    origin = get_origin(t)
    args = get_args(t)

    # Step 3: Check for bool|str union (e.g. zarr_compression: bool | str)
    # bool|str means: coerce bool-like strings to bool, leave others as str
    if args and bool in args and str in args:
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in ("true", "1", "yes", "on"):
                return True
            if lowered in ("false", "0", "no", "off"):
                return False
            return value  # leave as str (e.g., "zstd", "gzip")
        return value

    # Step 4: dict or list — JSON decode
    if origin is dict or (isinstance(t, type) and issubclass(t, dict)):
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            if value.lower() in ("none", "null", ""):
                return None if is_optional else value
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON dict for {field_name}: {value!r}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(
                    f"Expected JSON dict for {field_name}, got {type(parsed).__name__}"
                )
            return parsed
        return value

    if origin is list or (isinstance(t, type) and issubclass(t, list)):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            if value.lower() in ("none", "null", ""):
                return None if is_optional else value
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON list for {field_name}: {value!r}") from exc
            if not isinstance(parsed, list):
                raise ValueError(
                    f"Expected JSON list for {field_name}, got {type(parsed).__name__}"
                )
            return parsed
        return value

    # Step 5: Pure bool
    if t is bool:
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in ("true", "1", "yes", "on"):
                return True
            if lowered in ("false", "0", "no", "off"):
                return False
            raise ValueError(
                f"Invalid boolean for {field_name}: {value!r}. Use true/false/yes/no/1/0."
            )
        return bool(value)

    # Step 6: int
    if t is int:
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError as exc:
                raise ValueError(f"Invalid integer for {field_name}: {value!r}") from exc
        return value

    # Step 7: float
    if t is float:
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError as exc:
                raise ValueError(f"Invalid float for {field_name}: {value!r}") from exc
        return value

    # Step 8: Passthrough (str, Any, unknown)
    return value
