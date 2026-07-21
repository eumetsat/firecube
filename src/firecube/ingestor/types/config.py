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

"""Configuration infrastructure for Firecube plugins.

This module provides the `PluginConfig` base class, which enables strict options validation,
type conversion, and help generation for plugins.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, ClassVar, TypeVar, get_type_hints

T = TypeVar("T", bound="PluginConfig")


@dataclass
class PluginConfig:
    """Base configuration for all Firecube ingestors.

    Subclasses should define fields as dataclass fields.
    Use `from_options` to parse and validate a raw dictionary.

    Attributes:
        _allow_unknown: Class-level escape hatch for plugins that intentionally
            accept unknown option keys. Keep this ``False`` for strict validation.
    """

    # Common options for all ingestors
    # Engine fields have been moved to EngineConfig.
    # PluginConfig should only contain user-defined domain logic fields.

    # If the subclass wants to allow unknown keys, set this to True.
    # Generally false for strictness.
    _allow_unknown: ClassVar[bool] = False

    @classmethod
    def from_options(cls: type[T], options: dict[str, Any]) -> T:
        """Create a config instance from a dictionary, with strict validation.

        Args:
            options: Dictionary of options (e.g. from CLI or config file).

        Returns:
            An instance of the config class.

        Raises:
            ValueError: If unknown keys are present (and _allow_unknown is False)
                        or if type conversion fails.
        """
        known_fields = {f.name: f for f in fields(cls)}
        known_keys = set(known_fields.keys())
        input_keys = set(options.keys())

        # 1. Unknown Key Check
        if not cls._allow_unknown:
            unknown = input_keys - known_keys
            if unknown:
                # Filter out likely system keys that might be implicitly passed?
                # For now, strict means strict.
                raise ValueError(
                    f"Unknown configuration keys for {cls.__name__}: {', '.join(sorted(unknown))}. "
                    f"Valid keys: {', '.join(sorted(known_keys))}"
                )

        # 2. Type Conversion / Construction
        # We only pass known keys to the constructor
        from firecube.ingestor.config.coercion import coerce_cli_value

        type_hints = get_type_hints(cls)
        init_kwargs = {}
        for key, raw_value in options.items():
            if key not in known_keys:
                continue
            target_type = type_hints.get(key)
            init_kwargs[key] = coerce_cli_value(raw_value, target_type, key)
        return cls(**init_kwargs)
