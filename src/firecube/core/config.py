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

"""Centralised configuration loading for Firecube.

This module provides helpers to load a single user configuration file
and derive a StorageConfig (for API/chunks) plus per-plugin defaults.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click

try:  # Python 3.12+
    import tomllib  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - fallback for older interpreters
    tomllib = None  # type: ignore[assignment]


DATABASE_DUCKDB_KEYS = frozenset(
    {
        "duckdb_max_temp_directory_size",
        "duckdb_memory_limit",
        "duckdb_threads",
    }
)

KNOWN_TOP_LEVEL_SECTIONS = frozenset(
    {
        "storage",
        "database",
        "plugins",
        "archive",
        "metrics",
    }
)


@dataclass
class StorageConfig:
    """Global storage configuration for the service.

    Location-specific URI fields are intentionally excluded here; runtime
    storage code now consumes the trimmed config plus a separate target URI
    boundary where needed.

    Attributes:
        storage_type: Storage locality/class. Use ``"local"`` or ``"s3"``.
        endpoint_url: Optional S3-compatible endpoint URL.
        access_key: Optional access key for explicit S3 credentials.
        secret_key: Optional secret key for explicit S3 credentials.
        region: Optional S3 region name.
        path_style: Whether to use S3 path-style addressing.
        storage_driver: Storage I/O driver. Use ``"fsspec"`` or ``"obstore"``.
    """

    storage_type: str  # "local" or "s3"
    endpoint_url: str | None = None
    access_key: str | None = field(default=None, repr=False)
    secret_key: str | None = field(default=None, repr=False)
    region: str | None = None
    path_style: bool = True
    storage_driver: str = "fsspec"

    def validate(self) -> None:
        """Validate that required fields are present for the storage type."""
        if self.storage_type not in {"local", "s3"}:
            raise ValueError(f"Unknown storage type: {self.storage_type}")

        # Credentials are optional to support AWS ambient auth (instance role, IRSA, etc.).
        # If either key/secret is provided, require both to avoid half-configured clients.
        if (self.access_key is not None) != (self.secret_key is not None):
            raise ValueError(
                "StorageConfig requires both --access-key and --secret-key when using explicit credentials"
            )


def derive_target_uri(storage_config: StorageConfig) -> str:
    """Derive the legacy base target URI from a TOML-parsed StorageConfig.

    Free-function bridge between the ``[storage]`` TOML section and
    ``ProductTarget.resolve(...)``. Callers should pass the result (wrapped in
    ``StorageUri.parse(...)``) as ``default_base_uri=`` to ``ProductTarget``
    exactly once at the CLI/legacy boundary; everything downstream must work
    from the resulting ``ResolvedProduct`` instead of re-deriving the URI.

    Branches only on ``storage_type``: ``local`` reads ``target_path``;
    ``s3`` reads ``bucket``. Both attributes are populated by
    ``build_storage_config`` on the plain ``StorageConfig`` dataclass — no
    duck-typed ``target_uri`` short-circuit, no bridge subclass support.
    """
    if storage_config.storage_type == "local":
        target_path = getattr(storage_config, "target_path", None)
        if target_path is None:
            raise ValueError("Local StorageConfig must have target_path set")
        return target_path
    bucket = getattr(storage_config, "bucket", None)
    if bucket is None:
        raise ValueError("S3 StorageConfig must have bucket set")
    return f"s3://{bucket}"


def _default_config_path() -> Path:
    """Resolve default config path (~/.config/firecube/config.toml)."""
    env_path = os.getenv("FIRECUBE_CONFIG")
    if env_path:
        return Path(env_path).expanduser()
    return Path("~/.config/firecube/config.toml").expanduser()


def load_config_file(path: Path | str | None = None, *, strict: bool = False) -> dict[str, Any]:
    """Load the Firecube config file if present.

    Returns an empty dict when no config exists or TOML support is unavailable.

    When ``strict=True``, raises ``click.UsageError`` if the file declares any
    unknown top-level section. Internal callers leave ``strict=False`` so that
    config files shared with sibling firecube tools (which may define their own
    top-level sections) continue to load; the CLI entry point opts into strict
    mode so typos in user-facing config files surface at startup.
    """
    if path is None:
        path = _default_config_path()
    else:
        path = Path(path)

    if tomllib is None:
        if strict:
            raise click.UsageError("TOML support unavailable (Python 3.12+ required)")
        return {}

    if not path.exists():
        if strict:
            raise click.UsageError(f"Config file not found: {path}")
        return {}

    with path.open("rb") as handle:
        try:
            data = tomllib.load(handle)
        except Exception as exc:
            if strict:
                raise click.UsageError(f"Failed to parse config file {path}: {exc}") from exc
            return {}

    data = data or {}
    if strict:
        _reject_unknown_top_level_sections(data)
    return data


def _reject_unknown_top_level_sections(data: dict[str, Any]) -> None:
    unknown = [key for key in data if key not in KNOWN_TOP_LEVEL_SECTIONS]
    if unknown:
        raise click.UsageError(
            f"Unknown config section(s) {sorted(unknown)!r}. "
            f"Valid sections: {sorted(KNOWN_TOP_LEVEL_SECTIONS)}"
        )


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_env_placeholders(value: Any) -> Any:
    """Resolve ${VAR} placeholders in string values using the environment.

    This allows config files to reference environment variables, e.g.
    bucket = "${MY_BUCKET_NAME}". Returns the value unchanged if not a string.
    """
    if not isinstance(value, str):
        return value

    def _replace(match: re.Match) -> str:
        var = match.group(1)
        return os.getenv(var, match.group(0))

    return _ENV_PATTERN.sub(_replace, value)


def build_storage_config(
    cfg: dict[str, Any],
    env: Mapping[str, str],
    overrides: dict[str, Any],
) -> StorageConfig:
    """Build a StorageConfig from config file, environment, and CLI overrides.

    Precedence (highest first): CLI overrides → environment variables → config file → built-in defaults.
    """
    storage_cfg = cfg.get("storage", {}) or {}

    def _pick(
        key_cfg: str,
        env_keys: list[str],
        override_key: str | None = None,
    ) -> str | None:
        """Helper to resolve a config value from CLI, Env, or Config (in order)."""
        # CLI override wins if non-empty
        if override_key:
            val = overrides.get(override_key)
            if val not in (None, ""):
                return _resolve_env_placeholders(val)

        # Environment next
        for ek in env_keys:
            val = env.get(ek)
            if val not in (None, ""):
                return _resolve_env_placeholders(val)

        # Finally config file
        val = storage_cfg.get(key_cfg)
        if val not in (None, ""):
            return _resolve_env_placeholders(val)
        return None

    storage_type = _pick("type", ["FIRECUBE_STORAGE_TYPE"], "storage_type")
    if not storage_type:
        raise ValueError("Storage type must be provided via config, env, or CLI.")

    endpoint_url = _pick("endpoint_url", ["FIRECUBE_ENDPOINT_URL"], "endpoint_url")

    # Access/secret from Firecube-scoped env names only when not provided via CLI/config
    access_key = _pick(
        "access_key",
        ["FIRECUBE_ACCESS_KEY"],
        "access_key",
    )
    secret_key = _pick(
        "secret_key",
        ["FIRECUBE_SECRET_KEY"],
        "secret_key",
    )

    region = _pick("region", ["FIRECUBE_REGION"], "region")

    # path_style: bool with sane defaults
    if "path_style" in overrides and overrides["path_style"] is not None:
        path_style_val = bool(overrides["path_style"])
    else:
        env_val = env.get("FIRECUBE_PATH_STYLE")
        if env_val is not None:
            path_style_val = env_val.lower() in {"1", "true", "yes", "on"}
        else:
            cfg_val = storage_cfg.get("path_style", True)
            path_style_val = bool(cfg_val)

    storage_driver_val = (
        overrides.get("storage_driver")
        or env.get("FIRECUBE_STORAGE_DRIVER")
        or storage_cfg.get("driver")
        or "fsspec"
    )

    config = StorageConfig(
        storage_type=str(storage_type),
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        path_style=path_style_val,
        storage_driver=str(storage_driver_val),
    )
    target_path = _pick("target_path", ["FIRECUBE_TARGET_PATH"], "target_path")
    if target_path:
        config.target_path = str(target_path)  # type: ignore[attr-defined]
    bucket = _pick("bucket", ["FIRECUBE_BUCKET"], "bucket")
    if bucket:
        config.bucket = str(bucket)  # type: ignore[attr-defined]
    return config


def get_plugin_defaults(cfg: dict[str, Any], plugin_name: str) -> dict[str, Any]:
    """Return default options for a given plugin from config file.

    Returns only the plugin-specific settings under ``[plugins.<name>]``.
    Global settings under ``[database.duckdb]`` are intentionally **not**
    merged here — they are owned by the dedicated ``DatabaseDuckDB`` tier in
    ``TierConfigurator`` (see ``ingestor/runtime/configure.py``) and reach
    DuckDB setup via that tier, not by leaking into every plugin's options.

    Returns the plugin-specific defaults from the config.
    """
    plugins_section = cfg.get("plugins", {}) or {}
    plugin_cfg = plugins_section.get(plugin_name) or {}

    return dict(plugin_cfg)


def get_archive_defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    """Return default archive settings from ``[archive]`` config section."""
    return cfg.get("archive", {}) or {}
