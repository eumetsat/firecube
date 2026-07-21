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

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from firecube.core.credentials import Credentials

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig


@dataclass(frozen=True, slots=True)
class StorageDriverConfig:
    """Credentials are immutable and set once at the run boundary via `from_storage_config()`.
    No mid-run credential rotation is supported."""

    driver: Literal["fsspec", "obstore"] = "fsspec"
    endpoint_url: str | None = None
    credentials: Credentials | None = None
    region: str | None = None
    path_style: bool = True

    def __post_init__(self) -> None:
        _check_factory_origin()
        self.validate()

    def validate(self) -> None:
        if self.driver not in {"fsspec", "obstore"}:
            raise ValueError("driver must be one of {'fsspec', 'obstore'}")

        if self.endpoint_url is not None and (
            not self.endpoint_url or "://" not in self.endpoint_url
        ):
            raise ValueError("endpoint_url must be a non-empty URL-like string")

        creds = self.credentials
        if creds is not None and (creds.access_key is None) != (creds.secret_key is None):
            raise ValueError(
                "credentials.access_key and credentials.secret_key must be provided together"
            )

    @classmethod
    def from_storage_config(cls, sc: StorageConfig) -> StorageDriverConfig:
        """Boundary-only: convert parsed TOML StorageConfig to runtime driver config."""
        driver = cast(Literal["fsspec", "obstore"], sc.storage_driver or "fsspec")

        creds = None
        if sc.access_key is not None or sc.secret_key is not None:
            creds = Credentials(
                access_key=sc.access_key,
                secret_key=sc.secret_key,
            )

        return cls(
            driver=driver,
            endpoint_url=sc.endpoint_url,
            credentials=creds,
            region=sc.region,
            path_style=sc.path_style,
        )

    @classmethod
    def from_storage_config_or_default(
        cls,
        config: StorageConfig | None,
    ) -> StorageDriverConfig:
        """Build StorageDriverConfig from optional StorageConfig."""
        if config is None:
            return cls(driver="fsspec")
        return cls.from_storage_config(config)


def _check_factory_origin() -> None:
    """Boundary enforcement: construction must go through approved factories."""
    import inspect
    import os

    frame = inspect.currentframe()
    if frame is None:
        return

    caller = frame.f_back
    while caller is not None:
        caller_qualname = caller.f_code.co_qualname
        caller_file = caller.f_globals.get("__file__", "")

        if caller_qualname in {
            "_check_factory_origin",
            "StorageDriverConfig.__post_init__",
            "StorageDriverConfig.__init__",
            "__create_fn__.<locals>.__init__",
        }:
            caller = caller.f_back
            continue

        test_override = os.environ.get("_TEST_LOCKDOWN_AS_SRC")
        if not test_override and ("test" in caller_file.lower() or "/tests/" in caller_file):
            return

        approved = {
            "StorageDriverConfig.from_storage_config",
            "StorageDriverConfig.from_storage_config_or_default",
        }
        if caller_qualname in approved:
            return

        raise RuntimeError(
            "StorageDriverConfig must be constructed via from_storage_config() or "
            "from_storage_config_or_default(). Direct construction is not permitted. "
            f"Caller: {caller_qualname!r} in {caller_file!r}"
        )
