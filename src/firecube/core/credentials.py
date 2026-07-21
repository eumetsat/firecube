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

import hashlib
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Credentials:
    """Credentials are immutable and set once at the run boundary via `from_storage_config()`
    or `from_storage_config_or_default()`. No mid-run credential rotation is supported."""

    access_key: str | None = field(default=None, repr=False)
    secret_key: str | None = field(default=None, repr=False)
    session_token: str | None = field(default=None, repr=False)

    def fingerprint(self) -> str:
        """Stable hash for cache identity. Never logs raw values."""
        material = f"{self.access_key or ''}|{self.secret_key or ''}|{self.session_token or ''}"
        return hashlib.sha256(material.encode()).hexdigest()[:16]

    def is_anonymous(self) -> bool:
        return self.access_key is None and self.secret_key is None
