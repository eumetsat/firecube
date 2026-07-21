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


@dataclass(frozen=True, slots=True)
class StorageCacheKey:
    driver: str
    protocol: str
    authority: str | None
    endpoint_url: str | None
    region: str | None
    credential_fingerprint: str | None

    @classmethod
    def from_binding(cls, binding):

        creds = binding.driver.credentials
        return cls(
            driver=binding.driver.driver,
            protocol=binding.identity.product_uri.protocol,
            authority=binding.identity.product_uri.authority,
            endpoint_url=binding.driver.endpoint_url,
            region=binding.driver.region,
            credential_fingerprint=creds.fingerprint() if creds is not None else None,
        )
