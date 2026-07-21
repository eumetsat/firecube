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

"""Storage primitives: URI value objects, driver configuration, sessions, transfer.

Public imports remain stable at ``firecube.core.storage``.
"""

from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.cache_key import StorageCacheKey
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.results import StorageWriteResult
from firecube.core.storage.session import StorageSession
from firecube.core.storage.transfer import copy_file, session_for_uri
from firecube.core.storage.uri import (
    LOCAL_PROTOCOLS,
    REMOTE_PROTOCOLS,
    SUPPORTED_PROTOCOLS,
    StorageUri,
)

__all__ = [
    "LOCAL_PROTOCOLS",
    "REMOTE_PROTOCOLS",
    "SUPPORTED_PROTOCOLS",
    "StorageBinding",
    "StorageCacheKey",
    "StorageDriverConfig",
    "StorageSession",
    "StorageUri",
    "StorageWriteResult",
    "copy_file",
    "session_for_uri",
]
