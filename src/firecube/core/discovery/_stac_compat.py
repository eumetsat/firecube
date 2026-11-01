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

"""Optional pystac-client dependency, located without importing it.

Importing this module does not import ``pystac_client``. Callers import the client
classes inside the functions that use them, after `require_stac`.
"""

from __future__ import annotations

import importlib.util

# Read by `require_stac` at call time, so a test can set it to simulate the missing extra.
HAS_PYSTAC_CLIENT = importlib.util.find_spec("pystac_client") is not None


def require_stac(operation: str) -> None:
    """Raise ImportError with install instructions if pystac-client is not available.

    Args:
        operation: What needs the extra, named first in the message, for example
            the command a CLI is running.
    """
    if not HAS_PYSTAC_CLIENT:
        raise ImportError(
            f"{operation} requires the stac extra. Install with: uv pip install 'firecube[stac]'"
        )
