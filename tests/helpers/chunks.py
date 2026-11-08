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
"""Stand-in for a chunks-command product binding whose manager is a fake."""

from firecube.cli.chunks._manager import BoundProduct
from firecube.core.storage.uri import StorageUri


def bound(manager, label: str = "TEST_PRODUCT.zarr") -> BoundProduct:
    return BoundProduct(manager, label, StorageUri.parse(f"file:///tmp/{label}"))
