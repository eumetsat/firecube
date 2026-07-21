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

"""Product identity, target resolution, and product URI helpers.

Public imports remain stable at ``firecube.core.product``.
"""

from firecube.core.product.identity import (
    VALID_FORMATS,
    ProductIdentity,
    ensure_product_uri,
)
from firecube.core.product.resolver import (
    CompletionRoute,
    ProductResolver,
    WriteModePolicy,
    resolve_dataset_target,
    write_mode_policy,
)
from firecube.core.product.target import ProductTarget, ResolvedProduct

__all__ = [
    "VALID_FORMATS",
    "CompletionRoute",
    "ProductIdentity",
    "ProductResolver",
    "ProductTarget",
    "ResolvedProduct",
    "WriteModePolicy",
    "ensure_product_uri",
    "resolve_dataset_target",
    "write_mode_policy",
]
