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

"""Architecture tombstone: all concrete BaseIngestor subclasses must declare PRODUCT_NAME."""

from __future__ import annotations

from firecube.ingestor.runtime.base import BaseIngestor


def _get_concrete_subclasses(cls: type) -> list[type]:
    result: list[type] = []
    for sub in cls.__subclasses__():
        if not getattr(sub, "__abstractmethods__", frozenset()):
            result.append(sub)
        result.extend(_get_concrete_subclasses(sub))
    return result


def test_all_baseingestor_subclasses_declare_product_name() -> None:
    """Every concrete BaseIngestor subclass must declare a non-empty PRODUCT_NAME."""
    import firecube.ingestor.templates.direct_zarr
    import firecube.ingestor.templates.generic  # noqa: F401

    concrete = _get_concrete_subclasses(BaseIngestor)
    missing = [
        cls.__qualname__
        for cls in concrete
        if not (
            hasattr(cls, "PRODUCT_NAME") and isinstance(cls.PRODUCT_NAME, str) and cls.PRODUCT_NAME
        )
    ]
    assert not missing, f"These concrete subclasses lack PRODUCT_NAME: {missing}"
