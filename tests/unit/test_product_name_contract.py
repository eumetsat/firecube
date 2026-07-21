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

from abc import abstractmethod

import pytest

from firecube.ingestor.runtime.base import BaseIngestor


def test_subclass_without_product_name_raises_type_error() -> None:
    """Concrete subclasses without PRODUCT_NAME must fail at class definition."""
    with pytest.raises(TypeError, match="PRODUCT_NAME"):

        class BadPlugin(BaseIngestor):
            pass


def test_subclass_with_empty_product_name_raises_type_error() -> None:
    with pytest.raises(TypeError, match="PRODUCT_NAME"):

        class EmptyNamePlugin(BaseIngestor):
            PRODUCT_NAME = ""


def test_subclass_with_valid_product_name_succeeds() -> None:
    class GoodPlugin(BaseIngestor):
        PRODUCT_NAME = "good_product"

    assert GoodPlugin.PRODUCT_NAME == "good_product"


def test_abstract_intermediate_class_does_not_require_product_name() -> None:
    """Intermediate abstract classes should not need PRODUCT_NAME yet."""

    class AbstractMiddle(BaseIngestor):
        @abstractmethod
        def some_method(self) -> None: ...

    assert AbstractMiddle.__name__ == "AbstractMiddle"


def test_all_baseingestor_subclasses_declare_product_name() -> None:
    """All imported concrete subclasses declare valid PRODUCT_NAME."""
    import firecube.ingestor.templates.direct_zarr
    import firecube.ingestor.templates.generic  # noqa: F401

    def get_concrete(cls: type) -> list[type]:
        result: list[type] = []
        for sub in cls.__subclasses__():
            if not getattr(sub, "__abstractmethods__", frozenset()):
                result.append(sub)
            result.extend(get_concrete(sub))
        return result

    concrete = get_concrete(BaseIngestor)
    missing = [
        cls.__qualname__
        for cls in concrete
        if not cls.__qualname__.startswith(
            (
                "test_subclass_without_product_name_raises_type_error",
                "test_subclass_with_empty_product_name_raises_type_error",
                "test_concrete_subclass_of_template_without_product_name_fails",
            )
        )
        and not (
            hasattr(cls, "PRODUCT_NAME") and isinstance(cls.PRODUCT_NAME, str) and cls.PRODUCT_NAME
        )
    ]
    assert not missing, f"Concrete subclasses without PRODUCT_NAME: {missing}"
