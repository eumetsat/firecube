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

"""T11: built-in templates stay abstract; describe_options exposes PRODUCT_NAME."""

from __future__ import annotations

import pytest

from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor
from firecube.ingestor.templates.generic import (
    GenericParquetIngestor,
    GenericZarrIngestor,
)
from firecube.ingestor.templates.generic_tensogram import GenericTensogramIngestor

_TEMPLATES = (
    GenericZarrIngestor,
    GenericParquetIngestor,
    GenericTensogramIngestor,
    DirectZarrIngestor,
)


@pytest.mark.unit
@pytest.mark.parametrize("template_cls", _TEMPLATES, ids=lambda cls: cls.__name__)
def test_template_is_abstract(template_cls: type) -> None:
    abstracts = getattr(template_cls, "__abstractmethods__", frozenset())
    assert abstracts, (
        f"{template_cls.__name__} is no longer abstract — every built-in template "
        "must keep at least one @abstractmethod so PRODUCT_NAME stays a plugin contract."
    )


@pytest.mark.unit
@pytest.mark.parametrize("template_cls", _TEMPLATES, ids=lambda cls: cls.__name__)
def test_template_does_not_declare_product_name(template_cls: type) -> None:
    assert "PRODUCT_NAME" not in template_cls.__dict__, (
        f"{template_cls.__name__} declares PRODUCT_NAME directly; this would let "
        "user subclasses inherit a stale product name. Keep templates abstract instead."
    )


@pytest.mark.unit
def test_concrete_subclass_of_template_without_product_name_fails() -> None:
    with pytest.raises(TypeError, match="PRODUCT_NAME"):

        class _BadDirectZarr(DirectZarrIngestor):
            def zarr_schema(self, ctx):  # type: ignore[override]
                return []

            def build_write_intents(self, batch, ctx):  # type: ignore[override]
                return []


@pytest.mark.unit
def test_concrete_subclass_of_template_with_product_name_succeeds() -> None:
    class _GoodDirectZarr(DirectZarrIngestor):
        PRODUCT_NAME = "good_direct_zarr"

        def zarr_schema(self, ctx):  # type: ignore[override]
            return []

        def build_write_intents(self, batch, ctx):  # type: ignore[override]
            return []

    assert _GoodDirectZarr.PRODUCT_NAME == "good_direct_zarr"


@pytest.mark.unit
def test_describe_options_includes_product_name_section() -> None:
    class _TemplateProbe(DirectZarrIngestor):
        PRODUCT_NAME = "template_probe"

        def zarr_schema(self, ctx):  # type: ignore[override]
            return []

        def build_write_intents(self, batch, ctx):  # type: ignore[override]
            return []

    desc = _TemplateProbe.describe_options()
    assert "Product Name" in desc
    assert desc["Product Name"] == ["template_probe"]


@pytest.mark.unit
def test_describe_options_omits_product_name_when_abstract() -> None:
    desc = BaseIngestor.describe_options()
    assert desc.get("Product Name", []) == []
