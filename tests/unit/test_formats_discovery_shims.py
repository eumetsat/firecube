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

"""The old discovery module paths stay importable, warn, and re-export the same objects."""

from __future__ import annotations

import importlib
import sys
import warnings

import pytest

import firecube.core.discovery as discovery

pytestmark = pytest.mark.contract


@pytest.mark.parametrize(
    ("module", "names"),
    [
        (
            "firecube.core.formats.discovery",
            ("discover_input_files", "DEFAULT_INCLUDE_SUFFIXES", "KNOWN_EXTENSIONS"),
        ),
        (
            "firecube.core.formats.input_filters",
            (
                "LEGACY_INPUT_PATTERNS_MESSAGE",
                "reject_legacy_input_patterns",
                "split_input_filters",
                "validate_input_filters",
            ),
        ),
    ],
)
def test_old_module_path_warns_and_reexports(module: str, names: tuple[str, ...]) -> None:
    sys.modules.pop(module, None)

    with pytest.warns(DeprecationWarning, match="firecube.core.discovery"):
        shim = importlib.import_module(module)

    for name in names:
        assert getattr(shim, name) is getattr(discovery, name)


def test_formats_package_exports_discovery_without_a_warning() -> None:
    sys.modules.pop("firecube.core.formats", None)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        formats = importlib.import_module("firecube.core.formats")

    assert formats.discover_input_files is discovery.discover_input_files
    assert formats.KNOWN_EXTENSIONS is discovery.KNOWN_EXTENSIONS
