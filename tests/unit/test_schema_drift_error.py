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

from firecube.core.errors import FirecubeError, SchemaDriftError
from firecube.ingestor import errors as ingestor_errors


def test_schema_drift_error_inherits_firecube_error() -> None:
    assert issubclass(SchemaDriftError, FirecubeError)


def test_schema_drift_error_reexported_from_ingestor_errors() -> None:
    assert ingestor_errors.SchemaDriftError is SchemaDriftError
