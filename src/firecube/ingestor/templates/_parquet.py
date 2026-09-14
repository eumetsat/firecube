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

"""Validation for plugin-provided Parquet output paths."""

from pathlib import PurePosixPath

from firecube.core.errors import ConfigurationError


def relative_data_path(value: str) -> str:
    """Canonicalize a plugin output path before using it as a claim name."""
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in value
        or not path.parts
        or path.parts[0] == ".firecube"
    ):
        raise ConfigurationError("Parquet output_relpath must be a relative product data path.")
    return str(path)
