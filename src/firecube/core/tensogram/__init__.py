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

"""Tensogram integration for Firecube — optional dependency."""

from firecube.core.tensogram.schema import (
    ARCHIVE_VERSION,
    KEY_ARCHIVE_VERSION,
    KEY_GROUP,
    KEY_ROLE,
    ROLE_CONTROLPLANE,
    ROLE_DATA,
    make_controlplane_meta,
    make_data_meta,
)

__all__ = [
    "ARCHIVE_VERSION",
    "KEY_ARCHIVE_VERSION",
    "KEY_GROUP",
    "KEY_ROLE",
    "ROLE_CONTROLPLANE",
    "ROLE_DATA",
    "make_controlplane_meta",
    "make_data_meta",
]
