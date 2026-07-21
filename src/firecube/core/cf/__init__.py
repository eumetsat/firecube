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

"""CF metadata checks package."""

from __future__ import annotations

from .check_ids import CF_CHECK_IDS as CF_CHECK_IDS
from .check_ids import CheckDescription as CheckDescription
from .report import CFFinding as CFFinding
from .report import CFReport as CFReport
from .report import CFSeverity as CFSeverity
