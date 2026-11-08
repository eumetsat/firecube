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

"""The process exit codes of every firecube command."""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    """The exit status a firecube command ends with."""

    OK = 0
    """The command did what it was asked."""

    CHECK_FAILED = 1
    """A check found a problem, or the library refused the operation."""

    USAGE = 2
    """The invocation or the configuration is wrong."""

    STRICT_WARNINGS = 3
    """A `--strict` check found warnings and no errors."""

    RUNTIME_ERROR = 4
    """Storage, a path or an optional dependency failed the command."""

    INTERNAL = 70
    """An unexpected error ended the command; it is a bug."""

    INTERRUPTED = 130
    """The operator interrupted or declined the command."""
