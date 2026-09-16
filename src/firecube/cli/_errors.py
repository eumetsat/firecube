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

import errno
import functools
from collections.abc import Callable

import click

from firecube.core.errors import FirecubeError

# Explicit membership for user-facing exceptions that are NOT part of the
# ``FirecubeError`` hierarchy (built-in OS errors, third-party library errors,
# and ingestor-runtime ``RuntimeError`` subclasses). ``FirecubeError`` and its
# subclasses are covered by the ``isinstance`` branch in ``_is_known_user_error``
# and must NOT be added here — reparent them under ``FirecubeError`` instead.
_KNOWN_NON_FIRECUBE_USER_ERRORS: frozenset[str] = frozenset(
    {
        "FileNotFoundError",
        "GroupNotFoundError",
        "NodeNotFoundError",
        "NotADirectoryError",
        "PathNotFoundError",
        "PermissionError",
        "PipelineFailedBatchesError",
    }
)


def _is_known_user_error(exc: BaseException) -> bool:
    if isinstance(exc, FirecubeError):
        return True
    return type(exc).__name__ in _KNOWN_NON_FIRECUBE_USER_ERRORS


def _is_known_user_oserror(exc: BaseException) -> bool:
    """True when exc is an OS-level error that is the user's fault (not infra)."""
    return isinstance(exc, OSError) and exc.errno in {errno.ENOENT, errno.ENOTDIR, errno.EACCES}


def wrap_user_facing_errors[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """Convert known downstream errors to ``click.ClickException`` at the CLI boundary.

    Exceptions that are ``FirecubeError`` subclasses are wrapped via the
    ``isinstance`` branch in ``_is_known_user_error``. Non-Firecube user
    errors whose class name appears in ``_KNOWN_NON_FIRECUBE_USER_ERRORS``
    are also wrapped. ``click.ClickException`` and ``click.exceptions.Exit``
    pass through unchanged, and any other exception propagates so operators
    see the real traceback.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except click.exceptions.Exit:
            raise
        except click.ClickException:
            raise
        except Exception as exc:
            if _is_known_user_error(exc) or _is_known_user_oserror(exc):
                raise click.ClickException(str(exc)) from exc
            raise

    return wrapper


class UnknownOptionError(click.BadParameter):
    def __init__(self, key: str, plugin: str, valid_keys: list[str]) -> None:
        msg = (
            f"Unknown option '{key}' for plugin '{plugin}'.\n"
            f"Valid keys ({len(valid_keys)}): {', '.join(sorted(valid_keys))}\n"
            f"Hint: run `firecube ingest {plugin} --show-options` to inspect. "
            f"Experimental options must use the x_ prefix (e.g. --option x_foo=...)."
        )
        super().__init__(msg)


class MissingProductNameError(click.UsageError):
    def __init__(self, plugin: str) -> None:
        super().__init__(
            f"Missing product name for plugin '{plugin}'. Provide one of:\n"
            f"  1. CLI flag:  --product-name <name>\n"
            f"  2. Config file:  [plugins.{plugin}]\n  default_product_name = '<name>'\n"
            f"  3. Plugin class attr:  {plugin}.PRODUCT_NAME = '<name>'  (preferred; declared at plugin source)"
        )


class MissingStorageDriverError(click.UsageError):
    def __init__(self, target_uri: str) -> None:
        super().__init__(
            f"Missing --storage-driver for target '{target_uri}'. Provide explicitly:\n"
            f"  --storage-driver [fsspec|obstore]"
        )
