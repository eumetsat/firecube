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

"""The CLI error boundary: exit codes for exceptions, and their operator text.

The root group runs every command through `invoke_with_boundary`. A Click exception
passes unchanged; any other exception prints one `Error:` line and exits with the code
`classify` gives it.
"""

from __future__ import annotations

import os
import traceback
from collections.abc import Callable, Iterator
from typing import NoReturn

import click

from firecube.cli._exit_codes import ExitCode
from firecube.core.errors import (
    ConfigurationError,
    FirecubeError,
    OptionalExtraMissingError,
    StorageError,
)
from firecube.core.observability.logging import debug_enabled

# Packages whose exceptions mean a remote storage call failed, wherever they sit in the chain.
_REMOTE_CLIENT_PACKAGES = frozenset({"aiobotocore", "botocore"})

# OS errors about a path: it is missing, of the wrong kind, or not accessible.
_OS_PATH_ERRORS: tuple[type[OSError], ...] = (
    FileNotFoundError,
    IsADirectoryError,
    NotADirectoryError,
    PermissionError,
)

# The frames whose local `ctx` is the context of the command that runs in them.
_COMMAND_FRAME_CODES = frozenset(
    {click.Context.invoke.__code__, click.Command.make_context.__code__}
)


def render(value: BaseException | str, ctx: click.Context | None = None) -> str:
    """Return the operator text of an exception, a library `Message` or a string.

    The renderer module loads on the first call, so start-up loads nothing new.
    `ctx` defaults to the current Click context.
    """
    from firecube.cli._recovery import render as render_recovery

    return render_recovery(value, ctx)


def echo_rendered(line: str, *, err: bool = False) -> None:
    """Print one library line, rendered for the running command; pass `err=True` for stderr."""
    click.echo(render(line), err=err)


def exit_with_error(text: str, code: ExitCode) -> NoReturn:
    """Print `Error: ` and `text` to stderr, then end the command with `code`."""
    click.echo(f"Error: {text}", err=True)
    raise click.exceptions.Exit(code)


def _chain(exc: BaseException) -> Iterator[BaseException]:
    """Yield `exc` and each explicit cause set by `raise ... from`; implicit context is skipped."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__


def _is_remote_client_error(exc: BaseException) -> bool:
    """Return whether `exc` is an exception class of botocore or aiobotocore, or derives from one."""
    return any(
        cls.__module__.partition(".")[0] in _REMOTE_CLIENT_PACKAGES for cls in type(exc).__mro__
    )


def _is_zarr_not_found(exc: BaseException) -> bool:
    """Return whether `exc` is zarr's refusal for a missing array or group."""
    from zarr.errors import NodeNotFoundError

    return isinstance(exc, NodeNotFoundError)


def classify(exc: BaseException) -> ExitCode:
    """Return the exit code for an exception that is not a Click exception.

    The first matching rung wins:

    - 130: an interrupt or `click.Abort`.
    - 2: a `ConfigurationError`.
    - 4: an `OptionalExtraMissingError` or a botocore error anywhere in the chain.
    - 4: a `StorageError` or a `StoragePreflightError`.
    - 1: any other `FirecubeError`, or zarr's missing-node error.
    - 4: a missing or inaccessible path.
    - 70: anything else.
    """
    from firecube.ingestor.runtime.preflight import StoragePreflightError

    if isinstance(exc, click.Abort | KeyboardInterrupt):
        return ExitCode.INTERRUPTED
    if isinstance(exc, ConfigurationError):
        return ExitCode.USAGE
    chain = list(_chain(exc))
    if any(isinstance(e, OptionalExtraMissingError) or _is_remote_client_error(e) for e in chain):
        return ExitCode.RUNTIME_ERROR
    if isinstance(exc, StorageError | StoragePreflightError):
        return ExitCode.RUNTIME_ERROR
    # zarr's NodeNotFoundError subclasses FileNotFoundError, so it is checked before path errors.
    if isinstance(exc, FirecubeError) or _is_zarr_not_found(exc):
        return ExitCode.CHECK_FAILED
    if isinstance(exc, _OS_PATH_ERRORS):
        return ExitCode.RUNTIME_ERROR
    return ExitCode.INTERNAL


def _running_context(exc: BaseException, default: click.Context) -> click.Context:
    """Return the context of the innermost command that the traceback of `exc` passes through.

    The command's context has left Click's context stack by the time `exc` reaches the
    root, and `render` needs it to name the running command's options.
    """
    found = default
    tb = exc.__traceback__
    while tb is not None:
        if tb.tb_frame.f_code in _COMMAND_FRAME_CODES:
            candidate = tb.tb_frame.f_locals.get("ctx")
            if isinstance(candidate, click.Context):
                found = candidate
        tb = tb.tb_next
    return found


def _error_line(exc: BaseException, code: ExitCode, ctx: click.Context) -> str:
    """Return the one `Error:` line for `exc`; an unexpected error also names its type."""
    text = render(exc, ctx)
    if code is ExitCode.INTERNAL:
        name = type(exc).__name__
        return f"Error: {name}: {text}" if text else f"Error: {name}"
    return f"Error: {text}" if text else f"Error: {type(exc).__name__}"


def invoke_with_boundary[R](ctx: click.Context, invoke: Callable[[], R]) -> R:
    """Run `invoke`; turn any exception except a Click one into an `Error:` line and an exit.

    The exit code comes from `classify`. Under root `--debug` or `FIRECUBE_DEBUG=1` the
    traceback prints first; otherwise an unexpected error adds a hint naming both.

    Raises:
        click.exceptions.Exit: With the code `classify` gives the exception.
    """
    try:
        return invoke()
    except (click.ClickException, click.exceptions.Exit):
        raise
    except (Exception, KeyboardInterrupt) as exc:
        code = classify(exc)
        debug = bool(ctx.params.get("debug")) or debug_enabled()
        if debug:
            click.echo("".join(traceback.format_exception(exc)), err=True, nl=False)
        if code is ExitCode.INTERRUPTED:
            click.echo("Aborted!", err=True)
        else:
            click.echo(_error_line(exc, code, _running_context(exc, ctx)), err=True)
        if code is ExitCode.INTERNAL and not debug:
            click.echo(
                "Hint: rerun with --debug, or set FIRECUBE_DEBUG=1, to print the traceback.",
                err=True,
            )
        raise click.exceptions.Exit(code) from exc


def enable_debug(ctx: click.Context) -> None:
    """Set `FIRECUBE_DEBUG=1` for the run of `ctx`; its previous value returns when `ctx` closes."""
    previous = os.environ.get("FIRECUBE_DEBUG")
    os.environ["FIRECUBE_DEBUG"] = "1"

    def restore() -> None:
        if previous is None:
            os.environ.pop("FIRECUBE_DEBUG", None)
        else:
            os.environ["FIRECUBE_DEBUG"] = previous

    ctx.call_on_close(restore)


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
