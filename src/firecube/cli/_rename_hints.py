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

"""Click rename-hint layer.

Augments `click.NoSuchOption` errors with a navigation hint when the
user types a flag that was renamed in the strict-URI refactor.

This is NOT a deprecation alias - the old flag still errors out. It is a UX
layer that helps migrating users find the new flag name.

Click's native similarity-based "Did you mean ..." suggestion only fires when
the new flag is a close string match (e.g. ``--product`` -> ``--product-name``).
For semantic renames where the new name is unrelated (``--source`` ->
``--input-data``), Click stays silent. This module fills that gap.
"""

from __future__ import annotations

from typing import Final

import click

# Renames Click's similarity matcher will NOT auto-suggest.
# Layout: { old_flag: [ (command_path_prefix, new_flag_text, explanation), ... ] }
# Most specific prefix wins.
_RENAME_HINTS: Final[dict[str, list[tuple[tuple[str, ...], str, str]]]] = {
    "--source": [
        (
            ("archive", "restore"),
            "--archive (-a)",
            "the .tgm artifact path was renamed to --archive in the strict-URI refactor",
        ),
        (
            ("ingest",),
            "--input-data (-i)",
            "the plugin input flag was renamed in the strict-URI refactor; "
            "--source is now reserved for product URIs (archive create only)",
        ),
    ],
    "--target": [
        (
            ("archive", "create"),
            "--archive (-a)",
            "the .tgm artifact output was renamed to --archive; "
            "--target now exclusively refers to product target URIs",
        ),
    ],
}


def build_command_path(ctx: click.Context | None) -> tuple[str, ...]:
    """Build a dotted command path from a Click context chain.

    Excludes the root context (whose ``info_name`` is the program name -
    ``"firecube"`` in production, ``"cli"`` under `click.testing.CliRunner`).
    For ``firecube archive restore`` the returned tuple is ``("archive", "restore")``.
    """
    if ctx is None:
        return ()
    parts: list[str] = []
    current: click.Context | None = ctx
    while current is not None and current.parent is not None:
        if current.info_name:
            parts.append(current.info_name)
        current = current.parent
    return tuple(reversed(parts))


def get_rename_hint(option_name: str, command_path: tuple[str, ...]) -> str | None:
    """Return a hint string for a renamed flag on a specific command, or ``None``.

    Matches by longest command-path prefix - ``("archive", "restore")`` wins
    over ``("archive",)`` if both registered.
    """
    candidates = _RENAME_HINTS.get(option_name)
    if not candidates:
        return None
    # Sort by descending path length so the most specific match wins first.
    for path_prefix, new_flag, explanation in sorted(candidates, key=lambda entry: -len(entry[0])):
        if command_path[: len(path_prefix)] == path_prefix:
            return f"Use {new_flag} instead ({explanation})."
    return None


_INSTALLED_FLAG = "_firecube_rename_hints_installed"


def install_rename_hints() -> None:
    """Monkey-patch `click.Command.parse_args` once to inject rename hints.

    Subsequent calls are no-ops (idempotent). Safe to call from any module init.
    """
    if getattr(click.Command.parse_args, _INSTALLED_FLAG, False):
        return

    original_parse_args = click.Command.parse_args

    def hinted_parse_args(self: click.Command, ctx: click.Context, args: list[str]) -> list[str]:
        try:
            return original_parse_args(self, ctx, args)
        except click.NoSuchOption as exc:
            command_path = build_command_path(ctx)
            hint = get_rename_hint(exc.option_name, command_path)
            if hint is not None:
                # Inject hint into the message field. format_message() will pick it up
                # alongside Click's own "Did you mean ...?" suggestion (if any).
                object.__setattr__(exc, "message", f"{exc.message}\n\n  Hint: {hint}")
            raise

    setattr(hinted_parse_args, _INSTALLED_FLAG, True)
    click.Command.parse_args = hinted_parse_args  # type: ignore[method-assign]
