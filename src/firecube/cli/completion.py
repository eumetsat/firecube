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

from pathlib import Path

import click
from click.shell_completion import get_completion_class


@click.command(
    "completion",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  # enable completion for bash (add to ~/.bashrc)
  eval "$(firecube completion bash)"


  # enable completion for zsh (add to ~/.zshrc)
  eval "$(firecube completion zsh)"


  # enable completion for fish
  firecube completion fish | source
""",
)
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish"], case_sensitive=False))
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    help="write the completion script to a file instead of stdout",
)
@click.pass_context
def completion_cmd(ctx: click.Context, shell: str, output_path: Path | None) -> None:
    """generate shell completion scripts

    Generates a shell completion script for firecube for bash, zsh, or fish.
    Run the output through eval in your shell profile to enable tab
    completion for all firecube commands and options.
    """
    root = ctx.find_root()
    # Always generate scripts for the installed console command name.
    prog_name = "firecube"
    complete_var = "_FIRECUBE_COMPLETE"

    complete_cls = get_completion_class(shell)
    if complete_cls is None:
        raise click.ClickException(f"Unsupported shell: {shell}")

    script = complete_cls(root.command, {}, prog_name, complete_var).source()
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(script)
        click.echo(f"Wrote {shell} completion script to {output_path}")
        return
    click.echo(script)


__all__ = ["completion_cmd"]
