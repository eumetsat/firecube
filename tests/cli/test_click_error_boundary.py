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

"""Click error boundary policy tests.

CLI boundary contract:
- Known user-input errors must surface as ``click.UsageError`` / ``click.ClickException``
  so Click renders them as clean ``Error: ...`` messages (no Python traceback).
- The ``wrap_user_facing_errors`` decorator must convert a whitelist of known
  downstream errors into ``click.ClickException`` while letting unrelated
  exceptions propagate normally.
"""

from __future__ import annotations

import click
import pytest

from firecube.cli._audience import classify
from firecube.cli._errors import render, wrap_user_facing_errors
from firecube.cli._recovery import OPERATIONS, _command
from firecube.cli._slot_env import _parse_int
from firecube.core import errors as err
from tests.helpers.cli_hints import parse_hint


def test_parse_int_invalid_value_raises_click_usage_error() -> None:
    with pytest.raises(click.UsageError, match="must be an integer"):
        _parse_int("notanint", name="FIRECUBE_SLOT_START")


def test_classify_unknown_path_raises_click_exception() -> None:
    with pytest.raises(click.ClickException, match="Unknown command path"):
        classify(("nonexistent", "cmd"))


def test_wrap_user_facing_errors_converts_known_zarr_errors() -> None:
    class GroupNotFoundError(Exception):
        pass

    @wrap_user_facing_errors
    def boom() -> None:
        raise GroupNotFoundError("missing group 'g'")

    with pytest.raises(click.ClickException, match="missing group 'g'"):
        boom()


def test_wrap_user_facing_errors_does_not_swallow_unexpected_exceptions() -> None:
    @wrap_user_facing_errors
    def boom() -> None:
        raise RuntimeError("internal bug")

    with pytest.raises(RuntimeError, match="internal bug"):
        boom()


def test_wrap_user_facing_errors_passes_through_click_exceptions() -> None:
    @wrap_user_facing_errors
    def boom() -> None:
        raise click.UsageError("invalid flag")

    with pytest.raises(click.UsageError, match="invalid flag"):
        boom()


def test_wrap_user_facing_errors_preserves_return_value() -> None:
    @wrap_user_facing_errors
    def ok(x: int, y: int) -> int:
        return x + y

    assert ok(2, 3) == 5


P, M, Op, F = err.Param, err.Message, err.Operation, err.OPERATION_FUNCTIONS
U, R = "file:///a b", err.Placeholder('"<reason>"')
SAMPLES = {
    "abandon_run": ({"product_name": "p", "reason": R}, f"--product-name p --reason {R}"),
    "clear_claim": ({"product_name": "p", "yes_i_really_mean_it": 1}, "--product-name p"),
    "rebuild_compact": ({"product_name": "p"}, "--product-name p"),
    "delete_span": ({"product_name": "p", "force": 1, "dry_run": 0}, "--product-name p --force"),
    "preallocate": (
        {"target": U, "product_name": "n", "write_mode": "staged", "dry_run": 0},
        f"<plugin> --target {U!r} --product-name n --write-mode staged --no-dry-run",
    ),
    "rebuild_index": (
        {"target": U, "plugin": "p"},
        f"--target {U!r} --plugin p --product-name <product_name>",
    ),
    "validate": ({"product": U, "group_path": "g"}, f"--product {U!r} --group g"),
}
_ROOT = click.Context(_command(()), info_name="firecube")
_ZARR = click.Context(_command(("zarr",)), info_name="zarr", parent=_ROOT)
_CTX = click.Context(_command(("zarr", "validate")), info_name="validate", parent=_ZARR)
_DIM = M("a {o}", o=P("time_dim"))
_PARTS = [
    (P("time_dim"), "time_dim", "--time-dim"),
    (P("time_dim", "t"), "time_dim='t'", "--time-dim t"),
    (P("storage_anonymous", False), "storage_anonymous=False", "--storage-anonymous"),
    (P("config_file"), "config_file", "--config-file"),
    (P("nope", 1), "nope=1", "nope=1"),
    (err.PluginOption("k", "a b"), "k=a b", "--option 'k=a b'"),
    (err.CommandName((), "lib"), "lib", "firecube zarr validate"),
    (err.CommandName(("chunks", "runs")), "", "firecube chunks runs"),
    (M.join("; ", [_DIM, "plain"]), "a time_dim; plain", "a --time-dim; plain"),
]


@pytest.mark.contract
@pytest.mark.parametrize("name", SAMPLES)
def test_operation_renders_command_that_parses_against_the_real_cli(name: str) -> None:
    arguments, tail = SAMPLES[name]
    message = M("{op}", op=Op(name, **arguments))
    path = " ".join(OPERATIONS[name])
    assert (str(message), render(message)) == (F[name], f"firecube {path} {tail}")
    assert parse_hint(render(message), "firecube") == path
    assert set(SAMPLES) == set(OPERATIONS) == set(F) and __import__("pydoc").locate(F[name])
    assert name not in ("abandon_run", "clear_claim") or F[name].endswith("." + name)


@pytest.mark.contract
@pytest.mark.parametrize(("part", "library", "cli"), _PARTS)
def test_part_renders_library_text_and_cli_command(part, library: str, cli: str) -> None:
    assert (str(M("{x}", x=part)), render(M("{x}", x=part), _CTX)) == (library, cli)


@pytest.mark.contract
def test_every_part_kind_the_library_renders_has_a_case_here() -> None:
    # Placeholder has no _PARTS row: it appears only inside Operation arguments (SAMPLES).
    in_operations = {type(v) for arguments, _ in SAMPLES.values() for v in arguments.values()}
    covered = {type(p) for p, _, _ in _PARTS} | in_operations | {Op}
    assert covered & set(err._PART_TYPES) == set(err._PART_TYPES)


@pytest.mark.contract
@pytest.mark.parametrize("args", [(), ("more",)])
def test_error_exposes_message_parts_and_the_wrap_prints_the_cli_rendering(args) -> None:
    first, op = (Op("rebuild_compact", product_name=n) for n in "qp")
    inner = M("{slot_window} {o} {first}", slot_window=(1, 5), o=P("time_dim"), first=first)
    message = M("{group} {i} {run_id} {op}", group="g", i=inner, run_id="r", op=op)
    error = err.ConfigurationError(message, *args)
    assert (error.message, error.recovery) == (message, (first, op))
    c = error.context
    assert (c.run_id, c.group, c.slot_window, c.options) == ("r", "g", (1, 5), ("time_dim",))

    def boom() -> None:
        raise error

    with _CTX, pytest.raises(click.ClickException) as caught:
        wrap_user_facing_errors(boom)()
    cmd = "firecube chunks compact rebuild --product-name"
    assert caught.value.message == f"g (1, 5) --time-dim {cmd} q r {cmd} p"
