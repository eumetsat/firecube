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
- Every other exception reaches the root boundary, which prints one ``Error:`` line and
  exits with the code ``classify`` gives it; the traceback prints only under debug.
"""

from __future__ import annotations

import ast
import inspect
import os
from pathlib import Path
from typing import Any

import click
import pytest
from botocore.exceptions import NoCredentialsError
from click.testing import CliRunner
from zarr.errors import GroupNotFoundError

from firecube.cli import _errors
from firecube.cli._audience import classify
from firecube.cli._errors import invoke_with_boundary, render
from firecube.cli._exit_codes import ExitCode
from firecube.cli._recovery import OPERATIONS, _command
from firecube.cli._slot_env import _parse_int
from firecube.cli.main import cli
from firecube.core import errors as err
from firecube.core.archive.zip.backup_chain import BackupChainError
from firecube.core.zarr.guard.engine_owned import preallocate_operation
from firecube.ingestor import errors as ingestor_errors
from firecube.ingestor.runtime.engine import PipelineFailedBatchesError
from firecube.ingestor.runtime.preflight import StoragePreflightError
from tests.helpers.cli_hints import parse_hint


def test_parse_int_invalid_value_raises_click_usage_error() -> None:
    with pytest.raises(click.UsageError, match="must be an integer"):
        _parse_int("notanint", name="FIRECUBE_SLOT_START")


def test_classify_unknown_path_raises_click_exception() -> None:
    with pytest.raises(click.ClickException, match="Unknown command path"):
        classify(("nonexistent", "cmd"))


def _exit_through_boundary(
    exc: BaseException, capsys: pytest.CaptureFixture[str]
) -> tuple[int, str, str]:
    def raiser() -> None:
        raise exc

    with pytest.raises(click.exceptions.Exit) as caught:
        invoke_with_boundary(click.Context(click.Command("firecube")), raiser)
    captured = capsys.readouterr()
    return int(caught.value.exit_code), captured.err, captured.out


def test_click_exception_passes_the_boundary_unchanged() -> None:
    usage = click.UsageError("invalid flag")

    def raiser() -> None:
        raise usage

    with pytest.raises(click.UsageError) as caught:
        invoke_with_boundary(click.Context(click.Command("firecube")), raiser)
    assert caught.value is usage


_RUNGS = [
    (err.ConfigurationError("bad config"), ExitCode.USAGE, "bad config"),
    (err.ConfigFileError("not_found", path=Path("c.toml")), ExitCode.USAGE, "c.toml"),
    (err.ProductLabelMismatchError("label", given="a", recorded="b"), ExitCode.USAGE, "label"),
    (err.OptionalExtraMissingError("needs stac", extra="stac"), ExitCode.RUNTIME_ERROR, "stac"),
    (err.StorageError("storage"), ExitCode.RUNTIME_ERROR, "storage"),
    (StoragePreflightError("preflight"), ExitCode.RUNTIME_ERROR, "preflight"),
    (NoCredentialsError(), ExitCode.RUNTIME_ERROR, "credentials"),
    (FileNotFoundError(2, "No such file"), ExitCode.RUNTIME_ERROR, "No such file"),
    (PermissionError(13, "denied"), ExitCode.RUNTIME_ERROR, "denied"),
    (err.ClaimReleaseError("release", domain="d", owner_id="o"), ExitCode.CHECK_FAILED, "release"),
    (err.ClaimLostError("lost", domain="d"), ExitCode.CHECK_FAILED, "lost"),
    (
        BackupChainError("chain", archive="a", broken_at="b", reason=err.Message("why")),
        ExitCode.CHECK_FAILED,
        "Error: chain",
    ),
    (err.SchemaDriftError("drift"), ExitCode.CHECK_FAILED, "drift"),
    (err.ManifestError("manifest"), ExitCode.CHECK_FAILED, "manifest"),
    (ingestor_errors.ResumeConflictError("resume"), ExitCode.CHECK_FAILED, "resume"),
    (GroupNotFoundError("g"), ExitCode.CHECK_FAILED, "Error: g\n"),
    (
        PipelineFailedBatchesError(run_id="r", failed=[], not_attempted=0),
        ExitCode.CHECK_FAILED,
        "Pipeline run 'r'",
    ),
    (click.Abort(), ExitCode.INTERRUPTED, "Aborted!"),
    (KeyboardInterrupt(), ExitCode.INTERRUPTED, "Aborted!"),
    (RuntimeError("bug"), ExitCode.INTERNAL, "RuntimeError"),
]


@pytest.mark.parametrize(
    ("exc", "code", "diagnostic"), _RUNGS, ids=[type(e).__name__ for e, _, _ in _RUNGS]
)
def test_boundary_maps_each_ladder_rung_to_its_exit_code(
    exc: BaseException, code: ExitCode, diagnostic: str, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code, stderr, stdout = _exit_through_boundary(exc, capsys)

    assert exit_code == code
    assert diagnostic in stderr
    assert stdout == ""
    assert "Traceback" not in stderr
    assert ("FIRECUBE_DEBUG=1" in stderr and "--debug" in stderr) == (code is ExitCode.INTERNAL)


@pytest.mark.parametrize(("explicit", "code"), [(True, 4), (False, 70)])
def test_only_an_explicit_cause_carries_the_botocore_exit_code(
    explicit: bool, code: int, capsys: pytest.CaptureFixture[str]
) -> None:
    try:
        try:
            raise NoCredentialsError()
        except NoCredentialsError as inner:
            if explicit:
                raise RuntimeError("wrapped") from inner
            raise RuntimeError("raised while handling")  # noqa: B904
    except RuntimeError as exc:
        assert _exit_through_boundary(exc, capsys)[0] == code


@pytest.mark.parametrize("how", ["flag", "env"])
def test_debug_flag_and_env_print_the_traceback_and_restore_the_environment(
    how: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    @click.command(name="boom")
    def boom() -> None:
        raise err.ConfigurationError("bad config")

    monkeypatch.setitem(cli.commands, "boom", boom)
    monkeypatch.delenv("FIRECUBE_DEBUG", raising=False)
    if how == "env":
        monkeypatch.setenv("FIRECUBE_DEBUG", "1")

    result = CliRunner().invoke(cli, ["--debug", "boom"] if how == "flag" else ["boom"])

    assert result.exit_code == 2
    assert "Traceback" in result.stderr
    assert "Error: bad config" in result.stderr
    assert os.environ.get("FIRECUBE_DEBUG") == ("1" if how == "env" else None)


def test_message_error_prints_the_cli_rendering(monkeypatch: pytest.MonkeyPatch) -> None:
    first, op = (err.Operation("rebuild_compact", target=n) for n in "qp")
    inner = err.Message(
        "{slot_window} {o} {first}", slot_window=(1, 5), o=err.Param("time_dim"), first=first
    )
    message = err.Message("{group} {i} {run_id} {op}", group="g", i=inner, run_id="r", op=op)

    @click.command(name="validate")
    @click.option("--time-dim")
    def boom(time_dim: str | None) -> None:
        raise err.ConfigurationError(message)

    monkeypatch.setitem(cli.commands["zarr"].commands, "validate", boom)  # type: ignore[attr-defined]

    result = CliRunner().invoke(cli, ["zarr", "validate"])

    cmd = "firecube chunks compact rebuild --target"
    assert result.exit_code == 2
    assert result.stderr == f"Error: g (1, 5) --time-dim {cmd} q r {cmd} p\n"


@pytest.mark.parametrize(("raised", "code"), [(TypeError("bad type"), 70), (ValueError("bad"), 2)])
def test_storage_resolution_exit_code_follows_the_exception_kind(
    raised: Exception, code: int, tmp_local_zarr: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(**_: object) -> None:
        raise raised

    monkeypatch.setattr("firecube.cli._ctx.resolve_storage_config", fail)

    args = ["--profile", "cf-18", "-t", tmp_local_zarr.as_uri(), "-g", "g1"]

    result = CliRunner().invoke(cli, ["advise", "compliance", *args])

    assert result.exit_code == code
    assert str(raised) in result.stderr


def test_missing_tensogram_extra_under_archive_conversion_exits_4(
    tmp_local_zarr: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("firecube.core.archive.tensogram._compat.HAS_TENSOGRAM", False)
    out = tmp_path / "out.tgm"
    args = ["--target", tmp_local_zarr.as_uri(), "--archive", out.as_uri()]

    result = CliRunner().invoke(
        cli,
        ["archive", "create", *args, "--storage-type", "local", "--storage-driver", "fsspec"],
    )

    assert result.exit_code == 4, result.output
    assert "tensogram" in result.stderr
    assert "Traceback" not in result.output
    assert not out.exists()


def _firecube_error_classes() -> list[type[err.FirecubeError]]:
    return [
        cls
        for module in (err, ingestor_errors)
        for _, cls in inspect.getmembers(module, inspect.isclass)
        if issubclass(cls, err.FirecubeError) and cls.__module__ == module.__name__
    ]


_BUILD: dict[str, tuple[tuple[object, ...], dict[str, object]]] = {
    "ConfigFileError": (("not_found",), {"path": Path("c.toml")}),
    "MissingIrregularCoordinateError": (("c", 1), {}),
    "DuplicateIrregularCoordinateError": (("c", 1, "a", "b"), {}),
    "NoDiscoveredItemsError": (("c", None), {}),
    "OptionalExtraMissingError": (("m",), {"extra": "x"}),
    "ClaimReleaseError": (("m",), {"domain": "d", "owner_id": "o"}),
    "ClaimLostError": (("m",), {"domain": "d"}),
    "ExclusiveMaintenanceRefusedError": (("m",), {"product": "p"}),
    "ProductUnderMaintenanceError": (("m",), {"product": "p", "owner_id": None, "stale": None}),
    "ProductLabelMismatchError": (("m",), {"given": "a", "recorded": "b"}),
    "ProductLabelConflictError": (("m",), {"labels": ["a", "b"]}),
    "UnboundedAxisError": (("g",), {}),
    "RemovedConfigKeyError": (("k",), {}),
    "UnsupportedSourceSchemeError": (("h", "s"), {}),
    "AppendOverwriteRefused": ((["t"], "insert"), {}),
    "InsertRefusedError": ((["t"], "insert"), {}),
    "DuplicateExistingTimestampsError": ((["t"], "insert"), {}),
    "SchemaDriftReingestError": (
        (),
        {"store_uri": "u", "dataset_variable": "v", "reason": "extra_incoming_variable"},
    ),
}


@pytest.mark.parametrize("cls", _firecube_error_classes(), ids=lambda cls: cls.__name__)
def test_every_firecube_error_has_an_exit_code_below_internal(
    cls: type[err.FirecubeError],
) -> None:
    args, kwargs = _BUILD.get(cls.__name__, (("message",), {}))

    assert _errors.classify(cls(*args, **kwargs)) is not ExitCode.INTERNAL


P, M, Op, F = err.Param, err.Message, err.Operation, err.OPERATION_FUNCTIONS
U, R = "file:///a b", err.Placeholder('"<reason>"')
SAMPLES = {
    "abandon_run": ({"target": "p", "reason": R}, f"--target p --reason {R}"),
    "clear_claim": ({"target": "p", "yes_i_really_mean_it": 1}, "--target p"),
    "rebuild_compact": ({"target": "p"}, "--target p"),
    "delete_span": ({"target": "p", "force": 1, "dry_run": 0}, "--target p --force"),
    "preallocate": (
        {"target": U, "product_name": "n", "write_mode": "staged", "dry_run": 0},
        f"<plugin> --target {U!r} --product-name n --write-mode staged --no-dry-run",
    ),
    "rebuild_index": (
        {"target": U, "plugin": "p"},
        f"--target {U!r} --plugin p --product-name <product_name>",
    ),
    "validate": ({"target": U, "group_path": "g"}, f"--target {U!r} --group g"),
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


def _operation_call_sites() -> list[Any]:
    """One param per `Operation(...)` or `preallocate_operation(...)` call in src/.

    A `**mapping` argument is not enumerable, so the preallocate defaults are read from the factory.
    """
    defaults = frozenset(preallocate_operation().arguments)
    sites = [pytest.param("preallocate", defaults, id="preallocate_operation-defaults")]
    root = Path(__file__).resolve().parents[2] / "src"
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            callee = getattr(node.func, "id", getattr(node.func, "attr", ""))
            first = node.args[0] if node.args else None
            if callee == "Operation" and isinstance(first, ast.Constant):
                name = first.value
            elif callee == "preallocate_operation":
                name = "preallocate"
            else:
                continue
            keywords = frozenset(k.arg for k in node.keywords if k.arg)
            site = f"{path.relative_to(root)}:{node.lineno}"
            sites.append(pytest.param(name, keywords, id=site))
    return sites


@pytest.mark.contract
@pytest.mark.parametrize(("name", "keywords"), _operation_call_sites())
def test_operation_call_site_names_only_declared_command_parameters(
    name: str, keywords: frozenset[str]
) -> None:
    declared = {param.name for param in _command(OPERATIONS[name]).params}
    assert keywords <= declared, sorted(keywords - declared)


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
def test_error_exposes_message_parts_facts_and_recovery(args) -> None:
    first, op = (Op("rebuild_compact", target=n) for n in "qp")
    inner = M("{slot_window} {o} {first}", slot_window=(1, 5), o=P("time_dim"), first=first)
    message = M("{group} {i} {run_id} {op}", group="g", i=inner, run_id="r", op=op)
    error = err.ConfigurationError(message, *args)
    assert (error.message, error.recovery) == (message, (first, op))
    c = error.context
    assert (c.run_id, c.group, c.slot_window, c.options) == ("r", "g", (1, 5), ("time_dim",))
