"""Producer identity capture; a new module because no test covers ``ingestor/runtime/producer.py``."""

from __future__ import annotations

import dataclasses
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from cf_time_dim_test_plugin import CFTimeDimIngestor
from index_spec_integer_test_plugin import IntegerAxisIngestor, MixedAxisIngestor

from firecube.ingestor.registry import loader
from firecube.ingestor.runtime.producer import capture_producer

pytestmark = pytest.mark.unit

FIXTURES = "firecube-test-plugins"
SHA = "a" * 40


def _capture(cls: type, name: str = "p", **configs: object) -> dict:
    return capture_producer(plugin_name=name, plugin_cls=cls, configs=configs)


def _install(tmp_path, monkeypatch, direct_url: object = None) -> type:
    """A distribution named after the test dir, whose package re-exports class ``P``."""
    name = tmp_path.name
    pkg = tmp_path / name
    pkg.mkdir()
    (pkg / "_impl.py").write_text("class P: ...\n")
    (pkg / "__init__.py").write_text("from ._impl import P\n")
    info = tmp_path / f"{name}-1.0.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0\n")
    (info / "entry_points.txt").write_text(f"[firecube.plugins]\nplug = {name}:P\n")
    if direct_url is not None:
        (info / "direct_url.json").write_text(json.dumps(direct_url))
    monkeypatch.syspath_prepend(str(tmp_path))
    for module in (name, f"{name}._impl"):  # the import below adds them; undo removes them
        monkeypatch.setitem(sys.modules, module, None)
        del sys.modules[module]
    return __import__(name)._impl.P  # the class, whose module is `<name>._impl`


def _repo(path: Path) -> Path:
    if shutil.which("git") is None:
        pytest.skip("git is not available on PATH")
    path.mkdir()
    (path / "f.txt").write_text("one\n")
    git = ["git", "-C", str(path), "-c", "user.name=t", "-c", "user.email=t@t"]
    for args in (["init", "-q"], ["add", "f.txt"], ["commit", "-q", "-m", "c"]):
        subprocess.run([*git, *args], check=True, capture_output=True)
    return path


@pytest.mark.parametrize("reset", [False, True], ids=["cached", "after_cache_reset"])
@pytest.mark.parametrize(
    ("cls", "name"),
    [(IntegerAxisIngestor, "index_spec_integer"), (MixedAxisIngestor, "index_spec_integer_mixed")],
)
def test_two_entry_points_into_one_module_each_resolve_to_their_own_class(
    reset, cls, name, monkeypatch
):
    if reset:
        # The reset clears the registry in place; give it a copy so other tests keep theirs.
        monkeypatch.setattr(loader, "AVAILABLE_INGESTORS", dict(loader.AVAILABLE_INGESTORS))
        monkeypatch.setattr(loader, "_LOADED", loader._LOADED)
        loader.reset_plugin_discovery_cache()

    plugin = _capture(cls, name)["plugin"]

    assert plugin["distribution"] == FIXTURES
    assert plugin["version"] == "0.1.0"
    assert plugin["entry_point"] == f"index_spec_integer_test_plugin:{cls.__name__}"


def test_a_module_only_entry_point_resolves_the_classes_of_its_module():
    # The entry point names the package; the class lives in its submodule `.ingestor`.
    plugin = _capture(CFTimeDimIngestor, "cf_time_dim")["plugin"]

    assert (plugin["distribution"], plugin["version"], plugin["entry_point"]) == (
        FIXTURES,
        "0.1.0",
        "cf_time_dim_test_plugin",
    )


def test_a_class_re_exported_by_its_package_resolves_through_the_loaded_object(
    tmp_path, monkeypatch
):
    cls = _install(tmp_path, monkeypatch)

    plugin = _capture(cls, "plug")["plugin"]

    assert (plugin["distribution"], plugin["entry_point"]) == (
        cls.__module__.split(".")[0],
        cls.__module__.split(".")[0] + ":P",
    )


def test_a_class_without_an_entry_point_has_an_unknown_source():
    record = _capture(type("Orphan", (), {}))

    assert record["plugin"]["distribution"] is None
    assert (record["source"]["kind"], record["source"]["commit"], record["source"]["dirty"]) == (
        "unknown",
        None,
        None,
    )


@pytest.mark.parametrize(
    ("direct_url", "kind", "commit", "ref", "dirty"),
    [
        (
            {
                "url": "https://u:hunter2@h/r.git",
                "vcs_info": {"vcs": "git", "commit_id": SHA, "requested_revision": "v1"},
            },
            "vcs",
            SHA,
            "v1",
            False,
        ),
        ({"url": "https://h/x.whl", "archive_info": {}}, "wheel", None, None, None),
        (None, "wheel", None, None, None),
    ],
    ids=["vcs", "archive_wheel", "no_direct_url"],
)
def test_source_kind_follows_the_direct_url_and_leaks_no_credential(
    tmp_path, monkeypatch, direct_url, kind, commit, ref, dirty
):
    cls = _install(tmp_path, monkeypatch, direct_url)

    record = _capture(cls)

    source = record["source"]
    assert (source["kind"], source["commit"], source["requested_ref"], source["dirty"]) == (
        kind,
        commit,
        ref,
        dirty,
    )
    assert "hunter2" not in json.dumps(record)


@pytest.mark.parametrize(("edit", "dirty"), [(False, False), (True, True)], ids=["clean", "dirty"])
def test_editable_source_reports_git_state_and_leaves_the_git_index_untouched(
    tmp_path, monkeypatch, edit, dirty
):
    repo = _repo(tmp_path / "repo")
    cls = _install(
        tmp_path, monkeypatch,
        {"url": repo.as_uri(), "dir_info": {"editable": True}},
    )  # fmt: skip
    (repo / "f.txt").write_text("two\n" if edit else "one\n")  # a stat change git would refresh
    index = (repo / ".git" / "index").read_bytes()

    source = _capture(cls)["source"]

    assert (source["kind"], source["dirty"]) == ("editable", dirty)
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    assert source["commit"] == head
    assert (repo / ".git" / "index").read_bytes() == index


def test_editable_source_without_git_has_null_commit_and_null_dirty(tmp_path, monkeypatch):
    repo = _repo(tmp_path / "repo")
    cls = _install(
        tmp_path,
        monkeypatch,
        {"url": repo.as_uri(), "dir_info": {"editable": True}},
    )
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))

    source = _capture(cls)["source"]

    assert (source["kind"], source["commit"], source["dirty"]) == ("editable", None, None)


@dataclasses.dataclass
class _Cfg:
    a: int = 1
    token: str = dataclasses.field(default="s3cret", repr=False)


def test_config_digest_ignores_key_order_and_changes_with_a_value():
    base = _capture(object, plugin={"a": 1, "b": 2})["config"]["digest"]

    assert _capture(object, plugin={"b": 2, "a": 1})["config"]["digest"] == base
    assert _capture(object, plugin={"a": 1, "b": 3})["config"]["digest"] != base


def test_config_digest_leaves_out_repr_false_fields_and_x_keys_and_names_them():
    base_record = _capture(object, plugin=_Cfg(), extra={"k": 1})
    changed_record = _capture(object, plugin=_Cfg(token="other"), extra={"k": 1, "x_probe": 9})
    base, changed = base_record["config"], changed_record["config"]

    assert changed["digest"] == base["digest"]
    assert changed["excluded"] == ["extra.x_probe", "plugin.token"]
    # The configured value never reaches any part of its whole record.
    assert "s3cret" not in json.dumps(base_record)
    assert "other" not in json.dumps(changed_record)
