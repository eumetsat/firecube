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

"""Capture the producer identity a run records at its start.

The record names the plugin's distribution, its source and the digest of the typed
configuration. A run keeps it at ``meta["producer"]``. Null means unknown.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
from collections.abc import Mapping
from enum import Enum
from importlib.metadata import Distribution, EntryPoint, entry_points
from pathlib import Path, PurePath
from typing import Any
from urllib.parse import urlsplit
from urllib.request import url2pathname

from firecube import __version__ as _FIRECUBE_VERSION
from firecube.ingestor.config.engine import is_experimental_option_key

__all__ = ["capture_producer"]

SCHEMA_VERSION = 1
CANONICALIZATION = "firecube-config-json-v1"
PLUGIN_ENTRY_POINT_GROUP = "firecube.plugins"
GIT_TIMEOUT_S = 5.0

_COMMIT = re.compile(r"^[0-9a-f]{40}([0-9a-f]{24})?$")


@dataclasses.dataclass(frozen=True)
class _Resolution:
    """The plugin's distribution and entry point, and what was read to find them."""

    distribution: Distribution | None
    entry_point: str | None
    evidence: tuple[str, ...]


def capture_producer(
    *, plugin_name: str, plugin_cls: type, configs: Mapping[str, object]
) -> dict[str, Any]:
    """Return the producer record of one run, ready for JSON.

    The plugin's distribution comes from the installed plugin entry points. The
    source comes from the distribution's ``direct_url.json``. Git runs only for an
    editable install. The config digest leaves out ``repr=False`` fields and ``x_*``
    keys, and ``excluded`` names them.

    Args:
        plugin_name: The plugin's registered name.
        plugin_cls: The plugin class the run executes.
        configs: The run's typed configuration objects by tier name, for example
            ``engine``, ``template`` and ``plugin``. ``None`` stands for an absent tier.

    Returns:
        The record with ``schema_version``, ``captured_at``, ``plugin``, ``source``,
        ``firecube`` and ``config``.
    """
    captured_at = dt.datetime.now(dt.UTC).isoformat()
    resolution = _resolve_distribution(plugin_name, plugin_cls)
    dist = resolution.distribution
    source = _source_identity(dist, resolution.evidence)
    digest, excluded = _config_digest(configs)
    return {
        "schema_version": SCHEMA_VERSION,
        "captured_at": captured_at,
        "plugin": {
            "name": plugin_name,
            "distribution": _distribution_name(dist) if dist is not None else None,
            "version": dist.version if dist is not None else None,
            "entry_point": resolution.entry_point,
        },
        "source": source,
        "firecube": {"version": _FIRECUBE_VERSION},
        "config": {
            "algorithm": "sha256",
            "digest": digest,
            "canonicalization": CANONICALIZATION,
            "excluded": excluded,
        },
    }


def _distribution_name(dist: Distribution) -> str:
    """The distribution's project name as its metadata spells it."""
    return str(dist.metadata["Name"])


def _is_parent_or_same(module: str, of: str) -> bool:
    """Whether ``module`` names ``of`` itself or a package that contains it."""
    return of == module or of.startswith(module + ".")


def _entry_point_value(matches: list[EntryPoint], plugin_name: str) -> str | None:
    """The value of the match named ``plugin_name``, else the one value all matches share."""
    for ep in matches:
        if ep.name == plugin_name:
            return ep.value
    values = {ep.value for ep in matches}
    return values.pop() if len(values) == 1 else None


def _resolve_distribution(plugin_name: str, plugin_cls: type) -> _Resolution:
    """Find the distribution whose plugin entry point loads ``plugin_cls``.

    Only entry points under the class's top-level package count. An entry point
    that loads the class itself wins over a module entry point above the class.
    Matches from two distributions, or a match without one, resolve to none.
    """
    cls_module = plugin_cls.__module__
    top_level = cls_module.split(".", 1)[0]
    evidence: list[str] = []
    object_matches: list[EntryPoint] = []
    module_matches: list[EntryPoint] = []
    for ep in entry_points(group=PLUGIN_ENTRY_POINT_GROUP):
        if ep.module.split(".", 1)[0] != top_level:
            continue
        try:
            loaded = ep.load()
        except Exception as exc:
            evidence.append(
                f"entry point {ep.name!r} ({ep.value}) failed to load: {type(exc).__name__}: {exc}"
            )
            continue
        if loaded is plugin_cls:
            object_matches.append(ep)
        elif ep.attr is None and _is_parent_or_same(ep.module, cls_module):
            module_matches.append(ep)

    if object_matches:
        matches, how = object_matches, "load the class"
    else:
        matches, how = module_matches, "name a module that holds the class"
    if not matches:
        evidence.append(
            f"no {PLUGIN_ENTRY_POINT_GROUP} entry point resolves "
            f"{cls_module}:{plugin_cls.__qualname__}"
        )
        return _Resolution(None, None, tuple(evidence))

    names = sorted(ep.name for ep in matches)
    dists = [ep.dist for ep in matches]
    if any(dist is None for dist in dists):
        evidence.append(f"entry points {names} {how}; one has no distribution")
        return _Resolution(None, None, tuple(evidence))
    by_name = {_distribution_name(dist): dist for dist in dists if dist is not None}
    if len(by_name) != 1:
        evidence.append(f"entry points {names} {how}; their distributions are {sorted(by_name)}")
        return _Resolution(None, None, tuple(evidence))

    ((dist_name, dist),) = by_name.items()
    entry_point = _entry_point_value(matches, plugin_name)
    evidence.append(
        f"entry points {names} of {dist_name} {dist.version} {how}"
        + ("" if entry_point is not None else "; their values differ")
    )
    return _Resolution(dist, entry_point, tuple(evidence))


def _unknown_source(evidence: list[str]) -> dict[str, Any]:
    """A source record whose kind is not known."""
    return {
        "kind": "unknown",
        "commit": None,
        "requested_ref": None,
        "dirty": None,
        "evidence": evidence,
    }


def _source_identity(dist: Distribution | None, found: tuple[str, ...]) -> dict[str, Any]:
    """Classify the distribution's source from its PEP 610 ``direct_url.json``.

    No credential of the recorded URL is copied: the record keeps no URL.
    """
    evidence = list(found)
    if dist is None:
        return _unknown_source(evidence)
    try:
        text = dist.read_text("direct_url.json")
    except OSError as exc:
        evidence.append(f"direct_url.json unreadable: {type(exc).__name__}")
        return _unknown_source(evidence)
    if text is None:
        evidence.append("no direct_url.json")
        return _wheel_source(evidence)
    try:
        direct_url = json.loads(text)
    except ValueError:
        evidence.append("direct_url.json is not JSON")
        return _unknown_source(evidence)
    if not isinstance(direct_url, dict):
        evidence.append("direct_url.json is not a JSON object")
        return _unknown_source(evidence)

    vcs_info = direct_url.get("vcs_info")
    if isinstance(vcs_info, dict):
        commit = vcs_info.get("commit_id")
        commit = commit if isinstance(commit, str) and commit else None
        ref = vcs_info.get("requested_revision")
        evidence.append("direct_url.json vcs_info")
        return {
            "kind": "vcs",
            "commit": commit,
            "requested_ref": ref if isinstance(ref, str) else None,
            "dirty": False if commit is not None else None,
            "evidence": evidence,
        }
    dir_info = direct_url.get("dir_info")
    if isinstance(dir_info, dict) and dir_info.get("editable") is True:
        evidence.append("direct_url.json dir_info.editable")
        commit, dirty = _git_state(direct_url.get("url"), evidence)
        return {
            "kind": "editable",
            "commit": commit,
            "requested_ref": None,
            "dirty": dirty,
            "evidence": evidence,
        }
    if isinstance(direct_url.get("archive_info"), dict):
        evidence.append("direct_url.json archive_info")
    elif isinstance(dir_info, dict):
        evidence.append("direct_url.json dir_info, not editable")
    else:
        evidence.append("direct_url.json names no vcs_info, dir_info or archive_info")
        return _unknown_source(evidence)
    return _wheel_source(evidence)


def _wheel_source(evidence: list[str]) -> dict[str, Any]:
    """A source record for an installed build, whose commit is not known."""
    return {
        "kind": "wheel",
        "commit": None,
        "requested_ref": None,
        "dirty": None,
        "evidence": evidence,
    }


def _local_directory(url: object) -> Path | None:
    """The local directory a ``file://`` URL names, or ``None``."""
    if not isinstance(url, str):
        return None
    parts = urlsplit(url)
    if parts.scheme != "file" or parts.netloc not in ("", "localhost"):
        return None
    path = Path(url2pathname(parts.path))
    return path if path.is_dir() else None


def _git(directory: Path, *args: str) -> tuple[str | None, str]:
    """Run one read-only git command in ``directory``.

    Returns its stdout, or ``None`` and the reason it failed.
    """
    env = {**os.environ, "GIT_OPTIONAL_LOCKS": "0"}
    try:
        completed = subprocess.run(
            ["git", "-C", str(directory), *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_S,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, f"timed out after {GIT_TIMEOUT_S:g} s"
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f"not run: {type(exc).__name__}"
    if completed.returncode != 0:
        return None, f"exit {completed.returncode}"
    return completed.stdout, "ok"


def _git_state(url: object, evidence: list[str]) -> tuple[str | None, bool | None]:
    """The commit and dirty state of an editable install's directory; ``None`` when unknown.

    The status covers that directory only. ``GIT_OPTIONAL_LOCKS=0`` keeps git from
    writing its index.
    """
    directory = _local_directory(url)
    if directory is None:
        evidence.append("editable directory is not a local directory")
        return None, None
    head, outcome = _git(directory, "rev-parse", "--is-inside-work-tree", "HEAD")
    if head is None:
        evidence.append(f"git rev-parse in {directory}: {outcome}")
        return None, None
    lines = head.split()
    if len(lines) != 2 or lines[0] != "true" or not _COMMIT.match(lines[1]):
        evidence.append(f"git rev-parse in {directory}: not a work tree with a commit")
        return None, None
    commit = lines[1]
    evidence.append(f"git rev-parse HEAD in {directory}")
    status, outcome = _git(
        directory, "status", "--porcelain", "--untracked-files=normal", "--", "."
    )
    if status is None:
        evidence.append(f"git status in {directory}: {outcome}")
        return commit, None
    evidence.append(f"git status of {directory}")
    return commit, bool(status.strip())


def _config_digest(configs: Mapping[str, object]) -> tuple[str, list[str]]:
    """The SHA-256 of the canonical configuration JSON, and the names left out of it."""
    excluded: list[str] = []
    canonical = {
        str(tier): _canonical(value, str(tier), excluded) for tier, value in configs.items()
    }
    payload = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest(), sorted(excluded)


def _canonical(value: object, path: str, excluded: list[str]) -> Any:
    """Turn one configuration value into plain JSON values, skipping excluded names.

    A dataclass field declared ``repr=False``, an ``x_*`` key and a value with no
    JSON form are left out and named in ``excluded``.
    """
    if isinstance(value, Enum):
        return value.value if isinstance(value.value, str) else value.name
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, PurePath):
        return str(value)
    if isinstance(value, dt.datetime | dt.date | dt.time):
        return value.isoformat()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        out: dict[str, Any] = {}
        for field in dataclasses.fields(value):
            name = f"{path}.{field.name}"
            if not field.repr or is_experimental_option_key(field.name):
                excluded.append(name)
                continue
            out[field.name] = _canonical(getattr(value, field.name), name, excluded)
        return out
    if isinstance(value, Mapping):
        mapping: dict[str, Any] = {}
        for key, item in value.items():
            text = str(_canonical(key, path, excluded)) if isinstance(key, Enum) else str(key)
            name = f"{path}.{text}"
            if is_experimental_option_key(text):
                excluded.append(name)
                continue
            mapping[text] = _canonical(item, name, excluded)
        return mapping
    if isinstance(value, list | tuple):
        return [_canonical(item, f"{path}[{index}]", excluded) for index, item in enumerate(value)]
    if isinstance(value, set | frozenset):
        items = [_canonical(item, f"{path}[]", excluded) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, allow_nan=True))
    excluded.append(path)
    return None
