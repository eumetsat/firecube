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

"""Docs-coverage contract for the public SDK facades.

Invariant: every name exported from a public facade (``firecube.ingestor.api``,
``firecube.core.api``, ``firecube.ingestor.extensions``) is either rendered in
the API reference through a mkdocstrings ``:::`` directive or listed in
``INTENTIONALLY_UNDOCUMENTED`` with a reason. A new export fails this test
until it is documented or consciously excluded, and a documented name may not
stay on the exclusion list. Directives must also target facade paths only, so
internal module paths never appear in the public reference.

Coverage means the reference actually says something: a directive places the
symbol on a page, and its docstring supplies that page's text, so both are
required.
"""

from __future__ import annotations

import re
from importlib import import_module
from pathlib import Path

import griffe
import pytest

pytestmark = pytest.mark.docs_static

_REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = _REPO_ROOT / "docs"
SRC_DIR = _REPO_ROOT / "src"

_DIRECTIVE = re.compile(r"^::: (firecube\.[\w.]+)\s*$", re.MULTILINE)

PUBLIC_MODULES = (
    "firecube.ingestor.api",
    "firecube.core.api",
    "firecube.ingestor.extensions",
)

# Exports deliberately kept out of the public API reference. Each entry needs
# a reason; remove the entry when the name gains a reference page.
INTENTIONALLY_UNDOCUMENTED: dict[str, dict[str, str]] = {
    "firecube.ingestor.api": {
        # Content-addressed manifest types: public re-exports used by the engine
        # for IrregularTimeAxis AUTO planning. Plugin authors do not construct
        # these directly; they are engine-internal planning data.
        "ItemManifestEntry": "engine-internal planning type; plugin authors do not construct this",
        "validate_manifest_entries": "engine-internal manifest validation helper",
        # Engine write-strategy implementations, not an authoring surface.
        "AppendStrategy": "engine write strategy",
        "AppendWriteStrategy": "engine write strategy",
        "IndexedRegionStrategy": "engine write strategy",
        "RegionWriteStrategy": "engine write strategy",
        "TensogramWriteStrategy": "engine write strategy",
        # Structural protocols consumed by the engine, not implemented by
        # plugin authors directly.
        "DatasetProducer": "structural protocol",
        "Ingestor": "structural protocol",
        "PipelineHost": "structural protocol",
        "SourceFile": "structural protocol",
        "is_dataset_producer": "structural protocol helper",
        # Engine/CLI plumbing exported for host integration.
        "CatalogGroupInfo": "host integration type",
        "IngestContext": "engine-side input context",
        "IngestManifest": "engine manifest type",
        "LocalSourceFile": "engine source-file type",
        "SpanCoverage": "control-plane type",
        "WriteDomain": "control-plane type",
        "config_keys": "CLI introspection helper",
        "discover_ingestors": "CLI discovery helper",
        "merge_batch_metrics": "engine aggregation helper",
        # Engine-internal slot-range types and helpers; not part of the
        # plugin-authoring surface. Plugin authors declare index_spec() and
        # inspect_item(); the engine owns slot assignment and alignment.
        "PlannedRange": "engine-internal; not part of the plugin-authoring surface",
        "SlotRange": "engine-internal; not part of the plugin-authoring surface",
        "chunk_align_ranges": "engine-internal; not part of the plugin-authoring surface",
        "compute_covered_ranges": "engine-internal; not part of the plugin-authoring surface",
        "validate_chunk_alignment": "engine-internal; not part of the plugin-authoring surface",
        "validate_slot_range": "engine-internal; not part of the plugin-authoring surface",
        "warn_if_misaligned": "engine-internal; not part of the plugin-authoring surface",
    },
    "firecube.core.api": {
        # Content-addressed manifest types: public re-exports used by the engine
        # for IrregularTimeAxis AUTO planning. Plugin authors do not construct
        # these directly; they are engine-internal planning data.
        "ItemManifestEntry": "engine-internal planning type; plugin authors do not construct this",
        "validate_manifest_entries": "engine-internal manifest validation helper",
        # Control-plane, archive, and host-integration helpers.
        "CatalogGroupInfo": "host integration type",
        "RegionZarrWriterProtocol": "engine writer protocol",
        "RunInfo": "control-plane type",
        "FIRECUBE_GROUP_IDENTITY_HASH_ATTR": "engine identity-marker attr",
        "_compute_group_identity_hash": "engine identity helper; not a plugin-authoring surface",
        "compute_group_identity_hash": "engine identity helper; not a plugin-authoring surface",
        # Byte-parity legacy types superseded by IndexSpec + RegularTimeAxis;
        # older docs versions cover this surface.
        "SlotAxis": "byte-parity legacy; use IndexSpec + RegularTimeAxis; older docs versions cover this surface",
        "SlotIndexModel": "byte-parity legacy; use IndexSpec + RegularTimeAxis; older docs versions cover this surface",
        "decode_time_array": "archive integration helper",
        "describe_control_plane": "control-plane inspection helper",
        "ensure_product_uri": "engine URI helper",
        "read_chunk_grid_with_shards": "engine chunk-grid helper",
        "require_tensogram": "optional-dependency guard",
        "resolve_dataset_target": "engine target resolution helper",
    },
    "firecube.ingestor.extensions": {},
}


def _documented_targets() -> set[str]:
    targets: set[str] = set()
    for page in DOCS_DIR.rglob("*.md"):
        targets.update(_DIRECTIVE.findall(page.read_text(encoding="utf-8")))
    return targets


@pytest.fixture(scope="module", name="documented")
def _documented() -> set[str]:
    return _documented_targets()


@pytest.fixture(scope="module", name="model")
def _model():
    """Load firecube statically so docstrings are read without importing code.

    Static loading is required rather than ``inspect.getdoc``: a runtime lookup
    on an exported type alias returns the docstring of its underlying type
    (``SlotRange`` would report ``tuple``'s), which would pass silently.
    """
    return griffe.load("firecube", search_paths=[str(SRC_DIR)], submodules=True)


@pytest.mark.parametrize("module_name", PUBLIC_MODULES)
def test_documented_exports_have_docstrings(module_name: str, model) -> None:
    """A rendered entry with no docstring is an empty page section.

    The directive check above proves a symbol is on a reference page; this
    proves the page will actually say something about it.
    """
    exports = set(import_module(module_name).__all__)
    allowlist = INTENTIONALLY_UNDOCUMENTED[module_name]
    prefix = module_name.removeprefix("firecube.")

    undocumented: list[str] = []
    unresolved: list[str] = []
    for name in sorted(exports - set(allowlist)):
        try:
            obj = model[f"{prefix}.{name}"]
            target = obj.final_target if obj.is_alias else obj
        except (KeyError, griffe.AliasResolutionError):
            unresolved.append(name)
            continue
        if not (target.docstring and target.docstring.value.strip()):
            undocumented.append(name)

    assert not unresolved, (
        f"{module_name}: exports that static analysis cannot resolve: "
        f"{unresolved}. mkdocstrings resolves symbols the same way, so these "
        "would fail to render in the API reference."
    )
    assert not undocumented, (
        f"{module_name}: public exports rendered in the API reference without a "
        f"docstring: {undocumented}. The docstring is the reference text — add "
        "one (see plans/STYLE.md), or allowlist the export with a reason."
    )


@pytest.mark.parametrize("module_name", PUBLIC_MODULES)
def test_every_export_documented_or_allowlisted(module_name: str, documented: set[str]) -> None:
    exports = set(import_module(module_name).__all__)
    allowlist = INTENTIONALLY_UNDOCUMENTED[module_name]

    stale_allowlist = set(allowlist) - exports
    assert not stale_allowlist, (
        f"{module_name}: allowlist names no longer exported: {sorted(stale_allowlist)}"
    )

    # A symbol re-exported by more than one facade is the *same object* (see
    # test_reexports_are_identical_objects). It is documented once, on the page
    # whose audience it serves, under that page's facade: plugin-authoring
    # pages carry the ingestor path, core pages the core path. So a directive
    # under any facade documents the symbol for all of them — requiring one per
    # facade would render the identical entry two or three times.
    documented_exports = {
        name
        for name in exports
        if any(f"{module}.{name}" in documented for module in PUBLIC_MODULES)
    }
    missing = exports - documented_exports - set(allowlist)
    assert not missing, (
        f"{module_name}: exports missing from docs/reference and not "
        f"allowlisted: {sorted(missing)}. Add a `::: <facade>.<name>` directive "
        "on the page serving that symbol's audience, or an "
        "INTENTIONALLY_UNDOCUMENTED entry with a reason."
    )

    over_allowlisted = {name for name in allowlist if f"{module_name}.{name}" in documented}
    assert not over_allowlisted, (
        f"{module_name}: names both documented and allowlisted (remove the "
        f"stale allowlist entries): {sorted(over_allowlisted)}"
    )


def test_reexports_are_identical_objects() -> None:
    """A name shared by two facades must resolve to one object, not two.

    ``test_every_export_documented_or_allowlisted`` accepts a directive under
    any facade as documenting the symbol for all of them. That is only sound
    while the facades re-export the same object: two distinct types sharing a
    name would leave one of them silently undocumented.
    """
    modules = {name: import_module(name) for name in PUBLIC_MODULES}
    divergent: list[str] = []
    for i, left in enumerate(PUBLIC_MODULES):
        for right in PUBLIC_MODULES[i + 1 :]:
            shared = set(modules[left].__all__) & set(modules[right].__all__)
            divergent.extend(
                f"{name}: {left} and {right} export different objects"
                for name in sorted(shared)
                if getattr(modules[left], name) is not getattr(modules[right], name)
            )

    assert not divergent, (
        "facades re-export the same name as different objects, so a single "
        f"reference entry cannot cover both: {divergent}"
    )


def test_directives_use_public_facade_paths_only(documented: set[str]) -> None:
    facade_prefixes = tuple(f"{module}." for module in PUBLIC_MODULES)
    offenders = sorted(
        target
        for target in documented
        if target not in PUBLIC_MODULES and not target.startswith(facade_prefixes)
    )
    assert not offenders, (
        "mkdocstrings directives must target the public facades "
        f"{PUBLIC_MODULES}; internal paths found: {offenders}"
    )
