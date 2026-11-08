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

"""Run a metadata migration step on a product as a recorded maintenance run.

`migrate_product` runs one `MigrationStep` under the product's exclusive maintenance
claim. It journals the planned documents before the first store write, so an
interrupted run resumes from the journal. A real run refuses a store with no control
plane; `adopt_product` gives a store that Firecube did not write one, as a separate step. `apply_metadata_plan` writes planned documents and is
shared with the release executor.
"""

from __future__ import annotations

import json
import posixpath
import uuid
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, cast

from firecube.core.controlplane.product_label import check_product_label, stamp_product_label
from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    LAST_MODIFIER_ATTR,
    MAINTENANCE_OP_ADOPT,
    MAINTENANCE_OP_MIGRATE,
    RELEASE_ATTR,
    RUN_JOURNAL_FILENAME,
    RUNS_DIRNAME,
    canonical_json_bytes,
    exclusive_maintenance_domain,
)
from firecube.core.errors import (
    CommandName,
    FirecubeError,
    Message,
    Operation,
    Param,
    Placeholder,
    ProductUnderMaintenanceError,
)
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.zarr._reserved_attrs import RESERVED_ARRAY_ATTRS
from firecube.core.zarr._reserved_root_attrs import RESERVED_ROOT_ATTRS
from firecube.core.zarr.metadata_documents import (
    ABSENT,
    MetadataDocument,
    MetadataDocumentKind,
    build_consolidated_image,
    metadata_document_digest,
    parse_node_document,
    patch_attributes,
)
from firecube.core.zarr.migration import (
    AttributePatch,
    MetadataPlan,
    MigrationStep,
    NodeView,
    ReleaseRecord,
    StoreView,
)

if TYPE_CHECKING:
    from zarr.abc.store import Store

    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.product.open import Product
    from firecube.core.storage.session import StorageSession
    from firecube.core.zarr.guard.metadata_authority import MetadataScope

PHYSICAL: Final = "physical"
INTERPRETATION: Final = "interpretation"
DESCRIPTIVE: Final = "descriptive"
UNKNOWN: Final = "unknown"
ALLOWABLE_CLASSES: Final = frozenset({INTERPRETATION, UNKNOWN})
"""The attribute classes a caller may allow; a physical attribute is never allowed."""

PHYSICAL_ATTRIBUTE_PREFIX: Final = "firecube_"
INTERPRETATION_ATTRIBUTES: Final = frozenset(
    {
        "scale_factor",
        "add_offset",
        "units",
        "calendar",
        "grid_mapping",
        "crs",
        "missing_value",
        "valid_min",
        "valid_max",
        "valid_range",
    }
)
"""Attributes that change how a reader decodes values; a step changes them only when allowed."""

DESCRIPTIVE_ATTRIBUTES: Final = frozenset(
    {
        "title",
        "summary",
        "history",
        "institution",
        "source",
        "references",
        "comment",
        "license",
        "Conventions",
        "long_name",
        "standard_name",
        "description",
        "keywords",
        "creator_name",
        "creator_email",
        "creator_url",
        "publisher_name",
        "publisher_email",
        "publisher_url",
        "project",
        "acknowledgment",
        "product_version",
    }
)
"""Attributes that describe a store without changing how it decodes; a step changes them freely."""

OUTCOME_EXECUTED: Final = "executed"
OUTCOME_ADOPTED: Final = "adopted"
JOURNAL_SCHEMA_VERSION: Final = "v1"
_RUN_PREFIX: Final = "migrate-"
_DIGEST_PREFIX: Final = "sha256:"
_V3_NODE: Final = "zarr.json"
_V2_GROUP: Final = ".zgroup"
_V2_ARRAY: Final = ".zarray"
_V2_ATTRS: Final = ".zattrs"
_V2_IMAGE: Final = ".zmetadata"
_CONSOLIDATED_FIELD: Final = "consolidated_metadata"
_NODE_KINDS: Final = frozenset(
    {
        MetadataDocumentKind.V3_GROUP,
        MetadataDocumentKind.V3_ARRAY,
        MetadataDocumentKind.V2_GROUP,
        MetadataDocumentKind.V2_ARRAY,
        MetadataDocumentKind.V2_ATTRS,
    }
)
_ADOPT_COMMAND: Final = CommandName(("migrate", "adopt"), library="adopt_product")
_BEFORE, _AFTER, _THIRD = "before", "after", "third"
_JOURNALED: Final = "journaled"


def _ignore_line(line: str) -> None:
    """Discard a progress or warning line."""


class MigrationRefusedError(FirecubeError):
    """The executor refuses the migration.

    Attributes:
        step_id: The step the refusal concerns, or `None` when it concerns no step.
    """

    def __init__(self, message: str, *, step_id: str | None = None) -> None:
        self.step_id = step_id
        super().__init__(message)


class StepIdentityError(MigrationRefusedError):
    """The step is not installed, has no distribution, or ran before at another version.

    Attributes:
        recorded: `(distribution, version)` of an earlier run of the step, or `None`.
        installed: `(distribution, version)` of the installed step, or `None`.
    """

    def __init__(
        self,
        message: str,
        *,
        step_id: str | None,
        recorded: tuple[str, str] | None = None,
        installed: tuple[str, str] | None = None,
    ) -> None:
        self.recorded = recorded
        self.installed = installed
        super().__init__(message, step_id=step_id)


class StepPreconditionError(MigrationRefusedError):
    """The store has not applied a step this one depends on, or is at another release.

    Attributes:
        missing: The ids of required steps the store has not applied.
        from_release: The release the step requires, or `None`.
        current_release: The store's current release, or `None`.
    """

    def __init__(
        self,
        message: str,
        *,
        step_id: str,
        missing: tuple[str, ...] = (),
        from_release: str | None = None,
        current_release: str | None = None,
    ) -> None:
        self.missing = missing
        self.from_release = from_release
        self.current_release = current_release
        super().__init__(message, step_id=step_id)


class MigrationPlanError(MigrationRefusedError):
    """The step's plan cannot be applied to the store as it stands."""


@dataclass(frozen=True)
class AttributeVerdict:
    """The class of one attribute a plan sets or removes, and whether the run may change it.

    Attributes:
        node_path: The node path, `""` for the root.
        key: The attribute name.
        attribute_class: `"physical"`, `"interpretation"`, `"descriptive"` or `"unknown"`.
        allowed: Whether the run may change it.
    """

    node_path: str
    key: str
    attribute_class: str
    allowed: bool


class AttributeClassError(MigrationRefusedError):
    """The plan changes attributes its classes refuse.

    Attributes:
        refused: The refused attributes.
    """

    def __init__(self, message: str, *, step_id: str, refused: tuple[AttributeVerdict, ...]):
        self.refused = refused
        super().__init__(message, step_id=step_id)


class MetadataStateError(MigrationRefusedError):
    """Documents of the store hold bytes that neither the plan's before nor its after state has.

    Attributes:
        run_id: The run whose plan the documents are compared with, or `None`.
        paths: The store-relative paths of those documents.
    """

    def __init__(
        self,
        message: str,
        *,
        paths: tuple[str, ...],
        run_id: str | None = None,
        step_id: str | None = None,
    ) -> None:
        self.run_id = run_id
        self.paths = paths
        super().__init__(message, step_id=step_id)


@dataclass(frozen=True)
class InstalledStep:
    """A migration step that an installed distribution offers.

    Attributes:
        id: The step id, as its entry point names it.
        create: Builds the step from the run's step options.
        distribution: The name of the distribution that installs the step, or `None`.
        version: The version of that distribution, or `None`.
    """

    id: str
    create: Callable[[Mapping[str, Any]], MigrationStep]
    distribution: str | None
    version: str | None


@dataclass(frozen=True)
class PlannedDocument:
    """One store document a plan writes, with the digests that classify its state.

    Attributes:
        path: The store-relative path.
        before: The digest the document had before the plan, or `None` when it did not exist.
        after: The digest of the planned bytes.
        data: The planned bytes; `None` only when `before` equals `after`.
        image: Whether the document is the root consolidated image.
    """

    path: str
    before: str | None
    after: str
    data: bytes | None
    image: bool = False


@dataclass(frozen=True)
class MetadataTarget:
    """The product a plan writes: its control plane and the Zarr store of its documents.

    Attributes:
        manager: The product's control plane.
        product: The logical product name.
        store: The Zarr store the documents are put into.
        scope: The metadata scope of `store`, bound to it.
    """

    manager: ChunkManager
    product: str
    store: Store
    scope: MetadataScope


@dataclass(frozen=True)
class MigrationResult:
    """What `migrate_product` did.

    Attributes:
        outcome: `"executed"`, `"adopted"`, `"already-applied"` or `"dry-run"`.
        run_id: The maintenance run, or the run that applied the step before; `None` for a dry run.
        step_id: The step id.
        distribution: The step's distribution.
        version: The step's distribution version.
        before_snapshot: The snapshot id before the step, or `None`.
        after_snapshot: The snapshot id after the step, or `None`.
        documents: The paths the plan writes, in write order.
        verdicts: The class of every attribute the step sets or removes.
        resumed_from: The interrupted run the run resumed, or `None`.
        release: The release the run created, or `None`.
    """

    outcome: Literal["executed", "adopted", "already-applied", "dry-run"]
    run_id: str | None
    step_id: str
    distribution: str
    version: str
    before_snapshot: str | None
    after_snapshot: str | None
    documents: tuple[str, ...]
    verdicts: tuple[AttributeVerdict, ...]
    resumed_from: str | None = None
    release: ReleaseRecord | None = None


@dataclass(frozen=True)
class AdoptionResult:
    """What `adopt_product` did.

    Adopting a Zarr format 2 store also sends one warning, because Firecube cannot append to it.

    Attributes:
        outcome: `"adopted"`, `"already-product"` when the store had a control plane, or
            `"dry-run"` when a preview found a store it would adopt.
        run_id: The recorded `adopt` run, or `None`.
        zarr_format: The format of the store, `2` or `3`, or `None` when nothing was adopted.
        label_recorded: Whether the store records the product label on return. A dry run
            reports `False` when a real run would record it.
    """

    outcome: Literal["adopted", "already-product", "dry-run"]
    run_id: str | None
    zarr_format: int | None
    label_recorded: bool


@dataclass(frozen=True)
class MigrationRunInfo:
    """One recorded migration run of a product.

    Attributes:
        run_id: The maintenance run id.
        status: The run status: `"started"`, `"complete"`, `"failed"` or `"abandoned"`.
        step_id: The step id.
        distribution: The step's distribution.
        version: The step's distribution version.
        outcome: `"executed"` or `"adopted"`.
        started_at: Unix time in seconds when the run started.
    """

    run_id: str
    status: str
    step_id: str
    distribution: str
    version: str
    outcome: str
    started_at: float


class ReleaseCreator(Protocol):
    """Creates the release a step names, inside the migration's claim and run.

    The creator resumes from its own checkpoints. It calls `take_snapshot` only when
    no record of `version` names a snapshot yet.
    """

    def __call__(
        self,
        *,
        manager: ChunkManager,
        product: str,
        version: str,
        run_id: str,
        take_snapshot: Callable[[], str],
    ) -> ReleaseRecord:
        """Record release `version` and return it; `take_snapshot` takes the after-snapshot."""
        ...


@dataclass(frozen=True)
class _JournalEntry:
    path: str
    before: str | None
    after: str
    image: bool


@dataclass(frozen=True)
class _Journal:
    run_id: str
    step_id: str
    distribution: str
    version: str
    outcome: str
    before_snapshot: str
    to_release: str | None
    resumed_from: str | None
    overrides: tuple[dict[str, str], ...]
    entries: tuple[_JournalEntry, ...]

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "plan_kind": "metadata",
            "run_id": self.run_id,
            "step": {
                "id": self.step_id,
                "distribution": self.distribution,
                "version": self.version,
            },
            "outcome": self.outcome,
            "before_snapshot": self.before_snapshot,
            "to_release": self.to_release,
            "resumed_from": self.resumed_from,
            "overrides": list(self.overrides),
            "documents": [
                {"path": e.path, "before": e.before, "after": e.after, "image": e.image}
                for e in self.entries
            ],
        }


def _journal_from_bytes(run_id: str, data: bytes) -> _Journal:
    try:
        content = json.loads(data)
        if content.get("schema_version") != JOURNAL_SCHEMA_VERSION or content["run_id"] != run_id:
            raise ValueError("schema version or run id does not match")
        step = content["step"]
        return _Journal(
            run_id=run_id,
            step_id=str(step["id"]),
            distribution=str(step["distribution"]),
            version=str(step["version"]),
            outcome=str(content["outcome"]),
            before_snapshot=str(content["before_snapshot"]),
            to_release=content["to_release"],
            resumed_from=content["resumed_from"],
            overrides=tuple(dict(item) for item in content["overrides"]),
            entries=tuple(
                _JournalEntry(
                    path=str(item["path"]),
                    before=item["before"],
                    after=str(item["after"]),
                    image=bool(item["image"]),
                )
                for item in content["documents"]
            ),
        )
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise MigrationRefusedError(f"the journal of run {run_id} cannot be read: {exc}") from exc


def _error_text(exc: BaseException) -> str:
    """Return the `Message` an exception was raised with, else its text."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


def _blob_id(digest: str) -> str:
    return digest.removeprefix(_DIGEST_PREFIX)


def _json_equal(left: Any, right: Any) -> bool:
    return canonical_json_bytes({"v": left}) == canonical_json_bytes({"v": right})


# --- classifier -------------------------------------------------------------------------


def classify_attribute(key: str, *, descriptive_keys: Collection[str] = ()) -> str:
    """Return the class of an attribute name: physical, interpretation, descriptive or unknown.

    A reserved or `firecube_` name is physical whatever `descriptive_keys` says.
    """
    if key in RESERVED_ARRAY_ATTRS or key in RESERVED_ROOT_ATTRS:
        return PHYSICAL
    if key.startswith(PHYSICAL_ATTRIBUTE_PREFIX):
        return PHYSICAL
    if key in INTERPRETATION_ATTRIBUTES:
        return INTERPRETATION
    if key in DESCRIPTIVE_ATTRIBUTES or key in descriptive_keys:
        return DESCRIPTIVE
    return UNKNOWN


def _verdicts(
    plan: Sequence[AttributePatch], *, descriptive_keys: Collection[str], allow: Collection[str]
) -> tuple[AttributeVerdict, ...]:
    verdicts: dict[tuple[str, str], AttributeVerdict] = {}
    for patch in plan:
        for key in (*patch.set, *patch.remove):
            kind = classify_attribute(key, descriptive_keys=descriptive_keys)
            allowed = kind == DESCRIPTIVE or (kind != PHYSICAL and kind in allow)
            verdicts[(patch.node_path, key)] = AttributeVerdict(patch.node_path, key, kind, allowed)
    return tuple(verdicts[key] for key in sorted(verdicts))


def _refuse_classes(step_id: str, verdicts: Sequence[AttributeVerdict]) -> None:
    refused = tuple(verdict for verdict in verdicts if not verdict.allowed)
    if not refused:
        return
    listed = ", ".join(f"{v.node_path or '/'}:{v.key} ({v.attribute_class})" for v in refused)
    classes = sorted({v.attribute_class for v in refused} & ALLOWABLE_CLASSES)
    hint: str | Message = ""
    if classes:
        options = [Message("{allow}", allow=Param("allow", name)) for name in classes]
        hint = Message(" Pass {allow} to change them.", allow=Message.join(" and ", options))
    physical = (
        ""
        if PHYSICAL not in {v.attribute_class for v in refused}
        else (" A physical attribute is never changed by a migration step.")
    )
    raise AttributeClassError(
        Message(
            "Step {step_id} changes attributes it may not change: {listed}.{physical}{hint}",
            step_id=step_id,
            listed=listed,
            physical=physical,
            hint=hint,
        ),
        step_id=step_id,
        refused=refused,
    )


# --- documents and plans ----------------------------------------------------------------


def _node_documents(
    documents: Iterable[MetadataDocument],
) -> dict[str, dict[str, MetadataDocument]]:
    nodes: dict[str, dict[str, MetadataDocument]] = {}
    for document in documents:
        if document.path.split("/", 1)[0] == CONTROL_DIRNAME or document.kind not in _NODE_KINDS:
            continue
        node, name = posixpath.dirname(document.path), posixpath.basename(document.path)
        nodes.setdefault(node, {})[name] = document
    return nodes


def _primary(names: Mapping[str, MetadataDocument]) -> MetadataDocument | None:
    return names.get(_V3_NODE) or names.get(_V2_ARRAY) or names.get(_V2_GROUP)


def node_views(documents: Iterable[MetadataDocument]) -> list[NodeView]:
    """Parse the nodes of a store from its documents; a node that does not parse is left out."""
    views: list[NodeView] = []
    for names in _node_documents(documents).values():
        primary = _primary(names)
        if primary is None:
            continue
        attrs = (
            None
            if primary.kind in {MetadataDocumentKind.V3_GROUP, MetadataDocumentKind.V3_ARRAY}
            else (names.get(_V2_ATTRS))
        )
        try:
            views.append(parse_node_document(primary, attrs=attrs))
        except (ValueError, RecursionError):
            continue
    return views


def _zarr_format(documents: Mapping[str, MetadataDocument]) -> int:
    root = documents.get(_V3_NODE)
    if root is not None and root.kind == MetadataDocumentKind.V3_GROUP:
        return 3
    if root is None and _V2_GROUP in documents and _V2_ARRAY not in documents:
        return 2
    raise MigrationPlanError("the store has no root group document (zarr.json or .zgroup)")


def _image_path(documents: Mapping[str, MetadataDocument], zarr_format: int) -> str | None:
    """Return the root image path to rebuild, or `None`; refuse a group below the root with its own image."""
    for path, document in documents.items():
        node, name = posixpath.dirname(path), posixpath.basename(path)
        if not node or path.split("/", 1)[0] == CONTROL_DIRNAME:
            continue
        nested = name == _V2_IMAGE
        if document.kind in {MetadataDocumentKind.V3_GROUP, MetadataDocumentKind.V2_GROUP}:
            try:
                content = json.loads(document.raw)
            except (ValueError, RecursionError):
                content = None
            nested = nested or (
                isinstance(content, dict) and content.get(_CONSOLIDATED_FIELD) is not None
            )
        if nested:
            raise MigrationPlanError(
                f"group {node!r} holds its own consolidated metadata, which a migration "
                "does not rebuild; remove it or consolidate at the root only"
            )
    if zarr_format == 2:
        return _V2_IMAGE if _V2_IMAGE in documents else None
    content = json.loads(documents[_V3_NODE].raw)
    return _V3_NODE if isinstance(content.get(_CONSOLIDATED_FIELD), dict) else None


def _patch_node(
    current: dict[str, MetadataDocument], patch: AttributePatch, *, zarr_format: int
) -> tuple[str, bytes]:
    node = patch.node_path
    if node.split("/", 1)[0] == CONTROL_DIRNAME:
        raise MigrationPlanError(f"a plan cannot change {node!r}: it is not a Zarr node")
    if zarr_format == 3:
        primary = current.get(posixpath.join(node, _V3_NODE))
        attrs = None
        kinds = {MetadataDocumentKind.V3_GROUP, MetadataDocumentKind.V3_ARRAY}
    else:
        primary = current.get(posixpath.join(node, _V2_ARRAY)) or current.get(
            posixpath.join(node, _V2_GROUP)
        )
        attrs = current.get(posixpath.join(node, _V2_ATTRS))
        kinds = {MetadataDocumentKind.V2_GROUP, MetadataDocumentKind.V2_ARRAY}
    if primary is None or primary.kind not in kinds:
        raise MigrationPlanError(f"the store has no Zarr node at {node or '/'!r}")
    try:
        return patch_attributes(primary, attrs=attrs, set=patch.set, remove=patch.remove)
    except (ValueError, RecursionError) as exc:
        raise MigrationPlanError(f"node {node or '/'!r} cannot be patched: {exc}") from exc


def _rebuilt_image(documents: Mapping[str, MetadataDocument], zarr_format: int) -> bytes:
    nodes = {
        path: document.raw
        for path, document in documents.items()
        if path.split("/", 1)[0] != CONTROL_DIRNAME
    }
    try:
        return build_consolidated_image(nodes, zarr_format)
    except (ValueError, RecursionError) as exc:
        raise MigrationPlanError(f"the root consolidated image cannot be rebuilt: {exc}") from exc


def plan_consolidated_image(documents: Sequence[MetadataDocument]) -> tuple[PlannedDocument, ...]:
    """Return the root consolidated image rebuilt from `documents` when its bytes differ.

    A store without a root image, or whose image already has the rebuilt bytes, gets
    none. Nothing is read or written.

    Raises:
        MigrationPlanError: The store has no root group, a group below the root has its
            own image, or the image cannot be built.
    """
    original = {document.path: document for document in documents}
    zarr_format = _zarr_format(original)
    image = _image_path(original, zarr_format)
    if image is None:
        return ()
    data = _rebuilt_image(original, zarr_format)
    if data == original[image].raw:
        return ()
    return (
        PlannedDocument(
            path=image,
            before=original[image].digest,
            after=metadata_document_digest(data),
            data=data,
            image=True,
        ),
    )


def plan_documents(
    documents: Sequence[MetadataDocument], plan: Sequence[AttributePatch]
) -> tuple[PlannedDocument, ...]:
    """Return the documents a plan changes, with their bytes, in write order.

    Patches apply in order. When a document changes and the store has a root
    consolidated image, the image is rebuilt from the changed documents and goes
    last. Nothing is read or written.

    Args:
        documents: Every captured document of the store, as a snapshot holds them.
        plan: The attribute patches.

    Returns:
        The changed documents. An empty plan, or one that changes no byte, returns none.

    Raises:
        MigrationPlanError: A patch names no node, a node cannot be patched, the store
            has no root group, a group below the root has its own image, or the image
            cannot be built.
    """
    original = {document.path: document for document in documents}
    zarr_format = _zarr_format(original)
    image = _image_path(original, zarr_format)
    current = dict(original)
    for patch in plan:
        path, data = _patch_node(current, patch, zarr_format=zarr_format)
        kind = original[path].kind if path in original else MetadataDocumentKind.V2_ATTRS
        current[path] = MetadataDocument(
            path=path, kind=kind, raw=data, digest=metadata_document_digest(data)
        )
    changed = sorted(
        path
        for path, document in current.items()
        if path not in original or original[path].raw != document.raw
    )
    if changed and image is not None:
        data = _rebuilt_image(current, zarr_format)
        current[image] = MetadataDocument(
            path=image, kind=original[image].kind, raw=data, digest=metadata_document_digest(data)
        )
        changed = [path for path in changed if path != image] + [image]
    return tuple(
        PlannedDocument(
            path=path,
            before=original[path].digest if path in original else None,
            after=current[path].digest,
            data=current[path].raw,
            image=path == image,
        )
        for path in changed
    )


def _content(document: MetadataDocument | None) -> Any:
    if document is None:
        return {}
    return json.loads(document.raw)


def _is_noop(documents: Sequence[MetadataDocument], planned: Sequence[PlannedDocument]) -> bool:
    """Whether every changed document parses to the JSON it had before: the step changes no value.

    The consolidated image is derived, so it is left out; a v3 root keeps its own fields.
    """
    by_path = {document.path: document for document in documents}
    for item in planned:
        if item.image and item.path != _V3_NODE:
            continue
        before, after = _content(by_path.get(item.path)), json.loads(item.data or b"{}")
        if item.image:
            before.pop(_CONSOLIDATED_FIELD, None)
            after.pop(_CONSOLIDATED_FIELD, None)
        if not _json_equal(before, after):
            return False
    return True


def _addressed_paths(
    documents: Sequence[MetadataDocument], plan: Sequence[AttributePatch]
) -> list[str]:
    """Return the paths of the documents that hold the attributes the patches address."""
    current = {document.path: document for document in documents}
    zarr_format = _zarr_format(current)
    return sorted({_patch_node(current, patch, zarr_format=zarr_format)[0] for patch in plan})


# --- applying a plan --------------------------------------------------------------------


def _digests(documents: Iterable[MetadataDocument]) -> dict[str, str]:
    return {document.path: document.digest for document in documents}


def _state(live: Mapping[str, str], path: str, before: str | None, after: str) -> str:
    found = live.get(path)
    if found == after:
        return _AFTER
    if found == before:
        return _BEFORE
    return _THIRD


def _third_state_error(
    paths: Sequence[str], *, run_id: str | None, step_id: str | None, what: str
) -> MetadataStateError:
    return MetadataStateError(
        f"{what}: {', '.join(paths)} hold bytes that are neither the planned before nor "
        "after state; restore them or stop the writer that changed them",
        paths=tuple(paths),
        run_id=run_id,
        step_id=step_id,
    )


def _plan_states(
    live: Mapping[str, str],
    documents: Sequence[PlannedDocument],
    baseline: Mapping[str, str],
    *,
    owner_id: str,
) -> dict[str, str]:
    """Classify each planned document; refuse unplanned drift from `baseline` and third states."""
    planned = {item.path for item in documents}
    unplanned = (live.keys() | baseline.keys()) - planned
    drifted = sorted(path for path in unplanned if live.get(path) != baseline.get(path))
    if drifted:
        raise _third_state_error(
            drifted, run_id=owner_id, step_id=None, what="documents changed since the plan"
        )
    states = {item.path: _state(live, item.path, item.before, item.after) for item in documents}
    third = [path for path, state in states.items() if state == _THIRD]
    if third:
        raise _third_state_error(
            third, run_id=owner_id, step_id=None, what="planned documents changed"
        )
    return states


def apply_metadata_plan(
    *,
    target: MetadataTarget,
    owner_id: str,
    documents: Sequence[PlannedDocument],
    baseline: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Write planned documents in order under the exclusive maintenance claim, then verify the store.

    Each document is classified first: still in its before state, already in its
    after state, or neither. A document already after is skipped, so a rerun
    resumes. Each write follows a check of the exclusive claim and replaces the
    document only while it holds its before bytes.

    Args:
        target: The product and the store the documents are written to.
        owner_id: The run that holds the product's exclusive maintenance claim.
        documents: The planned documents, the root consolidated image last.
        baseline: Path to digest of every document before the plan. Without it, the
            unplanned documents the call reads first are the baseline.

    Returns:
        The paths written, in order.

    Raises:
        MetadataStateError: A planned document is in neither state, an unplanned one
            differs from the baseline, or the store does not verify after the writes.
        ProductUnderMaintenanceError: `owner_id` no longer holds the claim; later
            documents are not written.
    """
    from firecube.core.zarr.guard import metadata_authority

    manager, product = target.manager, target.product
    live = _digests(manager.enumerate_metadata_documents(product))
    planned = {item.path for item in documents}
    if baseline is None:
        baseline = {path: digest for path, digest in live.items() if path not in planned}
    states = _plan_states(live, documents, baseline, owner_id=owner_id)

    written: list[str] = []
    held = metadata_authority.HeldClaim(
        domain_id=exclusive_maintenance_domain(product).identifier, owner_id=owner_id
    )
    with metadata_authority.adopt_held_claim(target.scope, held) as authority:
        for item in documents:
            if states[item.path] != _BEFORE:
                continue
            if item.data is None:
                raise ValueError(f"planned document {item.path} has no bytes to write")
            manager.check_exclusive_maintenance(product=product, owner_id=owner_id)
            try:
                metadata_authority.publish_raw(
                    authority,
                    target.store,
                    item.path,
                    item.data,
                    expect=ABSENT if item.before is None else item.before,
                )
            except metadata_authority.DocumentPreconditionError as exc:
                raise _third_state_error(
                    [item.path], run_id=owner_id, step_id=None, what="a planned document changed"
                ) from exc
            written.append(item.path)

    after = _digests(manager.enumerate_metadata_documents(product))
    wrong = sorted(
        [item.path for item in documents if after.get(item.path) != item.after]
        + [
            path
            for path in (after.keys() | baseline.keys()) - planned
            if after.get(path) != baseline.get(path)
        ]
    )
    if wrong:
        raise _third_state_error(
            wrong, run_id=owner_id, step_id=None, what="the store does not verify after the plan"
        )
    return tuple(written)


def open_metadata_target(*, session: StorageSession, manager: ChunkManager) -> MetadataTarget:
    """Open the Zarr store of the session's product for document writes, bound to its scope."""
    from zarr.storage import FsspecStore

    from firecube.core.zarr.guard._unit_locks import _store_identity
    from firecube.core.zarr.guard.metadata_authority import MetadataScope

    product_uri = session.product.product_uri
    handle = session.zarr.create_store(product_uri, mode="r+")
    store = handle.store
    if isinstance(store, str):
        store = FsspecStore.from_url(store, storage_options=handle.storage_options, read_only=False)
    scope = MetadataScope(_store_identity(product_uri.to_str(), store), "", backing=store)
    return MetadataTarget(
        manager=manager, product=session.product.product_name, store=store, scope=scope
    )


# --- control plane ----------------------------------------------------------------------


def _adopt(
    manager: ChunkManager,
    product: str,
    documents: Sequence[MetadataDocument],
    report: Callable[[str], None],
    warn: Callable[[str], None],
) -> tuple[str, int]:
    """Record the `adopt` run that gives a foreign store its control plane; return its id and format."""
    zarr_format = _zarr_format({document.path: document for document in documents})
    run_id = f"adopt-{uuid.uuid4()}"
    scope_meta = {
        "run_kind": MAINTENANCE_OP_ADOPT,
        "imported": True,
        "chunks": "unverified",
        "zarr_format": zarr_format,
    }
    started = False
    try:
        manager.record_maintenance_started(
            product=product, run_id=run_id, op=MAINTENANCE_OP_ADOPT, scope_meta=scope_meta
        )
        started = True
        manager.record_maintenance_completed(
            product=product, run_id=run_id, op=MAINTENANCE_OP_ADOPT, scope_meta=scope_meta
        )
    except Exception as exc:
        # A refused start is recorded failed by the control plane already.
        if started or not isinstance(exc, ProductUnderMaintenanceError):
            try:
                manager.record_maintenance_failed(
                    product=product,
                    run_id=run_id,
                    op=MAINTENANCE_OP_ADOPT,
                    scope_meta=scope_meta,
                    error=str(exc),
                )
            except Exception as record_exc:
                warn(
                    Message(
                        "Warning: adoption failure not recorded: {error}",
                        error=_error_text(record_exc),
                    )
                )
        raise
    report(f"adopted the store as product {product} (run {run_id}, Zarr format {zarr_format})")
    if zarr_format == 2:
        warn(
            Message(
                "Warning: the store of product {product} is Zarr format 2. Firecube cannot "
                "append to it, so ingest and preallocate do not support this store. Convert "
                "it to Zarr format 3 outside Firecube before you ingest into it.",
                product=product,
            )
        )
    return run_id, zarr_format


def adopt_product(
    product: Product,
    *,
    report: Callable[[str], None] = _ignore_line,
    warn: Callable[[str], None] = _ignore_line,
    dry_run: bool = False,
) -> AdoptionResult:
    """Give a Zarr store that Firecube did not write a control plane, as one recorded `adopt` run.

    The store gets the handle's label. A store that records another label is refused
    before anything is written. A store with `.firecube/schema.json` or recorded runs
    is already a product and stays as it is; it gets the label if it records none.
    The run records the store as imported, with chunks unverified, and invents no
    span history. It writes no Zarr document. Adopting a Zarr format 2 store succeeds
    and warns that Firecube cannot append to it.

    Args:
        product: The open product, from `open_product`; its label is the one to record.
        report: Receives the line that names the run, and the line about the label.
        warn: Receives each warning line, "Warning: " included. A Zarr format 2 store gets one.
        dry_run: Read the store and report the run and the label it would record; write nothing.

    Returns:
        The outcome, the run id, the store's Zarr format and whether the label is recorded.

    Raises:
        ProductLabelMismatchError: The store records another label; nothing is written.
        ProductLabelConflictError: The records of an unstamped store name several labels.
        ManifestError: The label stamp or a control-plane record cannot be read.
        MigrationPlanError: The store has no root group document.
    """
    label = product.label
    manager = product.manager
    fs = cast(StorageFilesystemFull, product.session.fs())
    control_root = product.identity.control_root_uri
    label_recorded = check_product_label(fs, control_root, label) is not None
    if manager.has_control_plane(label):
        report(f"product {label} already has a control plane; nothing to adopt")
        if not label_recorded and not dry_run:
            # Such a store, for example a lone schema.json, is not adopted again; stamp it here.
            stamp_product_label(fs, control_root, label)
            report(f"recorded product label {label}")
            label_recorded = True
        result = AdoptionResult(
            outcome="already-product", run_id=None, zarr_format=None, label_recorded=label_recorded
        )
    elif dry_run:
        documents = manager.enumerate_metadata_documents(label)
        zarr_format = _zarr_format({document.path: document for document in documents})
        report(
            f"would adopt the store as product {label} in one adopt run "
            f"(Zarr format {zarr_format}); nothing is written"
        )
        result = AdoptionResult(
            outcome="dry-run", run_id=None, zarr_format=zarr_format, label_recorded=label_recorded
        )
    else:
        documents = manager.enumerate_metadata_documents(label)
        # The run's event writer stamps the label before its first record.
        run_id, zarr_format = _adopt(manager, label, documents, report, warn)
        return AdoptionResult(
            outcome="adopted", run_id=run_id, zarr_format=zarr_format, label_recorded=True
        )
    if dry_run:
        state = "is recorded" if label_recorded else "would be recorded"
        report(f"product label {label} {state}")
    return result


@dataclass(frozen=True)
class _JournaledRun:
    """A run whose journal exists and whose start was never recorded, so it wrote nothing."""

    run_id: str
    started_at: float
    status: str = _JOURNALED
    is_terminal: bool = False


def _history(manager: ChunkManager, product: str) -> list[tuple[Any, _Journal]]:
    """Read every migration run that has a journal, oldest first; refuse an unreadable one.

    A journal whose start was never recorded is dated by its before snapshot.
    """
    runs, unreadable = manager.scan_runs(product=product)
    broken = [run_id for run_id, _error in unreadable if run_id.startswith(_RUN_PREFIX)]
    if broken:
        raise MigrationRefusedError(
            f"migration runs of product {product} cannot be read: {', '.join(sorted(broken))}"
        )
    recorded = {run.run_id: run for run in runs}
    history: list[tuple[Any, _Journal]] = []
    for run_id in manager.list_run_journals(product, prefix=_RUN_PREFIX):
        data = manager.read_run_journal(product, run_id)
        if data is None:
            continue
        journal = _journal_from_bytes(run_id, data)
        run = recorded.get(run_id)
        if run is None:
            taken = manager.read_snapshot(product, journal.before_snapshot).record.taken_at
            run = _JournaledRun(run_id=run_id, started_at=taken)
        history.append((run, journal))
    return sorted(history, key=lambda item: (item[0].started_at, item[0].run_id))


def list_migrations(product: Product) -> tuple[MigrationRunInfo, ...]:
    """Return every recorded migration run of the product that has a journal, oldest first.

    Args:
        product: The open product, from `open_product`.

    Raises:
        MigrationRefusedError: A migration run or its journal cannot be read.
    """
    return tuple(
        MigrationRunInfo(
            run_id=run.run_id,
            status=run.status,
            step_id=journal.step_id,
            distribution=journal.distribution,
            version=journal.version,
            outcome=journal.outcome,
            started_at=run.started_at,
        )
        for run, journal in _history(product.manager, product.label)
        if not isinstance(run, _JournaledRun)
    )


def _current_release(manager: ChunkManager, product: str) -> ReleaseRecord | None:
    """Return the release that `.firecube/releases/refs/current.json` names, or `None`."""
    # Function-scope import: the release module imports this one.
    from firecube.ingestor.runtime.release import to_release_record

    stored = manager.current_release(product)
    return None if stored is None else to_release_record(stored)


def _refuse_existing_release(manager: ChunkManager, product: str, step: MigrationStep) -> None:
    """Refuse a fresh run whose `to_release` names a release that exists, before metadata writes."""
    if step.to_release is None:
        return
    existing = manager.find_release(product, step.to_release)
    if existing is not None:
        raise StepPreconditionError(
            f"step {step.id!r} creates release {step.to_release!r}, which exists already "
            f"and names snapshot {existing.snapshot}",
            step_id=step.id,
        )


def _snapshot_documents(
    manager: ChunkManager, product: str, snapshot_id: str
) -> list[MetadataDocument]:
    snapshot = manager.read_snapshot(product, snapshot_id)
    return [
        MetadataDocument(
            path=entry.path,
            kind=entry.kind,
            raw=manager.read_metadata_blob(product, _blob_id(entry.digest)),
            digest=entry.digest,
        )
        for entry in snapshot.documents
    ]


def _journal_path(run_id: str) -> str:
    return posixpath.join(CONTROL_DIRNAME, RUNS_DIRNAME, run_id, RUN_JOURNAL_FILENAME)


# --- the executor -----------------------------------------------------------------------


def _installed_step(
    load_steps: Callable[[], Iterable[InstalledStep]], step_id: str
) -> tuple[InstalledStep, str, str]:
    matches = [installed for installed in load_steps() if installed.id == step_id]
    if not matches:
        raise StepIdentityError(
            f"no installed migration step has the id {step_id!r}", step_id=step_id
        )
    if len(matches) > 1:
        raise StepIdentityError(
            f"{len(matches)} installed migration steps have the id {step_id!r}", step_id=step_id
        )
    installed = matches[0]
    if not installed.distribution or not installed.version:
        raise StepIdentityError(
            f"migration step {step_id!r} has no distribution and version, so its run "
            "would have no identity",
            step_id=step_id,
        )
    return installed, installed.distribution, installed.version


def _built_step(installed: InstalledStep, options: Mapping[str, Any]) -> MigrationStep:
    try:
        step = installed.create(dict(options))
    except Exception as exc:
        raise MigrationPlanError(
            f"migration step {installed.id!r} refuses its options: {exc}", step_id=installed.id
        ) from exc
    if getattr(step, "id", None) != installed.id:
        raise MigrationPlanError(
            f"migration step {installed.id!r} names itself {getattr(step, 'id', None)!r}",
            step_id=installed.id,
        )
    for name in ("from_release", "to_release"):
        if not isinstance(getattr(step, name, None), str | None):
            raise MigrationPlanError(f"{name} of step {installed.id!r} is not a string or None")
    depends_on = getattr(step, "depends_on", ())
    keys = getattr(step, "descriptive_keys", ())
    if isinstance(depends_on, str) or not all(isinstance(item, str) for item in depends_on):
        raise MigrationPlanError(f"depends_on of step {installed.id!r} is not a tuple of step ids")
    if isinstance(keys, str) or not all(isinstance(item, str) for item in keys):
        raise MigrationPlanError(f"descriptive_keys of step {installed.id!r} are not names")
    return step


def _step_plan(step: MigrationStep, view: StoreView) -> MetadataPlan:
    try:
        plan = tuple(step.plan(view))
    except Exception as exc:
        raise MigrationPlanError(
            f"step {step.id!r} failed to plan: {exc}", step_id=step.id
        ) from exc
    if not all(isinstance(item, AttributePatch) for item in plan):
        raise MigrationPlanError(
            f"step {step.id!r} planned something other than attribute patches", step_id=step.id
        )
    return plan


def _check_preconditions(
    step: MigrationStep, history: Sequence[tuple[Any, _Journal]], current: ReleaseRecord | None
) -> None:
    applied = {journal.step_id for run, journal in history if run.status == "complete"}
    missing = tuple(dep for dep in step.depends_on if dep not in applied)
    if missing:
        raise StepPreconditionError(
            f"step {step.id!r} needs {', '.join(missing)} applied first",
            step_id=step.id,
            missing=missing,
        )
    version = None if current is None else current.version
    if step.from_release is not None and version != step.from_release:
        raise StepPreconditionError(
            f"step {step.id!r} needs the store at release {step.from_release!r}, "
            f"and it is at {'no release' if version is None else repr(version)}",
            step_id=step.id,
            from_release=step.from_release,
            current_release=version,
        )


@dataclass(frozen=True)
class _Prepared:
    outcome: str
    verdicts: tuple[AttributeVerdict, ...]
    documents: tuple[PlannedDocument, ...]
    patches: MetadataPlan


def _classified(
    step: MigrationStep, view: StoreView, allow: Collection[str]
) -> tuple[MetadataPlan, tuple[AttributeVerdict, ...]]:
    """Plan the step and classify every attribute it sets or removes."""
    patches = _step_plan(step, view)
    descriptive = frozenset(step.descriptive_keys)
    return patches, _verdicts(patches, descriptive_keys=descriptive, allow=allow)


def _compose(
    *,
    step: MigrationStep,
    documents: Sequence[MetadataDocument],
    patches: MetadataPlan,
    verdicts: tuple[AttributeVerdict, ...],
    stamp: Mapping[str, Any],
) -> _Prepared:
    """Compose the one after-set: the step's patches, the managed stamps, the image last.

    An adopted step keeps its addressed documents and writes the stamps only. Every
    addressed document is in the after-set, changed or not.
    """
    managed: dict[str, Any] = {LAST_MODIFIER_ATTR: dict(stamp)}
    if step.to_release is not None:
        managed[RELEASE_ATTR] = step.to_release
    adopted = _is_noop(documents, plan_documents(documents, patches))
    stamped = AttributePatch("", set=managed)
    written = plan_documents(documents, (stamped,) if adopted else (*patches, stamped))
    changed = {item.path for item in written}
    by_path = {document.path: document for document in documents}
    kept = tuple(
        PlannedDocument(
            path=path, before=by_path[path].digest, after=by_path[path].digest, data=None
        )
        for path in _addressed_paths(documents, patches)
        if path in by_path and path not in changed
    )
    outcome = OUTCOME_ADOPTED if adopted else OUTCOME_EXECUTED
    return _Prepared(outcome, verdicts, (*kept, *written), patches)


def _report_classes(
    report: Callable[[str], None],
    patches: MetadataPlan,
    verdicts: Sequence[AttributeVerdict],
) -> None:
    for patch in patches:
        node = patch.node_path or "/"
        for key in patch.set:
            report(f"set {node}:{key}")
        for key in patch.remove:
            report(f"remove {node}:{key}")
    for verdict in verdicts:
        state = "allowed" if verdict.allowed else "refused"
        report(
            f"class {verdict.node_path or '/'}:{verdict.key}: {verdict.attribute_class}, {state}"
        )


def _report_plan(report: Callable[[str], None], prepared: _Prepared) -> None:
    _report_classes(report, prepared.patches, prepared.verdicts)
    _report_documents(report, prepared)


def _report_documents(
    report: Callable[[str], None], prepared: _Prepared, *, outcome_label: str = "outcome"
) -> None:
    for item in prepared.documents:
        action = "keep" if item.before == item.after else "write"
        report(f"{action} {item.path}{' (consolidated image)' if item.image else ''}")
    report(f"{outcome_label}: {prepared.outcome}")


def _journal_entries(
    journal: _Journal, manager: ChunkManager, product: str, *, with_data: bool = True
) -> list[PlannedDocument]:
    """Load the journaled after bytes from their blobs, each checked against its digest."""
    return [
        PlannedDocument(
            path=entry.path,
            before=entry.before,
            after=entry.after,
            data=None
            if entry.before == entry.after or not with_data
            else manager.read_metadata_blob(product, _blob_id(entry.after)),
            image=entry.image,
        )
        for entry in journal.entries
    ]


def _baseline(manager: ChunkManager, product: str, journal: _Journal) -> dict[str, str]:
    """Return path to digest of every document the journal's before snapshot captured."""
    snapshot = manager.read_snapshot(product, journal.before_snapshot)
    return {entry.path: entry.digest for entry in snapshot.documents}


def _already_applied(
    applied: tuple[Any, _Journal],
    *,
    step_id: str,
    identity: tuple[str, str],
    report: Callable[[str], None],
) -> MigrationResult:
    run, journal = applied
    report(f"step {step_id} already applied by run {run.run_id}")
    return MigrationResult(
        outcome="already-applied",
        run_id=run.run_id,
        step_id=step_id,
        distribution=identity[0],
        version=identity[1],
        before_snapshot=journal.before_snapshot,
        after_snapshot=None,
        documents=(),
        verdicts=(),
    )


def _classify_journal(live: Mapping[str, str], journal: _Journal) -> dict[str, list[str]]:
    states: dict[str, list[str]] = {_BEFORE: [], _AFTER: [], _THIRD: []}
    for entry in journal.entries:
        states[_state(live, entry.path, entry.before, entry.after)].append(entry.path)
    return states


def _resume_refusal(
    journal: _Journal, run: Any, paths: Sequence[str], product_uri: str
) -> Exception:
    recovery: str | Message = ""
    if not run.is_terminal and run.status != _JOURNALED:
        recovery = Message(
            " To give up the interrupted run, abandon it:\n  {abandon}",
            abandon=Operation(
                "abandon_run",
                target=product_uri,
                run_id=journal.run_id,
                reason=Placeholder('"<reason>"'),
            ),
        )
    return MetadataStateError(
        Message(
            "Run {run_id} of step {step_id} stopped part way, and {listed} now hold bytes "
            "that are neither its before nor its after state.{recovery}",
            run_id=journal.run_id,
            step_id=journal.step_id,
            listed=", ".join(paths),
            recovery=recovery,
        ),
        paths=tuple(paths),
        run_id=journal.run_id,
        step_id=journal.step_id,
    )


def _prior_runs(
    history: Sequence[tuple[Any, _Journal]],
    live: Mapping[str, str],
    step_id: str,
    identity: tuple[str, str],
    product_uri: str,
) -> tuple[tuple[Any, _Journal] | None, tuple[Any, _Journal] | None]:
    """Decide from the step's journaled runs: already applied, a run to resume, or neither.

    A completed run whose after state the store still holds means applied. Otherwise
    the latest unfinished, failed or unrecorded run resumes from its journal. The
    current digests never decide whether a run wrote.

    Raises:
        StepIdentityError: A completed, unfinished or failed run used another
            distribution version.
        MetadataStateError: The store left the after state of the completed run, or a
            resumable run's documents hold a third state.
    """
    own = [(run, journal) for run, journal in history if journal.step_id == step_id]
    for run, journal in own:
        recorded = (journal.distribution, journal.version)
        # An unrecorded run never started, so it wrote nothing.
        if recorded == identity or run.status in {"abandoned", _JOURNALED}:
            continue
        raise StepIdentityError(
            f"step {step_id!r} ran as {recorded[0]} {recorded[1]} in run {run.run_id} "
            f"({run.status}), and the installed step is {identity[0]} {identity[1]}",
            step_id=step_id,
            recorded=recorded,
            installed=identity,
        )
    same = [
        (run, journal)
        for run, journal in own
        if (journal.distribution, journal.version) == identity and run.status != "abandoned"
    ]
    completed = [item for item in same if item[0].status == "complete"]
    if completed:
        run, journal = completed[-1]
        states = _classify_journal(live, journal)
        if states[_BEFORE] or states[_THIRD]:
            raise _third_state_error(
                states[_BEFORE] + states[_THIRD],
                run_id=run.run_id,
                step_id=step_id,
                what=f"step {step_id!r} was applied by run {run.run_id}, and since then",
            )
        return completed[-1], None
    pending = [item for item in same if not item[0].is_terminal or item[0].status == "failed"]
    if not pending:
        return None, None
    run, journal = pending[-1]
    states = _classify_journal(live, journal)
    if states[_THIRD]:
        raise _resume_refusal(journal, run, states[_THIRD], product_uri)
    return None, (run, journal)


def migrate_product(
    product: Product,
    *,
    step_id: str,
    load_steps: Callable[[], Iterable[InstalledStep]],
    step_options: Mapping[str, Any] | None = None,
    allow: Collection[str] = (),
    dry_run: bool = False,
    create_release: ReleaseCreator | None = None,
    report: Callable[[str], None] = _ignore_line,
    warn: Callable[[str], None] = _ignore_line,
) -> MigrationResult:
    """Run one migration step on the product as a recorded maintenance run.

    The run holds the product's exclusive maintenance claim. It takes a coherent
    snapshot, plans against it and journals every planned document before the first
    write. It then writes the documents, the root consolidated image last, verifies
    the store and takes a second snapshot. A real run refuses a store without a control
    plane and writes nothing; `adopt_product` gives it one first. A step that ran before at the same identity is not run again. An
    interrupted, unrecorded or failed run of the step resumes from its journal.

    Args:
        product: The open product, from `open_product`.
        step_id: The id of the step to run.
        load_steps: Returns the installed steps with their distributions and versions.
        step_options: The options the step is built with.
        allow: Attribute classes the run may change beyond descriptive ones:
            `"interpretation"`, `"unknown"`, or both.
        dry_run: Report the plan and the classes, and write nothing: no claim. On a store
            without a control plane, report first that a real run needs adoption.
        create_release: Records the release a step names in `to_release`, inside the
            run's claim. Defaults to `release_in_run` of the release module.
        report: Receives progress and plan lines.
        warn: Receives each bookkeeping warning line, "Warning: " included.

    Returns:
        The outcome, the run, the two snapshots and the documents written.

    Raises:
        ValueError: `allow` names a class other than interpretation and unknown.
        StepIdentityError: The step is not installed, has no distribution, or ran
            before at another distribution version.
        StepPreconditionError: A step it depends on is not applied, the store is at
            another release, or the release the step creates exists already.
        ReleaseVersionUnstorableError: A storage driver cannot write, list or read the
            record of the step's `to_release`. Nothing is written.
        ReleaseConflictError: A resumed run's release record exists, and the live
            metadata differs from its snapshot.
        AttributeClassError: The plan changes an attribute its class refuses.
        MetadataStateError: Planned documents hold a third state.
        MigrationPlanError: The step refuses its options, fails to plan, or plans a
            change the store cannot take.
        ClaimConflictError: Another owner holds the exclusive maintenance claim.
        ExclusiveMaintenanceRefusedError: Other claims or unfinished runs exist.
        MigrationRefusedError: A real run finds the store without a control plane, and
            nothing is written; or the step's runs changed while the run waited for its claim.
    """
    unknown = sorted(set(allow) - ALLOWABLE_CLASSES)
    if unknown:
        raise ValueError(f"allow takes 'interpretation' and 'unknown', not {unknown}")
    allowed = frozenset(allow)
    installed, distribution, version = _installed_step(load_steps, step_id)
    step = _built_step(installed, step_options or {})
    if create_release is None:
        # Function-scope import: the release module imports this one.
        from firecube.ingestor.runtime.release import release_in_run

        create_release = release_in_run
    identity = (distribution, version)
    label = product.label
    product_uri = product.identity.product_uri.to_str()
    manager = product.manager
    if step.to_release is not None:
        # Before the control-plane refusal, the journal and the dry run: the stamp must never lack its record.
        manager.check_release_version(label, step.to_release)
    is_product = manager.has_control_plane(label)
    if not is_product:
        if not dry_run:
            raise MigrationRefusedError(
                Message(
                    "the store of product {product} has no control plane; adopt it with "
                    "{adopt} before running a migration",
                    product=label,
                    adopt=_ADOPT_COMMAND,
                ),
                step_id=step_id,
            )
        report(
            Message(
                "the store of product {product} has no control plane; a real run needs "
                "{adopt} first",
                product=label,
                adopt=_ADOPT_COMMAND,
            )
        )
    history = _history(manager, label) if is_product else []
    live = _digests(manager.enumerate_metadata_documents(label))
    applied, resume = _prior_runs(history, live, step_id, identity, product_uri)
    if applied is not None:
        return _already_applied(applied, step_id=step_id, identity=identity, report=report)
    if dry_run:
        return _dry_run(
            manager=manager,
            product=label,
            step=step,
            identity=identity,
            history=history,
            resume=resume,
            allow=allowed,
            report=report,
        )
    return _run(
        session=product.session,
        manager=manager,
        product=label,
        product_uri=product_uri,
        step=step,
        identity=identity,
        resume=resume,
        allow=allowed,
        create_release=create_release,
        report=report,
        warn=warn,
    )


def _dry_run(
    *,
    manager: ChunkManager,
    product: str,
    step: MigrationStep,
    identity: tuple[str, str],
    history: Sequence[tuple[Any, _Journal]],
    resume: tuple[Any, _Journal] | None,
    allow: Collection[str],
    report: Callable[[str], None],
) -> MigrationResult:
    """Report what a run would do, from live reads only."""
    distribution, version = identity
    if resume is not None:
        run, journal = resume
        live = _digests(manager.enumerate_metadata_documents(product))
        _plan_states(
            live,
            _journal_entries(journal, manager, product, with_data=False),
            _baseline(manager, product, journal),
            owner_id=run.run_id,
        )
        paths = tuple(entry.path for entry in journal.entries)
        report(f"would resume run {run.run_id} of step {step.id}: {len(paths)} planned documents")
        return MigrationResult(
            outcome="dry-run",
            run_id=None,
            step_id=step.id,
            distribution=distribution,
            version=version,
            before_snapshot=journal.before_snapshot,
            after_snapshot=None,
            documents=paths,
            verdicts=(),
            resumed_from=run.run_id,
        )
    _check_preconditions(step, history, _current_release(manager, product))
    _refuse_existing_release(manager, product, step)
    documents = manager.enumerate_metadata_documents(product)
    now = datetime.now(UTC)
    view = StoreView(node_views(documents), release=_current_release(manager, product), now=now)
    stamp = _stamp(run_id="<dry-run>", step=step, identity=identity, now=now)
    patches, verdicts = _classified(step, view, allow)
    _report_classes(report, patches, verdicts)
    refused = any(not verdict.allowed for verdict in verdicts)
    try:
        prepared = _compose(
            step=step, documents=documents, patches=patches, verdicts=verdicts, stamp=stamp
        )
    except MigrationPlanError:
        # A class refusal comes first, as in a run; the documents are then not reported.
        if not refused:
            raise
        prepared = None
    if prepared is not None:
        _report_documents(report, prepared, outcome_label="planned outcome")
    _refuse_classes(step.id, verdicts)
    assert prepared is not None
    return MigrationResult(
        outcome="dry-run",
        run_id=None,
        step_id=step.id,
        distribution=distribution,
        version=version,
        before_snapshot=None,
        after_snapshot=None,
        documents=tuple(item.path for item in prepared.documents),
        verdicts=prepared.verdicts,
    )


def _stamp(
    *, run_id: str, step: MigrationStep, identity: tuple[str, str], now: datetime
) -> dict[str, Any]:
    return {
        "operation": MAINTENANCE_OP_MIGRATE,
        "run_id": run_id,
        "step_id": step.id,
        "distribution": identity[0],
        "version": identity[1],
        "at": now.isoformat(),
    }


def _write_journal(
    manager: ChunkManager, product: str, journal: _Journal, planned: Sequence[PlannedDocument]
) -> None:
    """Store every after document as a blob, read each back, then create the journal."""
    for item in planned:
        if item.before == item.after:
            continue
        assert item.data is not None
        blob_id = manager.write_metadata_blob(product, item.data)
        if f"{_DIGEST_PREFIX}{blob_id}" != item.after:
            raise MigrationRefusedError(f"the blob of {item.path} does not match its digest")
        # An existing blob is trusted on create; reading it back checks its bytes.
        manager.read_metadata_blob(product, blob_id)
    manager.write_run_journal(product, journal.run_id, canonical_json_bytes(journal.payload()))


def _run(
    *,
    session: StorageSession,
    manager: ChunkManager,
    product: str,
    product_uri: str,
    step: MigrationStep,
    identity: tuple[str, str],
    resume: tuple[Any, _Journal] | None,
    allow: Collection[str],
    create_release: ReleaseCreator,
    report: Callable[[str], None],
    warn: Callable[[str], None],
) -> MigrationResult:
    """Admit, journal, apply, verify, snapshot and record one migration run.

    An unfinished or unrecorded run resumes under its own id; a failed one under a new id.
    """
    distribution, version = identity
    # An unrecorded run resumes under its own id too, and its start is recorded now.
    interrupted = resume is not None and not resume[0].is_terminal
    run_id = (
        resume[1].run_id if interrupted and resume is not None else f"{_RUN_PREFIX}{uuid.uuid4()}"
    )
    resumed_key = None if resume is None else resume[1].run_id
    scope_meta: dict[str, Any] = {
        "run_kind": MAINTENANCE_OP_MIGRATE,
        "step_id": step.id,
        "distribution": distribution,
        "version": version,
        "journal": _journal_path(run_id),
        "allow": sorted(allow),
    }
    started = interrupted and resume is not None and resume[0].status != _JOURNALED

    def record_failure(error: str) -> None:
        nonlocal started
        try:
            if not started:
                manager.record_maintenance_started(
                    product=product, run_id=run_id, op=MAINTENANCE_OP_MIGRATE, scope_meta=scope_meta
                )
                started = True
            manager.record_maintenance_failed(
                product=product,
                run_id=run_id,
                op=MAINTENANCE_OP_MIGRATE,
                scope_meta=scope_meta,
                error=error,
            )
        except Exception as exc:
            warn(
                Message("Warning: migration failure not recorded: {error}", error=_error_text(exc))
            )

    handle = manager.acquire_exclusive_maintenance(product=product, run_id=run_id)
    try:
        # Another process may have run the step since the first read: decide again under the claim.
        history = _history(manager, product)
        live = _digests(manager.enumerate_metadata_documents(product))
        applied, current = _prior_runs(history, live, step.id, identity, product_uri)
        if applied is not None:
            return _already_applied(applied, step_id=step.id, identity=identity, report=report)
        if (None if current is None else current[1].run_id) != resumed_key:
            raise MigrationRefusedError(
                f"the migration runs of product {product} changed while run {run_id} "
                f"waited for its claim; run step {step.id!r} again",
                step_id=step.id,
            )
        try:
            verdicts: tuple[AttributeVerdict, ...] = ()
            if resume is not None:
                journal = resume[1]
                if not interrupted:
                    # A failed run is terminal: a new run carries its plan on.
                    journal = replace(journal, run_id=run_id, resumed_from=journal.run_id)
                planned = _journal_entries(journal, manager, product)
                if not interrupted:
                    _write_journal(manager, product, journal, planned)
                report(f"resuming the plan of run {resume[1].run_id}")
            else:
                _check_preconditions(step, history, _current_release(manager, product))
                _refuse_existing_release(manager, product, step)
                before = manager.take_snapshot(
                    product, run_id=run_id, trigger="migrate-before", consistency="coherent"
                )
                documents = _snapshot_documents(manager, product, before.snapshot_id)
                now = datetime.now(UTC)
                view = StoreView(
                    node_views(documents), release=_current_release(manager, product), now=now
                )
                stamp = _stamp(run_id=run_id, step=step, identity=identity, now=now)
                patches, verdicts = _classified(step, view, allow)
                _refuse_classes(step.id, verdicts)
                prepared = _compose(
                    step=step, documents=documents, patches=patches, verdicts=verdicts, stamp=stamp
                )
                _report_plan(report, prepared)
                verdicts = prepared.verdicts
                planned = list(prepared.documents)
                journal = _Journal(
                    run_id=run_id,
                    step_id=step.id,
                    distribution=distribution,
                    version=version,
                    outcome=prepared.outcome,
                    before_snapshot=before.snapshot_id,
                    to_release=step.to_release,
                    resumed_from=None,
                    overrides=tuple(
                        {"node_path": v.node_path, "key": v.key, "class": v.attribute_class}
                        for v in prepared.verdicts
                        if v.attribute_class in ALLOWABLE_CLASSES
                    ),
                    entries=tuple(
                        _JournalEntry(item.path, item.before, item.after, item.image)
                        for item in planned
                    ),
                )
                report(f"before snapshot {before.snapshot_id}")
                _write_journal(manager, product, journal, planned)
            scope_meta.update(
                outcome=journal.outcome,
                before_snapshot=journal.before_snapshot,
                overrides=list(journal.overrides),
                to_release=journal.to_release,
                resumed_from=journal.resumed_from,
            )
            if not started:
                manager.record_maintenance_started(
                    product=product, run_id=run_id, op=MAINTENANCE_OP_MIGRATE, scope_meta=scope_meta
                )
                started = True

            target = open_metadata_target(session=session, manager=manager)
            written = apply_metadata_plan(
                target=target,
                owner_id=run_id,
                documents=planned,
                baseline=_baseline(manager, product, journal),
            )
            report(f"wrote {len(written)} of {len(planned)} planned documents")

            def take_after() -> str:
                snapshot = manager.take_snapshot(
                    product, run_id=run_id, trigger="migrate-after", consistency="coherent"
                )
                report(f"after snapshot {snapshot.snapshot_id}")
                return snapshot.snapshot_id

            release: ReleaseRecord | None = None
            if journal.to_release is None:
                after_snapshot = take_after()
            else:
                # The creator keeps the snapshot a checkpoint of the release already names.
                release = create_release(
                    manager=manager,
                    product=product,
                    version=journal.to_release,
                    run_id=run_id,
                    take_snapshot=take_after,
                )
                after_snapshot = release.snapshot
            scope_meta.update(
                after_snapshot=after_snapshot,
                release=None if release is None else release.version,
                documents_written=len(written),
            )
            manager.record_maintenance_completed(
                product=product, run_id=run_id, op=MAINTENANCE_OP_MIGRATE, scope_meta=scope_meta
            )
        except Exception as exc:
            record_failure(str(exc))
            raise
        report(f"step {step.id} {journal.outcome} in run {run_id}")
        return MigrationResult(
            outcome="adopted" if journal.outcome == OUTCOME_ADOPTED else "executed",
            run_id=run_id,
            step_id=step.id,
            distribution=distribution,
            version=version,
            before_snapshot=journal.before_snapshot,
            after_snapshot=after_snapshot,
            documents=tuple(item.path for item in planned),
            verdicts=verdicts,
            resumed_from=resume[1].run_id if resume is not None else None,
            release=release,
        )
    finally:
        try:
            handle.release()
        except Exception as exc:
            warn(
                Message(
                    "Warning: claim {claim_domain} not released: {error}",
                    claim_domain=handle.info.domain,
                    error=_error_text(exc),
                )
            )
