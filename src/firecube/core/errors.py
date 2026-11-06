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

"""Shared exception types for core storage/manifest operations.

A refusal's text may be a `Message`: a template whose command and option tokens are
typed parts. `str()` gives the library sentence; the CLI renders the same template
with its own flags and commands.
"""

from __future__ import annotations

import re
import string
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

OPERATION_FUNCTIONS: Mapping[str, str] = MappingProxyType(
    {
        "abandon_run": "firecube.core.controlplane.manager.ChunkManager.abandon_run",
        "clear_claim": "firecube.core.controlplane.manager.ChunkManager.clear_claim",
        "rebuild_compact": "firecube.core.controlplane.manager.ChunkManager.rebuild_compact",
        "delete_span": "firecube.core.controlplane.manager.ChunkManager.delete_spans",
        "preallocate": "firecube.ingestor.runtime.preallocate.preallocate_product",
        "rebuild_index": "firecube.ingestor.runtime.index_record.rebuild_index_record",
        "validate": "firecube.core.zarr.validation.validate_group_with_fs",
    }
)
"""Map each `Operation` name to the dotted path of the library function behind it."""


@dataclass(frozen=True)
class Placeholder:
    """Text that stands in for a value the refusing code does not know.

    Both renderings print it verbatim, for example `<uri>` or `"<reason>"`.

    Attributes:
        text: The text to print.
    """

    text: str

    def __str__(self) -> str:
        return self.text


@dataclass(frozen=True)
class Param:
    """A parameter of the operation that refused.

    `str()` gives `name`, or `name=value!r` when a value is given. The CLI prints the
    matching option of the running command instead, and keeps `str()` when it has none.

    Attributes:
        name: The parameter name, as the library function and the Click option spell it.
        value: The value to suggest, or `None` to name the parameter alone.
    """

    name: str
    value: object = None

    def __str__(self) -> str:
        if self.value is None:
            return self.name
        value = str(self.value) if isinstance(self.value, Placeholder) else repr(self.value)
        return f"{self.name}={value}"


@dataclass(frozen=True)
class PluginOption:
    """A plugin option and the value to set it to.

    `str()` gives `name=value`. The CLI prints `--option name=value`, shell-quoted.

    Attributes:
        name: The option key.
        value: The option value.
    """

    name: str
    value: object

    def __str__(self) -> str:
        return f"{self.name}={self.value}"


@dataclass(frozen=True, init=False)
class Operation:
    """A firecube operation the reader can run, with its arguments.

    `str()` gives the dotted path of the library function. The CLI prints the matching
    command with these arguments, and `<name>` for each required one missing.

    Attributes:
        name: A key of `OPERATION_FUNCTIONS`.
        arguments: Values by Click parameter name of the matching command. A
            `Placeholder` value prints verbatim.
    """

    name: str
    arguments: Mapping[str, object] = field(hash=False)

    def __init__(self, name: str, /, **arguments: object) -> None:
        """Name the operation and its arguments.

        Raises:
            ValueError: `name` is not a key of `OPERATION_FUNCTIONS`.
        """
        if name not in OPERATION_FUNCTIONS:
            raise ValueError(f"unknown operation {name!r}; known: {sorted(OPERATION_FUNCTIONS)}")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "arguments", dict(arguments))

    def __str__(self) -> str:
        return OPERATION_FUNCTIONS[self.name]


@dataclass(frozen=True)
class CommandName:
    """A command named as a label, never as something to run.

    `str()` gives `library`. The CLI prints `firecube` and `path`, or the running
    command when `path` is empty.

    Attributes:
        path: The command words after `firecube`, or `()` for the running command.
        library: The text a library caller reads instead.
    """

    path: tuple[str, ...] = ()
    library: str = ""

    def __str__(self) -> str:
        return self.library


_FIELD_ROOT = re.compile(r"[.\[]")


class Message(str):
    """Refusal text whose command and option tokens are typed parts.

    Build it as `Message(template, **parts)`; `template` uses `str.format` fields.
    A part is a `Param`, `PluginOption`, `Operation`, `CommandName`, `Placeholder`,
    a nested `Message`, or a plain value. The string value is the library rendering:
    each part prints its `str()`, and a plain value formats as `str.format` would.
    The CLI renders the same template with its own flags and commands.

    A plain value named after a `RefusalContext` field is a fact; callers read it
    through `FirecubeError.context`. Parts the template does not name never print.

    Joining with `+` or an f-string returns a plain `str` and loses the parts; use
    `Message.join` or a nested `Message` instead. Pickling and copying keep the parts.

    Examples:
        A refusal that names an option and a recovery operation:

            Message(
                "Run {run_id!r} is stale; rerun with {force} or run {abandon}.",
                run_id="run-1",
                force=Param("force", True),
                abandon=Operation("abandon_run", run_id="run-1"),
            )
    """

    _template: str
    _parts: dict[str, object]

    def __new__(cls, template: str, /, **parts: object) -> Message:
        """Render the library text and keep the template and parts.

        Raises:
            KeyError: The template names a field that `parts` lacks.
            ValueError: The template is malformed or uses positional fields.
        """
        rendered = {
            name: str(value) if isinstance(value, _PART_TYPES) else value
            for name, value in parts.items()
        }
        message = super().__new__(cls, template.format_map(rendered))
        message._template = template
        message._parts = dict(parts)
        return message

    @property
    def template(self) -> str:
        """The `str.format` template the parts fill in."""
        return self._template

    @property
    def parts(self) -> Mapping[str, object]:
        """The parts by field name, read-only."""
        return MappingProxyType(self._parts)

    def join(self: str, iterable: Iterable[str], /) -> Message:
        """Join `iterable` with this separator into one `Message`, keeping every part.

        Call it as `Message.join(sep, items)`. Each item is a `Message` or a plain
        string; the separator may be either too.
        """
        items = list(iterable)
        template = "{_sep}".join(f"{{_{index}}}" for index in range(len(items)))
        parts: dict[str, object] = {f"_{index}": item for index, item in enumerate(items)}
        if len(items) > 1:
            parts["_sep"] = self
        return Message(template, **parts)

    def without_trailing_stop(self) -> Message:
        """Return a copy whose text ends without trailing full stops and spaces.

        Only the final displayed text changes, and both renderings lose the same characters.
        A trimmed trailing field moves into a new `_stop_trimmed` part. Every part stays
        readable through `FirecubeError.context` and `FirecubeError.recovery`.
        A trailing `Param`, `PluginOption`, `Operation` or `CommandName` keeps its text.
        A trailing nested `Message` is trimmed only where the field shows it as itself,
        never under a format spec, a `!r` or `!a` conversion, or attribute or index access.

        Examples:
            `Message("Run {run_id!r} failed. ", run_id="r1").without_trailing_stop()`
            reads `Run 'r1' failed`.
        """
        # Literal text, or a field as (name, format spec, conversion), in template order.
        tokens: list[str | tuple[str, str, str | None]] = []
        for literal, name, spec, conversion in string.Formatter().parse(self._template):
            if literal:
                tokens.append(literal)
            if name is not None:
                tokens.append((name, spec or "", conversion))
        parts = dict(self._parts)
        new_name = "_stop_trimmed"
        while new_name in parts:
            new_name += "_"
        replaced: str | None = None
        while tokens:
            token = tokens.pop()
            if isinstance(token, str):
                if token.rstrip(". "):
                    tokens.append(token.rstrip(". "))
                    break
                continue
            name, spec, conversion = token
            root = _FIELD_ROOT.split(name, maxsplit=1)[0]
            value = parts[root]
            as_itself = root == name and not spec and conversion in (None, "s")
            if isinstance(value, Message) and as_itself:
                trimmed: str = value.without_trailing_stop()
            elif isinstance(value, Message | Param | PluginOption | Operation | CommandName):
                tokens.append(token)
                break
            else:
                library = {k: str(v) if isinstance(v, _PART_TYPES) else v for k, v in parts.items()}
                trimmed = _field_template(*token).format_map(library).rstrip(". ")
            if trimmed:
                tokens.append((new_name, "", None))
                parts[new_name] = trimmed
                if isinstance(value, Message):
                    replaced = root
                break
            # The field displays nothing once trimmed: the trim reaches the token before it.
        roots = {_FIELD_ROOT.split(t[0], maxsplit=1)[0] for t in tokens if not isinstance(t, str)}
        if replaced is not None and replaced not in roots:
            # The trimmed copy keeps every part of the replaced Message, so `recovery`
            # would list its operations twice if the original stayed as well.
            del parts[replaced]
        template = "".join(
            t.replace("{", "{{").replace("}", "}}") if isinstance(t, str) else _field_template(*t)
            for t in tokens
        )
        return Message(template, **parts)

    def __reduce__(self) -> tuple[Any, ...]:
        return (_restore_message, (str(self), self._template, dict(self._parts)))


# The part types `str()` renders; any other part value formats as a plain value.
_PART_TYPES: tuple[type, ...] = (Message, Param, PluginOption, Operation, CommandName, Placeholder)


def _field_template(name: str, spec: str, conversion: str | None) -> str:
    """Write one replacement field back as `str.format` template text."""
    conversion_text = f"!{conversion}" if conversion else ""
    return "{" + name + conversion_text + (f":{spec}" if spec else "") + "}"


def _restore_message(text: str, template: str, parts: dict[str, object]) -> Message:
    """Rebuild a pickled or copied `Message` without formatting its template again."""
    message = str.__new__(Message, text)
    message._template = template
    message._parts = dict(parts)
    return message


def _walk_parts(message: Message) -> Iterator[tuple[str, object]]:
    """Yield `(name, value)` for every part, depth first, in template order.

    Parts the template does not name follow the named ones, in keyword order.
    """
    named: list[str] = []
    for _literal, field_name, _spec, _conversion in string.Formatter().parse(message.template):
        if field_name is None:
            continue
        root = _FIELD_ROOT.split(field_name, maxsplit=1)[0]
        if root in message.parts and root not in named:
            named.append(root)
    named.extend(name for name in message.parts if name not in named)
    for name in named:
        value = message.parts[name]
        yield name, value
        if isinstance(value, Message):
            yield from _walk_parts(value)


@dataclass(frozen=True)
class RefusalContext:
    """The facts a refusal message carries, read without parsing its text.

    A field holds the first part of that name whose value is a plain `str`, `int`
    or `tuple`, searched depth first in template order.

    Attributes:
        product: The logical product name.
        product_uri: The product URI.
        run_id: The run the refusal concerns.
        claim_domain: The write-domain claim the refusal concerns.
        claim_owner: The owner holding that claim.
        group: The Zarr group the refusal concerns.
        slot_window: The half-open slot window `(start, end)`.
        array_path: The Zarr array path the refusal concerns.
        plugin: The plugin name.
        options: The name of every `Param` in the message, in order.
    """

    product: str | int | tuple[object, ...] | None = None
    product_uri: str | int | tuple[object, ...] | None = None
    run_id: str | int | tuple[object, ...] | None = None
    claim_domain: str | int | tuple[object, ...] | None = None
    claim_owner: str | int | tuple[object, ...] | None = None
    group: str | int | tuple[object, ...] | None = None
    slot_window: str | int | tuple[object, ...] | None = None
    array_path: str | int | tuple[object, ...] | None = None
    plugin: str | int | tuple[object, ...] | None = None
    options: tuple[str, ...] = ()


_FACT_FIELDS: frozenset[str] = frozenset(
    name for name in RefusalContext.__dataclass_fields__ if name != "options"
)


def _refusal_context(message: Message) -> RefusalContext:
    """Collect the facts and the `Param` names of `message` and its nested messages."""
    facts: dict[str, Any] = {}
    options: list[str] = []
    for name, value in _walk_parts(message):
        if isinstance(value, Param):
            if value.name not in options:
                options.append(value.name)
        elif (
            name in _FACT_FIELDS
            and name not in facts
            and isinstance(value, str | int | tuple)
            and not isinstance(value, _PART_TYPES)
        ):
            facts[name] = value
    return RefusalContext(**facts, options=tuple(options))


class FirecubeError(Exception):
    """Base exception for Firecube runtime errors.

    A refusal raised with a `Message` as its first argument also exposes that
    message's facts and recovery operations, so callers need not parse its text.
    """

    @property
    def message(self) -> Message | None:
        """The `Message` this error was raised with, or `None` for plain text."""
        first = self.args[0] if self.args else None
        return first if isinstance(first, Message) else None

    @property
    def recovery(self) -> tuple[Operation, ...]:
        """Every `Operation` in the message, depth first, in template order."""
        message = self.message
        if message is None:
            return ()
        return tuple(value for _name, value in _walk_parts(message) if isinstance(value, Operation))

    @property
    def context(self) -> RefusalContext:
        """The facts and `Param` names the message carries; empty without a `Message`."""
        message = self.message
        return RefusalContext() if message is None else _refusal_context(message)


class ConfigurationError(FirecubeError):
    """Invalid or conflicting configuration was supplied for a run.

    Raised during option validation and pre-write checks, e.g. unknown or
    malformed options, missing required inputs, or an existing store whose
    layout conflicts with the plugin's declaration.
    """


class ConfigFileError(ConfigurationError):
    """A strict read of the Firecube config file failed.

    `reason` names the failure. The CLI reports it as a usage error.

    Attributes:
        reason: One of `toml_unavailable`, `not_found`, `parse_failed` or `unknown_sections`.
        path: The config file path, when the failure concerns a file.
        detail: The parser's message; empty unless `reason` is `parse_failed`.
        unknown_sections: The unknown top-level sections, sorted; empty unless `reason` is
            `unknown_sections`.
        valid_sections: The accepted top-level sections, sorted; empty unless `reason` is
            `unknown_sections`.
    """

    def __init__(
        self,
        reason: Literal["toml_unavailable", "not_found", "parse_failed", "unknown_sections"],
        *,
        path: Path | None = None,
        detail: str = "",
        unknown_sections: Iterable[str] = (),
        valid_sections: Iterable[str] = (),
    ) -> None:
        """Build the error and its sentence from the failure facts.

        Args:
            reason: The failure kind.
            path: The config file path.
            detail: The parser's message, for `parse_failed`.
            unknown_sections: The unknown top-level sections, for `unknown_sections`.
            valid_sections: The accepted top-level sections, for `unknown_sections`.
        """
        self.reason = reason
        self.path = path
        self.detail = detail
        self.unknown_sections = tuple(sorted(unknown_sections))
        self.valid_sections = tuple(sorted(valid_sections))
        if reason == "toml_unavailable":
            text = "TOML support unavailable (Python 3.12+ required)"
        elif reason == "not_found":
            text = f"Config file not found: {path}"
        elif reason == "parse_failed":
            text = f"Failed to parse config file {path}: {detail}"
        else:
            text = (
                f"Unknown config section(s) {list(self.unknown_sections)!r}. "
                f"Valid sections: {list(self.valid_sections)}"
            )
        super().__init__(text)

    def __reduce__(self) -> tuple[Any, ...]:
        facts = {
            "path": self.path,
            "detail": self.detail,
            "unknown_sections": self.unknown_sections,
            "valid_sections": self.valid_sections,
        }
        return (_restore_config_file_error, (type(self), self.reason, facts), self.__dict__)


def _restore_config_file_error(
    cls: type[ConfigFileError], reason: Any, facts: dict[str, Any]
) -> ConfigFileError:
    """Rebuild a pickled or copied `ConfigFileError` from its constructor facts."""
    return cls(reason, **facts)


class MissingIrregularCoordinateError(ConfigurationError):
    """An irregular time-axis item lacks a resolvable coordinate."""

    def __init__(self, coordinate_name: str, item_id: str | int | None) -> None:
        self.coordinate_name = coordinate_name
        self.item_id = item_id
        super().__init__(
            f"IrregularTimeAxis discovery: item {item_id} has no resolvable coordinate for '{coordinate_name}'"
        )


class DuplicateIrregularCoordinateError(ConfigurationError):
    """Two discovered items resolve to the same irregular-axis coordinate."""

    def __init__(
        self,
        coordinate_name: str,
        coordinate_value: Any,
        first_item: str,
        second_item: str,
    ) -> None:
        self.coordinate_name = coordinate_name
        self.coordinate_value = coordinate_value
        self.first_item = first_item
        self.second_item = second_item
        super().__init__(
            f"IrregularTimeAxis discovery for '{coordinate_name}': items "
            f"{first_item!r} and {second_item!r} both resolve to coordinate "
            f"{coordinate_value!r}; coordinates must be unique"
        )


class NoDiscoveredItemsError(ConfigurationError):
    """Discovery found no items for an irregular time axis."""

    def __init__(self, coordinate_name: str, source_ref: str | None) -> None:
        self.coordinate_name = coordinate_name
        self.source_ref = source_ref
        super().__init__(
            f"IrregularTimeAxis discovery for '{coordinate_name}': no items found in source '{source_ref}'"
        )


class IndexedWriteCompilationError(ValueError):
    """IndexedWrite compilation failed for a coordinate-bound write intent."""

    def __init__(self, coordinate: Any, reason: str, iw_repr: str) -> None:
        self.coordinate = coordinate
        self.reason = reason
        self.iw_repr = iw_repr
        super().__init__(
            f"IndexedWrite compilation failed: {reason} (coordinate={coordinate!r}, iw={iw_repr})"
        )


class SchemaDriftError(FirecubeError):
    """Existing Zarr array metadata drifted from the declared schema.

    Compared fields: dtype, rank, shape[1:], chunks, fill_value.
    The time axis shape[0] is handled specially: smaller fails, larger warns.
    """


class StorageError(FirecubeError):
    """Storage backend operation failed (local, S3, etc.)."""


class ManifestError(FirecubeError):
    """Manifest read/write/update operation failed."""


class ResolvedIndexAttrsConflictError(ManifestError):
    """The zarr root attrs hold a resolved-index hash that has no record or disagrees with it."""


class ControlPlaneCorruptionError(ManifestError):
    """The `.firecube/` WAL or compact-cache contents are internally inconsistent."""


class ClaimConflictError(FirecubeError):
    """A write-domain claim is already owned by another active writer."""


def _restore_with_fields(cls: type[Exception], message: object, fields: dict[str, Any]) -> Any:
    """Rebuild a pickled or copied error from its message and keyword fields."""
    return cls(message, **fields)


class ClaimReleaseError(FirecubeError):
    """A claim was not released because its heartbeat write was still in flight.

    The claim record stays as it was. It goes stale, and an operator clears it.

    Attributes:
        domain: The claim's domain identifier.
        owner_id: The owner the claim was acquired for.
    """

    def __init__(self, message: object, *, domain: str, owner_id: str) -> None:
        self.domain = domain
        self.owner_id = owner_id
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, ...]:
        fields = {"domain": self.domain, "owner_id": self.owner_id}
        return (_restore_with_fields, (type(self), self.args[0], fields), self.__dict__)


class ClaimLostError(FirecubeError):
    """A run lost a claim it held, so it writes no further batch.

    Another writer replaced the claim record. The run cannot tell what that
    writer has stored since, so it stops instead of writing over it.

    Attributes:
        domain: The domain identifier of the first claim the run lost.
    """

    def __init__(self, message: object, *, domain: str) -> None:
        self.domain = domain
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            _restore_with_fields,
            (type(self), self.args[0], {"domain": self.domain}),
            self.__dict__,
        )


class ExclusiveMaintenanceRefusedError(FirecubeError):
    """Exclusive maintenance of a product is refused because other writers are present.

    The refusing call releases the exclusive claim it acquired before it raises.

    Attributes:
        product: The logical product name.
        claims: The domain id of each other claim of the product.
        runs: The id of each other non-terminal or unreadable run of the product.
    """

    def __init__(
        self,
        message: object,
        *,
        product: str,
        claims: Iterable[str] = (),
        runs: Iterable[str] = (),
    ) -> None:
        self.product = product
        self.claims = tuple(claims)
        self.runs = tuple(runs)
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, ...]:
        fields = {"product": self.product, "claims": self.claims, "runs": self.runs}
        return (_restore_with_fields, (type(self), self.args[0], fields), self.__dict__)


class ProductUnderMaintenanceError(FirecubeError):
    """Another owner holds the product's exclusive maintenance claim, so the write is refused.

    A refused run start is recorded failed before this error is raised. A claim that
    cannot be read refuses too; `owner_id` and `stale` are then `None`.

    Attributes:
        product: The logical product name.
        owner_id: The owner of the exclusive maintenance claim, or `None` when unread.
        stale: Whether that claim missed its heartbeat, or `None` when unread.
    """

    def __init__(
        self,
        message: object,
        *,
        product: str,
        owner_id: str | None,
        stale: bool | None,
    ) -> None:
        self.product = product
        self.owner_id = owner_id
        self.stale = stale
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, ...]:
        fields = {"product": self.product, "owner_id": self.owner_id, "stale": self.stale}
        return (_restore_with_fields, (type(self), self.args[0], fields), self.__dict__)


class SlotIndexModelError(FirecubeError):
    """Base class for slot-index-model failures (negotiation, persistence, claims)."""


class SlotIndexModelConflictError(SlotIndexModelError):
    """A different slot-index model is already recorded for this product.

    Raised when an incoming model's ``identity_hash`` does not match the hash
    that was previously persisted in the control plane or on the store's root
    attributes, indicating an incompatible partitioning scheme.
    """


class SlotIndexUnmanagedStoreError(SlotIndexModelError):
    """The target store has no slot-index model recorded.

    Raised when a slot-range-parallel reader or writer expects a model to be
    present (e.g. on resume) but neither the control plane nor the store root
    attributes carry a slot-index-model record.
    """


class SlotIndexModelClaimTimeoutError(SlotIndexModelError):
    """Failed to acquire the slot-index-model write claim within the deadline.

    Raised when negotiation cannot obtain the exclusive ``slot_index_model``
    claim before its timeout elapses, typically because another writer is
    mid-negotiation and has not yet released its claim.
    """


class ResolvedIndexError(FirecubeError):
    """Base class for resolved-index persistence and claim failures."""


class ResolvedIndexConflictError(ResolvedIndexError):
    """A different resolved index is already recorded for this product.

    Raised when an incoming resolved-index record's ``identity_hash`` does not
    match the hash already persisted in the control plane. Callers should include
    a field-level diff in the message so operators can see the incompatible
    groups, axis fields, and hashes without opening the record files manually.
    """


class ResolvedIndexClaimTimeoutError(ResolvedIndexError):
    """Failed to observe resolved-index convergence within the retry budget."""


class LegacyIndexRecordError(ResolvedIndexError):
    """A legacy ``.firecube/slot_index/current.json`` record was found without ``.firecube/index/current.json``.

    Raised at startup when the cube was previously written by a firecube
    version that predates the resolved-index record and the record has not
    yet been produced. The operator must migrate the cube by re-resolving the
    index, typically via ``firecube zarr index rebuild`` (from Python:
    ``firecube.ingestor.runtime.index_record.rebuild_index_record``). Continuing silently would
    let firecube stamp a fresh resolved-index record that could disagree with
    the legacy partitioning of already-written data.
    """
