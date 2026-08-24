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

"""Shared exception types for core storage/manifest operations."""

from typing import Any


class FirecubeError(Exception):
    """Base exception for Firecube runtime errors."""


class ConfigurationError(FirecubeError):
    """Invalid or conflicting configuration was supplied for a run.

    Raised during option validation and pre-write checks, e.g. unknown or
    malformed options, missing required inputs, or an existing store whose
    layout conflicts with the plugin's declaration.
    """


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


class ControlPlaneCorruptionError(ManifestError):
    """The `.firecube/` WAL or snapshot contents are internally inconsistent."""


class ClaimConflictError(FirecubeError):
    """A write-domain claim is already owned by another active writer."""


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
    index, typically via ``firecube zarr index rebuild``. Continuing silently would
    let firecube stamp a fresh resolved-index record that could disagree with
    the legacy partitioning of already-written data.
    """
