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

"""Public operations on a finished Firecube product.

Open a product with `open_product`, then pass the `Product` handle to an operation.
Each name keeps the name of its defining module, with no alias. The module exports the
phase A operations and the request and result types their signatures use. It also exports
the two marker attribute names that `firecube zarr preallocate` stamps. It does not
re-export errors. Most are in `firecube.core.errors`; a few live in the runtime module
that raises them, such as `SlotWindowError` in `firecube.ingestor.runtime.preallocate`.
"""

from __future__ import annotations

from firecube.core.archive.zip.backup_chain import ChainBackup
from firecube.core.controlplane.manager import ChunkManager
from firecube.core.controlplane.metadata_snapshots import (
    SnapshotConsistency,
    SnapshotPlan,
    SnapshotPreview,
    SnapshotRecord,
    take_snapshot_for_product,
)
from firecube.core.controlplane.releases import ReleaseVerification
from firecube.core.product.identity import resolve_product_name
from firecube.core.product.open import Product, open_product
from firecube.core.runtime import resolve_slot_window, resolve_storage_config
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.coord_consolidation import ConsolidationSummary, consolidate_time_coords
from firecube.core.zarr.multires import MultiresConfig, ZarrMultiresBuilder
from firecube.ingestor.contracts.interfaces import PreallocateHost
from firecube.ingestor.registry.loader import load_migration_steps
from firecube.ingestor.runtime.archive import (
    ArchiveCreatePlan,
    ArchiveCreateResult,
    ArchiveRestorePlan,
    ArchiveRestoreResult,
    ArchiveRestoreSource,
    create_archive,
    read_restore_source,
    restore_archive,
    restore_label,
)
from firecube.ingestor.runtime.index_record import (
    IndexRebuildResult,
    IndexVerification,
    rebuild_index_record,
    verify_index_record,
)
from firecube.ingestor.runtime.migrate import (
    AdoptionResult,
    AttributeVerdict,
    InstalledStep,
    MigrationResult,
    MigrationRunInfo,
    ReleaseCreator,
    adopt_product,
    list_migrations,
    migrate_product,
)
from firecube.ingestor.runtime.preallocate import PreallocateDryRunGroup, preallocate_product
from firecube.ingestor.runtime.release import (
    ReleaseCheck,
    ReleaseCreateResult,
    ReleaseListing,
    ReleasePlan,
    create_release_for_product,
    list_releases_for_product,
    read_release_for_product,
    verify_release_for_product,
)
from firecube.ingestor.runtime.release_retention import RetentionVerification
from firecube.ingestor.runtime.slot_plan import GroupSlotPlan, plan_slots

__all__ = [
    "ATTR_COORD_MANAGED",
    "ATTR_PREALLOCATED",
    "AdoptionResult",
    "ArchiveCreatePlan",
    "ArchiveCreateResult",
    "ArchiveRestorePlan",
    "ArchiveRestoreResult",
    "ArchiveRestoreSource",
    "AttributeVerdict",
    "ChainBackup",
    "ChunkManager",
    "ConsolidationSummary",
    "GroupSlotPlan",
    "IndexRebuildResult",
    "IndexVerification",
    "InstalledStep",
    "MigrationResult",
    "MigrationRunInfo",
    "MultiresConfig",
    "PreallocateDryRunGroup",
    "PreallocateHost",
    "Product",
    "ReleaseCheck",
    "ReleaseCreateResult",
    "ReleaseCreator",
    "ReleaseListing",
    "ReleasePlan",
    "ReleaseVerification",
    "RetentionVerification",
    "SnapshotConsistency",
    "SnapshotPlan",
    "SnapshotPreview",
    "SnapshotRecord",
    "StorageDriverConfig",
    "StorageUri",
    "ZarrMultiresBuilder",
    "adopt_product",
    "consolidate_time_coords",
    "create_archive",
    "create_release_for_product",
    "list_migrations",
    "list_releases_for_product",
    "load_migration_steps",
    "migrate_product",
    "open_product",
    "plan_slots",
    "preallocate_product",
    "read_release_for_product",
    "read_restore_source",
    "rebuild_index_record",
    "resolve_product_name",
    "resolve_slot_window",
    "resolve_storage_config",
    "restore_archive",
    "restore_label",
    "take_snapshot_for_product",
    "verify_index_record",
    "verify_release_for_product",
]
