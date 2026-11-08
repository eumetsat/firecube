# Operations

The module `firecube.operations` is the public Python facade over the operations the `firecube` command line runs. An operation takes the open product as its first argument. Open a product with `open_product`, used as a context manager. Exports keep their original names, without aliases. The module does not re-export errors. `Product.session` and `Product.manager` serve the engine and the operations. They are not part of the contract.

## Opening a Product

::: firecube.operations.open_product

::: firecube.operations.Product

::: firecube.operations.StorageDriverConfig

::: firecube.operations.StorageUri

::: firecube.operations.ChunkManager
    options:
        members: false

### Resolving Settings

::: firecube.operations.resolve_product_name

::: firecube.operations.resolve_storage_config

::: firecube.operations.resolve_slot_window

## Archive

::: firecube.operations.create_archive

::: firecube.operations.restore_archive

::: firecube.operations.read_restore_source

::: firecube.operations.restore_label

::: firecube.operations.ArchiveCreatePlan

::: firecube.operations.ArchiveCreateResult

::: firecube.operations.ArchiveRestorePlan

::: firecube.operations.ArchiveRestoreResult

::: firecube.operations.ArchiveRestoreSource

::: firecube.operations.ChainBackup

## Migrate

::: firecube.operations.migrate_product

::: firecube.operations.adopt_product

::: firecube.operations.list_migrations

::: firecube.operations.load_migration_steps

::: firecube.operations.MigrationResult

::: firecube.operations.MigrationRunInfo

::: firecube.operations.AdoptionResult

::: firecube.operations.AttributeVerdict

::: firecube.operations.InstalledStep

::: firecube.operations.ReleaseCreator

## Release

::: firecube.operations.create_release_for_product

::: firecube.operations.list_releases_for_product

::: firecube.operations.read_release_for_product

::: firecube.operations.verify_release_for_product

::: firecube.operations.ReleasePlan

::: firecube.operations.ReleaseCreateResult

::: firecube.operations.ReleaseListing

::: firecube.operations.ReleaseCheck

::: firecube.operations.ReleaseVerification

::: firecube.operations.RetentionVerification

## Snapshot

::: firecube.operations.take_snapshot_for_product

::: firecube.operations.SnapshotPlan

::: firecube.operations.SnapshotPreview

::: firecube.operations.SnapshotRecord

::: firecube.operations.SnapshotConsistency

## Zarr

### Index

::: firecube.operations.rebuild_index_record

::: firecube.operations.verify_index_record

::: firecube.operations.IndexRebuildResult

::: firecube.operations.IndexVerification

### Preallocate and Slots

::: firecube.operations.preallocate_product

::: firecube.operations.plan_slots

::: firecube.operations.PreallocateHost

::: firecube.operations.PreallocateDryRunGroup

::: firecube.operations.GroupSlotPlan

### Consolidate

::: firecube.operations.consolidate_time_coords

::: firecube.operations.ConsolidationSummary

### Multires

::: firecube.operations.ZarrMultiresBuilder

::: firecube.operations.MultiresConfig

## See Also

- [CLI Reference](../cli.md)
- [Exceptions](../exceptions.md)
- [Storage Drivers](../storage-drivers.md)
