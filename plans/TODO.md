# TODO

Accepted work only. Speculative ideas go to [IDEAS.md](IDEAS.md) until a design discussion promotes them.
New decisions are recorded in [DONE.md](DONE.md) with a date.

## Active Work

### Calendar-declared time axes - follow-ups (2026-10-20)

**Status**: OPEN. None started.

The design is in [DONE.md](DONE.md), 2026-09-23. These cases are refused with a clear error. They are not half-supported:

- `mode="floor"` (observed placement) with a non-Gregorian calendar.
- `end_date` with a non-Gregorian calendar. Use `slot_count`.
- Units other than the derived ones on a regular axis.
- A non-Gregorian calendar on a serial-mode `DirectZarrIngestor` that declares no `index_spec`.

Open items:

- **`mode="floor"` with a non-Gregorian calendar.** Blocked. xarray decodes a fill value in a `float64` non-Gregorian coordinate to a valid-looking date. An `int64` fill value fails to open the store. Both defeat the rule that a non-Gregorian coordinate is complete before a reader opens it. This needs an upstream fill-masking fix.
- **`end_date` with a non-Gregorian calendar.** Small. Parse an `end_date` string on the axis calendar. For a fixed cadence it is equal to `slot_count`, and it keeps the rule that the coordinate is complete up front.
- **Serial-mode `DirectZarrIngestor` with a non-Gregorian calendar.** Not supported. A serial plugin has no axis to carry the calendar. The loud refusal stays. The documented path is to declare an `index_spec`.
- An optional encoding unit that matches the source, on a fixed-cadence non-Gregorian axis. Today the unit is always derived from the reference point.
- Consolidation of a complete encoded coordinate into a denser chunk layout. The command changes nothing on a calendar store today. A real use case, for example a legacy sparse layout, needs its own encode-aware path.
- **Encoded storage for every calendar, Gregorian included.** See IDEAS.md §44. The stored `firecube_group_identity_hash` already includes the dtype string, so it needs no new field. The change moves the stored value of every existing Gregorian group. That needs a migration of the stored attribute and dual acceptance of the old and new value. The resolved-index record holds no dtype and no group hash. Adding the encoding identity to it is a proposed schema change and would need a version bump.
- **Archive and restore lose the CF encoding of a time coordinate.** Folded into the Tensogram entry in this file ("Archive restore loses time-coordinate encoding").
- **In-calendar comparison of control-plane time bounds.** A `360_day` span can record a bound such as 2049-02-30 that has no Gregorian date. `chunks list --time-range` and `chunks delete --time-range` refuse such a span with `UnparseableSpanBoundsError` and delete nothing. A comparison on the span calendar would let these queries select the span. It needs the calendar recorded with the bounds, and a filter in `_projection.py` that compares through it. The refusal is opt-in (`refuse_unparseable`) because force-reingest and resume record spans through the same filter.
- **Appending a Gregorian batch to a `360_day` store fails without a clear refusal.** The batch fails and the run is recorded failed. The message is a library comparison error (`cannot compare cftime...`, or a numpy `isnan` error for numeric CF batches). It does not name both calendars. Add a check where the append batch decodes its time coordinate.
- **Review items on encoded coordinates.** Found in the review of 77. Not fixed.
    - `months since` units pass axis construction and fail at encode time. Refuse them at declaration, because a month has no fixed length in seconds.
    - A numeric CF-encoded batch appended to a calendar append store fails with a `TypeError`. Decode it, or refuse it with a message that names the calendar.
    - The minimum `int64`, the missing-value sentinel of an encoded coordinate, is accepted as an explicit coordinate value. Refuse it, so that a written slot cannot read as missing.
    - A `float64` coordinate spec collapses values that differ by microseconds. The collapse is silent. It needs an explicit `float64` array spec.
- **Archive of a region-written (DirectZarr) Zarr cube fails.** `firecube archive create` fails with `cannot convert datetime64 to CBOR` on a cube whose time coordinate the region writer materialized. The converter copies the raw zarr fill value of the coordinate (a `datetime64` NaT) into the archive metadata, and the archive encoder cannot serialise it. Cubes from the append template are not affected. xarray CF-encodes their time coordinate to integers with an integer fill value. It reproduces on v0.1.7. Fix: write a `NaT` or `NaN` fill value in a serialisable form in the archive metadata. Map it back on restore. Add a region-written cube to the archive tests. A strict xfail test, `test_archive_create_gregorian_direct_zarr_store`, pins this in the tree of 77 only. It is not in this branch, so no test pins the defect here. owner: unassigned
- **The discovery of a Gregorian integer array in consolidation has no repository test.** `consolidate_time_coords` treats an integer array as a calendar coordinate only when its `calendar` attr is not Gregorian-like (`_is_encoded_time_coord` in `src/firecube/core/zarr/coord_consolidation.py`). No test in `tests/integration/test_consolidate_time_coord_command.py` runs a Gregorian integer array that carries `units` and `calendar`. A later change of the predicate is not guarded. Add a row to that test home.
- **A marked group at a calendar coordinate path passes pod startup, then fails with `AttributeError`.** `_verify_calendar_axes_preallocated_at_startup` in `src/firecube/ingestor/templates/direct_zarr.py` accepts a group that carries `firecube_preallocated`. `write_timestamp` then reads `dtype` from the group and raises `AttributeError`. The case came from 77 and was not introduced by the landing. No supported producer makes such a group. Fix: require an array before the startup check accepts its marker.
- **Eager `xarray` import on every plugin import path.** The core Zarr I/O module imports `xarray` when the module loads. The time-decode helper imports it lazily. Every plugin pays that cost, even when it never uses Zarr I/O. Fold it into a pass that makes imports lazy.

---

### Generic numeric flag helpers (2026-09-12)

**Status**: FUTURE; no implementation in the current core-readiness work.

Explore domain-agnostic numeric flag construction only after agreeing concrete caller contracts. Thresholds, bit meanings, missing-value rules, and published metadata remain caller-owned. Require behavior tests against caller-supplied rules; do not embed product conventions or change aggregation counts.

The same readiness scope defers `firecube.testing`, numeric aggregation counts, composite-source APIs, a new extraction lifecycle API, universal item-time filtering, new group/layout/slice hooks, and plugin-specific write ordering.

### Pattern parser adoption (2026-09-12)

**Status**: OPEN; intended first consumers are plugins that parse filename fields with their own code. No plugin migration is included in core readiness.

Replace each plugin's explicit filename-field parser with `parse_pattern` after verifying its accepted names, field values, and failure behavior. Keep timestamp selection, timezone policy, and source filtering in the plugin. Adoption must remove the replaced parsing code; the two-consumer duplication-removal condition is not yet met, so this is an optional utility addition, not a completed promotion.

### Typed Zarr codec entries (2026-09-14)

**Status**: FUTURE; not part of the plugin scaffolding fixes.

Zarr v3 codec objects are bare `dict` everywhere: `ZarrTemplateConfig.zarr_codecs` (`list[dict]`), `ZarrArraySpec.filters`/`serializer`/`compressors`, the codec pipeline, and the Zarr writer. Editors show their keys and values as `Unknown`. Validation already fixes the shape (`name` required, `configuration` an object when present, no other keys).

Leaning: a public `ZarrCodec` `TypedDict` (`name: str`, `configuration: dict[str, Any]`) applied to every codec field and signature, exported from `firecube.ingestor.api` with a reference entry. A `TypedDict` keeps the plain-dict runtime shape that JSON, TOML, and `coerce_cli_value` produce; a dataclass would need conversion wherever codecs flow. Overturn it if a runtime-validation library is adopted for configuration first.

Decide before implementing:
- Whether `configuration` is required. Validation treats it as optional, but the writer fails every batch without it (confirmed in the 2026-09-14 plugin-guide validation). Either require it in validation and the type, or normalize entries before writing and type it `NotRequired`.
- How the generated `ZarrStorageConfig` example names the type. An override must repeat the parent's exact type or pyright reports an incompatible override, and a live import is unused while the example stays commented.

---

### §36 `ZarrWriteContext` async-concurrency leak — convert to scoped config

**Status**: OPEN
**Scope**: Zarr write configuration hygiene
**Priority**: medium

`src/firecube/ingestor/runtime/zarr/write_context.py` still calls
`zarr.config.set({"async.concurrency": ...})` bare during `ZarrWriteContext.__enter__`.
This pre-existing setting is outside the Wave C2 `zarr_write_empty_chunks` work,
but it should be converted to a scoped context manager so async concurrency is
restored when the write context exits.

---

### §33 DirectZarr slot-range parallelism - roadmap

- **Landed:** `IndexSpec` + `RegularTimeAxis` + `ResolvedIndex`; byte parity for the existing production parallel plugins; recursion defect fixed.
- **Landed:** `IntegerAxis` + engine-owned `.firecube/index/current.json` record (`ResolvedIndexRecord`); `firecube zarr index show/verify/rebuild` CLI; atomic reader migration path documented.
- **Landed:** `IrregularTimeAxis` + `AUTO` sentinel + content-addressed item manifest; `--dry-run` for preallocate; `--derived` for index show; closes #27 spirit. See DONE.md 2026-08-24.
- **Landed:** `IndexedWrite` high-level abstraction, merged into a single `build_write_intents` contract accepting mixed `WriteIntent | IndexedWrite` lists; documented in `docs/guides/plugins/direct-zarr.md` and the API reference. See DONE.md 2026-08-24 and its 2026-09-02 amendment.
- **Future deprecation pass:** `SlotIndexModel`, `SlotAxis`, and `as_legacy_slot_index_model()` are kept for byte-parity compatibility with cubes written before the new index model. Schedule a deprecation pass once all known production cubes have been migrated via `firecube zarr index rebuild`. The deprecation should add `DeprecationWarning` on construction, update the allowlist in `test_api_docs_coverage.py`, and remove the symbols in a subsequent major version.

---

### Parallel chunk ownership follow-ups (2026-10-11)

- **Sparse staged writes by seeding.** Staged runs that write only part of a chunk or shard are refused because promotion replaces whole objects. Supporting them means seeding the touched units of every pod-written array into the workspace (as `GenericZarrIngestor` appends already do) and relaxing the promotion check; needs a design for seeded engine-owned and unchanged objects so they are not re-uploaded.
- **obstore partial write on copy failure (pre-existing; read in code, not run).** `StreamingObstoreWriteBuffer.close()` also runs when a copy fails partway and saves what it holds. Local targets now land that in the publication temporary, which is deleted; on S3/GCS it can still overwrite the object with a partial body.
- **`StorageSession.ls` raises on local fsspec targets (latent; seen raising in a test run, no src caller by grep).** `FsspecFilesystem.ls` returns bare paths and `_storage_uri_from_path("file", ...)` passes them to `StorageUri.parse`, which rejects bare paths.
- **Plugin discovery depends on import order in tests (pre-existing; seen once, not reproduced since).** Running the verify files of the chunk-ownership work together in one pytest process once failed 8 tests of a since-deleted pods test file with `Unknown plugin`: fixture plugins are found through import side effects, not only entry points. The combined files pass (345 tests) without that file, so the symptom is gone, but the cause (discovery by import side effect) is not fixed. Make discovery independent of test order.
- **Refuse serial staged runs with partial chunk coverage earlier (optimisation; read, not run).** Slot-window staged runs are refused before any batch because `inspect_item` yields the planned slots; plain serial staged runs are refused only just before promotion, after workspace writes (target untouched, wasted compute only). Idea: when the plugin overrides `inspect_item` and has a resolved index, inspect the already-filtered batch items before the first batch and reuse `require_planned_slots_fill_units`. Estimate from review: four files (`engine.py`, `direct_zarr.py`, two test files), ~150-250 production and 300-500 test lines, 1-2 coder days plus two review rounds. Hazards: `ItemInfo` carries no group, array or row/channel footprint, so completeness of rows and channels cannot be proven early; a fresh serial run writing 6 slots into shape `(6,)` with chunks `(12,)` succeeds today and would be falsely refused if the declared 12-slot extent were used (serial creation derives the extent from the intents' `ts_index`, the `max_ts` computation in `IndexedRegionStrategy._write_groups`); discovery overcounts plugin resume-skipped writes and cannot prove force-reingest coverage; a second `inspect_item` call may cost or have side effects. The late ledger check (the `_IncompleteUnitsRefusal` call in the engine's post-pipeline lifecycle method) must stay either way.
- **A refused `zarr preallocate` can leave files outside `.firecube/` changed (run on 2026-10-12; read in code).** The ownership refusals (`_refuse_ownership_change` in `src/firecube/ingestor/runtime/preallocate.py`) fire per array, inside the per-group loop of `preallocate_product`, after earlier work has already run. Arrays declared before the refused one in its group are created first (`_create_array`): a one-group run that adds two such arrays and then refuses exits 1 with their `zarr.json` files left behind, for both refusal kinds. Earlier groups are processed completely first, including observed coordinate values and engine-owned values (`_materialize_engine_owned_arrays`). What the order guarantees is only that the refused group's own coordinate and engine-owned materialization has not started. Separately, a preallocate that reaches an existing regular coordinate rewrites its `zarr.json`, refused or not (the `update_array_attrs` calls on an existing coordinate in `materialize_regular_coord_array`, `src/firecube/core/zarr/coord_materialization.py`): identical bytes with a new mtime when the coordinate's metadata is unchanged, new bytes when the spec's coordinate attrs changed. The adoption test watches only the engine-owned array for preallocate (`test_preallocate_then_pods_preserve_engine_arrays_and_layout` in `tests/integration/test_engine_owned_arrays.py`); its one-group scenario declares no earlier arrays, so watching the whole group passes there and would not catch any of these cases. Repair: check ownership for every group before the group loop creates or materializes anything.
- **Sharded time coordinates are claimed by inner chunk, and their layout depends on who creates them (pre-existing on `c160535`; run on 2026-10-12).** The coordinate claim compares a sharded coordinate by its inner chunk, not by its shard, so two windows in different inner chunks of one shard are admitted (the `chunk_axis_range` comparison in `ChunkManager.claim_coord_materialization_window`, `src/firecube/core/controlplane/manager.py`). One coordinate declaration also gets different layouts depending on who creates it: an observed (`mode="floor"`) preallocate makes `min(256, slots)` chunks, unsharded (`resolve_coord_chunks` in `src/firecube/core/zarr/_coord_chunks.py`, called from `src/firecube/core/zarr/coord_materialization.py`); grid or irregular preallocate and ingest make the largest divisor of the shard as chunk, inside the declared shards (`_divisor_chunks` and `resolve_effective_layout` in `src/firecube/core/zarr/guard/_layout.py`). For a coordinate that preallocate marked, no lost update is reachable through these claims: only preallocate writes its values, under product-wide claims on which a second preallocate fails at once, and ingest workers only verify it. Not examined: `firecube zarr consolidate-time-coord`. Unmarked (legacy) coordinates, which ingest writes, are covered by the bullet on unmarked time coordinates below. Repair, if one is needed: claim a sharded coordinate by its shard, and create the coordinate with one layout whoever creates it.
- **A slot-window ingest is accepted on an unmarked time coordinate, and two processes can lose a timestamp (pre-existing on `c160535`; run on 2026-10-12).** The group's time coordinate carries neither `firecube_preallocated` nor `firecube_coord_managed` when no preallocate ran or the store predates the markers. Pods then write it through the legacy path under an in-process lock only (`RegionZarrWriter._write_legacy_timestamp` in `src/firecube/core/zarr/region_writer.py`), and pod startup and the per-batch check skip the coordinate by name (`is_pod_written_slot_array` in `src/firecube/core/zarr/guard/engine_owned.py`, used by `DirectRegionAdapter.pod_written_specs` and `IndexedRegionStrategy._pod_written_paths`). Two processes with windows [0,1) and [1,2) in one coordinate chunk, held so that both read before either wrote, both exited 0 and slot 0's timestamp stayed NaT while its data was written (measured on `c160535`); unforced runs can lose a value, rarely (0 of 20 in the research sample; a review sample lost 2 of 20, not re-run); a control whose values were already stored lost nothing; a fresh target with concurrent pods was not tested. The unmarked store was simulated (markers stripped, values reset to NaT). Repair: refuse a slot-window ingest whose time coordinate is absent or unmarked, naming `firecube zarr preallocate`, as the engine-owned refusal does. Preallocate refuses an unmarked observed shell (`tests/unit/test_materialize_regular_coord_array.py::test_floor_mode_existing_unmarked_array_refuses_legacy_shell`), so an unmarked observed shell has no upgrade path today (grid and irregular coordinates were reported upgradable by a review probe; not run here).
- **Staged runs do not enforce static write-once (pre-existing on `c160535`; run on 2026-10-12).** Staged runs write and check static data in the workspace copy, never in the final target, and seeding strips `firecube_static_written` from the workspace (`src/firecube/ingestor/runtime/zarr/staged_metadata.py`), so the write-once check never sees the previous run's values. Promotion publishes array metadata through the session's publisher (`update_document` in `src/firecube/core/zarr/guard/metadata_authority.py`: target attributes win, the workspace adds missing keys), so the marker does reach and stay on the target; it is not consulted. Reproduced after the guarded write layer on 2026-10-14: after a preallocate, or on a target a staged run created, two successful staged runs (the second with `resume_existing=true`) changed a static array from [0, 0] to [1, 1], with the marker on disk throughout. A later staged run with another static payload exits 0 and overwrites it (100 to 900) in every sequence run before the layer: staged then staged, and direct then staged, each with and without a preceding preallocate. A direct run after an unmarked staged overwrite stamps the marker over data it never compared; direct after direct refuses. The `WriteIntent.static` docstring promises `SchemaDriftError` on any later run (`src/firecube/ingestor/templates/direct_zarr.py:861-864`), and the `plans/DONE.md` entry "2026-06-25 — DirectZarr plugin parity — core fixes" (T3, staged-seed marker strip) claim that the final target keeps the marker describes retention only, not enforcement. Repair: verify a staged static payload against the final target (as engine-owned verification does), at promotion or at the first batch.
- **A static array's write transaction is not serialised between processes (pre-existing on `c160535`; read in code on 2026-10-12, not reproduced).** `IndexedRegionStrategy` dispatches static intents outside the slot claims (the `static_intents` loops in `IndexedRegionStrategy._write_groups`, `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py`), and the whole-array lock that serialises a static array's marker check, write and stamp is held in one process only; non-owner suppression (`suppress_static_emission_for_non_owner`) defaults to false. Two slot-window processes that both emit the same unmarked static array can therefore both pass the marker check and write, the later replacing the earlier when their payloads differ. Identical payloads, the usual latitude and longitude grids, are harmless. Repair: let only the owner of the range that starts at slot 0 emit a static array, or claim the array across processes.
- **`zarr slots --input-data` does not narrow the plan (2026-10-16; read in code, reproduced under a memory cap).** Discovery binds the index only: ranges always cover the whole remaining axis (`src/firecube/cli/zarr/_slots.py` uses `input_data` only to build the ingestor and its context), so two OLCI granules on the 44,180,640-slot grid axis plan 44,180,640 one-slot ranges. Idea: an opt-in flag that keeps only ranges holding at least one discovered item (`inspect_item` per item, mapped through the resolved index, then merged with resume coverage). Costs a discovery pass at planning time; needs a design for several groups, AUTO axes and items outside the axis.

---

### Guard-check cost follow-ups (2026-10-17)

Left over from the guard-check cost work. Per-file costs were measured on 3,000 chunks and the totals scaled, so the totals are calculated, not run.

- **Staged seeding copies every coordinate chunk of a target that holds data (audit row 3; per-file cost ran, totals calculated).** `src/firecube/ingestor/runtime/zarr/staged_metadata.py` copies each file under the group whose path matches `<coord>/c/` into the workspace on the first staged batch (copy 36 us per file, existence test 7 us per file on later batches). On a target with 172,581 coordinate chunks that is 6.3 s for the first batch and 1.2 s for each later one; the same code at 1,725,807 chunks is calculated at 63 s and 12 s. A target that holds only metadata copies only `zarr.json` files. Repair: seed only the chunks of the pod's window, or none, since pods only verify the coordinate.
- **Staged seeding walks the final target twice per batch (audit row 4; per walk ran, totals calculated).** `_discover_zarr_groups` and the group `find` in `staged_metadata.py` each walk every object under the final target, for every staged batch whose target exists, a metadata-only target included. One walk over 173,283 files took 1.67 s (9.6 us per file); four walks per run are 6.7 s, and the same walk over 1.73M files is calculated at 16.7 s each, 67 s per run, with data objects not counted. Repair: discover the groups once per run, or read the children of the root `zarr.json`.
- **Promotion lists the whole workspace, which includes the seeded coordinate chunks (audit row 5; per listing ran on a stand-in, total calculated).** `list_workspace_objects` in `src/firecube/core/zarr/guard/promotion_plan.py` (called from `StorageSession.plan_promotion`) walks every file under the workspace once per staged delta promotion, a fresh target included. Over 173,283 files it took 5.3 s; at 1.73M files it is calculated at 52.8 s. The stand-in was `perf/new.zarr`, not a real seeded workspace. Repair: list only the prefixes the run wrote (the ledger arrays and the `zarr.json` documents), since seeded coordinate chunks are never written by the run.
- **Promotion reads every unclaimed workspace object twice (audit row 8; per file ran, totals calculated).** `_is_unchanged` in `src/firecube/core/storage/session.py`, driven by `promotion_plan.py`, reads the target object and the workspace file for each workspace object that is not a written unit: always the `zarr.json` documents (17 in the measured cube), and, only when the target held data, every seeded coordinate chunk. 22 us per file gave 3.8 s over 172,581 files; the 1.73M-file total is calculated at 38 s. Repair: compare size and mtime first, or skip arrays the run did not touch. Rows 3, 4, 5 and 8 share a cause, so fixing the seeding in row 3 shrinks rows 5 and 8. Since 2026-10-18 promotion no longer compares the chunks of a managed time coordinate. Its final-target `zarr.json` marks it preallocated or coordinate-managed. Other arrays are not classified. The plan lists those chunks UNCHANGED without a call to `_is_unchanged`. A legacy coordinate, the `zarr.json` documents and every other unclaimed object are still compared.
- **The irregular-axis index record hashes every axis value (audit row 31; per 1M values ran, totals calculated).** `canonical_index_payload` in `src/firecube/core/index_resolve.py` formats each axis value of an `IrregularTimeAxis` with explicit values, then `json.dumps(sort_keys=True)` and a hash of the whole list. It runs at startup, once for the record (`as_resolved_index_record`) and once for the identity hash, so up to twice per run. Cost is 1.22 s per 1M values plus 0.09 s for JSON and hash: 54 s per call at 44.18M values and about 540 s at 441.8M (calculated). The smoke-2 run did not exercise it, because its `time` axis is regular. The same code is in v0.1.7. Repair: hash the axis array bytes once, or record the axis length and a digest of its source.
- **Control-plane `ls` callers can read a stale s3fs listing (inferred from s3fs code; not run against S3).** With the fsspec driver on S3, `ls` is served from a per-instance dircache that never expires (`use_listings_cache` defaults to true, `listings_expiry_time` to `None`), and firecube sets none of the options and passes no `refresh`. `find` without `maxdepth` always calls S3 but only adds missing cache keys; it never repairs an existing entry. A listing taken once in a process can therefore stay stale for the rest of that process, and a write by another process or pod invalidates nothing here. The callers that pass no `refresh` are in `src/firecube/core/controlplane/claims.py` (`ls` calls with `# type: ignore[attr-defined]`), `events.py`, `_wal_reader.py`, `_projection.py` and `src/firecube/core/zarr/validation.py`. Repair: decide per caller whether it needs a fresh listing, and pass `refresh=True` where it does.
- **`ls` is missing from the `StorageFilesystem` Protocol (read; the pyright count ran).** Callers in `src/` reach `ls` without the Protocol declaring it, some through `type: ignore` comments. Declaring `ls(uri, detail=False, refresh=False)` makes pyright report 8 errors in 7 test files: `CountingFilesystem` subclasses that override `ls(uri, detail=False)` without `refresh` in `test_crash_recovery_bulk_sweep.py`, `test_bulk_abandon_stale_runs.py`, `test_bulk_clear_stale_claims.py`, `test_resume_guard_enforce_op_counts.py` and `test_time_coord_consolidations_scan.py`, and fakes with no `ls` in `test_driver_parity_deletion.py` and `test_parquet_cli.py`. Alternative not built: a capability Protocol, which the report notes would break `tests/unit/test_filesystem_capabilities.py:47`.
- **`FsspecFilesystem.read_bytes` on s3fs can fail on an object another process wrote (ran on moto, one run per cell).** `cat_file` looks the object up in the listing cache before its GET (`_size` via the cached `_info`; inferred, `_info` was not read), so a read raises `FileNotFoundError` for an object written after this process listed its directory, with zero S3 operations issued; with nothing cached it costs one HEAD before the GET. Every `read_bytes` caller shares this. The obstore driver has no listing cache and was not affected. Repair: bypass the cache for reads that follow another process's write, or disable the listing cache for the control-plane filesystem.

---

### Claim protocol and follow-up work after #86 (2026-10-18)

Internal contributor material: this section is a maintenance inventory for people working on firecube, not user or plugin documentation. Recorded when the abandon fix for #86 was planned. The first entry is a known limitation of the claim protocol, not scheduled. The second and third name the plans that follow #86, in order. The data-loss plan it lists first has landed (2026-10-18), so the second plan is next. The fourth lists reproduced findings that no plan schedules yet. The last list gives every other finding of the same review its disposition. Findings marked reproduced were run against real code during the review; the rest were read in code unless an entry marks them inferred or unverified, and those marks stand. Nothing was re-run when this section was written; the cited functions and the fail-open listing, the segment overwrite, the sweep and the release code were read again in the repository.

- **Claim transitions are check-then-act, in two storage operations (recorded, not scheduled, maintainer ruling, 2026-10-18).** `release_claim_if_unchanged` and `clear_claim` in `src/firecube/core/controlplane/claims.py` re-read a claim file and then delete it. Two concurrent callers can both pass the read. Then one deletes a claim that a successor acquired in between. Two holders then own one slot. The window needs two concurrent releases, so an operator who recovers one run at a time does not hit it. The same protocol has four related windows. First, `ClaimHandle.release` waits 2 s for the heartbeat thread and does not check that the thread stopped. A heartbeat that outlives it can rewrite a claim a successor took. Second, a stale handle's `release` deletes whatever claim file sits at its path, including a successor's. Third, deletion keeps no handle of its maintenance claim (`_acquire_maintenance_claims` in `src/firecube/core/controlplane/deletion.py`). It clears the claim by domain with `force=True` (`_clear_maintenance_claims`). So a heartbeat of the original acquisition can recreate it. Fourth, `ChunkManager.abandon_stale_runs` checks a run's heartbeat age again immediately before the abandon, but not atomically with it. So a run that heartbeats in that gap is abandoned. Drivers offer no conditional delete through the `StorageFilesystem` facade: `rm` takes no precondition. AWS S3 honours `If-Match` on `DeleteObject` (general-purpose buckets since 2025-09-16, directory buckets since 2024-11-25). s3fs reaches it only through `call_s3("delete_object", ..., IfMatch=etag)`, and its `rm` drops the argument. obstore 0.11.0 has no conditional delete, only conditional put, which its local backend refuses. MinIO and Ceph support is not certified. On a local file system every claim transition needs one lock around the read and the delete. On NFS that lock needs working cross-client locking. Repair direction: a claim protocol that checks the generation in every transition in both drivers, with one lock around every local transition. A patch to `release_claim_if_unchanged` alone cannot close it.
- **Next plans, in order (maintainer ruling, 2026-10-18).** First, closed on this branch on 2026-10-18 (entry in `plans/DONE.md`). A staged promotion published a seeded copy of a time coordinate chunk it did not own. A concurrent windowed `zarr preallocate` that filled a slot then saw it go back to NaT (reproduced through both CLIs). An overwrite restore removed the whole target, `.firecube/` included, before it validated the archive. That also deleted the restore's own claim, so a second restore without `--overwrite` saw no claim and both wrote (reproduced with two real restores). Promotion now leaves the data objects of a managed time coordinate alone. An overwrite restore now validates first and keeps its claim. The residuals are the open entries below. Second, control-plane listings that fail closed. `list_claims` in `claims.py` and `list_run_entries` in `_wal_reader.py` turn a failed `ls` into "nothing there". `_discover_next_part_index` in `events.py` turns it into segment 0. Then `flush` opens `events-00000.jsonl` with mode `"w"` and replaces the first segment (reproduced in memory).
- **Reproduced findings that no plan schedules yet (recorded, not scheduled; each with its consequence).**
    - GenericZarr append stamps the state array `present=1` before the lazy payload computes (`_write_batch` in `src/firecube/ingestor/runtime/zarr/append.py`, `write_dataset_to_zarr` in `write.py`): after a crash, resume reports `timestamps_skipped: 1` and keeps the hole.
    - Span keys `span_{run_id}_{batch_id}_{group}` (`build_span_entry` in `src/firecube/core/controlplane/types.py`) collide across groups such as `g` and `g_` and across runs; the projection keeps one span and drops the other's coverage. Span and schema-verification keys share one namespace in `src/firecube/core/controlplane/_wal_writer.py` (`ManifestWalWriter.record_span_event`, `record_schema_verification_event`).
    - A pod group recovered from the control plane is not URL-decoded (`parse_pod_run_id_slot` in `src/firecube/core/controlplane/repo_utils.py`): `g/x` comes back as `g%2Fx`, so a second `g/x` pod on the same window passes the resume guard.
    - `ObstoreFilesystem._absolute_key` (`src/firecube/core/filesystem/obstore_backend.py`) takes a relative key that equals or starts with the store prefix for an absolute one: two objects list as one URI and the wrong body is read (reproduced on moto).
    - `zarr consolidate-time-coord` (`consolidate_time_coords` in `src/firecube/core/zarr/coord_consolidation.py`, moved there from the CLI on 2026-10-18; its writes still bypass `guarded_write` and `MetadataAuthority`) takes no claim and deletes the original coordinate before it renames the replacement: a `write_timestamp` between snapshot and swap is lost, two consolidators share one fixed `.consolidating` path, and a crash during the delete followed by a retry seals NaT over good values.
    - WAL segment file names sort lexically, so replay goes out of order past index 99999 (`events-{index:05d}.jsonl`, written by `flush` in `src/firecube/core/controlplane/events.py` and sorted by `list_run_segment_paths` in `_wal_reader.py`).
    - Maintenance admission is list-then-claim on another domain (`_acquire_maintenance_claims`, `deletion.py`): deletion sees no claims, an ingest takes a slot claim, deletion takes its `maintenance:*` claim, and both proceed.
- **Disposition of the other findings of the same review (plan 1 is the branch data-loss plan, landed 2026-10-18 and recorded in `plans/DONE.md`, plan 2 the fail-closed listings plan, plan 3 the structured errors plan).** Items marked "see above" are in the entries above.
    - Reproduced data loss or broken coordination: staged promotion reverts a coordinate fill, closed by plan 1 (2026-10-18); overwrite restore deletes its own claim, closed by plan 1 (2026-10-18); `consolidate-time-coord` takes no claim, see above, recorded, not scheduled; failed WAL listing overwrites history, plan 2; archive create of a DirectZarr product with `datetime64` coordinates fails with "cannot convert datetime64 to CBOR" (reproduced; numeric coordinates work), recorded, not scheduled.
    - Listing failure treated as empty: `list_claims`, plan 2; `list_run_entries` and `list_run_segment_paths`, plan 2; `_discover_next_part_index`, plan 2; the sibling claim listings `_list_products` and `_list_claim_files` in `claims.py`, plan 2; metadata read failure read as "group absent" in `AppendResumeService._read_metadata` in `append_services.py` (a resume with `resume_existing=True` then takes `mode="w"` at cursor 0, reproduced), plan 2; strict staged resume skips required metadata seeding when discovery fails, and a test pins the swallow, in `seed_staged_metadata_pre_batch` in `batch_runner.py` (reproduced), plan 2; `ObstoreFilesystem.exists` falls back from a failed HEAD to a prefix listing, and an empty listing means absent, plan 2; deciding per caller where a fresh listing is required, plan 2; stale s3fs listings, already recorded in the guard-check cost entry above.
    - Check-then-act, control plane: `clear_claim` on a stale claim deleting a fresh successor, see above; sweep freshness re-check, see above; discarded maintenance handles, see above; heartbeat outliving release, see above; maintenance admission, see above; WAL segment number `max(listed)+1` in `_discover_next_part_index` with `flush` in `events.py` overwriting, so an abandon racing a run's own completion replaced the completion with `abandoned`, plan 2 (its create-only segment publication closes the overwrite; whether the two writers still pick the same number is not decided by that plan, recorded, not scheduled); two consolidations sharing the run id `TIME_COORD_CONSOLIDATION_RUN_ID` (`time-coord-consolidation`, in `core/controlplane/types.py`) and one losing its events, recorded, not scheduled; a deletion plan stale by the time maintenance starts (inferred, not run), recorded, not scheduled; the loser recovery mirror of the root attributes and stale S3 listings, already recorded (the first in `plans/DESIGN.md`, the second above); a heartbeat file truncated by its rewrite and readable as corrupt JSON, reported by the review as already known but not found in the repo's plans, recorded, not scheduled (unverified); local WAL segments readable half written, `LATEST.json` replaced non-atomically (`src/firecube/core/controlplane/repo.py`; the writing method was not located again, not checked), and the local mirror hard-coding `LocalStore` under the obstore driver, each recorded, not scheduled.
    - Check-then-act, storage and zarr: the append claim released before failed-batch repair, which can truncate or delete a successor's tail (`append_time_groups`, which holds the claim and dispatches the repair, and `_failed_batch`, which does it, in `src/firecube/ingestor/runtime/zarr/append.py`), recorded, not scheduled; archive admission list-then-claim (`_acquire_maintenance_claim` in `src/firecube/ingestor/runtime/archive.py`, moved there on 2026-10-19), recorded, not scheduled; two runs passing `ResumeGuard` before either records `started`, recorded, not scheduled; GenericZarr staged seeding outside its ordered gate, recorded, not scheduled; promotion metadata last-PUT-wins, clipped-unit window, static write-once across processes, legacy coordinate read-modify-write and fsspec root exists-then-set, already recorded in `AGENTS.md` or `plans/DESIGN.md` and in the static-array entries above; `zarr scrub`, `zarr state`, multires taking no claim, the zip archive output no-overwrite race, the shared explicit staged workspace, the unit-lock store identity taken from the caller's URI, and the obstore exclusive open being exists-then-put (unused in production), each recorded, not scheduled.
    - Operator commands that do not run: the structured-errors plan (`plans/DONE.md`, 2026-10-20) fixed the refusal texts of `core/` and `ingestor/` and the quoting of the index rebuild hint and `_json_list_hint`. What remains: the help and scaffold hints of `cli/` in the entry "Open after structured errors (2026-10-20)" below; the `multires` advice of `ZARR_MULTI_RES_REMOVED_MESSAGE` in `ingestor/templates/config.py`, which still names a positional `<target>` the command lacks; `chunks delete` with a product URI exiting 1 with "Storage configuration not available" as soon as a span matches (`delete_cmd` in `src/firecube/cli/chunks/_delete.py`, reproduced), recorded, not scheduled; command examples in the docs (archive create and restore previews that exit 2 without a TTY, paths with spaces in the chunk-ownership refusal page, output drift in the post-crash recovery, inspect and chunk-manager pages), recorded, not scheduled (#86's docs nodes fix the two recovery pages), and the docs lane runs none of them.
    - Error paths that replace or lose the original error: obstore close uploading a partial buffer and the PUT error replacing the body's, already recorded above (obstore partial write); `ClaimHandle.__exit__` replacing the body's exception when the delete fails, and `RunSchemaClaims` passing `(None, None, None)` to it, recorded, not scheduled; a claim GET failure during acquisition surfacing as `ControlPlaneCorruptionError` instead of `ClaimConflictError`, so convergence loops do not retry, recorded, not scheduled; restore's failure recording and claim release replacing the restore error, closed by plan 1 (2026-10-18). `restore_archive` in `src/firecube/ingestor/runtime/archive.py` (moved there from the command on 2026-10-19) now keeps the claim handle and releases it in `finally`. A failure to record the failure, to release the claim or to close the manager prints a `Warning:` line and never replaces the restore error. A failure to record the start or the success surfaces as the error itself. An interrupt, or a failed failure-recording, can still leave the run `started`; cleanup that replaces the original error in `WorkspaceManager.materialize` and `WorkspaceManager._materialize_remote_uri` (`src/firecube/ingestor/runtime/workspace.py`), `copy_file` (`src/firecube/core/storage/transfer.py`, plus a source handle leak), `zarr_to_zip` (`core/zarr/zip_archive.py`), `_restore_data_message` (`core/tensogram/restore.py`) and the `batch_teardown` calls in `_process_batch` of `GenericZarrIngestor`, `GenericParquetIngestor` and `DirectZarrIngestor` and in `process_tensogram_batch` (`generic_tensogram.py`), recorded, not scheduled; the engine reducing batch errors to `str` and logging "Run recorded as status=failed" before recording it (`_failed_batches_message` and the run loop in `src/firecube/ingestor/runtime/engine.py`; the logging call was not located again), plan 3 for the `str` (done 2026-10-20: `PipelineResult.failure` keeps the exception) and recorded, not scheduled for the log order; I/O failures relabelled as domain diagnoses in `_discover_auto_irregular_axis` (`ingestor/runtime/index_binding.py`), `slots` (`cli/zarr/_slots.py`) and `_stored_time_units` (`ingestor/runtime/slot_plan.py`), `discover_regular_observed_coord_values` and `stamp_coord_managed_marker` (`core/zarr/coord_materialization.py`) and `preflight` (`ingestor/runtime/preflight.py`), recorded, not scheduled.
    - Cost on hot and retry paths (calculated from source, not measured): `WalReader.read_run_entry` (`_wal_reader.py`) listing every segment before it reads `run.json`, the resume guard projecting spans twice (the two projection calls were not located again, so no symbol is named), `recursive=False` discovery walking the whole subtree (`discover_input_files` in `src/firecube/core/formats/discovery.py`, which calls `find` and then filters), the fresh-target check materialising a full listing for one object (`require_empty_data_target` in `src/firecube/ingestor/runtime/output_guard.py`) and `RegionZarrWriter` opening each array twice (`src/firecube/core/zarr/region_writer.py`; the two opens were not located again, so only the class is named), each recorded, not scheduled; staged seeding, promotion and engine-owned whole-array reads, already recorded in the guard-check cost entry above.
    - Placement and layering: operator CLI strings below `cli/` and Click in core, plan 3 (done 2026-10-20, `plans/DONE.md`); `_literal_delete` in `src/firecube/core/storage/session.py` reaching into both drivers' private fields (already recorded on 2026-10-15; it belongs in `core/filesystem/` as a capability), recorded, not scheduled; the claim retry and convergence loop existing three times (`ensure_resolved_index`, `ensure_slot_index_model`, `_setup_global_zarr_schema`), recorded, not scheduled; unit store keys encoded four ways: `unit_store_key` in `core/zarr/guard/guarded_write.py`, `_unit_object_key` in `core/zarr/guard/unit_ownership.py`, `_write_unit_store_key` in `ingestor/runtime/zarr/staged_metadata.py`, and the key built by hand in `_delete_span_by_chunk_keys` in `core/controlplane/deletion.py`. `_unit_object_key` named `'/c/0'` for an array at the store root, where the store holds `'c/0'`; it now guards a root array (2026-10-20). No caller can pass a root array, because the store root of a product is always a group. Merging the four is recorded, not scheduled. The other duplicated homes of the placement review (no symbol located, the review's list was not re-read), recorded, not scheduled.
    - Records written before the decision: GenericZarr append stamping state early, and consolidation deleting before the rename, see above; restore with overwrite deleting the target before it validates the archive, closed by plan 1 (2026-10-18, with the `.tgm` residual as an open entry below); multi-span deletion changing payload before its records, so a later span's refusal leaves changed data under `active` spans (`delete_spans` in `deletion.py`), recorded, not scheduled; the engine swallowing span-recorder failures as hook failures and recording the run complete with no spans (`_handle_completed_batch` in `engine.py`, which logs an `on_batch_success` hook failure and keeps the batch as success; the span recording itself is `_record_active_spans` in `src/firecube/ingestor/runtime/recording.py`), recorded, not scheduled; maintenance recorded `complete` despite collected errors (`_record_maintenance_completed`, called at the end of `execute_deletion` and of `delete_spans` in `deletion.py`), recorded, not scheduled.
    - Identity encoded in strings: obstore `_absolute_key`, span keys and the undecoded pod group, see above; an array named `<coord>.consolidating` deleted by consolidation (`_resolve_time_dim_for_group` and `_consolidate_group_time_coord` in `src/firecube/core/zarr/coord_consolidation.py`), fsspec folding `a//b` into `a/b` on S3 (no symbol located), a serial run id shaped like a pod id getting an invented range (`parse_pod_run_id_slot` in `repo_utils.py` is the parser; the place that invents the range was not located), groups `g` and `g/` sharing a path but not a claim (no symbol located), run id `outer/inner` invisible to `list_runs` (`ManifestProjection.list_runs` in `_projection.py`), the delete `--pattern` glob not escaping `.` (`ManifestRepository._matches_pattern` in `core/controlplane/repo.py`), credential fingerprints colliding on `|` (`Credentials.fingerprint` in `core/credentials.py`), an ingest run taking the reserved id `time-coord-consolidation` (`TIME_COORD_CONSOLIDATION_RUN_ID` in `types.py`), validation splitting at the first `/c/` of the absolute path (`_extract_indices_from_key` in `core/zarr/validation.py`), and catalog names colliding (`g/a` against `g_a`; `default_catalog_source_name` and `_normalize_source_suffix` in `core/intake.py`), each recorded, not scheduled; WAL segment ordering past 99999, see above.
- **Two test failures found while doing the CLI-boundary work (found 2026-10-18; open, not fixed). The first fails on the clean branch; the second is a load-sensitive candidate with no clean-base reproduction.**
    - `tests/integration/test_slots_auto_input_data.py::test_slots_auto_uses_input_data` fails alone on the clean branch with `JSONDecodeError: Extra data` (ran). The test parses `CliRunner` `result.output`, which mixes stderr, and `init_observability` binds the log handler to the first runner's stderr once per process (`src/firecube/core/observability/logging.py:112`; read). Under `pytest -n auto` it passes or fails with the scheduling.
    - `tests/integration/test_concurrent_preallocate_race.py::test_two_concurrent_preallocates_with_slot_group_env_share_a_single_claim_and_never_overlap` failed once under machine load with `ControlPlaneCorruptionError` on a `.firecube-atomic-*.tmp` claim file and passed twice alone on the same tree (ran). Cause not examined.

---

### Open after the branch data-loss fixes (2026-10-18)

Internal contributor material. These are residuals the branch data-loss plan left on purpose or found on the way. Each names its source. None is scheduled.

- **A `.tgm` message that fails to decode after validation leaves a partial target (accepted residual, maintainer ruling, 2026-10-18, not run).** `tgm_to_zarr` in `src/firecube/core/tensogram/restore.py` checks the message count, the version of message 0 and the structure before it deletes. It then replaces the target in place. A message that passes those checks and still fails to decode leaves the target half written. The CLI records `failed` for the restore run on a best-effort basis, so the loss is usually visible. Closing it needs stage-and-swap, which the maintainer ruled out.
- **What a claim refusal records into the target (recorded, not changed, 2026-10-18, read, not run).** `_record_failure` in `restore_archive` (`src/firecube/ingestor/runtime/archive.py`) first writes a `started` record for the restore run. It then writes a `failed` one. Both writes are best effort. A restore that another operation's claim refuses therefore attempts to leave a started and a failed run under the target's `.firecube/runs/`. Whether a refusal should record anything is undecided.
- **A restore holds no product barrier against other writer kinds (found by the storage-race review, 2026-10-18, inferred, not reproduced).** Archive admission lists claims and then takes a maintenance claim on its own domain. An ingest takes slot, schema and append claims on other domains. It can start after the restore passed admission and change files under the copy. One admission barrier shared by every writer and maintenance operation would close this.
- **No test for a staged slot-window run on an unmarked coordinate (2026-10-18).** Such a run is legitimate and writes the right values (a probe ran). The rule that publishes an unmarked coordinate's staged PUT depends on it. Four existing staged tests fail under a mutant that skips every coordinate chunk. They are two tests in `test_cf_time_dim_value_dedup.py`, `test_staged_mode_template_parity.py::test_staged_mode_preserves_cumulative_state[direct_zarr]` and `::test_value_dedup_reingest[value_dedup_staged]`. No test names the slot-window case. This is test-writing work.
- **Restore writes control-plane records through a symlinked `.firecube/` before the symlink refusal (2026-10-18, read).** `restore_archive` in `src/firecube/ingestor/runtime/archive.py` writes the claim and the `started` run first. Only afterwards does `_clear_target` (in `zip_archive.py` and `tensogram/restore.py`) refuse a symlink on a kept path. A symlinked `.firecube/` therefore receives those two writes, although nothing is deleted. The behaviour predates the plan.
- **Four restore refusals have no persistent test (2026-10-18).** Review probes showed them. No test pins them. The first is the symlink refusal: a symlinked target or kept path raises `ConfigurationError` before any delete. The second is the remote `.tgm` root-group replacement, where a remote target keeps mode `"w"`. The third is the tensogram structural-check refusal. The fourth is the relative-target refusal of `tgm_to_zarr`, where `parse_target` runs before any delete. This is test-writing work.
- **A comment in `staged_metadata.py` overstates (2026-10-18).** The comment in `_seed_root_metadata` (`src/firecube/ingestor/runtime/zarr/staged_metadata.py`) says a root "the writer created empty differs". An empty-attrs target root can be byte-identical. The seed then correctly leaves it alone. The fix is one line of wording. Nobody has made it.
- **The test budget missed by 25 lines (2026-10-18, the maintainer must rule).** The test budget check against base `300a3cc` counts +275 physical test lines against a budget of 250. Each change met its allocation in non-blank code. The tool also counts blank lines and the helper extension. The plan neither trimmed nor loosened the budget.
- **The perf gate's wall term was not applied at the 200-slot fixture (maintainer ruling, 2026-10-18).** The W3 median wall of the final tree was 1.269 s against an original control at 1.070 s (limit 1.124 s). An interleaved rerun gave control 1.269 s and 1.218 s, and final 1.218 s and 1.469 s, with one 3.2 s outlier. Rerun control-1 is 1.186 times the original control. Final-1 is below control-1. The two rerun control medians differ by 1.042 times. The maintainer accepted the counted events as the gate. Each one is at most the control's, after the allowance of one extra `open` and one extra `open@root` in W3. A wall regression of a few percent is not detectable at this fixture size.

---

### Open after the archive lifecycle move (2026-10-19)

Internal contributor material. These are residuals the archive-lifecycle plan left on purpose or found on the way. Each names its source. None is scheduled.

- **`create_archive` closes the control plane without a guard (read).** In `src/firecube/ingestor/runtime/archive.py` the `finally` of `create_archive` releases the claim and then calls `manager.close()` bare, so a failing close replaces the archive error or the record error that is pending. `restore_archive` guards the same call and warns. Kept as the command had it.
- **The index docs still say `rebuild` recovers a corrupted or foreign-format record (2026-10-19, read, a docs fix).** `docs/operations/firecube-index.md` promises a recovery that the code refuses. Lines 113-114 of the `rebuild` section say "Use this command when the record is missing, corrupted, or was written by an older Firecube version that used a different format". The table row near line 152 says "Recover a missing or corrupted index record, with no root attrs hash". `ChunkManager._apply_resolved_index_precedence` (`src/firecube/core/controlplane/manager.py:1341`) and `ResolvedIndexRecord.from_json_bytes` (`src/firecube/core/controlplane/types.py:665-712`) reject an unreadable record or a hash mismatch. The review of the prose change found it.
- **Two `ValueError`s of the archive library have no test (2026-10-19, read).** `create_archive` and `restore_archive` raise `ValueError` for an unknown `archive_format`, and `create_archive` for a zip archive given `group`, `start_date`, `end_date` or `variables`. The commands cannot reach either. This is test-writing work.

---

### Open after the consolidated-metadata sweep (2026-10-19)

Internal contributor material. These are the gaps the sweep (`plans/DONE.md`, 2026-10-19) left on purpose or found on the way. Each is recorded, not scheduled.

- **The converter fallback still trusts a group's own snapshot (recorded, not scheduled).** `_open_group_dataset` in `src/firecube/core/tensogram/converter.py`, after a `KeyError` naming `dimension_names`, walks group objects through `_group_from_root`. Its root now reads live through `ZarrIO.open_group`, but a group that carries its own consolidated snapshot is still trusted there.
- **Opens that already passed `use_consolidated=False` before the sweep were not audited for descent (recorded, not scheduled).** Examples are `region_writer.py` and `deletion.py`. Each may still descend through a group object that carries its own snapshot. The snapshot-leftovers plan (`plans/DONE.md`, 2026-10-19) fixed `_compute_touched_data_chunks` and `verify_post_write_integrity`, which now read their arrays by full path from the live root. The loss they caused, neighbouring target cells replaced by fill values on a staged resume, is recorded in the `plans/DONE.md` entry "Zarr and xarray opens that decide read live metadata" (2026-10-19). Reads that still go through a group object: in `append_services.py`, `_read_state_array`, `read_indexed_append_coordinate`, `classify_dataset` and `compute_state_aware_skip_set`; in `write.py`, `_preflight_compare_static_vars`, `_snapshot_group_attrs`, `_restore_group_attrs` and the `r+` group handle of `write_dataset_to_zarr`.
- **`group_exists` ignores `group` in its fallback and returns True for a missing group (recorded, not scheduled; ran in the sweep survey).** In `src/firecube/core/zarr/validation.py`.
- **`_read_existing_append_values` has no caller in `src` (recorded, not scheduled).** In `src/firecube/ingestor/runtime/zarr/append.py`. Its open goes through the live helper rule, so it stays as it is.
- **`validate_zip` does not check that a snapshot agrees with the documents (recorded, not scheduled).** A stale but well-formed snapshot still passes. The sweep makes it report a child document that does not parse, at any depth.
- **`zarr_consolidate` is documented only by a sample line (recorded, not scheduled, a docs fix).** `docs/reference/config.md:40`.
- **`tests/integration/test_cli_input_data_presence.py::test_override_discovery_no_input_data_succeeds` fails in the multi-file recording (recorded, not fixed; ran at `cfcbe27` and at `573748a`).** The plugin-registry reload in `tests/integration/test_cli_zarr_preallocate_slot_index.py` leaves the registry with a stale class.

---

### Open after the snapshot leftovers (2026-10-19)

Internal contributor material. These are the gaps the snapshot-leftovers plan (`plans/DONE.md`, 2026-10-19) left on purpose or found on the way. Each is recorded, not scheduled.

- **A remote `.tgm` restore ignores `--overwrite` and clears nothing (recorded, not scheduled; read).** `_prepare_target` in `src/firecube/core/tensogram/restore.py` returns early for a remote target, so the flag has no effect there.
- **A remote restore of a root-group `.tgm` writes with mode `"w"`, which deletes the target prefix, `.firecube/` included (recorded, not scheduled; simulated on a local path, not run on S3).** In the simulation the restore then failed on its own WAL segment.
- **`from zarr.api import synchronous as zs` is still not flagged by the live-metadata scanner (recorded, not scheduled; optional note from the review of the scanner node).** `test_zarr_metadata_opens_read_live_metadata` in `tests/unit/test_no_raw_fsspec_usage.py` resolves the dotted, aliased, name-imported and bare `import zarr.api.<module>` spellings of `zarr.api.synchronous` and `zarr.api.asynchronous`, but not this one. Closing it cost 9 lines against 2 allowed in the plan.
- **`build_multires_layers` over S3 with obstore fails in Dask tokenization before the snapshot refresh (recorded, not scheduled; found in the review of the item 2 fix, inferred to predate it, not run on S3).** The failure is a `PanicException`.
- **The restore snapshot test covers one group only (recorded, not scheduled).** `test_remote_grouped_restore_refreshes_existing_root_snapshot` in `tests/integration/test_tensogram_archive.py` restores one group (`--group g1`), because a multi-group archive made its output order vary before the name-order fix. A refresh after several groups is untested. A `g3` group can be added now that new archives list groups in name order.
- **The live-metadata scanner misses a literal `getattr` and an assigned callable (recorded, not scheduled; independent review of the snapshot leftovers, inferred, not a production caller).** `getattr(zarr, "open_group")(p)` and `op = zarr.open_group` followed by `op(p)` returned no hit in `test_zarr_metadata_opens_read_live_metadata` (`tests/unit/test_no_raw_fsspec_usage.py`) while opening a stale shape. The repair is to resolve both spellings or to reject indirect open forms. It adds to the `from zarr.api import synchronous as zs` entry above.

---

### Open after structured errors (2026-10-20)

Internal contributor material. These are the gaps the structured-errors plan (`plans/DONE.md`, 2026-10-20) left on purpose or found on the way. Each is recorded, not scheduled.

- **Eight help and scaffold hints under `cli/` do not run (recorded, not scheduled, maintainer ruling, 2026-10-19).** Each hint was read in the tree at the commit of the plan, against the inventory of the 2026-10-18 hints review. The brief for the change names the group but not its rows. So this list is the one the inventory yields.
    - `src/firecube/cli/advise.py:170` and `:174`: `compliance` examples pass `-p /path/to/cube.zarr` and `-p ./cube.zarr`; `--product` takes a strict product URI, so both exit 2 ("URI scheme required").
    - `src/firecube/cli/catalog.py:63` and `:67`: `intake` examples pass `-o <path>/catalog.yaml`; `--output` takes a strict artifact URI.
    - `src/firecube/cli/plugins/commands.py:279` and `:284`: `create_plugin` prints `cd {project_path}` and `firecube plugins install --editable {project_path}` with the path unquoted, so a path with a space splits into two arguments.
    - `src/firecube/cli/archive/_restore.py:61`: the `--overwrite` example omits `--yes-i-really-mean-it`, so it exits 2 without a terminal.
    - `src/firecube/cli/_shared_options.py:246`: the shared `--dry-run` help promises no changes in every command, but `chunks claims clear` refuses the flag without `--all-stale` (`src/firecube/cli/chunks/_claims.py:185`).
- **Some refusals reach the operator only as a traceback (text change 17 of the structured-errors entry in `plans/DONE.md`, recorded, not scheduled, read).** Their last line is now the library sentence, with no flags in it. Three classes:
    - (a) Uncaught `ValueError`s on the CLI path, where an operator sees a traceback: `ZarrTemplateConfig.from_options` in `src/firecube/ingestor/templates/config.py:274` (`zarr_multi_res`) and `:279` (unknown template options); `src/firecube/ingestor/runtime/configure.py:120` (unknown options) and `:243` (the obstore driver with a storage type other than local or s3).
    - (b) An uncaught `ManifestError` (a `FirecubeError`, not a `ValueError`): the one `manager.execute_deletion` raises under `firecube chunks delete` (`src/firecube/cli/chunks/_delete.py:275`).
    - (c) Library-only `ValueError`s, with no operator traceback: the slot-range checks in `src/firecube/ingestor/config/engine.py:190-233` for library callers, and `StorageConfig.validate` in `src/firecube/core/config.py:95` when called directly (the CLI adapter at `src/firecube/cli/_ctx.py:68` catches it and exits 1).
    - The fix for (a) and (b) is to raise caught `ConfigurationError` subclasses and map them at the boundary.
- **Five exception-text helpers do the same job (recorded, not scheduled; final review).** `exception_text` in `src/firecube/core/zarr/guard/engine_owned.py:62`, `_error_text` in `src/firecube/core/controlplane/deletion.py:196` and `src/firecube/ingestor/runtime/archive.py:136`, `_text_keeping_message` in `src/firecube/ingestor/runtime/index_record.py:102` and `_cause_text` in `src/firecube/ingestor/runtime/slot_plan.py:153` each return a `Message` when the exception carries one, else its text. Merge them beside `Message` in `src/firecube/core/errors.py`. `_cause_text` requires a `FirecubeError`; the others take any exception.
- **Two refusals of the resolved-index record still flatten or escape (recorded, not scheduled; review of the index node).**
    - The loser branch of `ensure_resolved_index` in `src/firecube/core/controlplane/manager.py:1384` raises a plain `ManifestError` for attrs drift, not `ResolvedIndexAttrsConflictError`; `zarr index rebuild` catches only the subclass, so under a held claim it still prints a traceback.
    - `verify_index_record` in `src/firecube/ingestor/runtime/index_record.py:338` builds the unreadable-record detail with `str(exc)`, the shape that flattened a `Message` in the other two verdicts.
- **The batch summary does not trim some trailing parts (recorded, not scheduled; review of the engine node).** `Message.without_trailing_stop()` in `src/firecube/core/errors.py` (moved unchanged from `src/firecube/ingestor/runtime/engine.py` on 2026-10-20) trims a trailing full stop or space from plain text only. A final `Param`, `PluginOption`, `Operation` or `CommandName` whose shown text ends that way, and a nested `Message` under a format spec, keep it. No site ends that way today. The repair is a change to that method.
- **The `product_uri` fact of two refusals holds the object's repr (recorded, not scheduled).** `src/firecube/core/duckdb/bridge.py:61` and `src/firecube/core/tensogram/metadata.py:199` pass `str(...)` of a `StorageUri`; use `.to_str()`.
- **The `multires` advice keeps text the command does not accept (recorded, not scheduled).** `ZARR_MULTI_RES_REMOVED_MESSAGE` in `src/firecube/ingestor/templates/config.py:35` uses `CommandName(("zarr", "multires"), library="ZarrMultiresBuilder.build")` so the CLI text stayed byte-identical, and that advice names a positional `<target>` the command lacks. `Operation("multires")` would print `--target <target> --product-name <product_name>`; switch to it as a listed text change. The `multires` key was deleted from `OPERATION_FUNCTIONS` and `OPERATIONS` on 2026-10-20 because nothing raised it. The switch must restore both entries and the `multires` sample in `tests/cli/test_click_error_boundary.py`.
- **Some CLI sites print `str(exc)` of a source that is not a `Message` (recorded, not scheduled).** `src/firecube/cli/_uri_policy.py:99` and `:109`, `src/firecube/cli/advise.py:126`, `src/firecube/cli/_command_schemas.py:85`, `src/firecube/cli/plugins/mgmt.py:150` and `src/firecube/cli/plugins/registry.py:118`. None of their sources is converted, so nothing is lost today. A source converted later needs `render` at the site.
- **The boundary gate does not scan module constants or `warn` arguments (recorded, not scheduled, read).** `tests/architecture/test_cli_boundary.py` checks the strings that raise, log or warn. Seven locations escaped it (the brief counted nine strings): `bridge.py:27`, `core/formats/_input_filters.py:22`, `engine_owned.py:56-57`, `templates/config.py:34`, `slot_plan.py:248`, `engine.py:95-96` and `ingestor/errors.py:137`. The change converted them by hand, and nothing stops a new one.
- **The generated plugin scaffolding names CLI commands (recorded, not scheduled; read, 57 strings).** `git grep -c 'firecube [a-z]\|--[a-z-]' -- src/firecube/ingestor/devtools` counts 57 matches across `scaffolding.py` (30) and the templates under `_templates/` (27). They are output for a plugin author, not a refusal, and they sit outside the gate.
- **16 of the 17 exceptions that carry fields fail a pickle round trip (recorded, not scheduled, not rerun here).** A survey script run at `573748a` found it. That survey found 16 of 17 field-carrying exceptions failing. `AppendBatchFailed` and `PipelineFailedBatchesError` carry fields now, and both also fail: `pickle.loads(pickle.dumps(x))` and `copy.copy(x)` on constructed instances raise `TypeError` for missing constructor arguments (ran at `9576ad3`). Nothing pickles a `PipelineResult` (`pipeline_workers` is a thread pool, `engine.py:598`), so no caller is hurt today. A process pool would need a `__reduce__` on each. `ConfigFileError` and `RemovedConfigKeyError` did not raise `TypeError`: they came back with a wrong sentence. Both now rebuild from their constructor facts (2026-10-20). The other classes stay as recorded.
- **The types of a refusal's message are not exported from a facade (recorded, not scheduled).** `Message`, `Param`, `PluginOption`, `Operation`, `CommandName`, `Placeholder` and `RefusalContext` live in `firecube.core.errors` and are in no `__all__`. A plugin author cannot raise a refusal that carries facts without a deep import.
- **`RangeOverlapError` prints a command in its docstring (recorded, not scheduled, read).** `src/firecube/ingestor/errors.py:103` says "Abandon the conflicting run first: firecube chunks runs abandon ...", and `docs/reference/exceptions.md:50` renders it. Name the operation without the shell text. Update the page after the docstring changes.
- **Carried from the plan (recorded, not scheduled).**
    - `tests/integration/test_cli_input_data_presence.py::test_override_discovery_no_input_data_succeeds` fails after `tests/integration/test_phase3_single_group_preserved.py` in one run; it also failed at `573748a`. It is the order-dependent failure of the entry above, with a second trigger.
    - `tests/integration/test_preallocate_slot_group_env.py::test_env_unbounded_slot_group_raises_usage_error` never reaches its "no resolvable extent" branch (x1 review).
    - `tests/integration/test_maintenance_claims.py:454` sleeps a fixed 2.5 s.

---

### Open after STAC source discovery (2026-10-26)

Internal contributor material. STAC source discovery (#97, `plans/DONE.md`, 2026-10-26) left these out of scope on purpose. Each is recorded, not scheduled.

- **Reading `https://` asset hrefs (out of scope, recorded).** The STAC backend hands each data-asset href to the plugin as the API publishes it, often `https://`. Firecube opens nothing over https: `open_source_filesystem` and the materializer serve `file://` and `s3://`. A plugin that reads such an href opens it itself. Signing URLs for APIs that need it is part of the same work.
- **Static `catalog.json` catalogues (out of scope, recorded).** `stac://` names one collection of a STAC API with item search. A static catalogue has no search endpoint; walking its links is a separate backend.
- **Zarr prefixes as source (out of scope, recorded).** An asset whose href is a Zarr store is handed through as an href, nothing more. Discovery does not open it, list it or treat its groups as granules.
- **Multi-collection joins (out of scope, recorded).** One `stac://` URI names one collection, and several sources are deduplicated and sorted, never joined. A plugin that pairs items across collections overrides `discover_source_files` and calls `discover` once per collection.
- **Item identity reaches the engine only for `SOURCE_ITEMS` plugins (recorded, not scheduled, read).** `SourceItem.identity` is `collection/item_id/asset_key` for a STAC asset. A plugin without `SOURCE_ITEMS` receives href strings, so `files_hash` (`src/firecube/ingestor/runtime/batching.py`) and the AUTO-axis item manifest (`_item_identity_hash` in `src/firecube/ingestor/runtime/index_binding.py`) hash the href. No resume path reads `files_hash` today; `ResumeGuard` decides on WAL state and `slice_meta_keys`. Item-keyed resume is its own change.
- **A walk href and a STAC asset with the same href are not deduplicated (recorded, not scheduled, read).** Their identities differ (href vs `collection/item_id/asset_key`), so a plugin without `SOURCE_ITEMS` can receive the same href twice when a file-tree source and a STAC source both reach it.
- **`cli_test_plugin` cannot complete a run against a `file://` target (recorded, not scheduled, ran).** Its `_process_batch` returns `OutputPaths(primary=Path(ctx.target))`, which turns `file:///x` into `file:/x`; the run then fails with `ValueError: Malformed URI`. Reproduced on clean HEAD (14e7f94) with `firecube ingest cli_test_plugin --input-data <dir> --target file:///<dir>/out.zarr ...`. `tests/integration/test_ingest_stac_options.py` patches `_process_batch`.

### Open after the CLI URI rule (2026-10-24)

Internal contributor material. The CLI URI rule (`plans/DONE.md`, 2026-10-24) left these gaps on purpose. Each is recorded, not scheduled.

- **A `file://` path is literal everywhere, and RFC 8089 does not read it so (recorded, not scheduled, 2026-10-24).** `file:///tmp/a%20b` names a directory `a%20b`. RFC 8089 and Python's `url2pathname` read it as `a b`. A move to decoding needs three parts. `StorageUri.to_str` must encode. The local adapters must decode once. A rule must cover the locations that are already persisted.
- **Core keeps URI helpers that accept bare paths for library callers (recorded, not scheduled, read).** The helpers are `parse_uri`, `local_path_from_target`, `storage_uri_from_target` and `parse_target` in `src/firecube/core/uris.py`. Three more sit beside them: `ProductTarget.resolve` in `src/firecube/core/product/target.py`, `resolve_dataset_target` in `src/firecube/core/product/resolver.py` and `_storage_uri_from_raw` in `src/firecube/ingestor/runtime/base.py`.
    - `local_path_from_target` and `storage_uri_from_target` match a lowercase `file://` only (`src/firecube/core/uris.py:137` and `:149`). A library caller that passes `FILE://` gets a path under the working directory.
    - Ran: `local_path_from_target("FILE:///tmp/x")` returns `<cwd>/FILE:/tmp/x`.
    - Merging these helpers onto `StorageUri` is its own change.
- **`--input-data` is exempt from the CLI rule, so the core helpers apply to it (recorded, not scheduled, inferred).** The `ingest` function of `src/firecube/cli/main.py` passes it to `is_remote_target` and `local_path_from_target` (`src/firecube/cli/main.py:761-770`). The previous entry therefore covers it. The gate test names both calls as `PERMANENT` rows.
- **`StorageUri.parse` keeps userinfo and a port of an `s3://` authority in the bucket name (recorded, not scheduled, inferred).** The authority is the parsed network location (`src/firecube/core/storage/uri.py:114`). Bucket-name validation is a separate policy.
- **The CLI rule does not check the `[storage]` config of a config file (recorded, not scheduled, read, and the example values inferred).** `_storage_uri_from_base` in `src/firecube/cli/chunks/_manager.py` refuses a base URI without `://` and passes any other value to `StorageUri.parse`. `derive_target_uri` in `src/firecube/core/config.py` returns `target_path` unchanged. A config value such as `FILE:///data` or `file://localhost/data` passes the first check.
- **The coherence refusal says "change --target" for every option (recorded, not scheduled, read).** `validate_uri_storage_coherence` in `src/firecube/cli/_uri_policy.py` ends its sentence with `change --target to ...`. The sentence is the same for `--product`, `--source` and `A_URI`. Pinned tests hold the text, so a fix needs a new text and a change to those tests.

---

### GenericZarr on the guarded write layer (2026-10-14)

**Status**: FUTURE; follows the DirectZarr write session. `guarded_write`, `metadata_authority` and `promotion_plan` exist in `src/firecube/core/zarr/`, and DirectZarr uses them; `GenericZarrIngestor` uses none of the three.

GenericZarr writes through xarray (`to_zarr`) in `write_dataset_to_zarr`, with a per-batch `zarr_append` claim and the ordered gate for in-process order. Zarr leaves overlapping writers to the caller, so nothing below the claim protects a stored unit or a `zarr.json`. Four parts, in this order:

- **`DatasetRegionAdapter`.** Turn each xarray region write of `write_dataset_to_zarr` into footprints (array path plus index box in stored geometry) and send one task per stored unit (shard if sharded, else chunk) through `guarded_write`. The Dask race inside one unit is already closed: with `align_chunks=True` and `xarray>=2026.2.0` (which aligns Dask tasks to shards), a region write whose inner chunks share a shard keeps every value (run on xarray 2026.2.0 and zarr 3.2.1: `[11, 12, 13, 14]`). What the adapter adds is what xarray does not give: the chokepoint's unit locks against other writers of the same unit, absent unit reporting for region writes, and bounded concurrency. Done when region writes go through `guarded_write` and report absent units, with a test that a region write whose inner chunks share a shard keeps every value; that test passes on the current path and is kept as regression coverage, not as a fail-first test.
- **Bounded region mode (opt-in).** A preallocated extent and a slot mapping the plugin declares; disjoint units are then written concurrently, each under its unit lock. Today two concurrent appends grow the shape and one writer's tail is lost silently, so the ordered gate stays the default. Tail growth stays serial: a plugin that does not declare the mapping keeps the append path.
- **Generic staged delta.** Promote a GenericZarr staged run through `promotion_plan` and `publish_plan` instead of `upload_tree`, with the units the run proved written. This also ends the staged all-fill retention: a staged run that rewrites a whole unit to the fill value deletes the object in the workspace (`write_empty_chunks=False`) and promotion by `upload_tree` only copies, so the target keeps the old values. The engine already takes the delta path for a host that reports `_staged_promotion_units`; GenericZarr does not report them. The smaller repair (staged workspace writes keep empty chunks) is not a substitute for the delta.
- **`AppendCoordinator`.** Allocation of the append tail, the cursor and batch order stay serial and move behind one object (`AppendResumeService`'s cursor, `AppendOrder` and `OrderedWriteGate` become its parts, with the per-batch `zarr_append` claim) so the adapter and the bounded region mode share one source of the next slot.

Fold into these the GenericZarr metadata sites that bypass the metadata authority (`metadata_authority`: `zarr.json` changes only under a held authority; its five editing operations `create_or_open_array`, `grow_array`, `set_attributes`, `set_attributes_if_absent` and `update_document` re-read the node from the store under the per-node lock; `grow_array` never makes an array smaller; `update_document` applies a caller-supplied merge under that lock and does not restrict the result, so a caller can shrink; `publish_document` replaces the document it is given and `publish_document_if_absent` writes only where none exists, and neither re-reads):
- Creation and append `to_zarr`, in `write_dataset_to_zarr` of `src/firecube/ingestor/runtime/zarr/write.py`. xarray writes `zarr.json` itself there, so these need a seam, or the authority is held around them. The region `to_zarr` of the same function (`mode="r+"`, no append dimension, `consolidated=False`) writes only chunk payload: a traced region write put a data chunk and no `zarr.json` on xarray 2025.10.1 and 2026.7.0. It belongs to the adapter above, not to this list.
- `_consolidate_metadata_best_effort` in the same file, a separate metadata writer (it rewrites consolidated metadata).
- `_restore_group_attrs` in the same file, which rewrites the first-write group attributes after an append (`attrs.put`).
- State-array creation: `ensure_timestamp_state_array` in `src/firecube/core/zarr/state.py`, reached from `ensure_existing` in `append_services.py`; it creates the array and then writes its attributes in a second metadata write.
- The shrink in `_truncate_group` of `append_failure.py` (`repair_failed_group` resizes every append-dimension array back to the batch start; a fresh group is deleted). The authority has no shrink or delete operation, because DirectZarr never needs one; this one needs a decision on whether the authority gains it.
- `root.attrs["receipts"]` in the `write_lock` docstring example of `GenericZarrIngestor` (`src/firecube/ingestor/templates/generic.py`): a plugin hook writes root attributes from the main thread, outside any authority. Only the example is in the repository; plugin code that follows it is not covered.

---

### §35 Repo-wide cross-package private-import hardening

**Status**: OPEN
**Scope**: architecture invariant, static test
**Priority**: medium

### Motivation

Post-implementation review during PR #40 preparation found that `test_import_boundaries.py` protects the templates/plugins boundary but not the CLI, and that no test enforces "no cross-package underscore imports" anywhere in the tree. RF-12 (`test_cli_no_private_cross_subsystem_imports.py`, added by PR #40) closes the CLI-specific gap, but the underlying repo-wide invariant is still missing. AST scan of `src/` at PR #40 review time found several cross-subsystem private imports that should be triaged individually:

- `src/firecube/ingestor/runtime/telemetry.py:22` — `from firecube.core.observability.metrics import _to_number, normalize_run_summary`
- `src/firecube/ingestor/runtime/tensogram/strategy.py:28` — `from firecube.core.tensogram.converter import _find_time_dim`
- `src/firecube/core/zarr/validation.py` (the `_open_fsspec_url` import) — `from firecube.core.filesystem.ops import _open_fsspec_url  # type: ignore` (already an accepted deviation per DESIGN.md fsspec allowlist)
- `src/firecube/core/tensogram/converter.py:31` — `from firecube.core.filesystem.ops import _open_fsspec_url` (same — accepted deviation)
- Possibly others surfacing when the invariant is added

### Proposed approach

1. AST-scan `src/firecube/**/*.py` for `ImportFrom` nodes with `name` starting with a single underscore and `module` in a different first-level subpackage than the importing file (e.g., `firecube.ingestor.X` importing from `firecube.core.Y` is cross-subsystem; `firecube.core.zarr.X` importing from `firecube.core.zarr._Y` is same-subsystem).
2. Build a `_PERMANENT_ALLOWLIST` seeded from the AST scan, with an explicit rationale per allowed entry (mirror the `test_no_raw_fsspec_usage.py` pattern).
3. For each currently-allowlisted entry, decide: **promote** (rename to public), **accept** (document in DESIGN.md §"Accepted Deviations"), or **defer** (leave in allowlist with a TODO reference).
4. Reject any NEW cross-subsystem private import that is not in the allowlist.

### Not this branch

PR #40 closes the CLI-specific gap only. The repo-wide invariant, allowlist seeding, and individual triage are follow-up work. The CLI boundary test planned alongside this section has landed (2026-10-18): `tests/architecture/test_cli_boundary.py` forbids Click and `firecube.cli` imports below `cli/`, CLI text in `core/` and `ingestor/` strings, private `core` and `ingestor` modules imported by `cli/`, and `cli/` modules over 500 lines. It covers private modules, not private names; the repo-wide private-name invariant above stays open.

### References

- `tests/architecture/test_cli_no_private_cross_subsystem_imports.py` — RF-12, the CLI-scoped precursor
- `tests/architecture/test_import_boundaries.py` — the existing templates/plugins invariant to model after
- `tests/unit/test_no_raw_fsspec_usage.py` — the canonical `_PERMANENT_ALLOWLIST` pattern

### §29 Test suite overhaul

**Goal:** Reduce noisy static/docs failures and raise behavioral bug-detection
power in the default test loop.

**Current state:**
- The default test loop must prioritize runtime behavior over static prose,
  broad snapshots, and implementation-shape checks.
- Test quality rules live in [TESTING_STANDARDS.md](TESTING_STANDARDS.md).
- High-risk missing behavior coverage lives in [TEST_GAPS.md](TEST_GAPS.md).
- Generic tests must use neutral sample product/plugin names unless the test is
  explicitly about an external plugin migration path.

**Direction:**
1. Keep the fast lane focused on behavior and failure modes.
2. Keep docs/static/help checks narrow, semantic, and outside the default
   behavioral signal when practical.
3. Replace mock-heavy or assertion-light tests with real behavior tests around
   deletion, workspace cleanup, storage operations, plugin management, and
   source readers.
4. Prioritize the P0/P1 gaps in [TEST_GAPS.md](TEST_GAPS.md).

**Acceptance criteria:**
- Default development and CI lanes are explicit and documented.
- A one-line help/prose diff does not fail the fast behavioral lane.
- Strict collection has no unexpected dependency skips.
- Every remaining xfail names an accepted TODO item and removal condition.
- The highest-risk control-plane and deletion bugs are covered by behavior
  tests, not only static checks.

---

### §8 Tensogram ingest routing

**Goal:** Clarify whether `GenericTensogramIngestor` is a public plugin base class, an internal write-path adapter, or both.

**Current state:**
- `GenericTensogramIngestor` exists in `src/firecube/ingestor/templates/generic_tensogram.py` and is exported from `firecube.ingestor.api`.
- `firecube ingest --output-format tensogram` currently accepts only plugins based on `GenericZarrIngestor`, then dynamically borrows `GenericTensogramIngestor._process_batch`.
- A plugin that directly subclasses `GenericTensogramIngestor` appears likely to be rejected by the current CLI guard because it is not a `GenericZarrIngestor` subclass.
- `GenericTensogramIngestor` currently supports only local `.tgm` targets.

**Decision (2026-06-11):** Tensogram is BOTH the archive format and a proper public output format, with the same first-class status as `GenericZarrIngestor`. `GenericTensogramIngestor` is a public plugin base class: direct subclassing must be supported and CLI-routable, the runtime `type()` class synthesis in `cli/main.py` must be replaced with explicit strategy/capability routing, and the local-only target restriction must be lifted via staged upload (reusing the `archive create` temp-file + transfer pattern). See `plans/AUDIT.md` (C4).

**Phase 0 — DONE (2026-06-11):** tensogram dependency upgraded `0.17.0` → `0.21.0` (all three packages, pins `>=0.21.0,<0.22`). See DONE.md 2026-06-11 for details, including the legacy-archive validate fix and the checked-in 0.17 compat fixture.

**Verified broken today (evidence for Phase 1, confirmed identical on tensogram 0.17 and 0.21 — pre-existing, not upgrade regressions):**
- `firecube ingest <plugin> --output-format tensogram --target file://.../out.tgm` fails with `[Errno 21] Is a directory`: the control-plane root (`.firecube/`) is created *inside* the `.tgm` target path before the strategy writes, turning the target into a directory. The synthesized-class path inherits zarr-oriented target preparation that is wrong for a single-file format.
- Archive restore loses time-coordinate encoding: CF `units`/`calendar` live in xarray `.encoding` and are never serialized by the converter, so restored cubes get a raw float64 ns-epoch time coord that xarray cannot decode back to datetime64. Restored variables also receive the raw base-entry dict (`name`, `zarr_chunks`, `zarr_compressor`, `zarr_fill_value`, nested `attrs`) as variable attrs — metadata pollution. Fix: serialize variable `.encoding` (at least time units/calendar) into the firecube message metadata and reapply on restore; map only real attrs onto restored variables. **Ownership: this is a firecube bug, not a tensogram one** — the metadata frame is free-form (room to store encoding), the engine's base-entry passthrough is fed by keys we put there, and our restore path already post-processes the decoded dataset. No upstream change required; optional upstream nice-to-haves (a variable-attrs convention in tensogram-xarray base entries, a logical datetime64 dtype hint in tensogram descriptors) are tracked in IDEAS.md, not blockers.
  - The same loss makes `archive create --archive-format tensogram` refuse a store with a non-Gregorian time coordinate. `zarr_to_tgm` raises `CalendarArchiveError`, because the archive carries no CF time encoding. The fix above must also lift that refusal.
  - The zip format keeps the encoding. `archive restore` accepts a `.zip` or a `.tgm`.

**Phase 1 — DONE (2026-06-11):** `DatasetProducer` protocol defined in `ingestor/contracts/`; CLI `type()` synthesis removed; direct `GenericTensogramIngestor` subclasses are now CLI-routable via protocol-checked strategy selection. `_GenericBatchIngestor` dissolved; `GenericZarrIngestor` and `GenericTensogramIngestor` re-parented directly onto `BaseIngestor`. Evidence: `tests/unit/test_tensogram_routing.py`, `tests/unit/test_template_hierarchy.py`. See DONE.md 2026-06-11 audit dispositions C3, C4.

**Phase 2 plan (separate feature branch) — OPEN:**
1. Fix single-file target handling: control-plane root must not be created inside a `.tgm` path (product-local control plane next to the file, or workspace-local) — resolves the `Is a directory` failure above.
2. Lift the local-only restriction via staged write: `.tgm` to the run workspace, upload through `StorageSession`/`create_filesystem` (extract the existing `archive create` remote pattern). `--write-mode staged` for remote targets; `direct` stays local-only with a loud error. One-driver invariant respected.
3. Control-plane parity: record spans with `SpanCoverage(write_strategy="tensogram", time_dim_name=...)`. Decide ResumeGuard semantics for an overwrite-only format (recommended: each run replaces prior spans for the same slice, `force_reingest`-like).
4. Restore fidelity: fix the time-encoding loss and attrs pollution described above; adopt `verify_hash=True` on decode/restore when frames carry `HASH_PRESENT` (0.21 feature; legacy frames skip with a note, mirroring the validate behavior shipped in Phase 0).
5. Config hygiene: implement or delete the unused `tensogram_message_granularity` field in `TensogramTemplateConfig`.
6. Public surface: scaffolding template, `tests/fixtures/tensogram_capable_test_plugin` (added to the conftest sessionstart guard), remote staged-upload integration test (moto), golden-help regen.
7. Docs + doctrine: output-format page under `docs/concepts/output-formats/` (via `write-user-doc`/`write-plugin-doc` prompts), DONE.md entry closing §8, AGENTS.md "Where things live" update.

---

### §5 Catalog and standards integration

**Goal:** Build a bridge from Firecube's internal view (Zarr + WAL-backed control-plane records) to higher-level discovery standards.

**Direction:**
- Starting from existing Intake support, explore emitting STAC or OGC EDR-style metadata as optional helpers.
- Keep this layer optional and generic. Firecube should not lock users into a single discovery stack.

---

### §7 Safe parallel ingestion patterns

**Goal:** Parallelize ingestion on Kubeflow/Argo safely without ACID/Icechunk complexity.

**Current state:**
- `AppendStrategy` (used by `GenericZarrIngestor`) serializes writes with a process-local lock. `pipeline_workers>1` parallelizes preprocessing but still writes one batch at a time.
- `IndexedRegionStrategy` (used by `DirectZarrIngestor`) writes pre-planned, disjoint index ranges via `RegionZarrWriter`. No appends. `IndexedRegionStrategy.write_groups()` auto-computes `expected_time_count` from `WriteIntent.ts_index`; undersized timestamped arrays hard-fail instead of resizing at write time. This is the building block for safe parallelism.
- `FilesystemClaimService` (`src/firecube/core/controlplane/claims.py`) provides exclusive write claims with heartbeat-based stale detection.
- T3 (§7-GENERIC) complete: `GenericZarrIngestor` now uses `WriteDomain(category="zarr_append", ...)` — the construction lives in `src/firecube/ingestor/runtime/zarr/batch_runner.py` since the §22 facade thinning, wired from `templates/generic.py`; integration fixtures updated in `tests/integration/test_maintenance_claims.py` and `tests/integration/test_obstore_claim_atomicity.py`. Code commit: `6329217`.
- T2 (§7-DIRECT) complete: `DirectZarrIngestor` now uses `WriteDomain(category="zarr_region", name=f"{group}:schema")` for schema setup and `WriteDomain(category="zarr_region", name=f"{group}:slot={ts_index}")` for per-slot intent dispatch via optional `claim_for_slot`. `IndexedRegionStrategy.write_groups()` groups intents by `ts_index` and uses fallback `claim_for_slot → claim_for_group → nullcontext`.

**Verified evidence (from review)**: `DirectZarrIngestor.claim_for_group()` used to build `WriteDomain(product=product, category="zarr_group", name=str(group_name))` — per-group granularity only. §7-DIRECT changed this to schema and per-slot `zarr_region` claims. The remaining safe-parallelism gap is the planner/orchestrator layer that assigns deterministic, disjoint index ranges before dispatch.

**Recommended safe model today:** Append writes serialize to one writer per `(product, group)`. DirectZarr writes may run concurrently only across pods with pre-planned disjoint slot/group ranges via the `firecube zarr slots` planner. Intra-pod `pipeline_workers > 1` no longer loses slots that share a stored chunk or shard: read-modify-write of one (store, array, unit) is serialised per process (`src/firecube/core/zarr/guard/_unit_locks.py`; `tests/integration/test_chunk_ownership_races.py`). The per-slot claim still has no retry loop, so a same-slot collision between in-flight batches can fail hard (reproduced 2026-07-12, before the unit locks; not re-run since). Cross-process safety is still slot windows over whole stored units. For large per-slot payload plugins (~14.8 GiB/slot measured for the largest so far), scale via `pipeline_workers=1` × N disjoint-range pods; total cluster memory is unchanged but sized per-pod it fits standard nodes.

**§7-sub / Phase 3 planner — DONE (2026-05-28):** See DONE.md (section 7-sub) for full deliverables. Engine/template-level planner with deterministic time-to-index mappings, chunk-aligned ranges, `firecube zarr slots` JSON output, `firecube zarr preallocate` schema preflight, `--slot-start/--slot-end/--slot-size` CLI flags, K8s env discovery, 6-row ResumeGuard conflict matrix, and per-pod `run_id` derivation are all shipped. (These commands originally shipped as `firecube plan` / `firecube zarr setup-schema` and were later renamed.)

    **§7-Phase 3.1 hardening — DONE (2026-05-28):** See DONE.md §7-Phase 3.1 for full deliverables. Closes 5 safety gaps surfaced by external review: strict `global_expected` coverage enforcement, intent-group-in-schema hard fail, all-arrays chunk validation, `--slot-group` CLI flag + env var, group-aware ResumeGuard, slot-range-aware completed-span check, split bypass semantics (`resume_existing` vs `force_reingest`), and `--slot-group` propagation through capability gate + plan output. All 4 Final Verification reviewers approved. Commits: C1=94d167e, C2=4a5212e, C3=ba90260.
    **Phase 3.2 follow-up DONE (2026-05-29):** See DONE.md §7-Phase 3.2 for full deliverables. Closes 6 review issues: run_id slot_group isolation, strict schema validation via SchemaDriftError + verify_array_spec, per-group slot_size in firecube plan, _ParallelExecutionState + ctx._ctx escape removal, pod-startup schema verification + audit record, docs drift cleanup. All 4 Final Verification reviewers approved. Commits: C1=37f7521, C2=80583a2, C3=84bb1bb, C4=6b6f3a1, C5=450f8f7, C6=416a4b6.
    **Phase 3.3 external-review follow-up DONE (2026-05-29):** See DONE.md §7-Phase 3.3 for full deliverables. Closes 6 external-review issues: sequential slot filter parity, terminal partial chunk + plan-to-ingest contract, URL-encoded slot_group in run_id + WAL reader, phantom group prevention in capability gate + pod-startup, operator docs refresh, sharding test assertion strengthening. All 4 Final Verification reviewers approved. Commits: C1=9fce649, C2=545689f, C3=40977ca, C4=8fc455d, C5=c6fd238, C6=269acdf.
    **Phase 3.4 external-review follow-up DONE (2026-05-30):** See DONE.md §7-Phase 3.4 for full deliverables. Closes 3 additional review issues: phantom global_expected validation added to the planner CLI preflights (now `firecube zarr slots` + `firecube zarr preallocate`), warn_if_misaligned terminal-partial awareness, sharding test spy on xr.Dataset.chunk. All 4 Final Verification reviewers approved. Commits: C1=604a4d8, C2=a9fc6e8, C3=adf41dd.
    **Phase 3.5 external-review follow-up DONE (2026-05-30):** See DONE.md §7-Phase 3.5 for full deliverables. Closes 2 additional review issues: firecube plan fail-closed on blocked partial-chunk ranges (silent slot-loss bug fixed), zarr_schema() called exactly once per plan invocation. All 4 Final Verification reviewers approved. Commits: C1=2992425, C2=8fda71a. Phase 3.6 follow-up DONE 2026-05-30 (closes 1 additional review issue). Phase 3.7 follow-up DONE 2026-05-30 (closes 2 additional review issues: CLI error remediation text + operator docs refresh). Phase 3.8 follow-up DONE 2026-05-31 (closes 2 additional review issues: delete-span remediation now includes --product/--force/full safety flow + regression test strengthened to lock runnable shape).

    **Builds on:** §4a (DONE.md) — `IndexedRegionStrategy` is the missing primitive that enables disjoint-region writes. §21, §23, §23-AUTO, §7-DIRECT, §7-sub, and Phase 3.1 are all DONE as of 2026-05-28. Safe within-group parallelism for `DirectZarrIngestor` plugins is delivered for pod-written time-indexed arrays over whole stored units; an unmarked time coordinate and the static-array transaction are still unsafe between processes (see the open items near the top of this file).

---

### §37 `compare_zarr_stores` chunk-wise value comparison (post-2026-08-24)

`src/firecube/core/zarr/validation.py::_values_equal` currently loads whole arrays into memory via `left[:]`. For production-size cubes this will OOM. If `firecube zarr compare` is to be used as a promotion gate on real cubes, replace the full-array load with chunk-wise comparison that iterates over chunk indices and compares one chunk at a time.

**Scope**:
- Modify `_values_equal` to iterate chunks (use `zarr.Array.chunks` and `numpy.ndindex` over chunk coordinates).
- Preserve NaN-aware equality semantics per dtype kind.
- Preserve the terse per-category mismatch message format (do NOT list individual chunks).
- Add a test with a large synthetic array that would OOM with `[:]` but passes with chunk-wise comparison.

**Deferred from**: Milestone C review, 2026-08-25. V1 boolean-only compare is fine for small stores.

---

### §38 `zarr validate` static-marker check: configurable time-dimension names (post-2026-08-25)

`src/firecube/cli/zarr/_validate.py::_static_marker_failures` classifies an array as
time-indexed when its first dimension is named `timestamp`, `time`, or
`firecube_timestamp_state`; any other first-dimension name is treated as
static and must carry the `firecube_static_written` marker. A plugin using a
custom `time_dim_name` outside that set gets false
`missing_or_false_static_marker` reports on genuinely time-indexed arrays.

**Scope**:
- Add a way to extend or override the recognized names (a `--time-dim` option
  on `firecube zarr validate`, or read the dimension name from the resolved
  index record).
- Keep the default set unchanged.

**Deferred from**: Wave C2 review, 2026-08-25. The recognized set is
documented in `docs/concepts/performance.md`.

### §9 Span planning (optional pre-declared spans)

**Goal:** Make spans a stable unit of work for orchestration and maintenance without making ChunkManager product-aware.

**Current state:** Spans are recorded post-hoc from write coverage (`span.time_index_ranges` + `meta.time_min/time_max`).

**Next step:** Optional SDK hook for "span intent":
- Plugins/templates may pre-declare spans (month windows or chunk-aligned index ranges).
- Engine records `status="started"` spans at begin and finalizes to `complete/failed/noop` with actual coverage at end.

**Builds on:** §4a (DONE.md) — Write strategies now produce explicit write intents; pre-declared spans become a natural extension where templates declare spans before executing strategies.

---

### §11 Multires as a post-step — DONE (2026-05-26)

See DONE.md §25 for details.

**Current state (post-§25):** The post-step CLI `firecube zarr multires <target> --storage-type <local|s3> --storage-driver <fsspec|obstore>` exists and wraps `ZarrMultiresBuilder`. Ingest-time multires has been removed: `AppendMultiresHandler` no longer exists (deleted in §25, commit `89b2737`). The `zarr_multi_res` config field is rejected with a `ValueError` pointing to the CLI subcommand.

**Direction:** Use `firecube zarr multires` as a post-step after ingestion completes. No ingest-time multires path remains.

**Builds on:** §4a (DONE.md) — Zarr is now a runtime subsystem with explicit finalize/commit boundaries. §25 completed the removal of the broken ingest-time path.

---

## Harden Implicit Logic and Heuristics

These items identify areas where the system used greedy logic to guess user intentions. The goal is to make these explicit or more robust. Status re-verified against the code on 2026-06-11.

### §12.2 CLI and Execution Engine

- **BASENAME heuristics — DONE:** `output_name` is no longer guessed from the target path basename. `ProductIdentity.from_uri` hard-fails without an explicit product name, and the `default_output_name` config key is rejected at parse time.
- **Magic output detection — DONE:** the CLI and engine read typed `PipelineResult.outputs` / `result.output_path` attributes; no dict key sniffing remains. The legacy `output_path=` constructor kwarg was removed entirely (DONE.md 2026-06-11).
- **ingest `dry_run` — OPEN:** implement or remove the option. `EngineConfig.dry_run` only sets `RuntimeFlags.dry_run`; no write path reads it, so `firecube ingest --option dry_run=true` writes normally. The docstring marks it reserved and points at the maintenance commands' `--dry-run` for previews (2026-09-12).
- **Explicit safety — OPEN:** refusal to upload `HOME` or `/` is hardcoded in the engine's upload-source resolution; should be documented or configurable.
- **Free-form option overload — DONE (2026-06-11):** keys owned by dedicated `firecube ingest` flags (`write_mode`, `slot_start`, `slot_end`, `slot_size`, `slot_group`) are hard-rejected at `--option` parse time with remediation naming the owning flag (`_TYPED_FLAG_OWNED_KEYS` in `cli/_typed_options.py`); the silent post-resolution override is closed. All other typed flags were never config fields and were already rejected as unknown keys. Engine options without a dedicated flag (`force_reingest`, `no_progress`, ...) remain the sanctioned `--option` surface.

### §12.3 Configuration Derivation

- **Automatic env resolution — PARTIALLY FIXED:** `${VAR}` expansion is now scoped to `[storage]` values only (not all config strings), but remains unconditional within that scope — a literal `${FOO}` storage value is impossible when `FOO` is set. Remaining work: opt-in flag or escape syntax.
- **DuckDB option tier is disconnected — OPEN:**
  `src/firecube/core/config.py:get_plugin_defaults()` deliberately does not
  merge `[database.duckdb]`, while
  `src/firecube/ingestor/runtime/configure.py:TierConfigurator` allowlists
  `duckdb_max_temp_directory_size`, `duckdb_memory_limit`, and
  `duckdb_threads` only after they are already present in `ctx.options`.
  `src/firecube/cli/_typed_options.py:_enumerate_all_valid_keys()` does not
  include that tier, so `--show-options` omits the keys and typed `--option`
  rejects them. `DuckDbMixin` in
  `src/firecube/ingestor/extensions/duck.py` therefore has no coherent public
  route for receiving its resource settings. Define one typed owner for these
  fields, load `[database.duckdb]` only for plugins that declare the DuckDB
  capability, and expose the same keys through CLI discovery and coercion.
  Acceptance: config-file and `--option` values reach `DuckDbMixin` with typed
  values; `--show-options` lists the keys only for compatible plugins; plugins
  without the capability reject them; public configuration docs describe the
  same surface.
- **`default_product_name` leaks into plugin option validation — OPEN:**
  `src/firecube/cli/main.py` reads the key from plugin defaults to resolve the
  product name, but leaves it in the `options` mapping passed to
  `IngestContext`. `TierConfigurator.configure()` in
  `src/firecube/ingestor/runtime/configure.py` then rejects it because
  `SYSTEM_KEYS` in `src/firecube/ingestor/config/engine.py` does not include the
  key and `PluginConfig` in `src/firecube/ingestor/types/config.py` does not own
  it. Consume this CLI-owned key before tier validation; do not weaken strict
  unknown-key rejection. Acceptance: a config-only `default_product_name`
  completes runtime configuration, while product-name precedence remains CLI
  flag > config `default_product_name` > plugin `PRODUCT_NAME` > hard failure.

### §12.4 Plugin Heuristics

- **Option aliases — DONE (2026-06-11):** `zarr_chunk` deleted (no shim, per STYLE.md); `zarr_chunk_shape` is the single chunking option and `zarr_chunk` now fails strict unknown-key rejection. Locked by `tests/unit/test_zarr_chunk_alias_removed.py`.
- **Regex guessing — FIXED in-repo:** no filename-regex horizon extraction or `F*` folder discovery remains in core or templates. Remaining occurrences live in an external plugin repository.
- **Hardcoded defaults — DONE (2026-06-11):** lat/lon soft limits gone; multires `(1.0, 0.5)` single-sourced as `DEFAULT_MULTIRES_RESOLUTIONS` (no silent fallback); `group="FWI"` fallback removed from `core/zarr/layers.py`; `"fire_risk.duckdb"` default removed from `extensions/duck.py`. Evidence: `tests/unit/test_domain_defaults_removed.py`.
- **Typed-vs-free-form drift — DONE (2026-06-11):** strict unknown-key rejection enforced on all declared typed configs; `x_*` experimental namespace implemented — keys matching `x_*` pass through without rejection. Evidence: `tests/unit/test_experimental_options.py`.
- **Discovery exclusions — DONE (2026-09-13):** Native `--input-filters` combines additive globs and `!` exclusions in one list on ingest, slots, and preallocate. Configuration uses `input_filters`; `include_patterns` is rejected with migration guidance. CLI introspection and the discovery guide show the native flag. Evidence: `tests/unit/test_input_filters.py` and `tests/integration/test_input_filters_cli.py`.
- **Other discovery controls — DEFERRED:** `include_suffixes`, `recursive`, and `sniff_hdf5` remain Python helper arguments. No additional CLI flags or engine options are selected for implementation. Custom discovery hooks retain responsibility for applying filters to their source items.

---

### §13 Harden PluginContext boundary

**Goal:** Make plugin hook context truly read-only and non-bypassable in normal usage.

**Current state (partially addressed):**
- `options` is a detached copy wrapped in `MappingProxyType`. Mutations to `RuntimeIngestContext.options` after `PluginContext` creation are invisible to plugins.
- `option()` reads from the frozen proxy. `ctx.options["k"]` and `ctx.option("k")` are consistent.
- `__getattr__` blocks direct access to `storage`, `_chunk_manager`, `_materializer`.
- 6 boundary tests in `tests/unit/test_plugin_context_boundary.py`.

**Remaining (structural):**
- `_ctx` escape hatch: `pctx._ctx` still gives full access to `RuntimeIngestContext` (see `PluginContext.__init__` in `src/firecube/ingestor/types/context.py`). Python cannot enforce true private attributes, but a frozen-snapshot design would eliminate the reference entirely.
- **Phase 3.2 partial closure**: The `_parallel_global_schema` escape hatch (the specific `ctx._ctx._parallel_global_schema` pattern used by `DirectZarrIngestor`) was removed in Phase 3.2 C4 (commit `6b6f3a1`). The broader `_ctx` reference-sharing issue remains open.
- Frozen snapshot blocked by initialization order: `PluginContext` is created in `BaseIngestor.run()` (`src/firecube/ingestor/runtime/base.py`) before `runtime_ctx.telemetry` is assigned a few lines later. Fixing this requires restructuring `run()` so telemetry is assigned before context construction.
- Per-worker snapshots: all workers share one `PluginContext` instance wrapping the same `RuntimeIngestContext`. Safe for reads (options detached), but `_ctx` reference-sharing means a convention-breaking plugin could mutate shared state.
- No tests assert `_ctx` is inaccessible (Python cannot enforce this without structural change).

---

### §31 Parquet template configuration parity

**Goal:** Eliminate accepted Parquet configuration keys that silently have no
effect on persisted output.

**Current state:** `ParquetTemplateConfig` in
`src/firecube/ingestor/templates/config.py` declares `parquet_partition_by` and
`parquet_row_group_size`. `GenericParquetIngestor.write_parquet()` in
`src/firecube/ingestor/templates/generic.py` calls `pyarrow.parquet.write_table`
without applying either value. Both keys therefore pass strict configuration
validation but do not change the output.

**Direction:**
1. Pass `parquet_row_group_size` to the default writer and validate invalid
   values before the first write.
2. Remove `parquet_partition_by` from the accepted config until the template has
   an explicit dataset-directory layout, deterministic part naming, write-domain
   ownership, and resume semantics for partitioned output. Do not emulate
   partitioning inside the current one-file-per-batch contract.
3. Keep `docs/reference/config.md` and
   `docs/guides/plugins/generic-parquet.md` aligned with the implemented surface.

**Acceptance criteria:**
- Every accepted `ParquetTemplateConfig` field changes persisted Parquet output;
  unsupported fields fail as unknown configuration.
- Row-group behavior is verified from real Parquet metadata for local output and
  through the selected storage driver for remote output.
- No plugin needs to override `write_parquet()` to use the advertised row-group
  setting.

---

### §32 Public output-writer contract for custom pipelines

**Goal:** Let an external `BaseIngestor` plugin write through Firecube's selected
storage driver using a stable, typed public contract.

**Current state:** `StorageContext` is exported by
`src/firecube/ingestor/api.py`, and `PluginContext.storage.output` exposes the
bound session at runtime. Its annotation in
`src/firecube/ingestor/types/context.py`, however, names the concrete
`StorageSession` from `src/firecube/core/storage/session.py` under
`TYPE_CHECKING`. That concrete class is not exported by
`src/firecube/ingestor/api.py` or `src/firecube/core/api.py`. A custom
`BaseIngestor._process_batch()` implementation can therefore use the object only
through an internal import, implicit duck typing, or `Any`; none defines a
supported plugin contract.

**Direction:** Define a narrow writer `Protocol` in the plugin contract layer,
type `StorageContext.output` against it, and export it from the appropriate
public `api.py` surface. Keep the concrete `StorageSession` internal. The
protocol must expose only operations required by custom output pipelines and
must preserve the one-driver rule; it must not provide a route around Firecube's
write coordination or control-plane ownership.

**Acceptance criteria:**
- An external `BaseIngestor` plugin can type-check and perform its output writes
  using imports from `firecube.ingestor.api` and `firecube.core.api` only.
- The public protocol has behavior-backed local and S3 driver coverage for its
  declared operations.
- Existing template implementations continue to use the same bound session and
  selected driver without deep imports becoming part of the plugin API.
- Public plugin documentation names only the protocol, not
  `firecube.core.storage.session.StorageSession`.

---

### §14 Expand concurrency and race-condition coverage — DONE (2026-06-11)

**Goal:** Catch production-only failures before deployment.

All four open points closed. Evidence:
- Concurrent ingestor instances (same product, `resume_existing`/`force_reingest` combinations): `tests/integration/test_concurrent_same_product.py`
- Workspace materialization races on the same remote URI: `tests/integration/test_workspace_materialization_race.py`
- Control-plane write ordering/consistency under concurrent batch completion: `tests/integration/test_wal_concurrent_ordering.py`
- OTel/thread context propagation under error and retry scenarios: `tests/integration/test_otel_context_concurrency.py`

---

## Zarr Runtime Follow-ups

These items address gaps and bugs surfaced after §4a landed. They are sequenced as refinements/fixes to work that has already shipped, not as new architectural directions. Items §25 and §26 are runtime-crashing bugs and should be prioritised.

### §21 Strategy Protocol unification — DONE (2026-05-27)

See DONE.md §21 for details.

Follow-up: Check external plugins for `ZarrWriteStrategy` import and migrate — out of scope for Phase 1.

---

### §22 GenericZarrIngestor full thinning — DONE (2026-05-27)

**Goal:** Move `GenericZarrIngestor._process_batch` toward a true thin facade.

**Original state (pre-§22):** `_process_batch` was 144 lines and owned five responsibilities inline: staged metadata seeding, lock construction, strategy construction, claim closure construction, and metrics assembly.

**Completed (2026-05-27):** §22 helper extraction is done in `src/firecube/ingestor/runtime/zarr/batch_runner.py` with 5 named helpers and `tests/unit/test_batch_runner.py` (13 tests). T5 wired those helpers into `GenericZarrIngestor._process_batch`, replacing all five inline blocks.

The core write step remains delegated to `AppendStrategy.write_groups()`.

**Result:** `_process_batch` is now a thin orchestrator around preparation, URI/option resolution, batch_runner helper wiring, `strategy.write_groups()`, and final result construction. ~66 lines (down from 143). See DONE.md §22 (helpers) and §22 (wiring complete).

**Builds on:** §4a (DONE.md).

---

### §23 RegionZarrWriter resize-on-write contradicts documented contract — DONE (2026-05-27)

See DONE.md §23 for details.

### §23-AUTO Auto-compute expected_time_count from WriteIntent.ts_index — DONE (2026-05-27)

See DONE.md §23-AUTO for details.

---

---

### §25 AppendMultiresHandler stale kwarg — DONE (2026-05-26)

See DONE.md §25 for details.

Follow-up: Check external plugins for `zarr_multi_res` usage and update if needed — out of scope for this PR.

---

### §27 Stale `build_dataset` documentation — DONE (2026-05-26)

See DONE.md §27 for details.

---

### Phase 2.1 Legacy zarr_group claim sweep removal — DONE (2026-05-28)

`_sweep_legacy_zarr_group_claims` removed in Phase 3 (T18). See DONE.md §7-sub.

---

### §30 `ensure_timestamp_slot` should treat `time_indexed=False` arrays as coords — DONE (2026-06-25)

**Goal:** Stop static (non-time-indexed) coordinate arrays from being mistaken for time axes during the time-slot bounds check, so declaring `time_indexed=False` is sufficient on its own.

**Current state:** `RegionZarrWriter.ensure_timestamp_slot` skips an array from the `ts_index < shape[0]` bounds check only if its name is in `coord_names` (constructor param, default `{"y","x","channel"}`) or it is scalar (`ndim == 0`). It does **not** consult the schema's `ZarrArraySpec.time_indexed=False` flag. The strategy builds `coord_names_by_group` from `spec.coord_names` only.

**Bug it caused:** A DirectZarr plugin declared static 2-D `lat`/`lon` and 1-D `ny`/`nx` projection coords with `time_indexed=False` but did not also list them in `coord_names`. `ensure_timestamp_slot` then read each static coord's dim-0 (the grid size, e.g. 1072/1332) as a time axis and raised "ts_index out of bounds" for any `ts_index` beyond it — i.e. any day far from the reference epoch (slot ~622080 for a 2023 day against a 2018 epoch). Latent for any multi-day ingest; was masked earlier by the axis-size error. The plugin worked around it by adding the names to `coord_names`.

**Direction:** Fold every `time_indexed=False` array into the writer's coord-skip set (the schema already knows which arrays have no time axis), as a union with the existing `spec.coord_names`. Then `time_indexed=False` alone is sufficient and a plugin never has to *also* name the array in `coord_names`.

**Acceptance criteria:**
- A DirectZarr group whose static coords are declared only via `time_indexed=False` (not in `coord_names`) ingests a high-`ts_index` timestamp without a spurious "ts_index out of bounds" error.
- `coord_names` remains honored for back-compat (union, not replacement).

**Surfaced by:** a plugin fix declaring `lat`/`lon`/`ny`/`nx` in `coord_names`.

See: DONE.md "DirectZarr plugin parity — core fixes" (2026-06-25).

---

## Parquet slice identity and replay

The fresh-target safety fix deliberately limits `GenericParquetIngestor` to fresh targets. Do not remove the target guard merely after adding run IDs to part names. Before supporting resume or force-reingest, define a stable, plugin-declared logical slice identity independent of batching and temporary paths; specify physical part ownership, replay deduplication, exact-slice replacement, and failure-safe promotion/recording through `ChunkManager` and the selected driver. Tests must verify rows and active spans across disjoint runs, repeated inputs, changed batch sizes, partial replacement, and failed promotion. The current target guard is covered by `tests/integration/test_parquet_target_safety.py`.

## Zarr migration framework (deferred from CF-1.8 plan)

**Goal**: a generic Zarr migration framework so future migrations (rename-dim, rename-array, set-default-attrs, schema upgrades, etc.) plug in via a Protocol implementation rather than each shipping its own CLI command. Decided against `migrate-dim` and similar name-coupled one-offs.

**Shape**: `MigrationStrategy` Protocol (`@runtime_checkable`) + in-source registry + a generic safety runner.

**Generic runner responsibilities**:
- Refuse to run if `ChunkManager.list_runs(non_terminal=True)` returns any non-terminal runs.
- Refuse to run if `ChunkManager.list_claims()` returns active claims on target groups.
- Acquire and release a write claim around the migration.
- Provide `--dry-run`.
- Group-explicit `--group <g>` or `--all-groups`.
- Use `ChunkManager` facade + `create_filesystem(config)` — never raw `.firecube/` writes or `fsspec.filesystem(...)`.

**CLI shape**: `firecube zarr migrate <strategy-name> [strategy-args] [--dry-run] [--group <g> | --all-groups]`, with `firecube zarr migrate --list` and per-strategy `--help`.

**First strategy to ship**: `rename-dim` (the operation the CF-1.8 plan would have shipped). Required behavior:
- Walk every Zarr array in the target.
- Rewrite `dimension_names` in each array's metadata.
- Rename the coordinate variable folder (`{group}/timestamp` to `{group}/time`).
- Rebuild consolidated metadata if present.
- Preserve internal control-plane array names (e.g. `firecube_timestamp_state` stays — only its inner dim name changes).

**Out of scope for v1**: multi-resolution pyramid groups; `--force` to bypass safety; auto-migration on append.

**Acceptance criteria for the follow-up plan**:
- Compatibility matrix tests for at least the 8 rows defined in `cf18-compliance` plan.
- E2E `legacy cube -> migrate -> append with new dim -> readback` test.
- Integration tests covering refusal-on-active-claim and dry-run-no-mutation.

---

## CF advisor enhancements (deferred from CF-1.8 plan)

- `firecube advise compliance --profile cf-18 --all-groups`: walk all Zarr groups in a product instead of requiring `--group`.
- Tier 2 — CF Standard Name Table vocabulary validation (check `standard_name` values against the canonical table).
- Tier 3 — UDUNITS-based unit validation. Likely via optional `cf-checker` runtime dependency or a NetCDF temp-export bridge.

---

### Concurrent-metadata-read follow-ups (deferred from the 2026-06-22 `read_bytes` fix)

The 2026-06-22 `StorageFilesystem.read_bytes` change fixed the s3fs 412 crash on the
existing-cube dim-compat read. Two optional hardening items were deliberately NOT
bundled (they change a safety check's timing / touch a separate path):

- **Reduce redundant dim-compat reads.** `_verify_existing_cube_batch_groups` runs
  the dim-compat check once *per batch* (`engine._create_batches_with_parallel_filter`),
  so a pod re-reads the same group `zarr.json` many times. Cache the result per-pod,
  and/or gate the check in parallel slot-range mode where `_verify_schema_at_pod_startup`
  already guarantees the dims from the plugin-declared `time_dim_name`. Care: this
  changes when a public safety check (`verify_dim_compatibility`, DONE.md 2026-06-20)
  runs — preserve the genuine pre-existing-cube append guard.
- **Route `core/zarr/validation.py::_load_array_metadata` through `read_bytes`** for
  consistency (still uses `open().read()`). Lower priority — it's the `firecube zarr
  validate` maintenance path, not the concurrent ingest hot path.

---

### §28 Audit closeout follow-ups (minor)

- **Thin `plugin` marker** — registered in `pyproject.toml` and applied in one module only (`tests/integration/test_slot_index_fixture_plugins.py:44`). Decide: delete the registration or apply it to the other plugin-specific suites.
- **T4 residual** — `tests/unit/test_append_strategy.py` still patches the strategy's own internals; real-store coverage lives in `tests/integration/test_append_strategy_behavior.py`. Candidate for a behavior-based rewrite of the remaining wiring test.
- **A7 watch-item** — the archive-create path (`src/firecube/core/tensogram/converter.py`, `_find_time_dim` call around line 112) does not thread a plugin-declared `time_dim_name`; only the ingest path does.
- **C3 watch-item** — the hierarchy flatten grew `BaseIngestor` to ~815 lines (flatten-by-hoisting); the depth test pins templates only, not plugin-subclassing-plugin chains. Watch for god-base growth; consider extracting batch-lifecycle hooks into a composed collaborator if it keeps growing.

---

### §46 DirectZarr template-level codec drift is not checked on resume (2026-10-14)

**Status**: OPEN

`verify_array_spec` in `src/firecube/core/zarr/region_writer.py` compares stored codecs only with the codecs a `ZarrArraySpec` declares; a spec that declares none is never compared. Codecs that come from the template's `zarr_codecs` therefore escape the check. Run on 2026-10-14: `_setup_global_zarr_schema` over one store with `zarr_codecs` zstd level 1, then again with level 7 and no per-array declaration, raised nothing and the stored array kept level 1. Decide whether a changed template-level codec on resume is drift (refuse with `SchemaDriftError`) or is allowed and documented; either way keep the intended compatibility of a plugin that declares no codec at all, which must still append to an existing store (run: a spec with no codec was accepted against a stored zstd level 1). The schema hash is an audit fingerprint and does not enforce this; drift errors come from `verify_array_spec` for declared codecs.

### §47 GenericZarr discards the filters and the serializer of `zarr_codecs` (2026-10-14)

**Status**: OPEN

`_build_zarr_encoding` in `src/firecube/ingestor/runtime/zarr/write.py` keeps only the compressors of the validated `zarr_codecs` pipeline and passes `compressors` alone to xarray. Run on 2026-10-14: a `ZarrTemplateConfig` that validated `bytes(endian=big)` then `zstd(level=3)` was written as `bytes(endian=little)` then `zstd(level=3)`. Forward the full validated pipeline (filters, serializer, compressors) in `GenericZarrIngestor`. GenericZarr only; DirectZarr forwards all three.

### §48 Document resume-time codec drift and the migration choice (2026-10-14)

**Status**: OPEN

Neither `docs/guides/plugins/configure-zarr-compression.md` nor `docs/reference/config.md` (nor the `ZarrTemplateConfig` docstring that generates its entry) says what a resume does when the codecs change. Document, for a reader who already has a cube: a resume that declares different codecs for an array raises `SchemaDriftError` naming the field; how to keep an existing cube's codecs on append (declare the same codecs, or none); and how to re-ingest from scratch under other codecs, since no in-place codec migration exists (the error says so). Describe the template-level case as §46 settles it.

---

### §44 `delete-span` cannot mark deleted integer variables that have no fill value (2026-10-05)

**Status**: OPEN

`delete-span` fills a deleted span with the array's Zarr `fill_value`. An integer data variable with no CF `units` ... `since` and no `_FillValue` (for example a `record_status (time)` `int64` array) is therefore filled with `0`, which cannot be told from data. Options: encode a `_FillValue` when such a variable is written, or make `delete-span` warn when it zero-fills an integer array with no declared fill. Only CF time arrays get a NaT sentinel today.

### §45 `delete-span` region-fill dry run skips the pre-flight (2026-10-05)

**Status**: OPEN

For region-filled spans, `--dry-run` in `DeletionEngine._delete_span_by_region_fill` returns before `delete_span_via_region_nan_fill` runs. It can report success for a span that execution refuses (missing array, out-of-range, unknown dimensions, unfit state array) and omits the skipped-static-array warning that execution emits. Run the pre-flight in dry-run mode and return its warnings.

---

### Ingest performance

- Report real per-run byte and file counts for direct-S3 completion via filesystem instrumentation instead of returning zeros. Follow-up to the direct-S3 completion fix in 0.1.7; requires wiring per-run counters into the completion path.
