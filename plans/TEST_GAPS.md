# Test Gaps

High-risk behavior that needs better tests. This is a backlog for new tests; it
is not permission to re-add broad snapshots, source-grep tests, or
assertion-light mock tests.

## P0 - Add Before Trusting The Suite

1. Control-plane deletion must be proven against real persisted state.
   Add real-store coverage for `ChunkManager.delete_spans(dry_run=True)`, partial
   deletion failure, and fail-closed behavior when storage deletion is requested
   without a usable storage filesystem/config. The assertions must inspect span
   records, timestamp state, and storage contents after the operation.

2. Chunk maintenance CLI commands need real effects coverage.
   `firecube chunks delete-span`, `claims clear`, `runs abandon`, and
   `snapshots rebuild/status` currently lean too much on typed/fake-manager
   checks. Add CLI tests that operate on a real `.firecube/` root and verify
   chunk rows, claims, run records, snapshots, and manifests.

3. Remote write paths need end-to-end parity tests.
   Add GenericZarr and DirectZarr remote write tests for both `fsspec` and
   `obstore` through the public ingest path. Cover staged-upload failure
   atomicity so a failed upload cannot leave a product marked complete.

4. Ingestor result typing must be enforced through `BaseIngestor.run()`.
   Add tests where plugins return bad result shapes, plain dict metrics, or
   invalid output containers, and assert the public runtime boundary fails
   clearly before downstream code consumes the result.

5. Artifact URI strictness is inconsistent.
   `AGENTS.md` says `--archive` and `--output` are strict artifact URIs, but
   archive restore still accepts bare local archive paths. Add behavior tests
   after the CLI implementation rejects bare artifact paths consistently.

## P1 - Next Coverage Targets

1. Clarify CLI smart defaults for write-tier commands.
   The manifest still marks write commands as not smart-default eligible while
   current behavior infers storage type from `file://` and `s3://` targets.
   Add focused tests for `ingest`, `zarr slots`, `zarr preallocate`, and
   `zarr multires` with omitted `--storage-type` and omitted `--storage-driver`,
   then update the manifest to match the accepted contract.

2. Validate `zarr validate` machine-readable behavior.
   Add tests for JSON payload shape, `--max-chunks`, `--timeout`, and
   `--on-timeout`. These should assert parsed output and exit semantics, not
   only that Click accepted the flags.

3. Exercise DirectZarr concurrent disjoint slot writes.
   Use a real local store, real manager, and real writer with non-overlapping
   slot ranges. Assert final arrays and control-plane rows, not just claim calls.

4. Prove GenericZarr staged metadata seeding through the runtime engine.
   Existing helper tests are useful but do not fully exercise the engine path
   that invokes pre-batch metadata seeding for staged Zarr outputs.

5. Tighten storage summaries and driver reporting.
   Add direct-S3 result-summary tests where metrics storage is absent and the
   selected driver must still be reported accurately.
## P2 - Lower-Risk Gaps

1. WAL negative branches: malformed records, duplicate run IDs, and missing
   projection files should have explicit failure-mode tests.

2. Maintenance claim races: snapshot rebuild and maintenance operations should
   prove they block or roll back correctly while active claims exist.

3. Plugin CLI JSON behavior: plugin management commands need focused JSON
   output tests for success and error paths.

4. DirectZarr retained-payload regression. No test or benchmark bounds how many
   `WriteIntent.data` payloads remain live during `_process_batch()` and
   `write_groups()`. Before any change to intent-lifetime semantics (lazy
   payload thunks, streaming variants, sub-slot batching), add an instrumented
   `slow`-marked regression that proves the max simultaneously-live payload
   count is bounded by the intended contract, using a synthetic plugin that
   emits multiple large data-bearing intents. Absolute RSS/memray evidence
   belongs in a performance lane, not the default behavioral gate. For MTG FCI
   FDHSI, the current retention floor is ~14.8 GiB/worker; any lazy-payload
   change must bring this measurably below that floor.

5. CoverageTracker sub-slot granularity for future sub-batching designs.
   `CoverageTracker.record_write` (`runtime/coverage.py`) records at
   `(group, ts_index)`. Any future proposal that emits multiple batches per
   `ts_index` (nc_part-per-batch, tile-per-batch, channel-per-batch) MUST first
   address partial-slot crash-resume detection: without sub-slot coverage
   tracking, a mid-slot crash where some but not all sub-batches succeeded
   produces a Zarr slot with fill-value regions indistinguishable from
   legitimate writes. Data integrity, not just batch failure. Prerequisite for
   accepting any such plugin design.

## Rule For Filling These Gaps

Each new test must name the behavior and risk it protects, use real Firecube
objects where practical, and assert Firecube-visible effects. If the only
assertion is "no traceback", "exit code is not 2", or a source string exists,
the test belongs in this gap file as a missing behavior, not in the suite.
