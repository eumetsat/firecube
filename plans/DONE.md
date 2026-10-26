# Done

## 2026-10-26 — stac:// is a source scheme, admitted by a sibling of the URI rule (97-source-files-from-stac-apis)

**Decision.** `stac://<host>/<api-root>/collections/<id>` names a source, never a storage location. `StorageUri` does not accept it, and `parse_and_validate_uri` keeps refusing it for every option. Source options admit it through `parse_and_validate_source_uri(value, *, param_hint) -> StorageUri | StacCollectionUri` in `src/firecube/cli/_uri_policy.py`. This amends design item 2 of #97, which put `stac://` into `parse_and_validate_uri` itself.

- Why not widen `StorageUri`. `StorageUri` is a storage location. `src/firecube/core/filesystem/obstore_backend.py:371` routes every non-`file` `StorageUri` into `S3Store`, so a `stac://` URI would become an S3 bucket named after the catalogue host. About 70 consumers assume `.path` and a storage type. `parse_and_validate_uri` also admits `--target`, `--source` and `--archive`; there `apply_smart_default` indexes `SCHEME_TO_STORAGE_TYPE` and would raise `KeyError` on `stac`.
- The sibling. A `file://` or `s3://` value returns the same `StorageUri` with the same refusals as `parse_and_validate_uri`. A `stac://` value returns a frozen `StacCollectionUri(host, api_root, collection)` (`src/firecube/core/discovery/_types.py`). It refuses a non-lowercase scheme, a missing host, a path that does not end in `/collections/<id>`, a `?`, a `#` and a control character. The rules are `StacCollectionUri.parse`, which the STAC discovery backend also uses, so the CLI and the library parse a source alike. The CLI wraps its `ValueError` as `click.BadParameter`, exit 2.
- Who uses it. Only the `--input-data` of `ingest`, `zarr preallocate` and `zarr slots`, through `admit_input_sources`. Only `stac://` values go through it; local paths and other schemes stay exempt, as before, because a plugin interprets them. Every other option keeps `parse_and_validate_uri`. A command without a STAC branch refuses the `StacCollectionUri` variant in its body with a `click.UsageError` before any I/O; none exists today.
- The gate rows of `test_cli_parses_uris_only_in_uri_policy` are unchanged: the scheme of an `--input-data` value is split only in `_uri_policy.py`, and `ingest` keeps one `is_remote_target` and one `local_path_from_target` call.
- These tests pin it: `tests/cli/test_uri_policy_stac.py` (admission, the same refusals for `file://` and `s3://`, each `stac://` refusal, `parse_and_validate_uri` refusing `stac://` for `--target`, `--source`, `--archive` and an argument, and `StorageUri.parse` refusing `stac`), and `tests/integration/test_ingest_stac_options.py` (the three commands).

## 2026-10-24 — One URI rule for every CLI option (17-cli-archive-create-restore-accept-bare-local-paths)

**Decision.** One function, `parse_and_validate_uri(value, *, param_hint)` in `src/firecube/cli/_uri_policy.py`, admits every CLI option and argument that takes a product or artifact URI. It returns core's `StorageUri`, and command code reads the scheme and the path only from that value. One signed commit replaces 300f48e on the branch. Nothing is pushed.

- The rule. It accepts lowercase `file://` and `s3://` only. A `file://` URI names no host, `localhost` included, and a path below the root. An `s3://` URI names a bucket. A `?`, a `#` and a control character are refused. The `file://` path is literal, and `%20` is not decoded, because Firecube serializes `file://` URIs literally. The first failing rule refuses. The order is: empty, control character, no `://`, unsupported scheme, uppercase scheme, `?` or `#`, host or missing bucket, `StorageUri.parse`, root path.
- Each refusal is a `click.BadParameter` with the option as its hint, so the text reads `Invalid value for '--archive': <sentence>`. The exit code stays 2. The bare-path suggestion is best-effort. A value with a `:` gets none, and a value whose home directory cannot be found is refused without one.
- `StorageUri` is the one representation. `ParsedUri`, `parse_product_uri`, `require_full_uri`, `_require_archive_scheme`, `_local_zip_archive`, `_parse_file_uri` and `_parse_s3_uri` are deleted. `resolve_product_identity` takes an admitted `StorageUri`. `_local_archive` in `src/firecube/cli/archive/_common.py` admits an `--archive` value and returns the literal path.
- A command admits a value in its body, not in a Click callback. The order of its refusals does not change. `zarr index` keeps `_manager(target: str, product_name)` and admits the value inside it. `zarr compare` keeps its refusal order: a bare argument check of A_URI and B_URI first, then the admission and coherence of A, then B.
- This closes issue #17. `archive create`, `restore`, `info`, `validate` and `list` refuse a bare `--archive` value. Before, create and restore accepted any bare path. Info, validate and list accepted a bare `.zip` path.
- The commands that percent-encoded a `file://` path now use it literally: `archive info`, `validate` and `list`, `catalog intake`, `parquet validate` and `consolidate`, `advise batch-size` and `compliance`, and `zarr validate`. A `file://` value in upper case or with `localhost` is refused in every command. Before, some commands read `FILE://` as a path under the working directory.
- `zarr index show`, `verify` and `rebuild` run their first URI check. Before, they accepted a bare path.
- `parquet consolidate` doubles each `'` in the two string literals of its DuckDB `COPY` statement through `_sql_string_body` in `src/firecube/cli/parquet.py`. A literal path with a quote would otherwise end the string.
- `IngestCommandConfig` calls the same function for `--target`, and its own scheme sentences are deleted. The storage-type coherence sentences are unchanged.
- The gate: `test_cli_parses_uris_only_in_uri_policy` in `tests/architecture/test_cli_boundary.py` forbids URI parsing in `cli/` outside `_uri_policy.py`. It has three `PERMANENT` rows, each scoped to one function: `is_remote_target` and `local_path_from_target` once each in the `ingest` function of `cli/main.py`, for `--input-data`, and `StorageUri.parse` once in `_storage_uri_from_base` of `cli/chunks/_manager.py`.
- These tests pin the behaviour.
    - `tests/cli/test_uri_scheme_policy.py`: `test_parse_and_validate_uri_returns_the_path_literally` and `test_parse_and_validate_uri_refuses_each_rule_naming_the_option`.
    - `tests/cli/test_archive_artifact_uri_contract.py`: `test_archive_commands_admit_archive_uri_through_the_uri_rule`, and the "sp ace" case of `test_archive_artifact_inspect_accepts_file_uri`.
    - `tests/cli/test_archive_create_contract.py`: `test_create_writes_nothing_for_a_refused_archive_uri`.
    - `tests/integration/test_cli_index_show.py`: `test_index_commands_refuse_a_target_the_uri_rule_refuses`.
    - `tests/integration/test_cli_chunks_runs_target.py`: `test_chunks_runs_refuse_a_value_the_uri_rule_refuses`.
    - `tests/cli/test_catalog_intake_contract.py` and `tests/cli/test_parquet_consolidate_output_artifact.py`: the two `..._writes_output_to_the_literal_path` tests.
    - `tests/unit/test_ingest_command_config.py`: the moved and added rows of the rejected table.
- Open items are in `plans/TODO.md`, "Open after the CLI URI rule (2026-10-24)", and in `plans/TEST_GAPS.md`, P0 item 5.

## 2026-10-20 — CF calendar time axes land on the guarded layers (fix/parallel-chunk-ownership)

**Decision.** The CF-calendar work of branch `feat/77-cf-calendars` (9a309ca, 42 commits on c160535) lands on `fix/parallel-chunk-ownership` (9576ad3) in four signed commits. Nothing is pushed. The full lane runs on the exact tree of each commit before the commit. Calendar writes use the guarded layers of this branch: payload through `guarded_write`, `zarr.json` through `MetadataAuthority`, live metadata reads. The entries of 2026-09-23 and 2026-10-10 hold the design.

- `f1dc750` `feat(time): add encoded time axes for CF calendars`. The `core/time/` package, `core/zarr/_fill.py`, and the calendar changes to `index_resolve.py`, `index_spec.py`, `time_decode.py` and `index_binding.py`.
- `e159147` `feat(zarr): write CF-calendar time coordinates`. Coordinate materialization, re-materialization checks, region writer, DirectZarr startup check, preallocate, validation, consolidation and the tensogram refusal.
- `b8e422c` `feat(append,chunks): calendar append order and time-range refusals`.
- `docs(calendar): document CF calendar time axes`. Hash pending. This entry belongs to it.
- Maintainer choices. The branch lands as four commits in this order, each green at its own position. A pre-registered performance allowance applies. One recorder allowance applies: the traceback of `test_indexed_write_error_fails_ingest_with_compilation_error#0`, for line numbers and the new reason suffix of `IndexedWriteCompilationError`. The branch adds at most 1,500 test lines.
- Design choice on tests. The branch keeps a curated test set. The 45 test paths of 77 add 7,689 lines. A scratch worktree runs every test that 77 added or changed against the landed source before each commit. So the assertions of 77 stay checked as evidence.
- Design choices on code. The refusals carry fields (`Datetime64RangeError`, `DuplicateAxisInstantError`, `CoordinateRelabelError`, `UnparseableSpanBoundsError.spans`, `CalendarArchiveError`). Nothing is exported from a facade.
- Read-only checks and pure helpers are in new modules. Every write stays in `coord_materialization.py`.
- A moved name leaves no alias. Only `CalendarArchiveError` joins the conversion errors of `create_archive`. The change also covers the preflight and the dry-run facts. The legacy nanosecond check reads the stored coordinate before `ensure_group`.
- Performance prediction. The same job on the base of 77 measured these extra calls. S0 open +12, S0 `os.listdir` +1, W1 open +18, W1 `os.listdir` +2, W2 open +92, W2 open at root +2, W2 `os.listdir` +10, W3 open +18, W3 `os.listdir` +2. Every other row is 0.
- The noise allowance is 5 for W2 open, 2 for W2 open at root and 1 for each other row. A row outside its allowance stops the run until a maintainer rules.

**Coverage.**

- Acceptance evidence is kept in a local folder that Git excludes.
- Done now: the acceptance run of family 1 ran 200 ids, and 200 passed. The run of families 1 and 2 ran 392 ids. 389 passed, 2 are class B, and 1 is the strict xfail.
- Scheduled: the acceptance run of all families and the tip checks. The results go in the same folder when the runs finish.
- The `kept.md` file of each test group maps every kept test to its behaviour. It maps every dropped test of 77 to a replacement or to "acceptance only".
- Tests added: 1,494 lines against a budget of 1,500 (measured, by the test budget check).
- The branch does not keep the byte-parity test of 77 or its 405-line manifest, because of the 1,500-line test budget. At 9576ad3 the three Gregorian scenarios of that manifest rebuild byte for byte (ran, 2026-10-20). The scratch acceptance run includes that test.
- Not in this entry: the recorder replay and the performance judge at the tip, which run after the commits.

## 2026-10-20 — Cleanup after the CLI-creep sequence (fix/parallel-chunk-ownership)

**Decision.** Two signed commits, nothing pushed, each revertable alone. Commit 1 is `9576ad3` `refactor(cleanup): delete dead code and duplicate tests`. Commit 2 is the commit that holds this entry. CLI output is unchanged.

- Deleted: `fsspec_kwargs_from_binding`, `ManifestRepository.record_entry`, `_group_path`, `_upload_tree_shape0`, `_walk_chunk_entries`, `resolve_product_name`, `_first_present_float` and `_raise_unknown_time_dim`. Also the `multires` key of `OPERATION_FUNCTIONS` and `OPERATIONS`, which nothing raised.
- Deleted tests, with the kept twin after the arrow: `test_no_markers_legacy_write` → `test_marker_absent_legacy_write_proceeds`. `test_schema_hash_changes_on_dtype_change` → `test_sensitive_to_dtype`. `test_generic_false_declared_codecs_raises` → `test_directzarr_false_declared_codecs_raises`. `test_known_slot_group_passes_capability_gate` → `test_slot_group_set_validates_only_that_group_chunks`. Each deletion was proved with an in-memory mutant that fails both tests.
- Moved: the trailing-stop trim, from `engine.py` to `Message.without_trailing_stop()` in `core/errors.py`. A differential probe against the old helper matched on 60 cases.
- Added: a check that the part kinds the CLI tests cover equal `_PART_TYPES`. Fixed `_unit_object_key` for an array at the store root. Fixed `ConfigFileError` and `RemovedConfigKeyError`, which came back from pickle or copy with a wrong sentence. Both now rebuild from their facts. Each fix has a test that failed first.

**Coverage.**

- Source lines under `src/firecube`: 67,731 at `9576ad3` and 67,572 at `9576ad3` (calculated from `git ls-tree` and `wc -l`).
- Tests added: 54 lines against a budget of 60 (measured, by the test budget check).

## 2026-10-20 — Structured errors rendered by the CLI (fix/parallel-chunk-ownership)

**Decision.** Two signed commits, nothing pushed, each revertable alone. E `9576ad3` `refactor(errors): carry refusal facts and render commands in the CLI` (105 files, +2811 -766: code, tests, `CHANGELOG.md`, `AGENTS.md`, `plans/DESIGN.md`). D `docs(plans): record structured errors` (`plans/TODO.md`, `plans/DONE.md`): hash pending. Base `573748a`. A refusal below `cli/` now carries facts instead of shell text. `str(error)` is the library sentence; one renderer under `cli/` rebuilds it with flags and runnable commands taken from the live Click definitions. `core/hints.py` is gone, Click is out of `core/` and `ingestor/`, and the boundary gate has no `UNTIL structured-errors` row.

- Maintainer choices. The test budget is 550 lines. Commands that ran with extra text print only the command (text changes 1 to 5 below). The commit edits `cli/chunks/_delete.py`. Preallocate's two cleanup failures keep the facts in the log line and print the command on stderr through `warn`. The eight help and scaffold hints that do not run stay (`plans/TODO.md`). One commit E and one records commit D land. The plan proceeds without `review-4c.md`, which did not exist yet. Commits use `git commit -sS`. The two `UNTIL unscheduled` input-filter rows join the boundary gate. The `cli/main.py` ceiling row stays.
- Design choices inside them.
    - `Message` plus the parts `Param`, `PluginOption`, `Operation`, `CommandName`, `Placeholder`, with `FirecubeError.message`, `.recovery` and `.context` (`RefusalContext`) in `src/firecube/core/errors.py`.
    - `render` in `src/firecube/cli/_recovery.py`, imported lazily through `cli/_errors.py`, so start-up loads no new module.
    - The CLI sites that print structured refusals call `render`. These sites still print `str(exc)` of sources that are not converted: `cli/_uri_policy.py:99` and `:109`, `advise.py:126`, `_command_schemas.py:85`, `plugins/mgmt.py:150` and `plugins/registry.py:118`.
    - The command-naming strings of `core/` and `ingestor/` become `Message`s, each fact a part. The `PERMANENT` rows of the gate and the generated scaffolding are the exceptions.
    - `PipelineResult.failure` and `PipelineFailedBatchesError(FirecubeError, RuntimeError)` carry the failed batch.
    - `ConfigFileError` and `RemovedConfigKeyError` replace the Click errors. The CLI maps both to exit 2.
    - `zarr index rebuild` catches the attrs-without-record and attrs-drift refusals of the two uncontended precedence sites. The contended loser branch (`manager.py:1384`) still raises a plain `ManifestError`.
    - CLI-built advice uses `Operation`.
    - Red tests come first, then the fixes.
    - The commit subjects are one line each.
- Changes to operator text, numbered 1 to 20 for the references in this entry.
    - Text changes 1 to 3: the `(from Python: ...)` text is gone from the three preallocate refusals.
    - Text changes 4 and 5: the legacy-index refusal and the slots warning print `firecube zarr index rebuild --target T --plugin P --product-name N`, values shell-quoted.
    - Text change 6: the schema-size refusal names the full preallocate command.
    - Text change 7: the unsorted-coordinate refusal prints `firecube zarr validate --product <uri> --group <group>`.
    - Text change 8: the `--option` token is shell-quoted.
    - Text change 9: `delete-span` prints `--force` and `--yes-i-really-mean-it`, not `force=True`.
    - Text change 10: under `zarr preallocate`, `--slot-group` reads `slot_group`.
    - Text change 11: `zarr index verify` advice carries the invocation's values.
    - Text change 12: the blocked-range advice prints `<target URI>` and Click's option order.
    - Text change 13: `zarr index rebuild` prints `Error: ...` and exits 1 for the two refusals, with no traceback.
    - Text change 14: the stale-snapshot log line names `ChunkManager.rebuild_snapshot`.
    - Text change 15: preallocate's cleanup log lines name the library operations, and `warn` prints the command.
    - Text change 16: `StorageConfig.validate` names `access_key` and `secret_key`.
    - Text change 17, by class: no Click boundary catches some refusals. They reach the operator as a traceback that ends in the library sentence (sites in `plans/TODO.md`).
    - Text change 18, by class: `PipelineResult.error`, persisted run errors and log lines hold the library sentence.
    - Text change 19, library only: `str(exc)` names parameters, config errors are not `click.UsageError`, and `PipelineFailedBatchesError` is a `FirecubeError`.
    - Text change 20 (maintainer-approved): the line numbers of the traceback frames of `test_indexed_write_error_fails_ingest_with_compilation_error#0` shifted by 22 lines in `direct_zarr.py`.
- Corrections to the brief. The pipeline workers are a thread pool (`engine.py:598`), so nothing pickles a `PipelineResult`. 16 of 17 field-carrying exceptions fail a pickle round trip anyway (`plans/TODO.md`). The scanner of the gate missed module constants, `warn` arguments and the failed-batches tail. The conversion of the command-naming strings covered them. The 57 strings of generated scaffolding in `ingestor/devtools` stay. `core/hints.py` is gone and `cli/_rename_hints.py` is unrelated. One test that parses every rendered command against the real CLI replaces "the hints sweep". The `--show-options` `ValueError` of `configure.py` is text change 17, not a CLI repro.
- Approved widenings. `src/firecube/ingestor/runtime/index_record.py` joined the conversion (`warn(str(exc))` and `detail=str(exc)` flattened the legacy-record `Message`). `src/firecube/ingestor/runtime/zarr/append_failure.py` joined it too: `AppendBatchFailed` became a `FirecubeError` that keeps the cause's `Message`, because the batch-failure line lost `firecube zarr validate` (the independent review reproduced it through `ingest`).
- Edit across modules inside the scope of the change. `ResolvedIndexAttrsConflictError` is a `ManifestError` subclass in `core/errors.py`. Two sites of `manager.py` raise it. It is the discriminator that `zarr index rebuild` catches. The first design probed current state and could convert an unrelated `ManifestError`.

**Coverage.**

- Red runs (ran, at `573748a`, `repro/`): the CLI group had 14 failing `test_repro_*` ids, one per broken hint, through the real CLI (`r1.txt`). The batch-path group had 5 failing ids (`tb.txt`). The config-contract group had 6 failing ids (`tc.txt`), with 3 unchanged-CLI cases green.
- Tests added since `573748a`: 547 lines by `git diff --numstat 573748a 9576ad3 -- tests/`, of the 550 budget. The line caps moved between test groups inside the 550.
- Replay (ran). Baseline pair, `base-1` against `base-2`: 861 records, no problems. Final pair `final-1` against `final-2`: 876 keys, 0 problems, 0 candidate-only keys. `base-1` against `final-1`: 15 candidate-only keys (the new tests) and 21 raw problems. The problems fall to 0 after 32 allowances. Each allowance names one of text changes 1, 2, 4, 5, 9, 10, 11, 12, 15, 18 and 20. Text change 20 covers four frames of one stderr. The maintainer approved one frame, and the other three shifted by the same 22 lines. The start-up module diff is the module rename of the input-filter rows only.
- Review. An independent review read every code, test and prose change. Most changes had two rounds. Two changes had one round. One change had three. The reviews ended APPROVE or APPROVE WITH NOTES. Every REJECT closed before completion.
- Not in this entry: the full lane, ruff, pyright and the revert checks, which run after this commit.

**Findings.**

- Open items found on the way, all in `plans/TODO.md` (section "Open after structured errors (2026-10-20)"): five duplicated exception-text helpers, the loser-branch attrs-drift raise and the `index_record.py` unreadable-record detail, trailing parts the batch summary does not trim, the `product_uri` repr in two facts, the `multires` advice, and the refusals that still end in a traceback.

## 2026-10-19 — The snapshot leftovers: staged-resume reads, dataset writes and archive order (fix/parallel-chunk-ownership)

**Decision.** Five signed commits, nothing pushed, each revertable alone. C1 `573748a` `fix(append): read staged-resume arrays by full path from the root` (code, tests, `CHANGELOG.md`). C2 `573748a` `fix(zarr): dataset writes refresh snapshots, never create them` (code, tests, `CHANGELOG.md`, `AGENTS.md`, `plans/DESIGN.md`). C3 `573748a` `fix(tensogram): archive groups and variables in name order` (code, test, `CHANGELOG.md`). C4 `573748a` `test(arch): catch zarr.api opens and mutated kwargs dicts` (the scanner in `tests/unit/test_no_raw_fsspec_usage.py`). D `docs(plans): record the snapshot leftovers` (`plans/TODO.md`, `plans/DONE.md`): hash pending. Base `573748a`.

- C1: `_compute_touched_data_chunks` and `verify_post_write_integrity` read each array by its full path from the live root, so a group's own snapshot no longer hides grown cells. The group lookups stay as existence checks.
- C2: `ZarrIO.write_dataset` passes `consolidated=False` as a literal keyword and never creates a snapshot. `ZarrIO.refresh_consolidated_metadata(uri)` rewrites the snapshot of each group that already has one and creates none. `build_multires_layers` calls it once after its level loop when a level was written, and `tgm_to_zarr` once after its last data message. The zip restore is untouched.
- C3: `zarr_to_tgm` sorts groups, data variables and coordinates by name (data variables before coordinates). An explicit `group` is unchanged. Existing archives restore as before.
- C4: the live-metadata scanner resolves calls through `zarr.api.synchronous` and `zarr.api.asynchronous` (dotted, aliased, name-imported, bare `import zarr.api.<module>`). It counts a `**name` as passing the flag only when the name is bound once to a qualifying dict display and never mutated. No new hit in `src/firecube`; the three `PERMANENT` rows are unchanged.

**Decision.** The maintainer's choices and the design choices inside them, short.

- Maintainer choices. Item 1 fixes both group-object reads. Item 2 never creates a snapshot and refreshes one that exists. A plain `xr.open_zarr` on a restored store then warns about a slower fallback read. The scanner tightening is done here, as C4. The test budget is 220 lines. Commits use `git commit -sS`, one-line subjects, one commit per item, and the records in a final commit.
- The full path is built as `seed_touched_data_chunks` builds it.
- Item 1 is a quiet-failure fix, because a missed lookup fails quietly.
- The other group-object reads stay. They are recorded in `plans/TODO.md`.
- One method, `ZarrIO.refresh_consolidated_metadata`, runs once per operation. One refresh helper costs 2N-1 group opens plus one consolidate per group that carries a snapshot. The operation as a whole costs 2N group opens, because the wrapper opens the root once first. N counts every group in the product, not only the written ones. The independent review measured it: N=4 gave eight opens. Its default open is the existing `PERMANENT` scanner row.
- A local `.tgm` restore has nothing to refresh. A remote grouped restore onto a full store does. In the simulation the root snapshot listed 14 of 19 nodes without the refresh and 19 with it.
- The multires tests are the first real multires write in the suite.
- The rule text goes in `AGENTS.md` and `plans/DESIGN.md`.
- The archive is written in `str` name order, so the converter fallback and `restore.py` need no edit.
- Byte identity cannot hold (`archived_at`). So the test asserts name order through `tgm_info` and `open_tgm_listing`. It uses four groups, five data variables and three coordinates created in an order that is neither sorted nor reversed.
- The `CHANGELOG.md` changes land in sequence, each in its own commit. This record is one commit. The replay judge is below. The commit subjects are one line each.

**Corrections to the brief.**

- Item 1: the touched-map fix alone was not enough. The integrity check still checked nothing for the grown cells, because it read through the group object too. A probe saw shape `(8,)` where `(12,)` was right. The chosen fix covers both reads.
- Item 2: both callers wrote a non-null root snapshot, because xarray 2026.7.0 consolidates the store root after every write in a mode other than `r+`. A multires write onto a store with a root snapshot refreshed it, so `consolidated=False` alone would have left it stale.
- Item 3: byte-identical `.tgm` files are impossible; coordinate order varied as well and is sorted too.
- The recorder exits 1 at `573748a` because of the known failing test (`plans/TODO.md`), so recordings are judged by record count and `cli_diff`.

**Coverage.**

- Red runs (ran, on `573748a` source, `repro/`): r1 4 failing tests (`None == [(5,)]`, `[999, 0] == [999, 111]`, two `DID NOT RAISE`); r2 cases 1 and 2 failing, cases 3 and 4 passing; f2 showed cases 3 and 4 failing with `write_dataset` changed alone (`f2-no-refresh.txt`) and case 4 failing again with only the restore call left out (`f2-no-restore-refresh.txt`); r3 9 failing subtests in each of three runs; ra the scanner self-check failing.
- Tests added since `573748a`: 206 added lines by `git diff --numstat 573748a -- tests/` (26, 29, 40, 27, 59 and 25 over six files), of the 220 budget. The scanner file also deletes 12 lines. The test budget check against base `573748a` printed `+0 lines` on the committed tree, so the numstat is the count.
- Replay (ran). The judge `order_only.py` accepts only an `archive info` or `archive list` record of a `.tgm` that differs in order alone, compared per group block; checked on four synthetic pairs and four adversarial ones, and on 4b's known result (2 order-only, 3 other).
- Baseline pair, `base-1` against `base-2` (855 records each): 1 difference, order-only (`test_full_archive_restore_cycle`, call 3). That is item 3's symptom at the baseline.
- Final pair, `final-1` against `final-2` (861 records each), no allowance: 0 problems. The order is the same on every run.
- `base-1` against `final-1`: 4 problems and 6 candidate-only keys (the new tests). Three problems are order-only in `archive info` and `archive list` of a `.tgm` (`test_archive_info_shows_groups` call 1, and `test_full_archive_restore_cycle` calls 2 and 3). One is other: `test_archive_info_shows_groups` call 0, the `archive create` summary line `Groups:`, now `Alpha, Beta` where it was `Beta, Alpha`. The maintainer accepted it. No rule or allowance was written.
- Not in this entry: the full lane and the revert checks, which run after this commit.

**Findings.**

- A remote `.tgm` restore ignores `--overwrite` and clears nothing (`_prepare_target`), and a remote restore of a root-group `.tgm` writes with mode `"w"`, which deletes the target prefix, `.firecube/` included (simulated on a local path, not run on S3). Both open in `plans/TODO.md`.
- The group-object reads that still trust a group's own snapshot are listed by function under the audit entry in `plans/TODO.md`.
- Also recorded in `plans/TODO.md`: `from zarr.api import synchronous as zs` is not flagged by the scanner; `build_multires_layers` over S3 with obstore fails in Dask tokenization before the refresh (a `PanicException`, inferred to predate the plan); case 4 of the restore snapshot test covers one group only, so a multi-group refresh is untested.
- Two test authors rewrote tracked test files with python heredocs, which the edit rule forbids. The deviations are recorded. The results were linted and reviewed.

## 2026-10-19 — Zarr and xarray opens that decide read live metadata (fix/parallel-chunk-ownership)

**Decision.** Two signed commits, nothing pushed, each revertable alone. G1 `573748a` `fix(zarr): read live metadata wherever an open decides` holds the source, the red tests, the architecture test, `CHANGELOG.md`, `AGENTS.md`, `plans/DESIGN.md` and `plans/TEST.md`. G2 (`docs(plans): record the consolidated-metadata sweep`: `plans/TODO.md` and `plans/DONE.md`): hash pending. Every zarr or xarray open in `src/firecube` whose result drives a decision passes `use_consolidated=False` (zarr) or `consolidated=False` (xarray), at the site or through the two `ZarrIO` helpers in `src/firecube/core/zarr/io.py`. The intended invariant is that a decision that reads below the root does not trust a group's own snapshot either. The sites this sweep changed do that. Four exceptions remain. The first is the three deliberate default opens. The second is the converter fallback. The third is the opens that were already live and were not audited for descent. The fourth is the known `_compute_touched_data_chunks` gap listed under Findings.

**Ledger.** 34 opens, read at `cfcbe27`.

- 16 stale decision opens fixed: 14 at the site and the two helpers.
- 4 flag-only opens that cannot go stale (`group_exists`, the two root-attribute reads in `manager.py`, `_read_node_type`).
- 4 display opens read live (`advise.py:123`, the restore summary, `zip_info`, `list_zip`).
- 2 `open_array` calls exempt (`metadata_authority.py:499`, `zip_archive.py:242`): `open_array` reads the array's own `zarr.json` and has no `use_consolidated` parameter.
- 3 deliberate defaults, `PERMANENT` rows in the architecture test: `_refresh_consolidated_metadata`, `_refuse_invalid_metadata`, and `validate_zip`'s whole-document parse (which gains a live walk).
- 1 recorded gap: the converter fallback.

**Decision.** Design choices inside the plan.

- Display sites read live (`zip_info`, `list_zip`, `zip_to_zarr`'s summary, advise's batch-size text). The allowlist has no "display only" row.
- `ZarrIO.open_group` passes `use_consolidated=False`. `ZarrIO.open_dataset` passes `consolidated=False` as a literal keyword, so a caller cannot pass `consolidated`. `layers.py` dropped its own `"consolidated": False`.
- Each helper has one red test, through one decision caller (`converter.py:103` for `open_group`, `converter.py:305` for `open_dataset`).
- Opens that cannot go stale still pass the flag, so the rule has no exception for them.
- `open_array` never gets the keyword. On its create path it raises `TypeError` (zarr 3.2.1).
- Three deliberate defaults are `PERMANENT` rows. `use_consolidated=False` drops a snapshot before it is parsed, so a live open no longer fails on a malformed snapshot. The two archive validators that judge whole documents keep a default open. `validate_zip` keeps its default open and walk and adds the live walk. So a child document that does not parse is reported at any depth wherever the snapshot sits.
- The static-marker check is not edited. `_candidate_time_dim` picks `firecube_timestamp_state` by leaf name from the live listing. So only that array's dimension names and shape stop coming from the snapshot.
- `metadata_authority.py:499` is an `open_array` inside the node lock that already reads only its own document. It has no change.
- The architecture test extends `tests/unit/test_no_raw_fsspec_usage.py`. It has a tagged row table modelled on `tests/architecture/test_cli_boundary.py`. A row that matches nothing, or whose count differs, fails.
- The rule covers `zarr.open_group`, `zarr.open`, `zarr.open_consolidated`, `xarray.open_zarr` and `engine="zarr"` opens, spelled through the module, an alias or a name imported from it, function-scope imports included. `**name` counts when the same function assigns that name a dict display holding the key as the literal `False`.
- `append.py:184` (`_read_existing_append_values`) has no caller in `src`, and `group_exists` ignores `group` in its fallback. Both stay as they are and are recorded in `plans/TODO.md`.
- G1 amends `CHANGELOG.md` and updates `AGENTS.md`, `plans/DESIGN.md` and `plans/TEST.md`. G2 closes the `plans/TODO.md` entry for the sweep.
- Descent. A group reached as `root["G"]` or through `require_group` keeps its own snapshot when the root was opened live. So a decision that reads below the root follows three rules. It reads each array by its full path from a live group. It lists a group's children only after opening that group with `use_consolidated=False`. It walks descendants by reopening every group with `use_consolidated=False` and listing it at depth 0. The walk never relies on `members(max_depth=None, use_consolidated_for_children=False)`. The sync `Group.members` of zarr 3.2.1 accepts that flag and does not pass it on. Sites: `state.py`, `staged_metadata.py` and the zip walks.
- The converter fallback (`converter.py` `_open_group_dataset`, after a `KeyError` naming `dimension_names`) still trusts a group's own snapshot through `_group_from_root`. It is recorded and `converter.py` is not edited (maintainer ruling, revision of the first choice).
- Opens that already passed the flag before the sweep were not audited for descent. This is recorded.

**Corrections to the brief.**

- Firecube writes consolidated metadata in two places, `_consolidate_metadata_best_effort` (`write.py`, only when `zarr_consolidate` is true, GenericZarr only) and the refresh in `coord_consolidation.py`; `append.py:995-996` never runs on zarr 3. The sweep changes neither.
- W1 to W3 run the DirectZarr fixture and never consolidate, so no judge sees the live-read cost on a consolidated store.
- `staged_metadata.py:96` was a data-loss path: seeding copies the target root `zarr.json` byte for byte, snapshot included, then reads workspace children through it; a target grown from 8 to 12 under a snapshot of 8 was seeded only to 8 (probe ran). The loss at promotion is inferred, not run.
- A flag at the root does not reach a group's own snapshot (review round 1).

**Coverage.**

- Red runs (ran, on `cfcbe27` source, `repro/`): rA 5 failing ids, rB 7, rC 10, and the architecture test red with exactly 21 offenders and no stale row (`repro/ta.txt`); one bogus row failed as a stale row (`repro/ta-stale.txt`).
- Tests added since `cfcbe27`: 368 added lines by `git diff --numstat cfcbe27 -- tests/` at G1, of the 370 budget. Two existing tests changed without weakening: `tests/unit/test_chunk_manager_slot_index.py` (call dictionary) and the transient-read leaf name in the seed test.
- Replay (ran): 852 base keys compared, 5 problems, 3 candidate-only keys. Three problems are the tree path in output text (`cc4b-base` against `cc4b-work`). Two are the variable order in `archive info` and `archive list` of a `.tgm` in `test_full_archive_restore_cycle` (`DSR,FWI` became `FWI,DSR`). The maintainer accepted all five.
- Perf (ran): at zero allowance the judge failed on S0, W1, W2 and W3 by 1 to 2 events. The events were W2 open +1 and W3 mkdir +2. Three more interleaved rounds each failed by 1 to 2 events, on different events. The maintainer accepted this. The cost on consolidated stores is calculated, not run. On a 5-node store, the survey probes measured these read counts. Zarr takes 4 reads against 11. Xarray takes 1 read against 4.
- Baseline failure, recorded not fixed: `tests/integration/test_cli_input_data_presence.py::test_override_discovery_no_input_data_succeeds` fails in the multi-file recording at `cfcbe27` and at `573748a`. The plugin-registry reload in `tests/integration/test_cli_zarr_preallocate_slot_index.py` leaves the registry with a stale class.

**Findings.**

- `_compute_touched_data_chunks` (`append.py`, about :412 and :421) read arrays through a group object. A group that carried its own snapshot taken before it grew made it return no touched chunks for the grown cells (review reproduced `{'G': {}}` where `{'G': {'val': [(5,)]}}` was right). A staged resume then skipped seeding for them, and whole-tree promotion replaced target cells next to the new data with fill values (review probe: cells 10 and 11 read `[999, 0]` where `[999, 111]` was right). `verify_post_write_integrity` read through the same stale shape and checked nothing for those cells. Fixed by `573748a` (snapshot-leftovers entry above), which reads both by full path from the live root.
- `validate_zip` still passes a stale but well-formed snapshot; `zarr_consolidate` has only a sample line in `docs/reference/config.md:40`. Both open in `plans/TODO.md`. `ZarrIO.write_dataset` was fixed in `573748a`.

## 2026-10-19 — The archive lifecycle runs below the CLI; recovery advice that works; a backfill is reported once recorded (fix/parallel-chunk-ownership)

**Decision.** Two commits, each revertable alone, nothing pushed. The new modules are internal and importable (no facade export, no reference page), as `index_record` is. AB changes no behaviour; C changes text and one output point.

- `cfcbe27` `refactor(archive): move lifecycle and inspection below the CLI` (AB). `src/firecube/cli/archive.py` became the package `src/firecube/cli/archive/` (`__init__.py`, `_common.py`, `_create.py`, `_restore.py`, `_inspect.py`; 59, 52, 260, 190 and 267 lines). `create_archive`, `restore_archive`, `ArchiveCreateResult`, `ArchiveRestoreResult`, `ArchiveClaimError`, `ArchiveConversionError` and `ArchiveRecordError` live in `src/firecube/ingestor/runtime/archive.py`. `tgm_info`, `open_tgm_listing`, `validate_tgm`, `TgmValidation` and `TensogramArchiveError` live in `src/firecube/core/tensogram/inspection.py`. The two `UNTIL archive-lifecycle` rows left `tests/architecture/test_cli_boundary.py`.
- C, `fix(index): recovery advice that works; report a backfill once recorded`, hash pending (not landed when this entry was written). `firecube zarr index verify` and `firecube zarr slots` give a recovery that was run for the three states whose advice named `firecube zarr index rebuild`, which refuses them. `consolidate_time_coords` reports a backfilled consolidated WAL event after `record_time_coord_consolidation` succeeds for it, not before.

**Decision.** Design choices inside the plan.

- The remote-artifact branches of the old `create` and `restore` (temporary paths and transfers) were unreachable and were dropped. They sat behind `StorageUri.from_local_path(...).is_remote()`, and `from_local_path` always yields protocol `file`.
- Three items stay as they were and are recorded in `plans/TODO.md`. Create's `manager.close()` is unguarded and can replace a pending error (restore guards it). A refused restore claim records the run started and failed. Admission is list-then-claim.
- `archive list` on a `.tgm` keeps its interleaving. `open_tgm_listing` is a context manager whose listing decodes each message's metadata as the command iterates. It decodes the objects when asked. So a late decode failure leaves the lines before it on stdout. `tests/unit/test_cli_archive_safety.py` pins it. The pin failed on a planted eager listing.
- `IndexRebuildResult` stays as `rebuild_index_record`'s return value for Python callers (`tests/integration/test_cli_index_rebuild.py` reads it). Rendering the CLI from it would print the two stdout lines after the terminal record, which the ordering test pins.
- Start-up: exactly four new modules at `import firecube.cli.main` (`firecube.cli.archive._common`, `._create`, `._restore`, `._inspect`), accepted by the maintainer.
- The five command decorators changed from `@archive.command(` to `@click.command(` with the same arguments, because the group imports the commands; the other order is circular.

**Coverage.**

- The replay compares CLI output, streams, exit codes and log records of every CliRunner call; it does not capture persisted run records or WAL events, which the existing tests hold.
- Replay (ran): 618 recorded CLI calls against the `cfcbe27` baseline. AB: 0 problems. C: 0 problems with the allowances. There are three kinds of allowance. Four stdout records have the backfill line moved. Two sentences swap the old advice text for the new. The maintainer waived two rules on 2026-10-19. The run-id rule covers only `tests/integration/test_archive_restore_audit.py::test_successful_restore_appears_in_chunks_runs_list_cli`, call 1, stdout. The group-order rule covers only `tests/integration/test_tensogram_archive.py::test_multigroup_archive_restore_cycle`, calls 0 and 1, stdout and stderr. Seven planted changes checked the differ: create, restore stream, info, validate, list, restore warning and claim refusal text. It reported each of them. A wrong-field allowance still differed. A right-field allowance did not differ. The control did not differ.
- Start-up (ran): `bash EV/recorder/startup_check.sh EV/startup-new.txt` exits 0 on AB's tree, so the only added modules are the four above.
- Tests added since `cfcbe27`: 350 lines of the 350 budget (AB 250, C 100). Reproductions failed before each fix: the two backfill cases (`tests/integration/test_consolidate_time_coord_command.py`) and the advice tests of `test_cli_index_verify.py` and `test_direct_zarr_resolved_index.py`. The recovery steps in the advice were run on scratch products and each ended in VERIFIED.

**Findings.**

- The library raises two `ValueError`s the commands cannot reach and no test pins: an unknown `archive_format`, and a zip archive in `create_archive` given `group`, `start_date`, `end_date` or `variables`.
- The converters now resolve at call time inside the library functions, after the CLI has built the session. Tests that patch them on their modules keep working.
- `zarr index rebuild` prints a raw `ManifestError` traceback in the attrs-without-record and attrs-drift states (`plans/DONE.md` 2026-10-18 already noted the raw propagation). Out of scope here; open in `plans/TODO.md`.
- `docs/operations/firecube-index.md` (the `rebuild` section and the table row for a missing or corrupted record) still says `rebuild` recovers a corrupted or foreign-format record. `ChunkManager._apply_resolved_index_precedence` (`manager.py:1341`) and `ResolvedIndexRecord.from_json_bytes` (`types.py:665-712`) refuse such a record. The docs fix is open in `plans/TODO.md`.
- The consolidated-metadata sweep is its own plan, next after this one. The full list of default opens is in `plans/TODO.md`.

## 2026-10-18 — CLI boundary gate; index rebuild and verify, and time-coordinate consolidation run below the CLI (fix/parallel-chunk-ownership)

**Decision.** A tagged architecture gate pins the CLI-boundary debt row by row, and three operations leave Click. Four groups of work on the branch (three committed, the fourth still uncommitted when this entry was written), nothing pushed, each new module internal and importable (no facade export, no reference page), as `coord_materialization` is. Behaviour-preserving except the one text change named below.

- `cfcbe27` `tests/architecture/test_cli_boundary.py`. Six tests: no Click outside `cli/`, no `firecube.cli` import outside `cli/`, no CLI command or flag text in the strings that raise or log in `core/` and `ingestor/`, no private `core` or `ingestor` module imported by `cli/`, a 500-line ceiling per `cli/` module, and well-formed tags. Each allowlist row carries `PERMANENT: <reason>` or `UNTIL <owner>: <reason>`. A row that matches nothing, or whose count or tokens differ, fails, so the allowlist only shrinks by edit. `src/firecube/__main__.py` is a `PERMANENT` row (`python -m firecube` is the CLI's own entry point). The two prose matches and the one dependency-install hint of check 3 are `PERMANENT` rows. The other check-3 rows, the two Click imports and `src/firecube/core/hints.py` are `UNTIL structured-errors`.
- `cfcbe27` `rebuild_index_record` and `verify_index_record` in `src/firecube/ingestor/runtime/index_record.py`, with frozen `IndexRebuildResult` and `IndexVerification` and `IndexRecordError(ConfigurationError)`. `src/firecube/cli/index.py` keeps `_manager`, `_load_plugin`, `_load_record`, `_exit_no_record`, `show` and the rendering. The legacy-index refusal (`check_legacy_index_record`) and the slots legacy warning also name `firecube.ingestor.runtime.index_record.rebuild_index_record`, right after the backticked command.
- `cfcbe27` `consolidate_time_coords` in `src/firecube/core/zarr/coord_consolidation.py`, with frozen `ConsolidationSummary` and `ProductIdentityError(ConfigurationError)`. `src/firecube/cli/zarr/_consolidate.py` shrank to 176 lines and keeps the options, URI policy, the `--chunk-size` check, the s3 refusal, the storage config, the dry-run summary and exit 1.
- The fourth group types `preallocate_product` and `plan_slots` against `PreallocateHost` and adds `BaseIngestor.bind_index_at_startup(ctx)`, which only calls the private `_bind_index_at_startup`. `slot_plan.py` reads `ingestor.time_dim_name` where it called `_resolve_time_dim_name()`. Docstrings stop naming `DirectZarrIngestor` as the host.

**Decision.** Output and error mapping, as in the entry below.

- `rebuild_index_record` takes `report` and `warn` callables, which the plan first left out (it had the CLI render from a result). Rendering after the call let an interrupted terminal record lose the stdout line and delayed the conflict stderr line, so the library calls them at the points where the command echoed, before the finalizing `finally`. The CLI passes `click.echo` and a stderr echo. `IndexRebuildResult` is returned but the CLI no longer renders from it.
- The `ClickException` of the rebuild moved range became `IndexRecordError` with identical text; the CLI catches `IndexRecordError` and `ResolvedIndexConflictError` and nothing else, so what propagated raw before (a plugin `TypeError`, `ManifestError`, `ResolvedIndexClaimTimeoutError`) still does. A plugin's `click.ClickException` from `index_spec` is re-raised unchanged by `verify` and wrapped by `rebuild`, as before.
- `consolidate_time_coords` calls `report(line)` where the command echoed. Its `ClickException`s became `ConfigurationError` (the two time-dim refusals) or `SchemaDriftError` (unexpected recovery state, coordinate-managed, non-datetime dtype, sealing failures) with identical text. `wrap_user_facing_errors` renders both as before. `ProductIdentityError` becomes `click.UsageError`, exit 2, as before.
- Identity resolution keeps its place after the store open and the reference read. The library refuses an s3 storage config with today's text before any store opens; from the CLI that check stays unreachable because the CLI refuses first. The unreachable remote rewrite (`_rewrite_time_coord_remote` and every `remote=True` branch) was deleted, not moved.
- The one intended text change: the legacy-index refusal and the slots legacy warning name the Python function. Four recorded calls carry the suffix.

**Decision.** The consolidation move also fixes the stale-metadata open (`plans/TODO.md`, the consolidated-metadata entry). The root and every descendant group are opened with `use_consolidated=False`, because a subgroup that carries its own consolidated snapshot still hid a live `firecube_coord_managed` stamp when only the root was opened live (found in review). Before the fix, a store whose consolidated metadata predated the stamp exited 0, rewrote the coordinate and replaced the marker with `firecube_preallocated`. The premise that after this only validation reads consolidated metadata is false: `src/firecube/core/zarr/validation.py` (:475, :476, :613, :625, :795), `_read_array_meta_from_store` in `src/firecube/ingestor/runtime/zarr/append.py` (:84), `src/firecube/core/zarr/state.py` (:190, :192, :265, :267), `src/firecube/core/zarr/io.py` (:50) and `src/firecube/core/tensogram/metadata.py` (:212) still open with the default (read in code; not probed).

**Coverage.**

- The replay compares CLI output, streams, exit codes and log records of every CliRunner call on `cfcbe27` against the final tree; it does not capture persisted run records or WAL events, which the existing tests that read them hold.
- Replay (`final.jsonl`, `final-raw.txt`; ran): 546 records, 542 base keys. Without allowances 4 problems, all the suffix. With `allow-b.args` 0 problems. 4 records carry the suffix and 4 keys exist only in the candidate (the new tests). The differ was checked by planted changes (`planted/`: consolidate text, consolidate exit, rebuild stream, verify text, index refusal each differ; the control does not).
- Start-up: `import firecube.cli.main` loads 1545 modules against 1546 on `cfcbe27` (`startup-modules.txt`).
- Tests added since `cfcbe27`: 768 lines by `git diff --numstat cfcbe27 -- tests/`. The line counts are: gate 440, index tests 137, consolidation reproduction 25, consolidation tests and patch repoint 103, host Protocol test and markers 63. The maintainer raised the budget from 650 to 800. The test budget check reads the frozen 650 in the plan header and reports over budget.
- The gate: red on the clean tree for every test but the tag test, green with rows, and stale rows, a malformed tag and a swapped token each fail (`gate/red.txt`, `gate/stale.txt`).
- The reproduction of the consolidation symptom failed before the fix and passes after it (`repro/rc-red.txt`): `tests/integration/test_cross_regime_marker_refusals.py::test_consolidate_refuses_coord_managed_despite_stale_consolidated_metadata`.

**Findings.**

- The earlier layering review (section 2) is stale: of its 97 rows, 27 still sit at their line, and its hints rows cite `core/controlplane/hints.py`, which no longer exists. The gate's rows come from a scan of the `cfcbe27` tree.
- The observability boundary test has no stale-row check and no `UNTIL` tag; the new gate adds both.
- Check 2 was not empty: `src/firecube/__main__.py` imports `firecube.cli.main`. Check 3 misses literals that reach the user through a variable, a return value or a `warn(...)` call (23 sites, for example `core/zarr/guard/engine_owned.py` and `ingestor/runtime/slot_plan.py`), and a command split around an f-string placeholder. Recorded, not closed.
- The consolidation writes still bypass `guarded_write` and `MetadataAuthority` and take no claim (`plans/TODO.md`, the `consolidate-time-coord` entry stays open). The move put that code inside `core/zarr/`. The post-delete recovery also renames a managed sibling before refusing (pre-existing).
- An array named `<coord>.consolidating` is still deleted by consolidation (`plans/TODO.md`, identity in strings), recorded, not scheduled.
- `rebuild` prints "Rebuilt index record" when `ensure_resolved_index` only re-mirrored the attribute (outcome `created`); a `ManifestError` from it propagates raw and is untested; `verify` passes `plugin or product_name` as the plugin name in the legacy message.
- The index commands configure no tier config on the plugin; preserved.
- Citations to `plans/DONE.md:97-103` and `:99` for the coordinate-materializer precedent were wrong; the precedent is the 2026-09-01 entry below.
- The three advice texts that name `firecube zarr index rebuild` for states the command refuses stay untouched: `plan_slots` in `slot_plan.py` and the two attrs-drift messages in `src/firecube/cli/index.py` (`ensure_resolved_index` refuses a root attribute without a record, a matching record with a diverging attribute, and a hash mismatch).
- Strings that name a CLI command or flag and moved below `cli/` verbatim: the `--time-dim` refusals in `core/zarr/coord_consolidation.py` (`consolidate_time_coords`, `_resolve_time_dim_for_group`). They are `UNTIL structured-errors` rows of the gate. The index move took none.
- Two failures found while executing, not fixed. The first is present without this work; the second is a load-sensitive candidate with no clean-base reproduction: `tests/integration/test_slots_auto_input_data.py::test_slots_auto_uses_input_data` fails alone on the clean branch (it parses `CliRunner` `result.output`, which mixes stderr; `init_observability` binds the log handler to the first runner's stderr per process, `src/firecube/core/observability/logging.py:112`), and `tests/integration/test_concurrent_preallocate_race.py::test_two_concurrent_preallocates_with_slot_group_env_share_a_single_claim_and_never_overlap` failed once under load with `ControlPlaneCorruptionError` on a `.firecube-atomic-*.tmp` claim file and passed twice alone. Both are open in `plans/TODO.md`.

## 2026-10-18 — `zarr preallocate` and `zarr slots` run below the CLI (fix/parallel-chunk-ownership)

**Decision.** The preallocate operation and the slot plan no longer live in Click modules. Four commits, behaviour-preserving, nothing renamed publicly. The new modules are internal and importable, as `coord_materialization` is: no export from `firecube.ingestor.api`, no reference page.

- `cfcbe27` `configure_ingestor` in `src/firecube/ingestor/runtime/configure.py`. It builds the contexts and runs `TierConfigurator` without Click. `_configure_ingestor_for_cli` still coerces the options and delegates.
- `cfcbe27` The slot algebra (`merge_intervals`, `complement_intervals`, `iter_partitions`, `partition_count`, `chunk_aligned_remaining`, `resolve_per_group_slot_sizes`, `smallest_executable_slot_size`) moved to `src/firecube/core/zarr/planning.py`. `plan_slots`, `GroupSlotPlan`, `PLAN_RANGE_CEILING`, `SlotPlanError`, `SlotPlanBlockedError`, `SlotPlanCeilingError`, `_stored_time_units` and `_query_slots_coverage` moved to `src/firecube/ingestor/runtime/slot_plan.py`. The commit deletes `src/firecube/cli/_slot_planning.py` and `_partition_remaining`, which had no caller. `PLAN_SCHEMA_VERSION` and `PLAN_RANGE_HINT` live in `src/firecube/cli/zarr/_slots.py`.
- `cfcbe27` `preallocate_product`, `PreallocateDryRunGroup` and `SlotWindowError` in `src/firecube/ingestor/runtime/preallocate.py`, with the helpers `_discover_engine_owned_values`, `_ensure_ancestors`, `_resolve_preallocate_windows`, `_refuse_ownership_change`, `_create_array` and `_materialize_engine_owned_arrays`. The command keeps options, input filters, URI policy, storage config, binding, session, `ChunkManager`, plugin lookup, ingestor construction and configuration. It closes the `ChunkManager` in `finally`.
- `cfcbe27` The four refusals that ask for `firecube zarr preallocate` also name `firecube.ingestor.runtime.preallocate.preallocate_product`.

Both operations are half-open `[start, end)` throughout. `firecube.core.controlplane.ranges` is inclusive. The one conversion happens where the code reads stored coverage.

**Decision.** Output and error mapping. The library calls `report(line)` where the command echoed to stdout. It calls `warn(line)` where the command echoed to stderr. The text and the point stay the same, both JSON documents included. Printing the summary after the call returns would drop it when the terminal record fails. The dry-run preview is the exception: `on_dry_run_group` receives one `PreallocateDryRunGroup` per group, and the command prints today's lines from it. The command passes the `firecube.cli.zarr` logger, so log records keep their name. `slots` passes `_warn_stderr` as `warn`, so the drift warnings still go to descriptor 2.

- Each `ClickException` in the moved preallocate range became `ConfigurationError` (caller input) or `SchemaDriftError` (store state) with identical text. `wrap_user_facing_errors` renders both as `ClickException(str(exc))`, exit 1.
- The window `click.UsageError` texts became `SlotWindowError`. The command maps it back to `click.UsageError`, so exit 2 and the usage block stay.
- `LegacyIndexRecordError` surfaces as `SchemaDriftError`.
- The change deletes two `ClickException` re-wraps of an already-typed error, because `wrap_user_facing_errors` renders the same text.
- `slots` has no `wrap_user_facing_errors`. The command catches `SlotPlanError` and nothing else, and raises today's `ClickException`. `resolve_per_group_slot_sizes` raises `ConfigurationError`. `plan_slots` converts it to `SlotPlanError` at both calls (`slot_plan.py`). The command converts the `SlotPlanError` family to `click.ClickException`.
- The failure and terminal run records carry fixed texts, so the exception type change leaves them unchanged.

**Findings.**

- Eight test files changed.
  - `tests/integration/test_cli_zarr_preallocate.py` and `tests/integration/test_preallocate_auto_rebuild.py` patch `firecube.core.observability.create_ingestion_telemetry` instead of `firecube.cli.zarr._preallocate.observability.create_ingestion_telemetry`. It is the same module attribute.
  - `tests/unit/test_cli_slot_planning.py`, `tests/integration/test_direct_zarr_shard_sharing_reproduction.py`, `tests/integration/test_phase3_5_blocked_ranges.py` and `tests/unit/test_cli_zarr_slots.py` import from or patch the new homes. The invalid-explicit case in `test_cli_slot_planning.py` expects `ConfigurationError` instead of `click.ClickException`.
  - `tests/integration/test_horizon_growth_rejects_shape_drift.py` expects the exact shape-refusal text with the new parenthetical.
  - `tests/unit/test_cli_zarr_preallocate.py` and `tests/unit/test_cli_zarr_slots.py` also hold the new `preallocate_product` and `plan_slots` tests. The three `plan_slots` tests carry no definition-time markers.
- `tests/integration/test_preallocate_auto_rebuild.py` never asserts that the code uses its telemetry fake. Its unbounded group bypasses the factory. The test therefore cannot prove that the patch intercepts the call. This limit existed before the change.
- `tests/architecture/test_cli_no_private_cross_subsystem_imports.py` flags underscore names in `ImportFrom` only. It does not catch private module paths (`guard._layout`), private calls (`_bind_index_at_startup`) or private attributes (`_storage_config`, `_open_root`). The change did not widen it.
- Those private accesses left `cli/` and still exist in `src/firecube/ingestor/runtime/`. `preallocate.py`, `slot_plan.py` and `parallel_gate.py` call `_bind_index_at_startup`. `preallocate.py` holds the `_storage_config` getattr and `_open_root`. `preallocate.py`, `parallel_gate.py` and `planning.py` import `guard._layout`. The change adds no public alias.
- CLI-naming strings moved below `cli/` verbatim, for the structured-errors plan.
  - `preallocate.py`: the `--input-data` text of the engine-owned discovery refusal, and the `--slot-group`, `--slot-start` and `--slot-end` window messages.
  - `slot_plan.py`: the `--input-data` AUTO-discovery text, two `firecube zarr index rebuild` texts, the coverage-lookup text naming `--no-resume`, and the unreadable-layout text.
  - `core/zarr/planning.py`: the two `--slot-size` texts.
- The coverage query builds its storage config without the `anonymous` override that the command's own config has. The change keeps that gap and does not fix it.
- The brief named shared window-planning helpers. They do not exist. The two commands share only plugin configuration and two functions of `parallel_gate.py`.
- A plugin-raised `click.ClickException` from `index_spec`, binding or `resolved_index` now shows `returned no index_spec`, because the library cannot import Click. No in-tree plugin or fixture raises it.
- Three open slots items stay as they were. An explicit `--slot-size` can plan a last window that pods refuse. Obstore `exists()` reads a denied target as empty. The `click.Abort` handling of `_stored_time_units` has not changed.

**Coverage.** Two test files lock the library contract.

- `tests/unit/test_cli_zarr_preallocate.py`: a live run reports through `report` and `warn` and writes nothing to stdout or stderr. A dry run calls `on_dry_run_group` once per bounded group and creates no array. An out-of-group window raises `SlotWindowError` before any array exists.
- `tests/unit/test_cli_zarr_slots.py`: one `GroupSlotPlan` per bounded group with a lazy `ranges()`. `SlotPlanBlockedError`. `SlotPlanCeilingError` and the override.

The plan measured behaviour and did not assert it (measured). 484 recorded `CliRunner` calls of the CLI-driving tests ran on the final tree and on `b07aa49`. 484 normalised records compare equal under the recorded allowances: the parenthetical of `cfcbe27` (4 calls) and two differences that the maintainer accepted. The first difference is the log `lineno` of `configure.py`, which moved with the added code. The second is a thread race over which of two parallel batches logs a failure in `tests/integration/test_chunk_ownership_races.py`. The recording covers stdout, stderr, exit code, exception type, `firecube` log records and descriptor-level writes. The harness has limits. It masks path-dependent identity digests when they are well-formed 64-hex. It re-serialises parallel-run JSON after sorting object lists. It decodes stderr and descriptor bytes with replacement. Its clock and 32-hex normalisation is broader than the identified fields. The allowances apply to every string field.

**Results.** Performance against a same-job `b07aa49` control, five runs per scenario (measured).

- The performance judge reported FAIL on the final tree. The judge allows no increase in the median of any counted event.
- W2 `open` is 10820 against 10819. W2 `os.scandir` is 45 against 44.
- The +1 `open` is in the `zarr preallocate` step: 1958 on the control and 1959 on the final tree, in 20 of 20 runs. The count is repeatable. Other event counts vary by run. For example, `os.scandir` in W2 ranged from 40 to 48 per run across both trees.
- The maintainer accepted the +1 `open`. The gate text stays.
- The ingest-window step keeps its median `open` count in all four measured rounds (8860 or 8861 on both trees).
- The cause is one more module file, `firecube.ingestor.runtime.preallocate`, opened once per preallocate process (inferred, not measured).
- Start-up modules: 1546, equal to the baseline. S0 (`firecube --help`) is identical on every counted event.
- W2 wall medians were 1.40–1.49 s on the final tree and 1.44–1.45 s on the control. The gate counts events. It reports wall time without gating it.

## 2026-10-18 — The branch's data-loss paths are closed (fix/parallel-chunk-ownership)

**Decision.** Before the branch merges, three reproduced data-loss paths close and one avoidable perf delta is cut. The staged `unclaimed` PUT, the ledger exclusion and the zip restore are new on this branch, so no release carries them. The `.tgm` delete-first, its late start record and its clear-by-domain are in v0.1.7. There is one commit per symptom. The revert check tests that each commit reverts alone. It has not run yet. Nothing is pushed. Five commits sit on top of 300a3cc:

- `b07aa49` `test: fix the pyright errors in two unit test modules`. `uv run pyright` exited 1 on 300a3cc with 11 errors in `tests/unit/test_chunk_manager_resolved_index.py` and `tests/unit/test_cli_archive_safety.py`.
- `b07aa49` `perf(zarr): seed the workspace root metadata only when it changed`.
- `b07aa49` `fix(archive): refuse a target that is not the session product`.
- `b07aa49` `fix(zarr): publish no seeded chunk of a managed coordinate`.
- `b07aa49` `fix(archive): validate before overwrite and keep the restore claim`.

**Decision.** Symptom 1: a staged promotion reverted a concurrent `zarr preallocate` fill. A slot of the time coordinate went back to NaT, and every exit code was 0. Seeding copies the coordinate's chunks into the workspace as a snapshot. Promotion PUT that copy as `unclaimed`. `b07aa49` closed it. A data object of a managed time coordinate is planned UNCHANGED with reason `engine-managed`. Promotion never compares it and never PUTs it. The rule covers only the configured time coordinate (`time_dim_name`), and only when its final-target `zarr.json` marks it preallocated or coordinate-managed. No other array is classified, marked or not. An unmarked time coordinate's staged PUT is how a run's timestamps reach the target. A mutant that skipped every coordinate chunk failed four existing staged tests. The write session classifies two sets. One is the coordinates that seeding reported, through a new one-method Protocol, `_SeededCoordinates`. The other is the coordinate of each group the run wrote. It reads one target `zarr.json` per candidate. An invalid marker state raises `SchemaDriftError` before the target is touched.

**Decision.** Symptom 2: `firecube archive restore --overwrite` deleted the target before it validated the archive. The delete took `.firecube/` and the restore's own claim with it. An invalid archive deleted the original. A second restore without `--overwrite` saw no claim and wrote beside the first. `b07aa49` closed it. The zip restore parses every `zarr.json` entry before any delete. The `.tgm` restore reads the message count, the version of message 0 and the structure. An overwrite on a local target keeps `.firecube/claims/`, `.firecube/schema.json` and `.firecube/runs/<keep_run_id>/`. It deletes the rest of the old control plane. A symlink at the target or on a kept path raises `ConfigurationError` before any delete. For library callers of `tgm_to_zarr`, `parse_target` refuses a relative target before any delete. A remote `.tgm` target is unchanged, and its root group still uses mode `"w"`. The CLI keeps one manager, one recorder and the claim handle for the whole restore. It records `started` after the claim and releases the claim by handle in `finally`. A failure to record the failure, to release the claim or to close the manager prints a `Warning:` line. It never replaces the restore error. A failure to record the start, or to record the success, surfaces as the error itself. The CLI still attempts to record the failure and releases the claim. The maintainer chose to replace in place and not to stage and swap (2026-10-18). The residual is in `plans/TODO.md`.

**Decision.** Symptom 3: `zip_to_zarr` deleted `target` and wrote to `session.product.product_uri`. Nothing compared the two. `b07aa49` closed it. `require_target_is_product` in `src/firecube/core/uris.py` raises `ConfigurationError` when the two differ. `zip_to_zarr` calls it first. `tgm_to_zarr` calls it after `require_tensogram` when a session is passed. It is a refusal and not a single identity, because dropping `target` would change the CLI calls.

**Decision.** Perf (`b07aa49`). `_seed_root_metadata` replaced the workspace root `zarr.json` before every batch, 20 batches in W3. The root document now goes through `update_document(..., lambda _current: data)`. It is written only when its bytes differ. The plain-file path compares the bytes itself. W3 `os.rename` fell from 253 to 234 (measured with `measure.py`).

**Results.** Per change, from reruns (ran):

- Pyright fix: `uv run pyright` reported 0 errors, and 22 tests passed. The independent review approved.
- Symptom 1: the second regression test failed on the fix tree with one slot reverted (1 failed). The union lane of the second fix passed 145 tests. The merged tree passed 74. The review approved the tests with notes and approved the fix. Its real-host probe showed the DirectZarr host forwarding `time_coord_name` `"t"` and classifying `g/t` and `h/nested/t`.
- Symptom 3: red on 300a3cc (two `DID NOT RAISE ConfigurationError` at `tests/architecture/test_storage_flows.py:532`). After the fix, 73 tests passed. The review approved. It probed normalization variants and the CLI binding, and both were equal.
- Perf change: `measure` printed 234 against a limit of 240. One test patched `publish_document` and now patches `update_document`. The review approved with notes.
- Symptom 2: the regression tests showed 10 failed on b07aa49. After the fix, 102 tests passed on its lane and 68 on the merged tree. The review rejected twice. The first blocker was a symlinked `.firecube` or `.firecube/runs` that the delete followed. The second was a remote `.tgm` root-group restore that kept stale arrays. Round 3 approved with notes and found a relative local target that deleted before `parse_target` refused it. The same commit fixed that. A narrow review in round 4 approved with notes.
- Test budget: the test budget check against base `300a3cc` counts +275 physical test lines against 250. The plan neither trimmed nor loosened it. The maintainer rules.

**Decision.** Perf delta against v0.1.7 (maintainer ruling, 2026-10-18). The perf job ran `PERF_RUNS=5` on a `git archive` export of the tip and of 300a3cc in the same job. After the allowance, every counted event of the tip is at most the control's. The allowance is one more `open` and one more `open@root` in W3. It is the price of the one target `zarr.json` read per staged promotion. The wall term did not pass at this fixture size of 200 slots. The W3 median of the tip was 1.269 s against an original control at 1.070 s. An interleaved rerun gave control 1.269 s and 1.218 s, and tip 1.218 s and 1.469 s, with one 3.2 s outlier. Rerun control-1 is 1.186 times the original control. Tip-1 is below control-1. The two rerun control medians differ by 1.042 times. The maintainer accepted the counted events as the gate. That acceptance rests on the event counts and does not claim a wall result. The medians below are measured over five runs on Python 3.13.15.

| Event | v0.1.7 | 300a3cc control | Tip |
|-------|--------|-----------------|-----|
| W3 `os.rename` | 230 | 253 | 234 |
| W3 `os.mkdir` | 524 | 542 | 523 |
| W3 `open` | 3192 | 3232 | 3233 |
| W3 `open@root` | 268 | 285 | 285 |
| W3 `os.scandir` | 157 | 157 | 157 |
| W2 `open@root` | 1092 | 1176 | 1176 |
| W2 `os.scandir` | 154 | 45 | 45 |

The plan records the causes of the remaining deltas against v0.1.7 and does not change them.

- W3 `os.rename` +4: atomic chunk publication and promoted `zarr.json` through `update_document`, less offsets. The control's +23 over v0.1.7 was +20 from the root re-seed, +3 chunk publications, +3 promoted `zarr.json` and -3 offsets (ran, audit-hook trace). `b07aa49` replaced the 20 per-batch publications with one, which is 19 fewer renames (253 to 234).
- W3 `open@root` +17: promotion's publish temporaries, `.partial` writes and comparison reads (per-site rows inferred).
- W3 `open` +41: the same promotion costs plus the one allowed `zarr.json` read. Nobody traced this delta site by site (not checked).
- W2 `open@root` +84 comes from three sources. They are per-batch `zarr.json` reads of the window ownership check, the read-only root open in the indexed region writer and the engine-owned checks. The per-site rows are inferred.
- W2 `os.scandir` fell.
- W1 on the tip equals the control: `open` 3154, `os.rename` 227, `open@root` 1043. Against v0.1.7 it differs by +24 `open`, -3 `os.rename` and -12 `open@root` (calculated from the three `raw.json` files).
- Walls do not compare with the saved v0.1.7 baselines. The earlier survey (F25) exported trees without bytecode. The `pf` recipe compiled both of its trees, and no compile log was kept.

**Coverage.** `tests/unit/test_storage_completion.py` (managed-coordinate promotion, including a group the run never wrote), `tests/architecture/test_storage_flows.py` (identity refusal), `tests/integration/test_maintenance_claims.py` and `tests/integration/test_maintenance_audit.py` (restore claim, ordering, recorded failure), `tests/unit/test_cli_archive_safety.py` and `tests/unit/test_zarr_staged_metadata.py` lock the behavior. `plans/TODO.md` lists the open and untested residuals under "Open after the branch data-loss fixes (2026-10-18)".

## 2026-10-11 — Parallel writers never share a Zarr chunk or shard

**Decision.** Slot-window pods, in-pod `pipeline_workers` threads and staged promotion silently lost slots when they shared a chunk or shard along time (measured on c160535: 12 single-slot pods on one 12-slot chunk lost slots in 10/10 barrier reps in direct and in staged mode, about 11 of 12 slots per rep). Fix shape: engine-owned routine plus safety net. `ZarrArraySpec(engine_owned=True)` with `ItemInfo(values=...)` makes preallocate write a rank-1 per-slot array once and pods verify it. One layout resolution (`core/zarr/guard/_layout.py`) applied at creation gives undeclared pod-written time-indexed arrays (no chunks, no shards) time chunk 1 with one warning, unless zarr's own whole-array chunks already have time chunk 1: those are kept and nothing is logged; shards-only declarations keep their shards. An ownership check on the stored layout (`core/zarr/guard/chunk_geometry.py`) refuses windows that split a stored unit, so existing unsafe stores are refused rather than repaired. Threads serialise per (store, array, unit); a run's batches share its schema claim.

**Decision.** Staged runs must write every cell of each promoted unit; sparse staged writes are refused (seeding deferred). Serial staged runs are refused just before promotion, after workspace writes, with the target untouched, because a serial run has no slot plan before its batches. Staged engine-owned values are verified against the final target instead of seeding the workspace, since seeded chunks would be promoted back.

**Decision.** Staged torn reads: promotion re-uploaded every staged file, including the seeded unchanged root `zarr.json` and time coordinate chunk, with an in-place truncating open, so a concurrent pod seeding or opening the target read zero bytes (measured: a torn read in 5 of 20 repetitions of 12 pods on c160535, 1 of 20 on the fixed tip). `StorageSession` now publishes every upload to a local target, both size branches, by writing a reserved `.firecube-publish-*.tmp` sibling and `os.replace`-ing it into place, after copying an existing regular destination's permission bits onto it; both drivers leave those names out of `find`/`ls`, and seeding also skips Zarr `*.partial` files. Remote uploads are unchanged (already whole-object). Skipping unchanged seeded files was not done. Coverage: none. No test under `tests/` exercises the publication temporary (a reader during a paused copy, copy-failure cleanup, the kept destination mode, the hidden `.firecube-publish-*.tmp` names or the skipped `*.partial` files); the torn-read numbers above came from the 12-pod measurement, not from a test.

**Decision.** Publication detail dropped from the CHANGELOG (2026-10-11): a copy that fails removes its temporary and leaves the old object (`src/firecube/core/filesystem/publication.py`; no test covers it); the replace does not fsync, so it adds no crash durability; a killed promotion can leave `.firecube-publish-*.tmp` files that Firecube's listings and seeding skip but Zarr's own listings can see. A payload or static write aimed at a preallocated or coord-managed time coordinate is verified against the stored values (identical: no write; different: `SchemaDriftError`) rather than overwriting them. A run's concurrent batches share its schema claim instead of failing on it.

**Decision.** Safe existing layouts keep their bytes: the stored chunks and shards of arrays whose layout was already safe on c160535 are unchanged by the new creation rules, checked against a manifest of those stores recorded before the change. `_compute_schema_hash` stays on declared values. `write_1d` raises `IndexError` for an out-of-range slot (was a silent no-op). Not in scope and unchanged: FCI plugin, repair of existing cubes, GenericZarr layouts.

**Decision.** Accepted exception to the "no shared mutation in workers" invariant (`plans/DESIGN.md`, Architectural Invariants): the write path adds process-wide coordination state that the `pipeline_workers` threads of one process share. `_UNIT_LOCKS` in `src/firecube/core/zarr/guard/_unit_locks.py` holds locks and their reference counts, keyed by (store identity, array path, stored unit); an entry exists only while a thread holds or waits for it. `RunSchemaClaims` (`src/firecube/ingestor/runtime/zarr/write_session.py`) holds a plain lock around the lookup and release of the run's schema claims, and one setup lock per (run, group) that serialises schema setup; the run's staged ledger is shared by its batches the same way. None of it carries payload data between workers. It is shared on purpose: two threads that read-modify-write one stored unit lose a slot unless they exclude each other, and passing each worker an immutable copy cannot do that. The static-array transaction takes the `_UNIT_LOCKS` table with the whole-array key `(store, array, ())`. This is per-process protection. Between processes, pod-written time-indexed arrays stay safe through slot windows that cover whole stored units and the control-plane claims; a static array is not covered that way, and its transaction is not serialised between processes.

**Coverage.** `tests/integration/test_engine_owned_arrays.py` (preallocate then ingest, refusals, partial stored units, staged promotion), `tests/integration/test_chunk_ownership_races.py` (forced interleavings, no timing) and `tests/unit/test_index_spec.py` lock the behavior. Finer cases are not tested.

## 2026-10-10 — Calendar time code lives in `core/time/` (#77)

**Decision.** The calendar vocabulary, the Gregorian fast path, the coordinate-encoding selection and the calendar value guard are in the package `firecube/core/time/`. The fill helpers are in `core/zarr/_fill.py`. The shared coordinate helpers and the re-materialization checks are in `core/zarr/_coord_common.py` and `core/zarr/coord_rematerialization.py`. `core/time/encoding.py` is the only place that derives the storage encoding. Nothing is added to `firecube.core.api` or `firecube.ingestor.api`. A moved name leaves no alias.

- A module of `core/time/` imports only the modules before it in this list: `_codecs`, `calendar`, `guard`, `gregorian`, `encode`, `canonical`, `convert`, `encoding`.
- `coord_to_datetime64` moved from `coord_materialization.py` to `core/time/convert.py`. `_fill_value_is_missing` and `_array_is_all_fill` moved from `region_writer.py` to `core/zarr/_fill.py`.
- The refusals that carry fields are `Datetime64RangeError` (`value`), `DuplicateAxisInstantError` (`coordinate`, `slots`), `CoordinateRelabelError` (`coord_path`, `stored`, `incoming`, `differs_in`), `UnparseableSpanBoundsError` (`spans`) and `CalendarArchiveError` (`coordinate`, `calendar`).

**Coverage.**

- `tests/unit/test_time_encode.py`, `tests/unit/test_zarr_fill.py`, `tests/unit/test_epoch_parse_helper.py` and `tests/unit/test_coord_rematerialization.py` cover the moved code.
- `tests/unit/test_group_identity_hash.py` and `tests/unit/test_time_axis_declaration.py` keep Gregorian hashes and declarations unchanged.
- The landing of 2026-10-20 records the gates and the acceptance evidence.

## 2026-10-05 — `delete-span` on cubes with static data variables (#83)

**Decision.** `GenericZarrIngestor` span records list time-indexed data variables only; static variables such as `lat_bnds` and `lon_bnds` are written once and have no time slots to cover. Records already written with static arrays in `span.arrays` stay valid: `delete-span` skips arrays whose known dimension names lack the span's time dimension and reports them as one warning per span. It does not rewrite old records.

**Decision.** A span region fill validates everything before it writes. Every listed array must exist, every range must lie inside its array's time length (zarr silently clips an out-of-range slice), each fill value must be representable, and with state updates the state array must be 1-D on the time dimension with the deleted-state value fitting its dtype. A failing pre-flight leaves the cube unchanged; before, data variables could be NaN-filled while the state stayed `present`. This is not a rollback: a storage failure during the write pass can still leave some arrays filled. An array with `ndim >= 1` and no dimension names is refused, because it cannot be shown to be static, and so is a span in which no array carries the time dimension (which is also what a wrong `--time-dim` looks like).

**Decision.** CF-encoded numeric time arrays (`units` containing `since`) are filled with NaT chosen by dtype, not through `encode_time_array`, which raises for all-NaT input with the `standard`/`gregorian` calendars. The sentinel is the declared `_FillValue` if present, else the `int64` minimum, else NaN for floats; an integer dtype other than `int64` without a declared `_FillValue` is refused. The Zarr fill value (`0`) decodes to the reference epoch and is a valid date. `chunks delete-span` exits 1 when it reports errors.

**Coverage.** `tests/integration/test_generic_zarr_static_vars_span.py`, `tests/integration/test_delete_span_region_nan_fill.py`, `tests/unit/test_deletion_fill_value.py`, `tests/unit/test_cli_chunks_delete.py`, `tests/unit/test_deletion_missing_key_not_error.py`, and `tests/unit/test_append_stale_consolidated_metadata.py` lock the behavior.

## 2026-09-23 — Calendar as a property of every time axis

**Context.** A time axis on the direct write path always resolved and stored coordinates in the Gregorian calendar. A source on another day-counting calendar was read again as Gregorian. The store held plausible but wrong dates, and no error was raised. A staged time coordinate on such a calendar crashed the second ingest run. A first design added the calendar as an optional extra with no default. That left "declared Gregorian" and "undeclared" as two states for one behaviour. Also, `firecube archive` had no defined behaviour for a non-Gregorian time coordinate, and `firecube zarr validate` did not treat the fill value of an encoded coordinate as missing.

**Decision.** Every time axis has a calendar. `calendar` defaults to `proleptic_gregorian`. `standard` and `gregorian` are aliases of it. A date before 1582-10-15 on a Gregorian axis follows the proleptic rule. An alias and an unset `calendar` resolve to the same declaration.

- One function, `encode_coordinate`, places every value on its axis for every calendar. A Gregorian value takes a fast path that needs no time-coding library. The engine has no calendar arithmetic of its own. It delegates calendar-shaped values to the existing time-coding dependency.
- One descriptor, `CoordinateEncoding`, selects the on-disk form. A Gregorian axis keeps the date type it always had. A non-Gregorian axis stores a CF-encoded count of a fixed unit since a reference point, with `units` and `calendar` attrs.
- The coordinate of a non-Gregorian axis must be complete before any ingest run. `firecube zarr preallocate` writes it. Ingest verifies incoming values against it. Ingest never creates or fills it.
- A non-Gregorian calendar with observed (`floor`) placement or an open-ended `end_date` raises at declaration time. The message names the supported alternative.
- A value on a calendar that the axis does not declare is refused. A Gregorian value given to a non-Gregorian axis is refused.
- The persisted index record and the group identity include the calendar only when it is not Gregorian-like. Every existing store, index record and identity hash stays byte-identical.
- `firecube archive create --archive-format tensogram` refuses a store whose time coordinate is on a non-Gregorian calendar. The message names the coordinate. The run is recorded failed. `--archive-format zip` copies the store, so it keeps `units`, `calendar` and values. `archive restore` restores a `.zip` or a `.tgm`. A restore of a `.tgm` does not restore the CF encoding (see `plans/TODO.md`).
- `firecube zarr validate` treats the fill value of an encoded calendar coordinate as missing. It keeps duplicate detection for every other coordinate.

**Decision (2026-10-10, review fixes).**

- A bare number on a Gregorian regular axis stays a `TypeError`, as in 0.1.7.
- The `datetime64[ns]` range check is at the shared Gregorian conversion. `gregorian_to_datetime64_ns` refuses an instant outside 1677-09-21 to 2262-04-11 and raises `Datetime64RangeError`. The conversion of plugin coordinate values to `datetime64[ns]` goes through it. The legacy timestamp writer applies it only when the stored coordinate dtype is `datetime64[ns]`. Conversions to epoch seconds, `encode_coordinate` and slot position accept later dates.
- Re-materialization of a stored coordinate is refused before any write when its epoch, `units` or calendar would change. The coordinate checks raise `CoordinateRelabelError`. The preflight runs for all groups before the first group is written.
- `chunks list --time-range` and `chunks delete --time-range` refuse spans whose bounds cannot be compared, for example a `360_day` 30 February. They delete nothing. The refusal is opt-in on the control-plane filter, so force-reingest and resume keep working on a `360_day` store.
- A consolidated snapshot of a child group does not hide a calendar coordinate from the relabel check. The check reads the array by its full path from a live root.
- `firecube zarr validate` and `consolidate-time-coord` treat an integer coordinate as a calendar coordinate only when its `calendar` attr is not Gregorian-like. A Gregorian integer coordinate that carries `units` and `calendar` keeps ordinary handling.

**Consequences.**

- A plugin author declares the data calendar, or writes `calendar="standard"` for clarity. A Gregorian declaration gives the same store as before.
- A staged time coordinate on a non-Gregorian calendar no longer crashes the second ingest run and no longer records empty coverage bounds.
- A coordinate-resolution error names the calendar of the value and of the axis.
- `firecube zarr index show --derived` and the preallocate dry run render calendar dates on the axis calendar.
- `firecube zarr consolidate-time-coord` changes nothing on a calendar store, because preallocate left the coordinate complete.
- The startup check that refuses a non-Gregorian group with an unpreallocated coordinate always resolves the final target. A staged ingest is not refused by mistake.

**Coverage.** Landed test homes, by behaviour:

- Axis declaration, calendar names, encoding, identity hash and resolution: `tests/unit/test_time_encode.py`, `tests/unit/test_time_axis_declaration.py`, `tests/unit/test_group_identity_hash.py`, `tests/unit/test_index_resolve_irregular.py`, `tests/unit/test_index_binding.py`, `tests/unit/test_as_resolved_index_record.py`.
- Decode and range guard: `tests/unit/test_time_decode.py`, `tests/unit/test_epoch_parse_helper.py`, `tests/unit/test_zarr_fill.py`.
- Coordinate materialization and relabel refusal: `tests/unit/test_materialize_regular_coord_array.py`, `tests/unit/test_materialize_irregular_coord_array.py`, `tests/unit/test_coord_rematerialization.py`.
- Writer, startup and compile error: `tests/unit/test_region_zarr_writer.py`, `tests/unit/test_pod_startup_phantom_group.py`, `tests/unit/test_indexed_write_compile.py`, `tests/unit/test_indexed_write_exception.py`.
- Preallocate and ingest through the CLI: `tests/unit/test_cli_zarr_preallocate.py`, `tests/integration/test_preallocate_regular_axis_dense.py`, `tests/integration/test_irregular_axis_pipeline.py`, `tests/integration/test_zarr_index_show_derived.py`.
- Append, coverage and time-range refusal: `tests/integration/test_cf_time_dim_value_dedup.py`, `tests/unit/test_append_coverage_builder_time_decode.py`, `tests/unit/test_append_overwrite_errors.py`, `tests/unit/test_chunk_manager_queries.py`, `tests/unit/test_chunks_list_cli.py`, `tests/unit/test_cli_chunks_delete.py`.
- Validate, consolidate and archive: `tests/unit/test_validate_time_coord_uniqueness.py`, `tests/integration/test_consolidate_time_coord_command.py`, `tests/unit/test_tensogram_core.py`, `tests/cli/test_archive_create_contract.py`.
- The branch keeps no byte-parity manifest test. The Gregorian layout is unchanged. The landing of 2026-10-20 records that 9576ad3 rebuilds the three Gregorian scenarios of that manifest byte for byte.

## 2026-09-18 — Ingest hot-path scan fixes

**Decision.** Completion of a direct write must not depend on store size; control-plane checks on the ingest path must not scale with run history unless their result is used.

## 2026-09-15 — Append integrity for staged and force-reingest writes

**Decision.** A staged append or force-reingest into a chunk that already holds data must leave the other slots of that chunk intact. Before writing, the runtime seeds every touched chunk of every data array and of `firecube_timestamp_state` from the target, once per run, under the existing per-`(product, group)` append claim; `resume_zarr_store` stays `None` in staged mode because that claim is sufficient. Seeding is all-or-abort: a read error during seeding fails the run instead of continuing with an empty chunk, and the workspace is removed. Chunk-key existence checks follow the array's declared chunk-key encoding, so the dot separator is seeded exactly once like the slash form.

**Decision.** Variables without the time dimension are stored once. The generated `GenericZarrIngestor` pins `data_vars="minimal"`, `coords="minimal"`, `compat="equals"`, and `join="exact"` in `xr.concat`, and the append path strips such variables from the batch after the first write. A later run whose stored value differs, or that introduces a new static variable, raises `SchemaDriftError` naming the variable, on append and on force-reingest region writes alike. The message points to adding the time dimension in `read_dataset` or creating a new store.

**Decision.** Group attributes are first-write-wins. The append path snapshots the group attributes before xarray's `to_zarr(mode="a")` clears them and restores them atomically, so the second batch of a fresh staged run no longer writes empty attributes. A later append batch with different attributes logs one warning listing the changed keys and keeps the stored values; tuple and list values with the same content are not a difference. Region-overwrite batches check static-variable drift only and do not warn about attributes.

**Decision.** The post-write integrity guard verifies the coordinate values of touched slots as well as their state markers, so a store whose timestamp coordinate was overwritten while the state array survived is not promoted. The guard takes the append dimension as a required argument instead of guessing it from the dataset.

**Coverage.** `tests/integration/test_staged_partial_chunk_preserves_data.py`, `tests/integration/test_staged_force_reingest_chunk_integrity.py`, `tests/integration/test_staged_seed_publish_s3.py`, `tests/integration/test_append_static_var_drift_raises.py`, `tests/integration/test_append_group_attrs_first_write_wins.py`, `tests/integration/test_post_write_integrity_guard.py`, `tests/integration/test_post_write_integrity_guard_coord_values.py`, `tests/integration/test_e2e_zarr_ingest_preflight_wiring.py`, `tests/unit/test_seed_touched_data_chunks.py`, `tests/unit/test_seeding_claim_held.py`, `tests/unit/test_data_chunk_seeding_call_order.py`, `tests/unit/test_zarr_drift.py`, `tests/unit/test_zarr_write_preflight_narrow_exception.py`, and `tests/unit/test_errors_messages.py` lock the behavior.

## 2026-09-13 — Showcase notebooks and documentation navigation

**Decision.** Showcase owns the executable examples. Its landing page is `docs/showcase/index.md`; the old tutorial pages have been removed. Quickstart runs an installed plugin, and Plugin Development covers authoring tasks. The merged source-discovery guide owns defaults and filters; the old customize URL remains a compatibility page excluded from navigation and search.

**Delivered.** Three downloadable notebooks cover incremental NetCDF-to-Zarr append/resume, point detections to Parquet, and slot-based parallel ingestion benchmarks. The first two scaffold their readers in notebook cells; the benchmark uses a public plugin. Setup and execution stay in the notebooks, including parallel worker launch. Notebook pages use the available width with the table of contents on the left.

**Data interpretation.** Each notebook states what its data does and does not represent: detections are observations, not confirmed events; derived quantities follow the provider's documented filtering and thresholds; incomplete time coverage in a saved selection is stated explicitly; and benchmark timings are recorded measurements on the documented host, not guaranteed speeds on other hardware.

**Verification retained.** Saved notebook outputs and asset provenance remain in the repository. Earlier local checks compared source values with persisted outputs and exercised the notebooks with the checkout and released Firecube. Those runs used temporary tooling; their full executions are not a current CI gate. The standalone verification scripts and bundled source fixtures were removed, along with the proposed Showcase CI steps. No repository command should refer to that discarded tooling.

The latest documentation check passed a strict MkDocs build and checked 735 rendered article links/images, including all three notebook downloads. Existing append behavior coverage remains in `tests/integration/test_append_strategy_behavior.py` and `tests/integration/test_append_coordinate_safety.py`; these protect the ingestion contract and do not replace notebook execution against provider data.

## 2026-09-13 — Append-coordinate and Parquet target safety

**Decision.** Append, overwrite, and refill validate all time-aligned arrays before mutation, including auxiliary coordinates. Datetime values must round-trip through the stored encoding. Fresh input must have unique timestamps; new append tails must follow the committed maximum, including earlier batches in a staged run. Authorized replacement and refill remain supported.

**Decision.** `GenericParquetIngestor` requires a fresh target until stable slice identity and replay are defined. A product claim spans promotion and terminal recording; part paths must be relative, unique, outside the control plane, and absent before writing. Staged promotion checks the final destination again. Recovery messages do not promise that resume or force bypasses schema or insertion constraints.

**Coverage.** The retained behavior tests are `tests/integration/test_parquet_target_safety.py`, `tests/integration/test_append_coordinate_safety.py`, `tests/unit/test_insert_recovery_guidance.py`, and `tests/unit/test_region_write_schema_drift.py`. Earlier session runs recorded full, focused, and storage-driver checks; their counts are not a claim about the current working tree after subsequent edits.

## 2026-09-13 — Native input-file filters

**Decision.** Ingest, slots, and preallocate accept one `--input-filters` JSON list. Positive patterns add to built-in discovery; exclusions take precedence before content inspection. Explicit patterns are case-sensitive, while suffix recognition remains case-insensitive. CLI lists replace saved lists, and `[]` restores defaults. Custom discovery hooks apply filters themselves.

**Migration.** Replace `include_patterns` with `input_filters` in configuration and SDK calls, and use the native CLI flag. The old name is rejected without an alias. Other discovery controls remain Python helper arguments.

**Coverage.** `tests/unit/test_input_filters.py`, `tests/unit/test_discover_empty_source_fails.py`, and `tests/integration/test_input_filters_cli.py` cover matching, invalid inputs, precedence, empty sources, and public CLI behavior.

## 2026-09-12 — Plugin utilities and authoring contracts

**Decision.** ZIP cleanup removes partial output only from fresh, exclusively owned destinations. Existing and shared directories and parent directories are preserved. The behavior is covered by `tests/unit/test_zip_extract_safety.py`.

**Decision.** `firecube[patterns]` provides optional complete-string Trollsift parsing with explicit formats and visible failures. Core assigns no meaning to parsed fields. External plugin adoption and duplication removal remain pending in TODO; `tests/unit/ingestor/extensions/test_patterns.py` covers the helper.

**Decision.** Non-empty `zarr_time_encoding` and dynamic `time_encoding` settings raise `ConfigurationError` rather than being silently ignored. Coverage is in `tests/unit/test_zarr_time_encoding.py`.

**Documentation.** Guides describe source materialization, reader resource ownership, configuration, and telemetry. The earlier NetCDF tutorial sequence was executed during development and subsequently replaced by Showcase notebooks; its removed pages are not current verification instructions.

## 2026-09-02 — Control-plane specification page (normative, drift-pinned)

**Decision — the `.firecube/` on-disk format has a public normative spec at `docs/reference/control-plane-spec.md`**, registered under Reference in the nav. It documents the layout, atomicity discipline, `schema.json`, run records and WAL event envelope plus all event types, claim files and staleness, snapshots and `LATEST.json`, both index records with their canonical-bytes and identity-hash rules, the seven-row resolved-index precedence table, reserved array attributes, the co-location rule, and an explicit versioning statement (`v2` control-plane envelope, `v1` index records). Every claim was verified against the source before writing; the handoff that motivated the page had already drifted (it said the envelope was v1 and omitted `LATEST.json`), confirming the drift risk. Because the page is static by nature, it is exempted from the docstring-generation rule not by allowlist (it needs no Python fences) but pinned by a dedicated guard, `tests/sdk/test_control_plane_spec_consistency.py`: layout paths bind to the `types.py` constants, the event-type table binds to the `EVENT_*` constants, schema versions and the claim staleness default bind to their defining values, and the precedence table rows bind to the exhaustive row enumeration in the precedence-matrix test docstring. Adding an event type or renaming a path without updating the spec now fails the docs lane.

## 2026-09-02 — `_FillValue` attr: xarray-decodable encoding for float dtypes

**Decision — the stamped `_FillValue` follows xarray's Zarr v3 attribute convention**: `_fill_value_attr_value` in `core/zarr/region_writer.py` is now dtype-aware. For float dtypes it emits the base64-encoded little-endian IEEE-754 double, which is the only form xarray's Zarr backend decodes for kind `f`; the previous bare number made every float array with a finite fill (`0.0`, `-999.0` sentinels) raise `TypeError` at `xr.open_zarr` time. Integer, bool, and string fills keep their plain JSON scalars; NaN and NaT still stamp nothing. The gap survived because the fill-value tests covered integer fills and NaT-absence only, and the flagship product's float fills are all NaN, which the stamping path declines — so no validated cube ever carried a bare float attr. The regression test now asserts what the old suite never did: `xr.open_zarr` succeeds on a finite-float-fill array and decodes the sentinel and masks unwritten cells.

## 2026-09-02 — `zarr compare`: streamed slab comparison and NaN/NaT-aware equality

**Decision — value comparison streams in chunk-aligned slabs**: `_values_equal` in `core/zarr/validation.py` no longer materializes whole arrays. It walks the array in chunk-aligned slabs bounded by a 512 MiB per-store byte budget (`_COMPARE_SLAB_BYTES`), comparing and discarding each slab, and short-circuits at the first differing slab. Slabbing splits along the leading axis first; when a single chunk along that axis already overruns the budget (a leading chunk extent of 1 over a large spatial plane can be several GB), it takes one chunk there and splits along the next axis too, recursively, so peak memory is genuinely bounded by the budget with a one-chunk floor. Rationale: on a full product cube (7.19 GB on disk) the old whole-array path reached 116 GiB resident and was killed by the OOM killer before any diagnostic could print — the largest single array alone needs 190.5 GB decompressed across both stores. Slabs step whole chunks on every axis because a sub-chunk slice still decompresses the entire chunk; slab-size-only streaming was measured to be memory-safe but unusably slow. Verified at product scale on the benchmark host against a known-equal cube pair, including a value flip confined to the final slab.

**Decision — equality is NaN/NaT-aware for kinds `f`, `c`, `M`, `m`**: `equal_nan=True` previously applied only when both dtypes were float, so `datetime64` fell through to a plain comparison where `NaT == NaT` is false: two identical partially-filled cubes reported `values differ` on the time coordinate. Newly reachable because the dense time coordinate stores explicit NaT for unfilled slots. The widened kind set also fixes the same latent defect for `timedelta64` NaT and complex NaN. Regression tests cover multi-slab equality, a mismatch confined to the final slab, NaT-identical stores, and chunk alignment of every slab start.

## 2026-09-01 — Coordinate materializer extraction and zarr CLI package split

**Decision — materialization engine lives in core**: The coordinate-materialization engine (both materializers, per-slot reconcilers, marker stamping, attrs builders, extent predicate, epoch parsing) moved from the zarr CLI module to `src/firecube/core/zarr/coord_materialization.py`, beside the rest of the single-writer machinery (`_coord_lifecycle`, `_coord_chunks`, `planning`, `region_writer`). The module is CLI-free: progress reporting is caller-injected via a `report` callable (the CLI passes `click.echo`; message wording unchanged), store-state conflicts raise `SchemaDriftError`, and caller-input problems raise `ConfigurationError` (rendered at the CLI boundary by `wrap_user_facing_errors`). Exported names are public so the CLI imports no private cross-subsystem names. Rationale: the engine-owned single-writer surface must be materializable without the CLI, and tests of engine behavior now import the engine module rather than CLI internals.

**Decision — one fixture-plugin distribution**: The thirteen in-tree test fixture plugins merged into a single editable distribution at `tests/fixtures/firecube_test_plugins/` (one `pyproject.toml` shipping every fixture module and all `firecube.plugins` entry points under their existing names). Module names, entry-point names, CLI plugin names, and test imports are unchanged; only the packaging is unified. Rationale: thirteen separate editable installs were repeated across six install lists and produced stale-metadata drift (entry points added to a fixture's `pyproject.toml` were invisible until the operator happened to reinstall that one package; 18 of ~45 declared entry points were live before unification). `demo_plugin` stays standalone as the docs-facing example of a real plugin package.

**Decision — zarr CLI is a package**: `src/firecube/cli/zarr.py` (3,019 lines, six commands) split into the `src/firecube/cli/zarr/` package following the `cli/chunks/` precedent: the group and command wiring in `__init__.py` (public contract `from firecube.cli.zarr import zarr` unchanged), one underscore module per command family (`_slots`, `_validate` with `compare`, `_multires`, `_preallocate`, `_consolidate`), shared helpers in `_common.py` (which owns the `firecube.cli.zarr` logger under an explicit dotted name). Slot-interval algebra joined `cli/_slot_planning.py`. Pure move: no behavior, flag, or message changes; the click tree and command contracts are byte-compatible.


## 2026-08-27 — Single-writer time coordinates and axis authoring API

**Decision 1 — engine-owned time-coordinate arrays**: Time-coordinate arrays became single-writer surfaces owned by the engine. For `mode='floor'` axes with observed coordinate values, `preallocate` writes `inspect_item.coordinate` values into the coord array and stamps `firecube_coord_managed`; pods that subsequently write a timestamp verify the stored value against their incoming coordinate and raise `SchemaDriftError` on mismatch rather than overwriting. Only exact-mode and irregular prefill+seal regimes stamp `firecube_preallocated`. The floor+grid combination is rejected at construction and never reaches the stamp step. The planner refuses concurrent windows that would split a coordinate chunk, so no two pods can race on the same chunk boundary.

**Amendment (2026-09-01)**: Freshly created observed shells stamp `firecube_coord_managed=True` at creation time, before any coord values are written. The first materialization run follows a marker-then-fill ordering guarantee so the managed marker exists before the array is populated. Partial-`NaT` under the managed marker is a reconcilable state, not corrupted data. Re-runs reconcile per slot under the held global materialization claim: `NaT` slots receive the incoming value, slots whose stored value matches the incoming value are no-ops, and divergent values raise `SchemaDriftError` naming the slot and both values. An interrupted run therefore resumes by re-running with the same input, and a window extends as new items arrive. The `exact` regime reconciles identically because grid values are deterministic: stored `NaT` slots from earlier windowed runs are filled with the nominal value.

**Decision 2 — `coordinate_values` policy and `TimeAxis` facade**: `RegularTimeAxis` gained a `coordinate_values` policy field that controls how the engine populates the time-coordinate array (`"grid"` for prefilled epoch+cadence labels, `"observed"` for values from `inspect_item.coordinate` written by `preallocate`; pods verify-or-error against those stored values). `TimeAxis` was added as a factory facade exposing four intent-named constructors (`grid`, `observed`, `explicit`, `discovered`) with input guards, so plugin authors pick a constructor by intent rather than assembling `RegularTimeAxis` kwargs by hand. The `coordinate_values` field was removed pre-release on 2026-09-01. After the floor+grid and exact+observed construction rejections landed, every constructible explicit `coordinate_values` value equaled what `effective_coordinate_values` derived from `mode` alone: the field became a pure identity splitter with no behavioral consequence. The four intent-named `TimeAxis` constructors (`grid`, `observed`, `explicit`, `discovered`) remain and steer `mode` directly. If a future regime re-legalizes a currently-rejected combination, the field is additively re-introducible with byte parity for existing cubes (asymmetric emission ensures this).

**Superseded prior attempt**: An earlier dense-prefill approach shipped but was invalidated at production integration. The current design was arrived at after empirical measurement of concurrent-write behavior. Two exploratory notes (`nat-as-gap-investigation.md`, `coord-array-states.md`) that did not fit the final design have been removed; their substantive findings are folded into `DESIGN.md`.

## 2026-08-27 — Time coordinate densification (D + E)

**Problem**: A DirectZarr plugin declared its time coordinate with `chunks=(1,)`. Each ingested slot created a separate chunk file. For a one-month cube of ten-minute slots (4 320 slots), this produced 4 320 time-coordinate chunk files, causing xarray to issue approximately 4 320 object-storage GET requests just to open the cube — making exploratory access impractical at scale.

**Why the naive fix was rejected**: The obvious fix — widening the chunk size at schema declaration time — would have caused every write pod to overwrite the entire chunk on each timestamp write, producing silent data loss under parallel ingestion.

**Design choices**:
- *Option D* (new cubes): `preallocate` now materializes `RegularTimeAxis` time coordinates densely. The full epoch + cadence value sequence is written once during preallocate, and a `firecube_preallocated=True` marker attr is stamped on the coord array. Subsequent `RegionZarrWriter.write_timestamp` calls detect the marker, compare the incoming timestamp against the pre-written value (raising `SchemaDriftError` on mismatch), and skip the write.
- *Option E* (existing cubes): The new `firecube zarr consolidate-time-coord` command retroactively densifies any cube whose time coord still uses per-slot chunks. It reads the existing values, rewrites the coord at the configured chunk size, stamps both `firecube_preallocated=True` and `firecube_consolidated_at=<iso>`, and records a `ConsolidatedTimeCoord` WAL event. That event causes `ResumeGuard` to block any further ingest into the sealed cube.

**Coord chunk resolution**: A new `resolve_coord_chunks(spec, n)` helper centralizes the `max(1, min(256, n))` default formula. Plugins can override via `ZarrArraySpec.chunks`; passing `chunks=None` defers to the helper.

**Cross-references**: The sparse-data policy investigation that cleared Option D and the four coord-array lifecycle states with equality semantics for `write_timestamp` were documented in `nat-as-gap-investigation.md` and `coord-array-states.md` respectively; both files have been removed and their substantive findings folded into `DESIGN.md`.

## 2026-08-25 — Stale-sweep fan-out fix (Issue #26)

`abandon_stale_runs` and `clear_stale_claims` each re-enumerated the full runs or claims directory once per candidate found during the sweep. With N candidates and S stale entries, the re-check loop cost O(N × S) directory listings. Under a 4900-run product with 20 stale entries, that meant 20 extra full-directory scans per sweep call. This wave replaces every re-enumeration with a targeted single-file read, reducing the total listing cost to O(N + S).

A secondary fan-out existed in `_get_run_entry`: it called `_list_run_entries` (O(N)) to locate a single run directory. A new `run_dir_for` helper derives the path directly from the run ID, cutting that call to O(1).

`clear_claim` internals also enumerated the claims directory to find the claim file before deleting it. Targeted path derivation via `read_claim_by_domain` eliminates that listing too.

### What

**T1 — pin read_run_entry semantics** (`f422ee2`): Added `tests/unit/test_wal_reader_read_run_entry.py` to characterize `WalReader.read_run_entry` behavior for missing, malformed, and orphan run directories. Establishes the contract the targeted-read helpers depend on.

**T2 — add run_dir_for helper** (`c8e99e5`): Added `run_dir_for(product_uri, run_id)` pure function to `src/firecube/core/controlplane/repo.py`. Derives the run directory path directly from the run ID without listing the parent directory. Used by `_get_run_entry` (T8) and the re-check paths (T9, T10).

**T3 — add read_claim_by_domain** (`1c80955`): Added `ManifestRepository.read_claim_by_domain(domain)` wrapping the existing `_read_claim` helper. Returns the claim record for a single domain without enumerating the claims directory. Used by the `clear_stale_claims` re-check (T10).

**T4 — bound abandon_stale_runs enumeration (RED)** (`b0531be`): Added `CountingFilesystem`-based bound test for `abandon_stale_runs` asserting runs-dir list calls stay at O(N + S). Fails until T9 lands.

**T5 — bound clear_stale_claims enumeration (RED)** (`d7a860b`): Added `CountingFilesystem`-based bound test for `clear_stale_claims` asserting claims-dir list calls stay at O(N + S). Fails until T10 lands.

**T7 — orphan run-directory behavior lock** (`94b6792`): Added `tests/unit/test_bulk_abandon_stale_runs.py` characterization tests for orphan run directories (directory present, `run.json` absent). Confirms sweep behavior is preserved through the targeted-read refactor.

**T8 — _get_run_entry targeted read** (`2ec3e46`): Rewired `ManifestRepository._get_run_entry` to call `run_dir_for` + `read_run_entry` instead of `_list_run_entries`. Eliminates the O(N) scan per single-run lookup.

**T9 — abandon_stale_runs re-check** (`726cc6f`): Replaced the `_list_run_entries` re-enumeration inside `abandon_stale_runs` with a `read_run_entry` call via `run_dir_for`. The re-check now reads one file. Fixes #26 for the runs sweep path. T4 bound test turns green.

**T10 — clear_stale_claims re-check + clear_claim fix** (`ab4b97b`): Replaced the claims-directory re-enumeration inside `clear_stale_claims` with `read_claim_by_domain`. Also replaced the per-claim directory listing inside `clear_claim` with targeted path derivation + `fs.rm`. Fixes #26 for the claims sweep path. T5 bound test turns green.

**T12 — concurrent operators idempotent** (`7edbc8d`): Added `tests/unit/test_bulk_abandon_stale_runs.py` concurrency test confirming two simultaneous `--all-stale` operators produce no errors and leave the store consistent. Heartbeat refresh, terminal transition, and deletion-by-other-operator are all covered.

**T13 — partial-abandon crash recovery** (`08533f0`): Added test confirming that a crash mid-sweep leaves the store in a state where a re-run completes cleanly. No batch semantics were introduced; each record is processed independently.

Note: the snapshot path was excluded from this scope after code inspection confirmed `_snapshot.py` has only one enumeration call, not two. The full remedy for the snapshot cost belongs to the LSM active-run index tracked under IDEAS.md §16.

### Guardrails preserved

- **DESIGN §11** (one container == one run): unchanged.
- **DESIGN §27** (derived read models): upheld. `_RunEntriesCache` lifetime unchanged; no new persistent state in `.firecube/`.
- **Public API stability**: no changes to `list_runs`, `list_claims`, `abandon_stale_runs`, `clear_stale_claims`, or `_get_run_entry` signatures. `AbandonSweepResult` and `ClearSweepResult` dataclass fields unchanged.
- **Race semantics**: heartbeat refresh, terminal transition, and deletion-by-other-operator all still caught (T9/T10 with updated monkeypatch targets, T12).
- **Orphan run-directory behavior**: preserved (T7 characterization + T8 preservation).
- **Partial-abandon crash recovery**: preserved. No batch semantics introduced (T13).
- **Snapshot path untouched**: `git diff --stat src/firecube/core/controlplane/_snapshot.py` shows no changes.

### Verification

- `uv run pytest --strict-deps tests/unit/test_bulk_abandon_stale_runs.py tests/unit/test_bulk_clear_stale_claims.py tests/unit/test_wal_reader_read_run_entry.py tests/unit/test_filesystem_claim_service_read_by_domain.py tests/unit/test_run_dir_for.py -q` — 33 passed.
- `uv run ruff check .` clean; `uv run ruff format --check .` clean; `uv run pyright` 0 errors on changed files.
- **Cost reduction**: `CountingFilesystem` bound tests confirm enumeration bounded by O(N + S), down from O(N × S). With N=20, S=10: runs-dir list calls dropped from 21 to at most 2; claims-dir list calls dropped from 21 to at most 2.

### Follow-up

- **IDEAS.md §16** — Wave 2 (LSM active-run marker + completed-slots bitmap) still UNDECIDED. The `_snapshot.py` single-enumeration bullet and `resume_guard.py` bitmap remain OPEN.
- **IDEAS.md §17** — Wave 3 (terminal run pruning + auto-rebuild) still UNDECIDED, blocked on Wave 2.

## 2026-08-24 — Milestone D — Optional cleanup

### What

Two capabilities shipped on branch `feat/plugin-to-core-migration` (commit `c2ec4ab`):

1. Chunk-geometry helpers (`chunk_range_for_slot`, `chunk_ranges_for_slots`, `ChunkRange`) extracted from `src/firecube/ingestor/runtime/zarr/indexed_region.py` to `src/firecube/core/zarr/chunk_geometry.py`; exported from `firecube.core.api`; `indexed_region.py` now imports them from the new module. Behavior is bit-identical to the pre-extraction implementation.

2. `BatchResourceRegistry` added to `src/firecube/core/batch_registry.py`; exported from `firecube.core.api`. Provides idempotent teardown and fail-loud-but-close-all semantics: all registered resources are closed even when earlier closures raise, and all exceptions are collected and re-raised together. `_IdentityRef` is an internal implementation detail and is not exported.

### Consequences

The chunk-geometry helpers were promoted to `firecube.core` to enable cross-plugin deduplication of identical chunk-range math. Any plugin or operator code that needs to compute chunk ranges for a given slot can now import from `firecube.core.api` rather than reaching into the ingestor runtime internals.

`BatchResourceRegistry` provides a convenience over the lifecycle hooks for managing closable resources within a batch run. Registering a resource guarantees it will be closed on teardown regardless of earlier failures, removing the need for nested try/finally chains in plugin code.

### Verification

```bash
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

All seven commands exit 0 on branch `feat/plugin-to-core-migration`, commit `c2ec4ab`.

## 2026-08-24 — Milestone C — Bounded operational tooling

### What

Five capabilities shipped on branch `feat/plugin-to-core-migration` (commit `8d79dd8`):

1. `compare_zarr_stores()` helper and `ZarrCompareReport` dataclass added to `src/firecube/core/zarr/validation.py`; exported from `firecube.core.api`; `firecube zarr compare` CLI subcommand added; operator documentation at `docs/operations/zarr-compare.md`.

2. `static_owner` field added to `firecube zarr slots` JSON output. The owner is the pod whose planned `slot_start` is smallest within each group (deterministic, plan-input only). Documentation updated in `docs/operations/parallel-zarr-writes.md`.

3. `firecube zarr validate` static-marker check: the validate command now reports a failure for any static (`time_indexed=False`) array that is missing the `firecube_static_written` marker attribute, without mutating the store.

4. `EngineConfig.suppress_static_emission_for_non_owner` (bool, default `False`) and `EngineConfig.static_owner_slot_start` (int, default `0`) fields added; corresponding CLI flags `--suppress-static-emission-for-non-owner` and `--static-owner-slot-start` added. `IndexedRegionStrategy._dispatch_static_intent` suppresses static writes on non-owner pods and emits a structured log line at `DEBUG` level. Scope limitation: `static_owner_slot_start` is compared against the single slot-start value for the current run; multi-group ownership resolution is deferred (see `plans/TODO.md` §36).

5. `ZarrTemplateConfig.zarr_write_empty_chunks: bool = False` typed option added. A scoped `zarr.config.set({"write_empty_chunks": ...})` is applied around `IndexedRegionStrategy.write_groups` (DirectZarr path) and inside `ZarrWriteContext` (Generic path). The effective value is recorded as the `zarr_write_empty_chunks_effective` metric. A follow-up entry for the pre-existing bare `zarr.config.set({"async.concurrency": ...})` in `ZarrWriteContext` is tracked in `plans/TODO.md` §36.

### Consequences

Operators can now diff two Zarr stores programmatically or via CLI and get a structured report of shape, dtype, chunk, attribute, and data mismatches. The `firecube zarr compare` exit code is 3 on semantic mismatch, 0 on match, non-zero on error.

The `static_owner` field in the fan-out plan gives each pod a deterministic, plan-derived signal for which pod owns static writes. Combined with `--suppress-static-emission-for-non-owner`, non-owner pods skip static write intents entirely, preventing redundant writes and potential race conditions in parallel ingestion.

The validate marker check catches stores where a static array was never committed (e.g. a failed or interrupted ingest), surfacing the gap before a downstream consumer reads stale fill values.

`zarr_write_empty_chunks=False` (the default) prevents Zarr from writing chunks that contain only fill values, reducing storage footprint for sparse arrays. The scoped config ensures the setting is applied only during the write window and does not leak into other Zarr operations in the same process.

The `static_owner_slot_start` v1 implementation compares a single integer against the run's slot-start. Multi-group scenarios where different groups have different owners in the same run are not handled; each group's owner must be resolved by the operator and passed as a separate run. Multi-group deferred to `plans/TODO.md`.

### Verification

```bash
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

All seven commands exit 0 on branch `feat/plugin-to-core-migration`, commit `8d79dd8`. One ruff format issue in `src/firecube/core/zarr/validation.py` and `tests/unit/test_compare_zarr_stores.py` was fixed as part of this gate run before the final pass.

### References

`plans/TODO.md` §36 — `ZarrWriteContext` async-concurrency bare call, deferred from Wave C2.

## 2026-08-24 — Milestone B — Data integrity and archive safety

### What

Two fixes shipped on branch `feat/plugin-to-core-migration` (commit `5490ce8`):

1. `read_hdf5_array` (`src/firecube/core/formats/hdf5.py`) now preserves the source array's dtype by default. Previously the function always returned `float64`, silently coercing integer, boolean, and other numeric dtypes and discarding precision. A new optional `dtype=` parameter lets callers request an explicit cast; omitting it keeps the on-disk dtype. Both the h5py direct-read path and the xarray fallback path honour this contract. Tests cover dtype preservation for `int16`, `uint8`, `bool`, `float32`, and `float64`, plus explicit cast via `dtype=`, plus the xarray fallback path.

2. `extract_hdf5_from_zip` and `stream_hdf5_from_zip` (`src/firecube/core/formats/zip.py`) now reject unsafe ZIP member paths before any extraction or streaming begins. Rejected patterns: dotdot components (`../`), absolute paths (`/tmp/...`), Windows-style traversal (`..\\`, `\\server\`, `C:/`), and any path that resolves outside the destination directory. Archives containing multiple HDF5 candidates are also rejected unless the caller passes an explicit `member=` parameter. Tests cover all four traversal patterns, the zero-candidate and multi-candidate rejection cases, the explicit-member acceptance path, safe nested paths, and the shared-validation invariant between the extract and stream entry points.

### Consequences

`read_hdf5_array` dtype coercion was a silent data-integrity defect: plugins reading integer or boolean HDF5 arrays received `float64` values without any error or warning, making downstream comparisons and schema checks unreliable. The fix makes the function's output match the on-disk representation by default.

The ZIP extraction guards close a zip-slip vulnerability class. An attacker or malformed archive could previously supply a member path such as `../../etc/cron.d/evil` or `/etc/passwd` and have `extract_hdf5_from_zip` write outside the declared destination directory. The new path-safety check raises `ValueError` before any bytes are written, so no partial extraction occurs. The multi-candidate guard prevents silent selection of the wrong HDF5 file when an archive contains more than one candidate.

### Verification

```bash
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

All seven commands exit 0 on branch `feat/plugin-to-core-migration`, commit `5490ce8`. Two pyright errors in `tests/unit/test_zip_extract_safety.py` (missing `assert extracted is not None` before `.read_bytes()` calls) and one ruff format issue in `src/firecube/core/formats/zip.py` were fixed as part of this gate run.

### References

External plugin migration (Milestone B).

## 2026-08-24 — Milestone A — Migration unblockers

### What

Four changes that unblock external plugin migration to the current public API:

1. `runtime/base.py` + `templates/direct_zarr.py` — serial-mode `ExtentUnknownError` now raises `ConfigurationError` naming the group; `direct_zarr.py` adds defense-in-depth for the parallel path so the same error surfaces if the upstream gate is bypassed.
2. `firecube.core.api` — exports `ExtentUnknownError`, `RESERVED_ARRAY_ATTRS`, `assert_attrs_safe`, and `FIRECUBE_STATIC_WRITTEN_ATTR`.
3. `firecube.ingestor.api` — re-exports all four of the above plus `resolve_index_spec`.
4. Docs: `:::` directives added in `docs/reference/exceptions.md` and `docs/reference/core-utilities.md` for both facade paths; `docs/reference/parallelism.md` gains the `resolve_index_spec` ingestor-facade directive.

### Consequences

External plugins can now import `ExtentUnknownError`, `RESERVED_ARRAY_ATTRS`, `assert_attrs_safe`, `FIRECUBE_STATIC_WRITTEN_ATTR`, and `resolve_index_spec` from either `firecube.core.api` or `firecube.ingestor.api` without reaching into private submodules. Serial-mode plugins with an unbounded axis receive a `ConfigurationError` with a clear group name instead of a bare `ExtentUnknownError`. The architecture boundary test (`test_import_boundaries`) now passes because `direct_zarr.py` no longer imports from `firecube.core.index_resolve` or `firecube.ingestor.runtime.parallel_gate`.

### Verification

```bash
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

All seven commands exit 0 on branch `feat/plugin-to-core-migration`.

### References

External plugin migration (Milestone A).

## 2026-08-24 — IndexedWrite + build_indexed_write hook + DirectZarr lifecycle parity + bounded region-write concurrency

**Amendment (2026-09-02)**: The separate `build_indexed_write(item, ctx)` hook was merged away before release. `build_write_intents` is now the single write hook and may return a flat list mixing `WriteIntent` and `IndexedWrite` elements; the template compiles any `IndexedWrite` after the hook returns, so compilation cannot be bypassed by overriding. Rationale: the two-hook shape was a template-method pair (`build_indexed_write` was only reachable through the default `build_write_intents`), which produced a silent-loss trap (overriding `build_write_intents` without calling `super()` discarded the per-item compilation) and repeated comprehension failures. The merged contract keeps the coordinate-keyed ergonomics (`coordinate=` instead of a manual `position()` lookup, loud `IndexedWriteCompilationError`) with one hook, no precedence rules, and no `super()` obligation. For every slot compiled from an IndexedWrite the template also auto-emits the time-coordinate verify-write (suppressed when the plugin emitted an explicit one for that slot), so the coordinate-keyed path needs no position() lookups and no coordinate intents at all: it is the documented golden path, and self-resolved indexes are the advanced path. The hook never shipped in a release (last release 0.1.4.post1 predates it), so this is a clean cut.

Added the `IndexedWrite` abstraction with `.region()` and `.slot()` builders
(`src/firecube/core/indexed_write.py`), the pure-function compiler
`_compile_indexed_write`, the `build_indexed_write(item, ctx)` hook on
`DirectZarrIngestor`, and `IndexedWriteCompilationError(ValueError)` with
structured `coordinate`, `reason`, and `iw_repr` fields. Re-exported from both
SDK façades. Negative API contract enforced: no `.static()` or `.coordinate()`
builder methods on `IndexedWrite`.

Lifecycle parity: `DirectZarrIngestor._process_batch` now wraps
intent-generation and strategy-execution with `batch_setup` →
`prepare_batch_data` → dispatch → `cleanup_batch_data` → `batch_teardown`,
matching `GenericZarrIngestor._process_batch` order and exception policy.
`prep_metrics` merged additively into DirectZarr metrics.

Bounded region-write concurrency: added
`zarr_region_write_concurrency: int = 1` to `ZarrTemplateConfig` (rejects
values < 1). Extended `verify_array_spec` to reject `spec.shards=None` when
the on-disk array is sharded. Rewrote `IndexedRegionStrategy.write_groups` to
support a `ThreadPoolExecutor` path when `region_write_concurrency > 1`:
coordinator-owned writer (root opened serially before executor startup),
physical-chunk disjointness pre-check, serial callable resolution in
coordinator thread, bounded pending futures, non-region intents as ordering
barriers, coverage recorded only on future success, drain/cancel before claim
release. Serial path (`concurrency=1`) preserved byte-for-byte.

Evidence: `uv run pytest -m "not slow and not s3"` 2815 passed, 0 failures;
the DirectZarr and production-plugin-shaped test selection: 0 failures;
`uv run mkdocs build --strict` clean; `uv run pyright` 0 errors;
`uv run ruff check .` clean; `uv run ruff format --check .` clean.

## 2026-08-24 — IndexedWrite + build_indexed_write hook: documentation

Extended `docs/guides/plugins/direct-zarr.md` with a section on letting the
engine resolve slots via `build_indexed_write`: when to prefer it over
`build_write_intents`, the static-emission pattern (override both hooks; call
`super()` for indexed compilation, append `WriteIntent.static` for statics),
hook precedence, and the `IndexedWriteCompilationError` failure surface. A
separate guide page was considered and rejected: the hook exists to reduce
plugin code, and its details belong in the reference docstrings.

Added `:::` directives for `IndexedWrite` and `IndexedWriteCompilationError` to
`docs/reference/templates.md` and `docs/reference/exceptions.md` (both
`firecube.ingestor.api` and `firecube.core.api` facades). Fixes the two
docs-coverage test failures that blocked `uv run pytest -m "not slow and not s3"`.

Evidence: `uv run mkdocs build --strict` clean; `uv run pytest -k "docs"` 29/29
passed; `uv run pytest -m "not slow and not s3"` 2792 passed, 0 failures.

Supersedes: `plans/TODO.md §33` "Planned: IndexedWrite high-level abstraction +
read_item hook" (now Landed).

References: B9 (docs).

## 2026-08-24 — IrregularTimeAxis + AUTO sentinel + content-addressed item manifest

Added `IrregularTimeAxis` as a third concrete `AxisSpec` alongside
`RegularTimeAxis` and `IntegerAxis`. Plugins declare
`IrregularTimeAxis(coordinate=..., values=<tuple>)` when items have timestamps
that are not evenly spaced. Setting `values=AUTO` tells the engine to discover
coordinates at planning time by calling `inspect_item` on every source item
before preallocate.

Added `AUTO`: a module-level singleton sentinel. `IrregularTimeAxis(values=AUTO)`
opts into engine-owned discovery. The sentinel is re-exported from both
`firecube.core.api` and `firecube.ingestor.api`.

Added content-addressed item manifest: `ItemManifestEntry` pins one source item
to one axis coordinate via a content-address (`identity_hash`) and a stable
`source_ref`. `validate_manifest_entries` checks uniqueness invariants.
`compute_resolved_index_identity_hash` folds the manifest into the
`identity_hash` when items are present, making the on-disk record a
freeze-detection mechanism for irregular-axis cubes.

Added `MissingIrregularCoordinateError` and `NoDiscoveredItemsError`: both
inherit from `ConfigurationError` and are raised during AUTO discovery when an
item has no resolvable coordinate or when discovery finds zero items.

Added `--dry-run` flag to `firecube zarr preallocate`: runs discovery and
resolves the index without writing any Zarr arrays, claim files, or control-plane
records. Output matches `firecube zarr index show --json`.

Added `--derived` flag to `firecube zarr index show`: computes and prints
derived coordinate values for `regular_time` groups from the stored epoch,
cadence, and size. Read-only; no files are written.

All new symbols are exported from `firecube.core.api` and
`firecube.ingestor.api`. Documentation added: capability reference in
`docs/reference/parallelism.md`, plugin-authoring guide at
`docs/guides/plugins/irregular-axis.md`, exceptions reference updated in
`docs/reference/exceptions.md`, CLI docs updated in
`docs/operations/parallel-zarr-writes.md` and
`docs/operations/firecube-index.md`.

Supersedes: `plans/TODO.md §33` "Planned: IrregularTimeAxis + AUTO sentinel +
content-addressed item manifest" (now Landed).

References: A1 (IrregularTimeAxis + AUTO), A2 (resolver), A3 (errors),
A4 (manifest types), A5 (AUTO discovery), A6 (fixture plugin), A7 (contracts),
A8 (--dry-run), A9 (--derived), A10 (coord materialization), A11 (integration
tests), A12 (docs).

## 2026-08-23 — DirectZarr lazy WriteIntent payload (§F3)

`WriteIntent.data` now accepts `Callable[[], np.ndarray]` in addition to
`np.ndarray | Any`. The callable is resolved exactly once at dispatch time,
in dispatch order, immediately before the writer call. Eager payloads remain
valid; existing plugins run unchanged.

**Baseline peak** (single-slot smoke ingest of a large per-slot payload, eager path):
14.5201 GiB.

**Acceptance measurement**: pending — adoption by the plugin that motivated it
(WI-3) and measurement to be performed by operator with the adopted build.
Target: ≤ ~3.13 GiB retained peak.

**P=8 12-slot capacity confirmation**: pending — requires operator-supplied
adopted plugin build and P=8 fixture.

**Fallback**: not invoked (acceptance measurement pending; fallback decision
deferred to operator measurement run).

**Lifetime contract**: callables must close over stable inputs (paths,
configuration, immutable references). Open file handles and per-batch scratch
objects must not be captured.

**TEST_GAPS P2 §4**: closed — regression harness at
`tests/benchmarks/lazy_writeintent_harness/`; synthetic quantitative smoke
(`test_callable_plugin_retained_peak_smoke`) runs in CI using
`callable_payload_test_plugin`; the production-scale quantitative lane is operator-run
with the adopted build.

Supersedes: `plans/TODO.md §F3` (removed).
References: `plans/TEST_GAPS.md P2 §4` (closed).

## 2026-08-20 — IntegerAxis + ResolvedIndexRecord + firecube zarr index CLI

Added `IntegerAxis` as a second concrete `AxisSpec` alongside `RegularTimeAxis`.
Plugins declare `IntegerAxis(size=N)` in `index_spec()` when items map to a
zero-based integer position rather than a timestamp. A single `IndexSpec` can
mix both axis kinds across groups.

Added `ResolvedIndexRecord`: the on-disk control-plane record written to
`.firecube/index/current.json` after the engine resolves an `IndexSpec`. The
record stores the `identity_hash` of the resolved spec; subsequent runs verify
the hash before writing, protecting the cube from silent schema drift.

Added `firecube zarr index show/verify/rebuild` CLI commands for inspecting and
recovering the resolved-index record. `rebuild` is the migration path for cubes
written by Firecube v0.1.4.post1 and earlier that used the legacy slot-index
format.

Both `IntegerAxis` and `ResolvedIndexRecord` are exported from
`firecube.core.api` and `firecube.ingestor.api`.

Supersedes: 2026-08-18 DirectZarrIngestor IndexSpec clean cut (extends it with
the integer-axis kind and the persisted record type).

## 2026-08-18 — DirectZarrIngestor IndexSpec clean cut

Replaced the defective `DirectZarrSlotAllocationMixin` with a declarative
`IndexSpec` plus engine-owned `ResolvedIndex` architecture. The old mixin path
hit a reproducible `RecursionError` and kept `ClassVar` epoch logic incompatible
with real plugins. Byte identity was preserved for
the existing production cubes. Both `canonical_bytes()` and `identity_hash`
were verified. Plugins stay pinned to `firecube==0.1.4` until they migrate.

Supersedes: 2026-08-05 mixin acceptance decision.

## 2026-08-12 — Plugin API reference restructure + docs-coverage contract

Rebuilt the public API reference around a discovery layer and made its coverage machine-checked. Motivated by a plugin-author question that the docs could not answer: multi-group Zarr writes via `get_batch_groups` were reachable in code, documented for the Parquet template only, and absent from the Zarr template's page and every guide.

### Why this shape

Modeled on two projects whose reference docs plugin authors navigate well:

- **earthkit-data** keeps a small hand-written landing page — tables of top-level names with one-line descriptions — in front of a generated tree. The friendliness is the curated front door, not the generation.
- **xarray** fans a landing page out to per-topic subpages, each listing hand-picked names under task headings as autosummary tables. Its plugin-author treatment is the model copied here: a narrative how-to kept deliberately separate from a reference page that documents the stable extension surface.

Both are Sphinx; this repo is MkDocs + mkdocstrings, so the transferable parts were the patterns (summary tables, task grouping, per-topic pages, drift guards), not the tooling. Two adaptations went further than the source projects:

- **Summary tables are generated, not hand-written.** `docs/_macros.py` already imported firecube at build time, so `render_api_summary(module, names)` emits *name → first docstring line* by static griffe inspection. The curated-list drift both reference projects fight with CI scripts is impossible by construction here.
- **Examples live in docstrings, not in the Markdown.** They then reach editors and `help()` as well as the rendered page, and render wherever the symbol renders.

A full auto-generated module dump was rejected: the SDK boundary is deliberately two facades, and dumping `firecube.core.*` would advertise modules plugins are forbidden — and test-enforced — not to import.

### What

**Reference split** into `docs/reference/index.md` (grid-card landing) plus `templates`, `hooks`, `context`, `exceptions`, `parallelism`, `extensions`, `core-utilities`. `api.md` and `advanced-plugin-api.md` deleted; 19 inbound links across guides, tutorials, and contributing pages repointed. One canonical `:::` directive per symbol — a duplicate render is a hard `--strict` failure (ambiguous autorefs anchor), so second pages carry prose and links instead. This also resolved pre-existing duplicate renders of `PipelineBatch`, `ZarrTemplateConfig`, `ParquetTemplateConfig`, and `PluginConfig`.

**Coverage closed** for the surface plugin authors actually reach: all `BaseIngestor` hooks (three were rendered before), the exception hierarchy, `PluginContext` including `storage` — the one property with a real docstring, excluded from the old `members:` allowlist and absent from all of `docs/` — plus `StorageContext`, slot-range types and validators, `firecube.core.api` helpers, and the extensions surface.

**`firecube.ingestor.extensions` given an explicit facade**: `__init__.py` with `__all__` and lazy `__getattr__`, mirroring the `api.py` idiom so optional dependencies stay lazy. It was a public import surface per `tests/sdk/test_plugin_contract.py` with no declared boundary. `DuckDbMixin` is documented hooks-only; its resource-config route is the open item in TODO §12.3.

**Docs-coverage contract** (`tests/sdk/test_api_docs_coverage.py`, `docs_static`): every facade export is rendered *and* has docstring text, or is allowlisted with a reason; directives are restricted to facade paths. The docstring half exists because a directive alone proves nothing — `SlotRange` was rendering as a bare signature, its only documentation a `#` comment invisible to griffe. Static griffe inspection is required rather than `inspect.getdoc`: a runtime lookup on an exported type alias returns the underlying type's docstring (`SlotRange` would report `tuple`'s) and would pass silently.

**Docstring conventions recorded** in `plans/STYLE.md` (write the docstring with the code; Google style; `Examples:` plural — the singular parses as a collapsed admonition; section bodies are Markdown, so RST `::` leaks as visible text) and reference architecture in `.prompts/docs-policy.md` §6, led by the rule the work itself needed: enrich the docstring, do not relocate the entry.

### Consequences

- Adding a name to a facade `__all__` now fails CI until it is documented and has a docstring, or is allowlisted with a reason. The previously implicit "authoring surface" scoping is an explicit, reviewable list.
- The docs build depends on three external hosts (python, numpy, xarray inventories) for annotation cross-links. An unreachable host fails `mkdocs build --strict` and therefore blocks the docs deploy — verified by pointing an inventory at an invalid host. zarr, pyarrow, and pandas inventories were measured to resolve zero links (their types appear only in docstring prose, which mkdocstrings does not auto-link) and were excluded rather than carried as failure surface.
- CI lanes now match `plans/TESTING_STANDARDS.md`: `test` excludes `docs_static`/`snapshot`, `docs` runs them. The `docs` job needed the fixture-plugin install step, because a conftest guard fails collection for any pytest run without them.
- `griffelib` added to the `test` extra and the `--strict-deps` guard rather than guarded by `importorskip`, which would have silently skipped the coverage check in the PR-gating job. SBOM and README dependency tables regenerated; `hatchling` version drift (1.30.1 → 1.32.0) corrected while there.

### Verification

- `uv run mkdocs build --strict` — passes, no warnings.
- `uv run pytest --strict-deps -m "docs_static or snapshot"` — 29 passed.
- `uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot"` — 2410 passed, 1 pre-existing unrelated failure (`test_resolve_file_localhost`, macOS `/tmp` symlink assertion; reproduced on a clean tree with these changes stashed).
- `uv run ruff check`, `ruff format --check`, `uv run pyright` — clean.
- Coverage measured before/after: `firecube.ingestor.api` 19 → 43 of 63 exports documented, `firecube.core.api` 3 → 21 of 30, `firecube.ingestor.extensions` 0 → 11 of 11; reference pages 6 → 12; unique documented symbols 26 → 97; 38 numpy-style docstring sections converted across 14 modules, 0 remaining.

### Follow-ups

- TODO §12.4 records that `discover_input_files` exposes `exclude`, `include_suffixes`, `recursive`, and `sniff_hdf5` while `EngineConfig` surfaces only `include_patterns`, so excluding a file requires plugin code.

## 2026-08-10 — #40 — DirectZarr codec CLI parity + default alignment + RF-11/RF-12 invariants

Closes GitHub #40. Fixes three linked issues in the codec configuration surface: a silent PR #25 regression (default was uncompressed on-disk), the DirectZarr CLI routing gap for codec options, and divergent template defaults between GenericZarr and DirectZarr. Also promotes an internal codec-derivation helper to a public runtime API and closes an adjacent behavioral parity gap in `firecube zarr preallocate`.

### What

**Default flip**: `ZarrTemplateConfig.zarr_compression` now defaults to `True` (was `False`). Restores the pre-PR-25 effective on-disk behavior — where zarr's own default codec (`ZstdCodec(level=0)`) applied — and aligns firecube with zarr-python v3's upstream default. Explicit `zarr_compression = false` still disables compression.

**DirectZarr CLI routing wired**: `DirectZarrIngestor.template_config_class = ZarrTemplateConfig`. Without this declaration, `TierConfigurator` computed `template_keys = set()` and rejected `--option zarr_compression=...` and `--option zarr_codecs='[...]'` as "Unknown configuration options" for every DirectZarr plugin. The internal codec plumbing was correct but unreachable from the CLI.

**Preallocate codec routing parity**: `firecube zarr preallocate` was passing schema kwargs (shape, dtype, chunks, attrs) to `ensure_group()` but not the effective codec pipeline. `--option zarr_compression=false` and `--option zarr_codecs='[...]'` were silently ignored at the preallocate stage even after the TierConfigurator wiring. Fixed by calling `derive_effective_codecs_for_spec(arr_spec, ingestor.template_config)` and forwarding filters/serializer/compressors to `ensure_group()`.

**Preallocate validation parity**: `firecube zarr preallocate` now calls `validate_zarr_specs_against_template` before mutating arrays, matching `DirectZarrIngestor._process_batch`. Previously the CLI could create broken zarr arrays before the validator would have caught the codec conflict; now the failure is fail-fast, no partial mutation.

**`derive_effective_codecs_for_spec` promoted from private to public runtime helper**: the codec-priority derivation helper (per-array > template > default) moved from `templates/direct_zarr.py._derive_effective_codecs_for_spec` to `runtime/zarr/write.py:derive_effective_codecs_for_spec`. New home is next to its sibling public helper `resolve_codec_pipeline`. Shared between `DirectZarrIngestor` (per-batch schema setup and write dispatch) and the `firecube zarr preallocate` CLI command, both of which materialize Zarr arrays from a plugin-declared schema and must apply the same codec precedence rules. The old private name is removed with no alias (per STYLE.md "Option Aliases" rule).

**`cli/zarr.py` imports `DirectZarrIngestor` via `firecube.ingestor.api`**: the CLI command previously did a deep import from `firecube.ingestor.templates.direct_zarr`. Now uses the public re-export. Small correctness fix aligning CLI with the "public surfaces" convention documented in STYLE.md.

**RF-11 architecture invariant** (`tests/architecture/test_template_config_class_declared.py`): AST-based static invariant that enumerates concrete `BaseIngestor` subclasses referencing `self.template_config` and requires each to declare a non-None `template_config_class`. Would have caught the DirectZarr routing gap before implementation. Abstract classes (via `ABC`/`abstractmethod`) are exempt.

**RF-12 CLI private-import invariant** (`tests/architecture/test_cli_no_private_cross_subsystem_imports.py`): AST-based static invariant scoped to `src/firecube/cli/**`, rejects any `ImportFrom` naming a single-underscore-prefixed symbol from a module outside `firecube.cli.*` (i.e., `firecube.ingestor.*` or `firecube.core.*`). Same-package private imports and dunders are allowed. Closes the enforcement gap: prior to RF-12, no static test rejected underscore-prefixed cross-subsystem imports from `src/firecube/cli/**`, so the initial `cli/zarr.py:_derive_effective_codecs_for_spec` leak was not caught before implementation.

### Consequences

- Default codec on-disk for both GenericZarr and DirectZarr default ingests is now `ZstdCodec(level=0)` (matching zarr-python v3). Pre-fix, GenericZarr default was uncompressed on-disk (PR #25 regression); DirectZarr default was already compressed via the fallback in the old `_derive_effective_codecs_for_spec`. The two templates now behave identically.
- Existing cubes written with `zarr_compression=false` remain valid on resume as long as the ingest continues to pass the flag explicitly.
- Codec drift detection at `verify_array_spec()` is spec-gated (line 567): only fires when the plugin's `ZarrArraySpec` declares codec fields. Default resumes (no per-array codec declaration) never trigger drift, so the default flip does not break existing stores.
- `firecube plugins describe direct_zarr_capable_test_plugin` now shows `zarr_compression` and `zarr_codecs` under Template Options.
- `firecube zarr preallocate` behavioral parity with `DirectZarrIngestor` for codec validation and routing.
- Cross-subsystem private imports from `src/firecube/cli/**` are now statically forbidden by RF-12.

### Test coverage

- 7 new test files: CLI matrix for DirectZarr ingest / GenericZarr ingest / `firecube zarr preallocate` (3 × 3 codec cases + 1 malformed-JSON negative + 1 preallocate validation-parity); per-array override preservation (unit); resume-safety regression (integration); RF-11 architecture invariant (AST, mutation-verified); RF-12 CLI private-import invariant (AST, mutation-verified).
- Full test suite (excluding the one pre-existing failure): 2430 passed, 0 new failures. One pre-existing failure (`test_phase3_3_plan_to_ingest_contract::test_plan_remainder_range_accepted_by_ingest`, JSONDecodeError, unrelated to codecs) confirmed present on HEAD before this work.

### Verification

Commit hash: TBD (same commit as this DONE.md entry).

### Source

GitHub #40 — https://github.com/eumetsat/firecube/issues/40

## 2026-08-09 — §33+§34 — Per-array codec overrides and shared codec pipeline module

Closes TODO.md §33 and §34. Firecube no longer injects an opinionated default codec; zarr's own default applies when no codec is declared. Per-array codec overrides are now first-class in the plugin contract.

### What

**`ZarrArraySpec` codec fields**: `filters`, `serializer`, and `compressors` optional fields added to `ZarrArraySpec`. Plugins can declare per-array codec pipelines without touching `ZarrTemplateConfig`.

**`ZarrTemplateConfig.zarr_codecs` validator relaxed**: accepts full Zarr v3 pipelines (flat list, ordering validated: filters first, then serializer, then compressors). Previously only a single-element list was accepted.

**`resolve_compressor()` replaced by `resolve_codec_pipeline()`**: the old helper injected a Blosc/zstd/clevel=5 default when no codec was declared. The new helper has no opinionated default — zarr's own default applies when the pipeline is empty.

**`RegionZarrWriter.ensure_group()` accepts codec kwargs**: `filters`, `serializer`, and `compressors` are forwarded to zarr array creation.

**`_derive_effective_codecs_for_spec()` helper**: resolves the effective codec pipeline for a `ZarrArraySpec` in DirectZarr mode, merging spec-level overrides with template-level defaults.

**`verify_array_spec()` extended**: per-field codec drift checks added. Canonical comparison uses `arr.metadata.codecs` (not a string repr).

**`_compute_schema_hash()` extended**: conditional codec fields included in the hash when present. Backward-compatible: schemas with no codec declarations hash identically to before.

**`validate_zarr_specs_against_template()` cross-config validator**: catches codec conflicts between `ZarrArraySpec`-level overrides and `ZarrTemplateConfig`-level defaults at schema-setup time.

**`src/firecube/core/zarr/codec_pipeline.py`**: new shared module for codec normalization, comparison, and pipeline splitting (filters / serializer / compressors). Used by both the DirectZarr and GenericZarr paths.

### Consequences

- Firecube no longer injects Blosc/zstd/clevel=5 as a default. Existing stores with that codec are unaffected (the codec is stored in `zarr.json`); new stores without a declared codec get zarr's default.
- `GenericZarrIngestor` no longer sets a default compressor. Plugins that relied on the implicit default must now declare it explicitly via `zarr_codecs` or per-array `compressors`.
- `ZarrArraySpec.filters`, `.serializer`, `.compressors` are all optional and default to `None` (no override). Existing plugin code requires no changes.
- Schema hash is backward-compatible: the new codec fields are conditional, so schemas without codec declarations produce the same hash as before.

### Verification

Commit hash: TBD (same commit as this DONE.md entry).

Source: closes TODO.md §33 and §34.

---

## 2026-08-07 — #25 — `zarr_codecs` config field and codec pipeline wiring (recorded retroactively 2026-08-09)

Closes GitHub #25. Shipped the first iteration of user-configurable Zarr codecs for `GenericZarrIngestor` and the staged/append write path.

### What

**`zarr_codecs: list[dict] | None` on `ZarrTemplateConfig`**: new optional config field accepting a single-element list of codec entries in Zarr v3 metadata format (`[{"name": "...", "configuration": {...}}]`). Requires `zarr_compression = true`. Selects any codec registered via zarr's extension mechanism by name.

**`resolve_compressor()` helper**: resolved the active compressor from `zarr_codecs` (when set) or fell back to the Blosc/zstd/clevel=5 default for `GenericZarrIngestor`. (This helper was superseded by `resolve_codec_pipeline()` in §33.)

**Blosc/zstd/clevel=5 default for `GenericZarrIngestor`**: applied when `zarr_compression=true` and no `zarr_codecs` entry was declared. (Removed in §33 — firecube no longer injects an opinionated default.)

**Staged/append write path wiring**: `resolve_compressor()` threaded through the staged metadata seeding path and the append write executor so codec selection was consistent across write modes.

### Test coverage

542 lines across 5 files.

### Commits

- `fa41f78` — main implementation
- `174534b` — follow-up fix

### Source

GitHub #25 — https://github.com/eumetsat/firecube/issues/25

---

Date: 2026-08-05 Task: #27

Decision: Accept GitHub #27 — automate slot allocation for `DirectZarrIngestor` via an opt-in cadence mixin. Promotes IDEAS.md §21 Idea 2 to TODO.md §33 (decision-only entry; implementation follows on a feature branch, milestone v0.2.0).

Context: Plugins opting into `SUPPORTS_SLOT_RANGE_PARALLELISM` must hand-implement `timestamp_to_ts_index` / `global_expected_time_count` / `slot_index_model`, and the same epoch/cadence math is duplicated near-verbatim across the tutorial, ~20 test fixtures, and both production parallel plugins — the trigger condition IDEAS.md §21 set for promotion. `SlotIndexModel` already carries `(epoch, cadence_s, mode)` per group but those fields are inert identity metadata, never consumed for arithmetic anywhere in `src/`; only the derivation behavior is missing. Design decided: an opt-in mixin (working name `CadenceSlotAllocation`) rather than base-class defaults, so the `__init_subclass__` guard at `templates/direct_zarr.py` and its tests stay untouched (the MRO satisfies the guard naturally, and the base never learns about the mixin). Scope is fixed integer-second cadence with a finite horizon in v1; irregular-cadence products (polar orbiters: non-integer periods, drifting overpass times) explicitly keep the manual three-hook contract. `ChunkManager` is unchanged — a derived model is content-addressed identically to a hand-written one. Full design constraints and acceptance criteria in TODO.md §33.

Consequences: TODO.md §33 created (accepted scope + mixin design constraints); IDEAS.md §21 Idea 2 marked PROMOTED (Ideas 1 and 3 remain UNDECIDED); IDEAS.md §34 added for the unrelated gridding-extension boundary findings surfaced by the same evaluation.

Source: GitHub #27 — https://github.com/eumetsat/firecube/issues/27

---

Date: 2026-08-03 Task: #22

Decision: Ship PEP 561 `py.typed` marker for downstream IDE type support

Context: Downstream plugin authors' type checkers (pyright, mypy) could not resolve firecube imports with type information because the package lacked a PEP 561 marker. Root-level placement at `src/firecube/py.typed` is required — subpackage-only triggers mypy #16149. The marker is zero-byte per PEP 561 standard for typed packages (`partial\n` is for stub packages only). Attribution file `py.typed.ABOUT` added per repo convention for files that cannot carry header comments. One contract test added asserting both file existence and zero-byte size.

Files changed:
- `src/firecube/py.typed` (new — zero-byte PEP 561 marker)
- `src/firecube/py.typed.ABOUT` (new — attribution metadata)
- `tests/sdk/test_py_typed_marker.py` (new — contract test)
- `plans/DONE.md` (this entry)

Verification: `uv run pytest tests/sdk/test_py_typed_marker.py --strict-deps -q` → 1 passed. `unzip -l dist/firecube-*.whl | awk '{print $4}' | grep -x 'firecube/py.typed' | wc -l` → 1. `uv run ruff check .` clean; `uv run pyright` clean. Regression: test fails when marker deleted, passes when restored.

Source: GitHub #22 — https://github.com/eumetsat/firecube/issues/22

---

## 2026-07-13 — `write_1d` numpy>=2 one-slot idiom (production blocker)

A production DirectZarr plugin's ingest failed under the pinned `numpy>=2.3.3` runtime with `ValueError: Could not convert object to NumPy datetime` at `RegionZarrWriter.write_1d`. Root cause: `arr[scalar_int] = one_element_ndarray` on a `datetime64[s]` Zarr array is silently accepted by numpy 1.x (broadcast) but rejected by numpy 2.x (strict shape). The failing pattern was `self._open_root()[f"{group}/{array_name}"][ts_index] = data` at `src/firecube/core/zarr/region_writer.py:639`. Reproduced end-to-end against real source NetCDF files from object storage.

### What

**T1 — `write_1d` numpy-2-safe one-slot idiom** (`3da6468797291b85edec15786fc4d81fff27fc7e`): Replaced scalar-index setitem with the one-element slice-assign idiom `arr[i:i+1] = payload.reshape(1)` for 1-D targets. Explicit `ValueError` for zero-length or multi-element payloads on 1-D targets locks the "write EXACTLY one timestamp slot" contract (DESIGN.md §"Risks To Avoid": coverage tracker records only one `ts_index` per intent; multi-slot payloads would create silent data-integrity gaps). Higher-rank target behavior preserved byte-identical (`arr[ts_index] = payload` with `arr.shape[1:]` validation). Regression tests added covering datetime64 1-elem writes, parametrized numeric dtypes, 0-D scalar payloads (preserves scalar-payload path used by `test_indexed_region_time_1d_coverage.py`), invalid-payload rejection on 1-D targets, and shape-mismatch rejection on higher-rank targets.

### Consequences

- `write_1d` contract now documented explicitly: "write exactly one timestamp slot" — never multi-slot.
- Callers that previously wrote 0-D scalars OR 1-elem ndarrays are unaffected.
- Callers passing multi-element ndarrays to 1-D targets (previously silently mis-writing via numpy 1.x broadcast) now fail loudly with `ValueError`. No such caller was found in firecube-core; external plugins should validate their payload construction.
- Blast radius bounded: only `RegionZarrWriter.write_1d` in `region_writer.py` had the failing pattern; `write_timestamp` uses 0-D scalar (safe), `write_region` uses `y_slice` (safe), `write_static` uses `arr[...]` (safe), `state.py:212` uses scalar-to-slice fill (safe).
- Tech debt flagged (out of scope for this fix): `write_1d` is a poorly-named method (a slot payload, not necessarily a 1-D array). Future rename candidate: `write_slot_payload` or `write_time_indexed_intent`.

### Verified

- `uv run pytest tests/unit/test_region_zarr_writer.py -q` → all pass (35 total, 8 in `TestWrite1D`)
- `uv run pytest tests/unit/test_indexed_region_time_1d_coverage.py -q` → all pass (scalar payload path preserved)
- `uv run ruff check src/firecube/core/zarr/region_writer.py tests/unit/test_region_zarr_writer.py` → clean
- `uv run pyright src/firecube/core/zarr/region_writer.py` → 0 errors
- End-to-end smoke: real source file → `normalize_string_vars` → `.isel(time=0).values` → `np.atleast_1d` → `write_1d` → `E2E_SMOKE_OK`

### Evidence

- Commit: `3da6468797291b85edec15786fc4d81fff27fc7e` fix(zarr): write_1d writes exactly one slot, numpy>=2-safe idiom
- Source: internal plan `fix-write-1d-numpy-2x-slice-index`.

**Confidence:** HIGH

## 2026-07-11 — Resume-guard perf Wave 1 — repo-boundary memoization + bulk stale sweep CLIs

A production cube backfill hit ~4900 runs. Between the "Parallel capability validated" log line and the resume-guard decision, pods paused for roughly 3.5 minutes. Root cause: `ResumeGuard.enforce()` called `_list_run_entries` twice per invocation (once in `_snapshot.py` and once in the guard itself), with no shared cache. At 4900 runs each call enumerated the full `.firecube/runs/` directory, so every pod paid the cost twice. A second axis: after a cluster crash, stale claims and abandoned runs blocked `snapshots rebuild` until an operator manually cleared each record one at a time.

Wave 1 ships two targeted fixes without touching the underlying O(N) scan (deferred to Wave 2, IDEAS.md §16): an enforce-scoped repo-boundary cache that eliminates the redundant enumeration call, and `--all-stale` bulk CLIs that let operators unblock a crashed cluster in one confirmed action instead of N.

### What

**T1 — repo-boundary run-entry cache scaffold** (`12788d6`): Added `_RunEntriesCache` to `src/firecube/core/controlplane/repo.py` — an enforce-scoped ephemeral dict keyed by product. Cache is NOT persisted (DESIGN §27: derived read models only; no hidden state). Lifetime is one `enforce()` call.

**T2 — `list_stale_claims` helper** (`010c6bb`): Added `ManifestRepository.list_stale_claims()` to enumerate claims whose heartbeat has expired. Feeds the bulk-clear CLI (T10) and the crash-recovery workflow (T13).

**T3 — `list_stale_runs` helper** (`e109c49`): Added `ManifestRepository.list_stale_runs()` to enumerate runs stuck in a non-terminal state past their heartbeat window. Feeds the bulk-abandon CLI (T11) and the crash-recovery workflow (T13).

**T4 — resume-guard metrics in `RUN_SUMMARY_SCHEMA`** (`b2bfd1e`): Extended the canonical metric schema with `resume_guard_enforce_duration_s`, `resume_guard_runs_enumerated`, and `resume_guard_spans_scanned`. Collector goes through `TelemetryService` (DESIGN §63: no ad-hoc Prometheus imports).

**T5 — counting-filesystem test harness** (`502adf8`): Added `CountingFilesystem` wrapper in `tests/unit/_helpers/counting_fs.py` for op-count assertions. Used by T6 tests to assert the redundant `_list_run_entries` call is gone.

**T6 — enforce-scoped cache wired into `ResumeGuard`** (`7847254`): `ResumeGuard.enforce()` now scopes the repo-bound `_RunEntriesCache` with `ManifestRepository.run_entries_cache_scope()`. Both `list_runs()` and `_load_current_state()` transparently hit the shared repository cache, so op-count tests (via T5) confirm the directory is enumerated exactly once per enforce call regardless of run count.

**T7 — bulk clear stale claims via manager** (`040af8f`): `ChunkManager.clear_stale_claims(dry_run: bool = True)` iterates `list_stale_claims()` and releases each only when the caller passes `dry_run=False`. The CLI adds the `--yes-i-really-mean-it` confirmation gate on top (DESIGN §36: no silent auto-reclaim).

**T8 — bulk abandon stale runs via manager** (`b6201ce`): `ChunkManager.abandon_stale_runs(dry_run: bool = True)` iterates `list_stale_runs()` and records each as abandoned only when the caller passes `dry_run=False`. The CLI adds the same explicit-confirmation guard as T7.

**T9 — emit resume-guard timing and counters** (`dc5c203`): `ResumeGuard.enforce()` records wall time and scan counts into the run summary via `TelemetryService`. No bare `except:` added (DESIGN §93: fail-loud).

**T10 — `chunks claims clear --all-stale`** (`a084567`): New CLI flag on `firecube chunks claims clear`. Requires `--yes-i-really-mean-it`. Prints a per-claim summary and a final count. Documented in T12.

**T11 — `chunks runs abandon --all-stale`** (`cf1ed29`): New CLI flag on `firecube chunks runs abandon`. Same confirmation gate as T10. Documented in T12.

**T12 — `--all-stale` bulk sweep flags documented** (`e6087b9`): CLI reference updated with flag descriptions, confirmation requirement, and expected output shape.

**T13 — post-crash recovery workflow** (`c7bfeeb`): New `docs/operations/chunk-manager/post-crash-recovery.md` page. Covers the full sequence: identify stale claims, bulk-clear, identify stale runs, bulk-abandon, re-trigger `snapshots rebuild`. One confirmed action per step, not N.

**T14 — Wave 2 (§16) and Wave 3 (§17) captured in IDEAS.md** (`e04e781`): IDEAS.md §16 records the LSM active-run index + completed-slots bitmap design (UNDECIDED). IDEAS.md §17 records terminal run pruning + auto-rebuild triggers (UNDECIDED, blocked on Wave 2).

**T15 — end-to-end crash recovery integration test** (`eb697ae`): `tests/integration/test_crash_recovery_bulk_sweep.py` simulates a mid-run cluster crash, verifies stale claims and runs are detected, bulk-clears both, and confirms `snapshots rebuild` succeeds afterward.

### Guardrails preserved

- **DESIGN §11** (one container == one run): unchanged. The cache is enforce-scoped ephemeral state; it does not alter run identity or lifecycle.
- **DESIGN §27** (derived read models): upheld. `_RunEntryCache` is an in-memory, enforce-lifetime dict. Nothing is persisted to `.firecube/`.
- **DESIGN §36** (explicit abandonment): preserved. `--all-stale` requires `--yes-i-really-mean-it` on every bulk operation. No silent auto-reclaim path exists.
- **DESIGN §63** (observability boundaries): upheld. All new metrics go through `TelemetryService`; no ad-hoc `prometheus_client` imports added.
- **DESIGN §93** (fail-loud): upheld. No bare `except:` blocks added anywhere in this wave.

### Verification

- `uv run pytest tests/unit/ tests/integration/test_crash_recovery_bulk_sweep.py -q` — 52 unit + 2 integration tests green.
- `uv run ruff check .` clean; `uv run ruff format --check .` clean; `uv run pyright` 0 errors on changed files.

### Follow-up

- **IDEAS.md §16** — Wave 2: LSM active-run index + completed-slots bitmap for O(range) resume-guard. UNDECIDED.
- **IDEAS.md §17** — Wave 3: terminal run pruning + auto-rebuild triggers. UNDECIDED, blocked on Wave 2.

---

## 2026-06-29 — fsspec atomic writer is content-atomic on local disk (no zero-length window)

Fixed a flaky high-contention concurrency failure where a loser thread racing for the `slot_index_model:current` claim read `current.json` mid-write and got `ManifestError: slot-index record is not valid JSON: Expecting value: line 1 column 1 (char 0)`. Reproduced ~1-in-40 with `test_concurrent_same_model_high_contention` (20 threads, same model). Root cause: `FsspecAtomicWriter.write_atomic` published via `open(path, "xb")`. On local disk `O_EXCL` makes only the *name* appear atomically — the file is created empty and filled incrementally, so a concurrent reader observes a 0-byte file. Losers in `ChunkManager.ensure_slot_index_model` read `current.json` *outside* the claim the instant they see `ClaimConflictError` (`manager.py:694`), i.e. exactly inside the winner's write window. S3 (s3fs `xb` uploads-on-close) and obstore (`PutMode.Create`, `LocalStore` temp+rename) were already content-atomic; only the local fsspec path had the gap, silently violating the "complete old-or-new object, never partial" invariant the rest of the system already relies on (see the 2026-06-19 `read_bytes` entry).

### What

**Content-atomic local publish via temp-file + `os.link`.** For the local filesystem, `write_atomic` now writes a sibling temp file (`tempfile.mkstemp` in the destination's directory), `fsync`s it, then `os.link(tmp, dst)` to publish the fully-written inode in a single step, and removes the temp in a `finally`. `os.link` raises `FileExistsError` if the target already exists, so the create-if-not-exists `FileExistsError` contract (`protocol.py`) the claim layer depends on is preserved. Non-local backends keep the unchanged `open("xb")` + 412→`FileExistsError` normalization path, selected by a new `_is_local()` probe on the fsspec protocol. `os.link` (not `rename`) is deliberate: `rename` clobbers, which would break create-only semantics — this is the create-only counterpart to the overwrite primitive deferred in the 2026-06-26 orphan-slot entry.

- The fix lives in the fsspec storage driver, NOT the control plane, per DESIGN.md "One driver everywhere / No ad-hoc storage wiring". The `AtomicWriter` Protocol is the seam; `claims.py`/`manager.py` stay driver-agnostic. Because the fix is at the `write_atomic` seam it also closes the same zero-length-read window for concurrently-read **claim** lock files (`list_claims`/heartbeat parse their JSON).

### Files touched

- `src/firecube/core/filesystem/fsspec_backend.py` — `FsspecAtomicWriter.write_atomic` local branch (`_is_local`, `_write_atomic_local`); docstring corrected (`O_EXCL` is name-atomic, not content-atomic).
- `tests/unit/test_fsspec_atomic_writer.py` — 3 local-fs tests: full-payload persist, conflict→`FileExistsError` with no clobber, no temp-file leak.
- `tests/integration/test_slot_index_s3_precondition_retry.py` — forces the writer's remote branch (`_is_local()→False`) so the injected s3fs 412 surface still exercises the normalization now that local no longer routes through `open("xb")`.

### Verification

- `uv run pytest tests/integration/test_slot_index_concurrency.py::test_concurrent_same_model_high_contention -q` — 60 consecutive runs green (was failing ~1/40 before the fix).
- `uv run pytest --strict-deps tests/unit/test_fsspec_atomic_writer.py tests/unit/test_no_raw_fsspec_usage.py tests/integration/test_slot_index_concurrency.py tests/integration/test_slot_index_precedence_matrix.py tests/integration/test_slot_index_s3_precondition_retry.py tests/unit/test_chunk_manager_slot_index.py tests/integration/test_obstore_claim_atomicity.py tests/integration/test_maintenance_claims.py -q` — all green.
- `uv run ruff check .` clean; `uv run ruff format --check .` clean; `uv run pyright` 0 errors on changed files.

---

## 2026-06-26 — Parallel ingest — orphan slot metadata recovery

Eliminated the `ResumeConflictError "Non-range run ... is active"` failure that blocked `--parallelism 8` backfill runs against a fresh S3 target. Root cause: `WalReader.read_run_entry()` returns an orphan run entry (no `slot_range`, no `slot_group`) when `run.json` is missing during a parallel-start race. The projection faithfully propagated the orphan state into `RunInfo`, and `ResumeGuard` correctly (but unhelpfully) rejected a new pod invocation as a conflicting non-range run. Five targeted fixes were applied: a canonical suffix parser for recovery (T1), projection-layer orphan healing (T2), resume-meta orphan healing (T3), full slot-metadata threading through the three-facade terminal-record chain (T4), and CLI diagnostic exposure (T5). `ResumeGuard` itself is unchanged — the guard's logic was correct; the fix is upstream.

### What

**T1 — `parse_pod_run_id_slot` parser** (`232e3d1`): Added `parse_pod_run_id_slot()` parser to `src/firecube/core/controlplane/repo_utils.py`, alongside the existing `deserialize_slot_*` helpers. Recovery-only inverse of `derive_pod_run_id`; never used at write time. Architecture-independence test kept passing — `firecube.core` does not import `firecube.ingestor`.

**T2 — projection orphan healing** (`ecb8d32`, fixup `1ad2618`): Extended `_run_info_from_entry` in `_projection.py` to fall back to `parse_pod_run_id_slot` when the payload lacks `slot_range` or `slot_group`. Explicit payload values always win; fallback only fires on `None`. Logs a healing notice.

**T3 — resume_meta orphan healing** (`e4d35f6`): Extended `_resume_meta_for_run` in `_wal_writer.py` with the same fallback. Prevents orphan resume metadata from being propagated into `RunEventWriter`, which would otherwise write a slotless `run.json` on the next `_write_run_meta` call (the corruption-persistence vector).

**T4 — terminal slot preservation** (`929505c`): Added `slot_range`/`slot_group` kwargs (defaults `None`) to `record_run_terminal` across all three control-plane facades (`ChunkManager`, `ManifestRepository`, `ManifestWalWriter`) and the engine recording boundary (`SpanRecorder.register_run`). Mirrored the existing `record_run_started` slot-field signature at every layer. The terminal WAL event payload and the final `run.json` now carry slot fields when the caller supplies them.

**T5 — CLI slot field exposure** (`74684b0`): Added `slot_range` and `slot_group` to the JSON output of `firecube chunks runs list -f json`. `slot_range` serializes as a JSON array `[start, end]` or `null`; `slot_group` as a string or `null`. Text output unchanged.

### Files touched

- `src/firecube/core/controlplane/repo_utils.py` — `parse_pod_run_id_slot` parser added.
- `src/firecube/core/controlplane/_projection.py` — `_run_info_from_entry` orphan healing.
- `src/firecube/core/controlplane/_wal_writer.py` — `_resume_meta_for_run` orphan healing + `record_run_terminal` slot kwargs.
- `src/firecube/core/controlplane/repo.py` — `ManifestRepository.record_run_terminal` slot kwargs.
- `src/firecube/core/controlplane/manager.py` — `ChunkManager.record_run_terminal` slot kwargs.
- `src/firecube/ingestor/runtime/recording.py` — `SpanRecorder.register_run` slot kwargs.
- `src/firecube/cli/chunks/_runs.py` — JSON output slot fields.
- `tests/unit/test_run_id_slot_parser.py` — round-trip + malformed parser tests.
- `tests/unit/test_projection_orphan_slot_fallback.py` — orphan projection healing tests.
- `tests/unit/test_resume_meta_orphan_slot_fallback.py` — orphan resume_meta healing tests.
- `tests/integration/test_run_terminal_slot_preservation.py` — terminal slot preservation integration tests.
- `tests/cli/test_runs_list_slot_fields.py` — CLI JSON slot fields test.

### Verification

- `uv run pytest tests/unit/test_run_id_slot_parser.py -v` — round-trip and malformed-input parser cases.
- `uv run pytest tests/unit/test_projection_orphan_slot_fallback.py -v` — orphan projection healing.
- `uv run pytest tests/unit/test_resume_meta_orphan_slot_fallback.py -v` — orphan resume_meta healing.
- `uv run pytest tests/integration/test_run_terminal_slot_preservation.py -v` — terminal slot preservation.
- `uv run pytest tests/cli/test_runs_list_slot_fields.py -v` — CLI JSON slot fields.
- `uv run pytest tests/architecture/test_core_independence.py -v` — `firecube.core` does not import `firecube.ingestor`.
- `uv run pytest --strict-deps -q -m "not slow and not s3" tests/unit tests/integration tests/sdk tests/cli tests/architecture` — full local gate.

### Deferred

Atomic `run.json` overwrites. The existing `AtomicWriter.write_atomic` protocol is create-only (by design, raises `FileExistsError` on existing targets). Replacing the overwrite-heavy `_write_run_meta` calls requires a new `overwrite_atomic`/`replace_atomic` primitive with per-driver implementations (local POSIX `rename`, S3 conditional `PutObject`, obstore `PutMode.Overwrite`). The T2/T3 recovery primitives in this plan render the torn-read window harmless without it; eliminating the underlying race window is a separate, follow-up plan.

Reference: internal plan for the parallel resume-conflict fix.

---

## 2026-06-26 — Static-array replay equality — dtype-safe

Fixed a latent `TypeError` in `_dispatch_static_intent`'s replay path: `np.array_equal(..., equal_nan=True)` raises `TypeError: ufunc 'isnan' not supported` when either operand has `dtype.kind` in `{"U", "S", "O", "V"}` (str/bytes/object/structured). No production callers were affected (all static arrays are `float64` today), but the bug would surface for any future plugin using non-float static arrays (e.g. `uint8` cloud masks, `datetime64[s]` scan-time tables, `U3` flag strings).

### What

**T1 — dtype-safe static replay helper** (`9fd9274`): Added private helper `_arrays_equal_missing_aware(a1, a2) -> bool` and constant `_ARRAY_EQUAL_NAN_UNSAFE_KINDS = frozenset({"U", "S", "O", "V"})` to `region_writer.py`, colocated with the existing `_fill_value_is_missing` / `_fill_values_equal` / `_array_is_all_fill` family. The helper uses `np.array_equal(..., equal_nan=True)` for kinds `{i, u, b, f, c, M, m}` (preserving NaT-aware semantics for `datetime64`/`timedelta64`) and falls back to plain `np.array_equal` for the four unsafe kinds. Both operand dtypes are checked. Replaced the direct `np.array_equal(equal_nan=True)` call at `indexed_region.py:365` with `_arrays_equal_missing_aware(existing, intent.data)`.

### Files touched

- `src/firecube/core/zarr/region_writer.py` — new `_ARRAY_EQUAL_NAN_UNSAFE_KINDS` constant + `_arrays_equal_missing_aware` helper.
- `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — import + single call-site replacement.
- `tests/unit/test_indexed_region_static_dispatch.py` — `test_static_replay_dtype_safety` parameterized test (5 dtype variants: int32, uint8, bool, datetime64-with-NaT, str-U3).

### Verification

- `uv run pytest tests/unit/test_indexed_region_static_dispatch.py::test_static_replay_dtype_safety -v` — all 5 parameterized cases pass.
- `uv run ruff check src/firecube/core/zarr/region_writer.py src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — clean.
- `uv run pyright src/firecube/core/zarr/region_writer.py src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — 0 errors.

Reference: internal plan `static-replay-dtype-safe`.

---

## 2026-06-25 — firecube zarr preallocate — typed-config and spec-attrs parity

Closed two gaps that prevented cadence-based plugins from using `firecube zarr preallocate` as the sanctioned pre-sizing step before a parallel backfill campaign. A third commit records a speculative follow-on idea.

### What

**T1 — typed-config in preallocate** (`ec9bdc8`): `firecube zarr preallocate` now calls `TierConfigurator.configure()` inline before invoking plugin hooks (`slot_index_model`, `global_expected_time_count`, `zarr_schema`). All three typed configs (`engine_config`, `template_config`, `plugin_config`) reach the hooks. Previously, hooks that read `self.plugin_config` (e.g. `reference_epoch`, `expected_timesteps_per_group`, `product_groups`, `cadence_overrides`) saw only defaults, blocking a dense-time cube rebuild.

**T2 — spec attrs/shards/dimension_names forwarded** (`f8f5168`): `firecube zarr preallocate` now forwards `attrs`, `shards`, and `dimension_names` from each `ZarrArraySpec` to `RegionZarrWriter.ensure_group()`. A subsequent `firecube ingest` against a preallocated store no longer raises `SchemaDriftError: attrs['units'] existing=None spec='K'`.

**T3 — §A idea recorded** (`14b8097`): Pure `kind="static"` for `time_indexed=True` arrays noted as a speculative follow-on in `plans/IDEAS.md §A` (DEFERRED-V2+). No code change.

### Files touched (summary)

- `src/firecube/cli/zarr.py` — T1 `TierConfigurator.configure()` call; T2 `attrs`/`shards`/`dimension_names` forwarding.

### Verification

- `uv run pytest tests/integration/test_preallocate_typed_config.py` — T1 plugin_config reaches hooks.
- `uv run pytest tests/integration/test_preallocate_spec_attrs.py` — T2 no SchemaDriftError on subsequent ingest.

Reference: internal plans `preallocate-typed-config-and-attrs` and `handoff-firecube-core-dense-time`.

---

## 2026-06-25 — DirectZarr plugin parity — core fixes

Closed 4 bugs and one dead-code removal surfaced during a production plugin's migration to the DirectZarr write path.

### What

**T1 — §30 union fix** (`7ff079c`): `IndexedRegionStrategy` (in `indexed_region.py`) now unions every `time_indexed=False` array name into the coord-skip set before constructing `RegionZarrWriter`. Previously, `ensure_timestamp_slot` read the dim-0 size of static 2-D arrays (e.g. grid height 1072) as a time-axis length and raised "ts_index out of bounds" for any slot index beyond it. The fix is a union, not a replacement, so existing `coord_names` entries remain honored for back-compat.

**T2 — slot-index ungate** (`7eb0975`): Extracted `_ensure_slot_index_model_at_startup` from `_verify_schema_at_pod_startup` in `direct_zarr.py`. The new method is called from `base.py` on ALL ingest paths (parallel and non-parallel). Non-declaring plugins (no `SUPPORTS_SLOT_RANGE_PARALLELISM`) see zero behavior change via a first-line early return. A `_slot_index_model_stamped` flag provides an idempotent per-pod fast-path.

**T3 — staged-seed marker strip** (`db4752f`): `_seed_group_via_session` in `staged_metadata.py` now strips `firecube_static_written` from seeded array `zarr.json` payloads before writing them into the workspace. All other attrs (`_ARRAY_DIMENSIONS`, `_FillValue`, codecs, shape, dtype) are preserved. The final published target still carries the marker after second-run commit, so write-once enforcement is unaffected.

**T4 — ScratchManager deletion** (`ba5542f`): Deleted `src/firecube/ingestor/runtime/scratch.py` and `tests/unit/test_scratch_manager.py`. Added `tests/unit/test_scratch_module_deleted.py` as a negative guard. Rationale: (a) zero internal callers in `src/`; (b) stdlib `tempfile.TemporaryDirectory` + `zipfile.ZipFile.extractall` is equivalent; (c) the variety of product package formats rules out a generic helper; (d) continues the API minimization trajectory (see P1 entry above). External: one plugin's `_scratch.py` shim needs a follow-up inline stdlib replacement.

### Files touched (summary)

- `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — T1 union fix.
- `src/firecube/ingestor/templates/direct_zarr.py` — T2 method extraction.
- `src/firecube/ingestor/runtime/base.py` — T2 call site.
- `src/firecube/ingestor/runtime/zarr/staged_metadata.py` — T3 marker strip.
- `src/firecube/ingestor/runtime/scratch.py` — T4 deleted.
- `tests/unit/test_scratch_manager.py` — T4 deleted.
- `tests/unit/test_scratch_module_deleted.py` — T4 negative guard added.

### Verification

- `uv run pytest tests/integration/test_direct_zarr_parity_slot_range.py` — T1 static-array slot check.
- `uv run pytest tests/unit/test_slot_index_model_non_parallel.py` — T2 non-parallel path.
- `uv run pytest tests/unit/test_staged_metadata_static_marker_strip.py` — T3 marker strip.
- `uv run pytest tests/unit/test_scratch_module_deleted.py` — T4 deletion guard.
- `uv run pytest --strict-deps` passes on `feat/directzarr-plugin-parity-core-fixes`.

Reference: internal plan `directzarr-plugin-parity-core-fixes`.

---

## 2026-06-22 — CF-1.8 advisor: CF010 exempts grid-mapping container variables

`_check_cf010` (the "data var missing `units`" check in `core/cf/validator.py`) no longer flags variables that are CF grid-mapping containers. A new `_grid_mapping_targets(ds)` collects every variable referenced via a `grid_mapping` attribute; CF010 skips those.

**Why**: a grid-mapping variable carries the CRS in its attributes and, per CF, has no data semantics and no `units` — so requiring units on it is a false positive. A production plugin's `projection_definition` is exactly such a container; this was the last advisor error after the group-globals + time-coordinate work.

**Locked design decisions**:
- Reference-based, not self-tag: a variable IS a grid-mapping container precisely because a data variable references it via `grid_mapping` (that is CF's definition). Keying the exemption on a self-declared `grid_mapping_name` was rejected — it is data-dependent (e.g. geostationary vs LAEA) and would force plugins to hardcode a projection name.
- Narrow: only `grid_mapping` targets are exempted, NOT the other `_REF_ATTRS` kinds (`bounds` share their parent's units, `cell_measures` have units). The exemption only ever removes a false-positive error, so it cannot make previously-compliant data fail.
- Generic/core: lives in `firecube.core.cf`, the sanctioned CF home (DESIGN.md:86 carves the advisor out of the "no CF as a domain concept" rule). Benefits any plugin. The plugin half (declaring `grid_mapping` references on its georeferenced fields) lives in the plugin's own repository.

Evidence: `tests/unit/test_cf_validator.py::test_cf010_exempts_grid_mapping_container` (referenced container exempt; non-referenced unitless var still errors — keeps the exemption narrow). Plugin-side end-to-end advisor check (`tests/test_ingestor.py::test_schema_dataset_is_cf_advisor_error_free`) now asserts zero errors across all 5 groups.

---

## 2026-06-22 — `ZarrGroupSpec.attrs`: generic group-level attributes for DirectZarr

Added an optional `attrs: Mapping[str, Any] | None = None` field to `ZarrGroupSpec` so DirectZarr plugins can declare group-level Zarr attributes (the missing sibling of the existing `ZarrArraySpec.attrs`). Firecube writes the mapping verbatim onto the group's `zarr.json` at schema setup and does not interpret it — **the mechanism is convention-agnostic** (no CF/STAC/ACDD knowledge in core; plugins decide what to publish). Motivation: a DirectZarr plugin had no way to emit dataset-level metadata (e.g. CF `Conventions`/`title`), so the CF-1.8 advisor's CF001-003 fired on DirectZarr plugin stores; this closes that gap generically.

**Locked design decisions**:
- Group-level, NOT root-level. The CF advisor (and the GenericZarr/append parity path) read attrs from the *group* the data lives in (`xr.open_zarr(group=…).attrs`), not the root. Group attrs also avoid the root entirely, so they never collide with the slot-index identity-hash root attrs (`manager._mirror_attrs`) or need claim coordination. Root-attr writing was considered and rejected as unnecessary complexity.
- Canonical write seam, no parallel path: a new `RegionZarrWriter.set_group_attrs(group, attrs)` (`require_group(...).attrs.update(...)`, guarded by the existing `assert_attrs_safe`) is called once per group from the SAME two schema-setup sites that already stamp array attrs — `_setup_global_zarr_schema` (parallel slot-range startup) and `IndexedRegionStrategy.write_groups` (single-pod). `attrs.update` is idempotent.
- CF-agnostic naming throughout: `attrs` / `set_group_attrs` / `assert_attrs_safe`; the guard rejects only firecube-reserved names. Docstrings describe "convention-agnostic group-level metadata" with CF mentioned at most as one example (matching `ZarrArraySpec.attrs`). Per DESIGN.md:86 "firecube does NOT expose CF as a domain concept"; CF-1.8 lives only in plugins and the standalone `firecube advise compliance` module.
- Schema identity: `_compute_schema_hash` folds group attrs into the hash ONLY when present, so schemas declaring none hash identically to before (backward-compatible); divergent group attrs are caught as drift. Additive optional field per the 2026-06-18 `ZarrArraySpec` parity precedent (cold-migration only).
- NOT addressed here (plugin-side, by design): the CF *time coordinate* (CF004) is already plugin-declarable — a plugin declares a `time` `ZarrArraySpec` with CF attrs and `ensure_group` preserves them through `write_timestamp` resizes (DONE.md 2026-06-18 "Gap C dissolved into Gap B"); no core change needed. `grid_mapping` wiring (CF010 for grid-mapping container vars) remains a separate structural follow-up.

Evidence: `tests/unit/test_zarr_group_attrs.py` (set_group_attrs roundtrip + reserved-name rejection + empty no-op; `ZarrGroupSpec.attrs` non-Mapping rejection + None default; schema-hash unchanged-without-attrs / changed-with-attrs / deterministic; end-to-end `_setup_global_zarr_schema` writes group attrs to the store). Test fake `_Writer` in `tests/unit/test_indexed_region_schema_claim.py` gains `set_group_attrs`. Regression batch (region_writer, setup-global-schema, indexed-region, existing-cube-check, schema-claim-retry) green.

---

## 2026-06-22 — Single-shot `StorageFilesystem.read_bytes` for concurrent-safe metadata reads

Added `read_bytes(uri) -> bytes` to the `StorageFilesystem` Protocol and routed the existing-cube dim-compatibility check (`existing_cube_check._read_json`) through it. This is the **read-side complement** to the same-day `write_atomic` 412 fix: both isolate an s3fs quirk with S3-compatible object stores at the fsspec adapter seam.

**The bug**: in parallel slot-range mode, every pod runs `verify_dim_compatibility` at startup (once *per batch*) and reads each group's `zarr.json` via `fs.open(...,"rb").read()`. On s3fs that takes the range-cached fetch, which adds an `If-Match: <etag>` precondition. While pod A is creating/overwriting a group's `zarr.json` (`_verify_schema_at_pod_startup → ensure_group`, under the `zarr_schema_global` claim), pod B's read of the same object gets HTTP **412 PreconditionFailed** (ETag changed mid-read). The S3-compatible object store returns the 412 with a `None` message, and s3fs crashes in its own handler (`core.py`: `"pre-conditions" in ex.args[1]`, `args[1]` is `None` → `TypeError`). The pod dies at startup. Confirmed the upstream s3fs bug is **still present in s3fs 2026.6.0** (latest at time of fix), so an s3fs bump does NOT fix it.

**Locked design decisions**:
- The fix lives in the storage driver, not the ingestor/control plane. `read_bytes` is a driver-neutral Protocol method; both backends implement a single GET (fsspec `cat_file`, obstore `store.get`) with **no conditional/range-cached fetch**. Per DESIGN.md "One driver everywhere", driver-specific behavior stays at the fsspec adapter seam; the ingestor read path just calls `fs.read_bytes(uri)`.
- Rejected: retry/catch in `_read_json` (wrong layer; the crash is a `TypeError` raised *inside* s3fs — catching it would be exactly the over-broad except STYLE.md forbids). Rejected: reading dims from the control plane (ChunkManager stores `arrays`/`time_dim_name` but NOT `dimension_names`/`shape`, so the data store must be read). Rejected: claim-coordinated read (needs a consistent read, not a lock).
- Correctness rests on object-store PUT atomicity: a single GET returns a complete old-or-new object, never partial. For a dim-*name* check where every pod writes the same plugin-declared `time_dim_name`, old-vs-new is identical → no false positive, no retry needed.
- `read_bytes` is instrumented (`InstrumentedFilesystem`, bytes_read + latency) like `open`.

**s3fs/fsspec bump**: 2026.4.0 → 2026.6.0 (general freshness; does NOT fix this bug — the read_bytes change does).

**Not done** (deliberate scope): `core/zarr/validation.py::_load_array_metadata` still uses `open().read()` — it's the `firecube zarr validate` maintenance path, not the concurrent hot path; route it later for consistency. The redundant per-batch re-read of the same `zarr.json` (and gating the check in parallel mode where schema-verify already covers it) is a separate optional follow-up — left in TODO, not bundled, because it changes a safety check's timing.

Evidence: `tests/unit/test_storage_filesystem.py` (read_bytes roundtrip fsspec+obstore; single-shot-cat_file-not-open spy via isolated fake raw fs — never mutates the shared LocalFileSystem singleton); `tests/unit/test_existing_cube_check_read_bytes.py` (`_read_json` uses `read_bytes`, never `open`); `tests/unit/test_filesystem_instrumentation.py` (read_bytes records bytes_read); `tests/unit/test_storage_filesystem_protocol.py` (mock updated for the new Protocol member). Boundary unchanged: `tests/unit/test_no_raw_fsspec_usage.py` green (`cat_file` on `self._fs` is inside the allowlisted seam).

---

## 2026-06-22 — fsspec atomic writer normalizes S3 412 PreconditionFailed to FileExistsError

`FsspecAtomicWriter.write_atomic` (the `"xb"` exclusive-create primitive) now translates the s3fs lost-race signal to `FileExistsError`, honoring the `AtomicWriter` Protocol contract (`protocol.py`: "Raises FileExistsError if uri already exists") on S3 as it already did on local disk.

**The bug**: s3fs implements exclusive-create as a conditional `PutObject` (`If-None-Match: *`). When a concurrent writer wins the race, S3 returns HTTP 412 `PreconditionFailed`, which s3fs surfaces as `OSError(EINVAL)` (errno 22) with a botocore `ClientError` cause — NOT `OSError(EEXIST)` (errno 17). The writer only translated `EEXIST`, so on S3 the raw `OSError` escaped through `claims.acquire` (which catches only `FileExistsError` → `ClaimConflictError`) and out of `ChunkManager.ensure_slot_index_model`'s `ClaimConflictError` retry/convergence loop. Result: on a fresh S3 store, slot-range parallel pods that lost the schema-claim race crashed at startup (`_verify_schema_at_pod_startup`) instead of converging. Local-disk and obstore runs were unaffected (`ObstoreAtomicWriter` already maps `AlreadyExistsError` → `FileExistsError`).

**Locked design decisions**:
- The fix lives in the fsspec storage driver, NOT the control plane. Per DESIGN.md "One driver everywhere / No ad-hoc storage wiring", `claims.py`/`manager.py` must stay driver-agnostic; the `AtomicWriter` Protocol is the seam that hands them a driver-neutral `FileExistsError`. Teaching the control plane to catch botocore exceptions would have been the actual boundary violation.
- Detection is duck-typed on the botocore response dict (`Error.Code == "PreconditionFailed"` or `HTTPStatusCode == 412`) by walking the `__cause__`/`__context__` chain — no hard botocore import, no keying on the ambiguous `EINVAL` errno. Lives in `fsspec_backend.py`, which is the permanent-allowlisted fsspec adapter seam (`test_no_raw_fsspec_usage.py`).
- The control-plane claim/retry machinery is unchanged; it was already correct and simply never received the signal.
- Stale docstring corrected: s3fs now does native conditional writes (not the old `head_object`+`put`), satisfying the Protocol's "MUST use native primitives" clause; the only gap was exception translation.

Evidence: `tests/unit/test_fsspec_atomic_writer.py` (translation matrix: wrapped s3fs 412, bare ClientError 412, local EEXIST regression, native FileExistsError passthrough, non-conflict errors propagate, happy path); `tests/integration/test_slot_index_s3_precondition_retry.py` (end-to-end: injected one-shot s3fs 412 into the real writer → `ensure_slot_index_model` retries and converges, claim released). Boundary unchanged: `tests/unit/test_no_raw_fsspec_usage.py` green.

---

## 2026-06-21 — Slot-index model promoted to control-plane authority

Plugin-side ad-hoc root-attribute stamping and per-plugin epoch/model-protection mechanisms have been replaced by a single `ChunkManager` service backed by the `.firecube/slot_index/current.json` control-plane record. Both `firecube zarr preallocate` and `DirectZarrIngestor` startup now route through the same shared service, which persists a content-addressed model record under an exclusive claim and mirrors the identity hash as a Zarr root attribute.

**Shipped surfaces**:
- `firecube.core.api.SlotAxis`, `firecube.core.api.SlotIndexModel` (with `canonical_bytes()` and `identity_hash`)
- `firecube.core.api.iso_to_epoch_s`, `firecube.core.api.epoch_s_to_iso`, `firecube.core.api.normalize_epoch_iso`
- `ChunkManager.ensure_slot_index_model` and `ChunkManager.get_slot_index_model` (6-row precedence matrix, scoped retry, full CP+attrs convergence check)
- `DirectZarrIngestor.slot_index_model(ctx)` hook with `__init_subclass__` enforcement when `SUPPORTS_SLOT_RANGE_PARALLELISM = True`
- New error hierarchy: `SlotIndexModelError`, `SlotIndexModelConflictError`, `SlotIndexUnmanagedStoreError`, `SlotIndexModelClaimTimeoutError` (in `firecube.core.errors`)
- New control-plane storage layout: per-product `slot_index/current.json` (schema version `v1`)
- New reserved-root-attr guard module (`_reserved_root_attrs.py`) for user/plugin code paths; slot-index service bypasses it by design
- `firecube zarr preallocate` now accepts `--option` and `--input-data`, routed through the same service as `DirectZarrIngestor` startup

**Locked design decisions**:
- Three-axis discipline: claim names describe the resource (`"current"`), schema versions describe the on-disk record format (`"v1"`), model names describe the plugin algorithm (`"opera_v1"`)
- v1 supports exactly two rounding modes: `"exact"` and `"floor"` — no others
- Per-group axes only in v1; no `default_axis` syntactic sugar
- Hook lives on `DirectZarrIngestor` only; concrete default raises `NotImplementedError` (not `@abstractmethod`)
- Fresh-store precedence matrix is the sole adoption surface; the row "CP absent + attrs present" always raises `SlotIndexUnmanagedStoreError`
- Concurrency retry scoped inside `ensure_slot_index_model` only; `ClaimManager.acquire()` semantics unchanged globally
- Loser threads require full CP+attrs convergence before emitting `EVENT_SLOT_INDEX_MODEL_VERIFIED`; CP-only match is not a convergence signal

**Not done** (deliberate scope):
- No migration, adoption path, legacy-attr reader, or admin command for attrs-only stores — they fail loud
- No core inference of slot epochs or cadences from filenames, store contents, or group names; plugin is the sole source of truth
- No external plugin-repository migration (those are separate plans in their own repositories)
- No new public docs page or migration guide; this entry is the sole prose artefact
- No second hook name or compatibility alias
- No rounding modes beyond `"exact"` and `"floor"` in v1
- No widening of `_reserved_attrs.py` (array-attrs guard); slot-model root attrs use a new module to avoid conflating the two protection surfaces

Evidence: `tests/sdk/test_public_api_surface.py` (public-API identity + KEPT set); `tests/integration/test_slot_index_precedence_matrix.py` (6-row matrix); `tests/integration/test_slot_index_concurrency.py` (same-model race, different-model race, high-contention, loser-cp-only regression); `tests/unit/test_chunk_manager_slot_index.py` (service unit tests).

---

## 2026-06-20 — Public API: promote three plugin-facing helpers

Three helpers required by `plans/DESIGN.md:45` (plugins use `firecube.ingestor.api` / `firecube.core.api` only) were previously deep-imports only. They are now re-exported through the public API; implementation locations are unchanged.

**Promoted helpers**:
- `decode_time_array` (from `firecube.core.zarr.time_decode`) → `firecube.core.api.decode_time_array`. Already had its own module-level `__all__` signalling export intent.
- `read_chunk_grid_with_shards` (from `firecube.core.zarr.validation`) → `firecube.core.api.read_chunk_grid_with_shards`. Returns `(dim_names, shape, outer_chunk_shape, inner_chunk_shape)` for sharded-Zarr-aware inspection.
- `verify_dim_compatibility` (from `firecube.ingestor.runtime.zarr.existing_cube_check`) → `firecube.ingestor.api.verify_dim_compatibility`. Pre-write time-dim consistency check.

**Not done** (deliberate scope):
- No plugin migrations. Plugin-side shard-reading compat code and a `RegionZarrWriter` deep import remain in external plugins and become separate plans in those repositories.
- No compatibility shims, aliases, or `__getattr__` fallbacks. Pure re-exports only (verified by symbol-identity assertion in tests).
- No DESIGN.md broadening — only the now-stale internal path on line 86 was corrected to use the public name. DESIGN.md remains architectural guidance, not a changelog.

Evidence: `tests/sdk/test_public_api_surface.py` `_KEPT_CORE_API`, `_KEPT_INGESTOR_API`, and `_IDENTITY_PAIRS` lock all three names and their identity to the internal implementation.

---

## 2026-06-11 — Audit finding dispositions (A1-A7, S4-S8, C1-C9, T1-T7, P1-P2)

All open findings from the 2026-06-11 repository audit are closed. Resolved-same-day findings (S1, S2, S3, and the guideline-doc drift items) were already recorded in DONE.md on 2026-06-11. The entries below cover the remaining open findings.

**A1 — fsspec lint hole closed.** `open_fsspec_url` removed from `core/api.py` public surface; the raw-fsspec detector in `tests/unit/test_no_raw_fsspec_usage.py` extended to catch the unscored alias. Evidence: `tests/unit/test_no_raw_fsspec_usage.py`.

**A2 — Raw-fsspec fallback removed from `GenericParquetIngestor.write_parquet`.** The `storage_config is None` fallback that called fsspec directly is gone; the method now raises `ConfigurationError` instead. Evidence: `tests/unit/test_parquet_storage_invariant.py`.

**A3 — Basename inference deleted from `ZarrMultiresBuilder._store_uri_for`.** The URI basename heuristic is removed; `firecube zarr multires` requires `--product-name` explicitly. Evidence: `tests/unit/test_cli_zarr_multires.py`.

**A4 — Plugin contract test anchored to repo root.** `ALLOWED_PREFIXES` is now enforced and the vacuous-pass path is closed; `tests/sdk/test_plugin_contract.py` resolves paths from the repo root regardless of cwd. Evidence: `tests/sdk/test_plugin_contract.py`.

**A5 — `_storage_uri_from_target` centralized.** Five copy-pasted locality branches were centralized into a single helper in `core/uris.py`. Evidence: `tests/unit/test_uris.py`.

**A6 — Scheme inference deleted from `core/intake.py`.** The `storage_config` field is now required; scheme-inferred storage options are rejected. Evidence: `tests/unit/test_intake_explicit_storage.py`.

**A7 — `time_dim_name` threaded into tensogram converter.** The `("timestamp", "time")` preference-order guess is replaced by the plugin-declared `time_dim_name`. Evidence: `tests/unit/test_tensogram_time_dim.py`.

**S4 — Dead aliases removed.** `IngestionPipeline = PipelineExecutor` alias deleted from `runtime/engine.py`; `load_plugins` renamed to `discover_ingestors` in `registry/loader.py`. Evidence: `tests/unit/test_single_names.py`.

**S5 — `output_name` hard-rejected.** Removed from `SYSTEM_KEYS`; passing `output_name` in a config file now raises with a migration message pointing to `--product-name`. Evidence: `tests/unit/test_output_name_rejected.py`.

**S6 — Hardcoded domain defaults removed.** `group="FWI"` fallback deleted from `core/zarr/layers.py`; multires `(1.0, 0.5)` default single-sourced as `DEFAULT_MULTIRES_RESOLUTIONS` (no silent fallback on invalid input); `"fire_risk.duckdb"` default removed from `extensions/duck.py`. Evidence: `tests/unit/test_domain_defaults_removed.py`.

**S7 — `[database.duckdb]` scoped to DatabaseDuckDB tier.** Global merge into every plugin's defaults removed from `core/config.py`; the section is now only applied when the plugin declares a DatabaseDuckDB config tier. Evidence: `tests/unit/test_duckdb_tier_scoping.py`.

**S8 — `x_*` experimental namespace implemented.** Keys matching `x_*` are accepted at `--option` parse time and passed through to the engine without strict-unknown-key rejection. All other unknown keys continue to fail. Evidence: `tests/unit/test_experimental_options.py`.

**C1 — `ManifestRepository` split.** The ~1,300-line god class decomposed into `_wal_writer.py` (WAL write path), `_projection.py` (event projection), and `_legacy.py` (legacy migration); `repo.py` becomes a thin coordinator. Evidence: `tests/unit/test_manifest_repo_parity.py`.

**C2 — `ChunkManager` facade width accepted.** The 34-method facade mirrors the repo one-for-one by design: it is the single public surface for all control-plane operations and its width is a consequence of the domain, not a violation. Documented rationale in `plans/DESIGN.md` "Accepted Deviations" §C2. No code change.

**C3 — `_GenericBatchIngestor` dissolved.** The intermediate abstract class removed; `GenericZarrIngestor` and `GenericTensogramIngestor` re-parented directly onto `BaseIngestor`, reducing the inheritance depth to 3 levels. Evidence: `tests/unit/test_template_hierarchy.py`.

**C4 — `type()` synthesis deleted.** Runtime `type()` class synthesis for `--output-format tensogram` replaced by `DatasetProducer` protocol-checked strategy selection; direct `GenericTensogramIngestor` subclasses are now CLI-routable. Evidence: `tests/unit/test_tensogram_routing.py`. See also TODO §8 Phase 1.

**C5 — `SlotRangeCapable` protocol replaces `isinstance(DirectZarrIngestor)`.** Engine capability check at `runtime/engine.py` now uses the `@runtime_checkable` `SlotRangeCapable` protocol instead of a concrete class check. Evidence: `tests/unit/test_slot_range_capability.py`.

**C6 — `WriteModePolicy` replaces string comparisons.** Seven `write_mode == "staged"/"direct"` if-ladders across four layers replaced by a `WriteModePolicy` strategy object. Evidence: `tests/unit/test_write_mode_policy.py`.

**C7 — Storage completion extracted.** ~250 lines of storage completion logic moved from `PipelineExecutor` in `runtime/engine.py` to `core/storage/completion.py`. Evidence: `tests/unit/test_storage_completion.py`.

**C8 — `isinstance(self, DuckDbMixin)` replaces `getattr` probing.** The `# type: ignore` duck-type probe in `templates/generic.py` replaced by a proper `isinstance` check. Evidence: `tests/unit/test_duckdb_capability_check.py`.

**C9 — `install_option_groups_patch()` accepted.** The global Click monkey-patch at import time is a deliberate CLI formatting choice with no runtime side effects on ingestion. Documented rationale in `plans/DESIGN.md` "Accepted Deviations" §C9. No code change.

**T1 — `--strict-markers` active; s3/concurrency markers applied.** `--strict-markers` added to `addopts` in `pyproject.toml`; `s3` marker applied to all moto-backed integration suites; `concurrency` marker applied to concurrency tests. Evidence: `pyproject.toml`, `tests/integration/test_obstore_*.py`.

**T2 — Architecture/contract tests anchored to repo root.** `test_plugin_isolation.py` and `test_plugin_contract.py` now resolve paths from the repo root; vacuous-pass path closed. Evidence: `tests/architecture/test_plugin_isolation.py`, `tests/sdk/test_plugin_contract.py`.

**T3 — All 8 markers registered; `--strict-markers` in addopts.** `architecture`, `contract`, `s3`, `concurrency`, `plugin`, `slow`, `integration`, `unit` all registered in `pyproject.toml`; `--strict-markers` enforced. Evidence: `pyproject.toml`.

**T4 — Real-behavior tests for AppendStrategy and IndexedRegionStrategy.** Real-store behavior tests were added; `tests/unit/test_append_strategy.py` still patches the strategy's own internals. Evidence: `tests/integration/test_append_strategy_behavior.py`, `tests/integration/test_indexed_region_behavior.py`.

**T5 — External-binary skip accepted.** `pytest.skip("uv is not available on PATH")` in `tests/unit/test_version_consistency.py` is a sanctioned skip for a binary dependency; documented in `plans/TEST.md` "Sanctioned skip reasons". No code change.

**T6 — All 5 fixture plugins guarded in `pytest_sessionstart`.** `tests/conftest.py` now raises `UsageError` at session start if any of the 5 required fixture plugins is missing. Evidence: `tests/conftest.py`.

**T7 — 4 concurrency integration tests added.** Covers shared-product contention, WAL write ordering, workspace materialization races, and OTel context propagation under concurrency. Evidence: `tests/integration/test_concurrent_same_product.py`, `tests/integration/test_wal_concurrent_ordering.py`, `tests/integration/test_workspace_materialization_race.py`, `tests/integration/test_otel_context_concurrency.py`.

**P1 — 5 unused exports removed from public API.** `ScratchManager`, `CoverageTracker` removed from `ingestor/api.py`; `RegionZarrWriter`, `fs_kwargs_for_uri`, `open_fsspec_url` removed from `core/api.py`. `RuntimeIngestContext` is deliberately kept. Evidence: `tests/sdk/test_public_api_surface.py`.

**P2 — Internal `.output_path` reads migrated.** All internal reads of `result.output_path` migrated to `outputs.primary`; the compatibility property remains for external readers. Evidence: `tests/architecture/test_no_internal_output_path_reads.py`.

---

## §7-Phase 3.9 — Target/Storage-Type Coherence Validation Hardening (2026-06-01)

Cross-validates `--target` URI scheme against `--storage-type` during CLI parsing so incompatible configurations fail fast before ingestion starts.

**Commits**: C1 = dda65ab, C2 = 16151ef, C3 = c6b174f, C4 = 3c72561, C5 = 4789c37, C6 = 0306085

**Deliverables shipped**:
- `src/firecube/cli/_command_schemas.py`: cross-validation added in `IngestCommandConfig.__post_init__`; mismatched scheme/storage-type is now rejected at parse time
- Help text updated for `--storage-type` and `--target` flags in `firecube ingest`
- Coherence validation section added to plugin migration guidance

Final verification pending.

Full task breakdown captured in internal plan `storage-type-uri-cross-validation`.

## §7-Phase 3.8 — Parallel Ingestion External-Review Fixes #6 (2026-05-31)

Closes 2 verified external-review follow-up issues that surfaced after Phase 3.7 shipped.

**Commits**: C1 = 3c72561, C2 = 4789c37, C3 = <this commit>

**Deliverables shipped**:
- `firecube plan` blocked-range error message uses runnable `delete-span` form (LOW): the Phase 3.7 message at `plan.py:241-248` recommended `firecube chunks delete-span --run-id <id>` — but the CLI requires `-p, --product` (hard-fail) and the blocked-range scenario also requires `--force` (since the trigger IS non-alignment). The recommendation was non-runnable 100% of the time. Updated message now shows the canonical preview/commit pattern: `firecube chunks delete-span --product <product> --run-id <id> --force --dry-run` to preview, then swap `--dry-run` for `--yes-i-really-mean-it` to commit. Docs pointer uses mkdocs anchor (`#fail-closed-planning-behavior`) instead of bare file path. Regression test at `test_phase3_5_blocked_ranges.py::test_plan_error_message_format` strengthened with 4 new substring assertions locking the full runnable shape (`--product`, `--dry-run`, `--force`, `--yes-i-really-mean-it`) — previously only the verb `firecube chunks delete-span` was asserted, so the test passed even when `--product` was missing (commit C1)
- `docs/concepts/parallel-ingestion.md` `delete-span` remediation includes `--product`, `--force`, full safety flow (LOW): the Phase 3.7 fail-closed subsection at lines 266-274 and troubleshooting row at line 413 also showed the non-runnable `--run-id <id>` form. Fail-closed subsection expanded into a 4-step numbered procedure (list → preview → commit → re-ingest) with an explicit explanation of WHY `--force` is required (non-alignment IS the trigger condition). Troubleshooting row updated to be independently paste-runnable with the full command shape, plus a cross-link to the fail-closed subsection. Phase 3.7's `firecube chunks delete --range` clarification preserved (commit C2)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.8-followup-fixes`.

---

## §7-Phase 3.7 — Parallel Ingestion External-Review Fixes #5 (2026-05-30)

Closes 2 verified external-review follow-up issues that surfaced after Phase 3.6 shipped.

**Commits**: C1 = a3ee0fe, C2 = 2e464f6, C3 = <this commit>

**Deliverables shipped**:
- `firecube plan` blocked-range error message lists real remediation primitives (LOW): the Phase 3.5 fail-closed message at `plan.py:237-248` previously suggested "extend `global_expected_time_count()` to absorb the gap" as a blanket fix. This works for terminal-stub blocked ranges (e.g. `[950, 1000)` with `total=1000`) but is WRONG for prefix-misalignment cases (e.g. `[73, 100)` with `slot_size=100`) — extending the total cannot fix a misaligned prefix. Updated message now points operators at the actual span-level cleanup command (`firecube chunks delete-span --run-id <id>`) and the re-ingest path (`firecube ingest ... --option force_reingest=true`). `global_expected_time_count()` extension is preserved as a QUALIFIED option for terminal-stub cases only. Note: `firecube chunks delete --range` parses DATE strings, not slot indices — use `delete-span` for slot cleanup. Integration test `test_plan_error_message_format` strengthened with substring assertions for `firecube chunks delete-span` and `force_reingest=true` (commit C1)
- `docs/concepts/parallel-ingestion.md` refreshed for Phase 3.5 + 3.6 plan JSON contract (LOW): JSON example in the operator guide showed group objects WITHOUT `blocked_ranges` (Phase 3.5 added the field) and resume-semantics + troubleshooting sections did not mention Phase 3.5 blocked-partial-ranges fail-closed or Phase 3.6 coverage-lookup fail-closed behavior. Updated JSON example to show `blocked_ranges: []` on every group, added a `Fail-closed planning behavior` subsection documenting both modes with `--no-resume` and `firecube chunks delete-span` remediation cross-refs (plus an explicit note that `chunks delete --range` accepts dates only), and added 2 new troubleshooting table rows. Schema version unchanged; this is doc drift catch-up, not a schema change (commit C2)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.7-followup-fixes`.

---

## §7-Phase 3.6 — Parallel Ingestion External-Review Fixes #4 (2026-05-30)

Closes 1 verified external-review follow-up issue that surfaced after Phase 3.5 shipped.

**Commits**: C1 = 2615127, C2 = <this commit>

**Deliverables shipped**:
- `firecube plan` fail-closed on coverage lookup failure (MEDIUM): `_query_coverage` previously swallowed ALL exceptions and returned empty coverage, causing `firecube plan` to silently downgrade resume-aware planning to full planning on auth errors, corrupt control-plane state, driver bugs, or transient S3 errors. Orchestrators would then dispatch a full product worth of pods unnecessarily. First-run no-control-plane case is correctly handled WITHOUT exceptions (via `_load_current_state` returning empty dict), so the broad except was only masking real failures. Fix: removed `try: ... except Exception:` block from `_query_coverage`; caller now wraps the call with `click.ClickException` mapping that names the underlying failure and suggests `--no-resume` as the explicit bypass (commit C1)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.6-followup-fixes`.

---

## §7-Phase 3.5 — Parallel Ingestion External-Review Fixes #3 (2026-05-30)

Closes 2 verified external-review follow-up issues that surfaced after Phase 3.4 shipped.

**Commits**: C1 = 2992425, C2 = 8fda71a, C3 = <this commit>

**Deliverables shipped**:
- `firecube plan` fail-closed on blocked partial-chunk ranges (HIGH): `_chunk_aligned_remaining` previously dropped misaligned ranges silently (e.g. `[(950, 1000)] slot=100` → `[]`), so orchestrators ran nothing for the dropped slots. Function now returns `(aligned, blocked)` tuple. `firecube plan` raises `click.ClickException` BEFORE JSON emission when any group has non-empty `blocked_ranges`. Each group's JSON output gains a `blocked_ranges` diagnostic field. Misleading existing tests rewritten to assert the new fail-closed semantics (commit C1)
- `firecube plan` calls `zarr_schema()` exactly once (LOW): Phase 3.4 T1 added a phantom-validation call at `plan.py:156`, but `_resolve_per_group_slot_sizes` was still calling `zarr_schema()` separately. `_resolve_per_group_slot_sizes` signature changes to `(schema, explicit)` — accepts the already-loaded schema. Caller threads the line-156 schema through. Spy test asserts `call_count == 1` (commit C2)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.5-followup-fixes`.

---

## §7-Phase 3.4 — Parallel Ingestion External-Review Fixes #2 (2026-05-30)

Closes 3 verified external-review follow-up issues that surfaced after Phase 3.3 shipped.

**Commits**: C1 = 604a4d8, C2 = a9fc6e8, C3 = adf41dd, C4 = <this commit>

**Deliverables shipped**:
- Phantom `global_expected` group rejected at `firecube plan` and `firecube zarr setup-schema` preflights (HIGH): runtime ingest had the validator since Phase 3.3 T4, but CLI preflights silently emitted phantom ranges / reported success. Now both CLI commands call `validate_global_expected_subset_of_schema(global_expected, schema)` BEFORE any range output or zarr writes. `firecube plan` loads `zarr_schema(plugin_ctx)` unconditionally (preflight correctness, not slot-gated) (commit C1)
- `warn_if_misaligned` respects terminal partial chunks (MEDIUM): closes noisy false-positive warning where `validate_chunk_alignment` accepted `[900, 950)` with `global_expected=950` but the adjacent `warn_if_misaligned` call still logged "misaligned, suggested [900, 1000)". `warn_if_misaligned` now accepts optional `global_expected` parameter mirroring the validator's terminal-partial exception. `parallel_gate.py:129` passes `global_expected=global_schema` (commit C2)
- Sharding test no longer tautological (LOW): `test_write_dataset_skips_rechunk_when_chunks_already_match` previously compared the caller's Dask graph before/after `write_dataset_to_zarr()`, but the writer rebinds `ds = ds.chunk(...)` internally — Python rebinding doesn't mutate the caller's reference, so the assertion was always true. Now uses `monkeypatch.setattr(xr.Dataset, "chunk", spy)` with call-through to count actual chunk calls. Asserts `spy.call_count == 0` when shapes match. Test-only fix; production code unchanged (commit C3)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.4-followup-fixes`.

---

## §7-Phase 3.3 — Parallel Ingestion External-Review Fixes (2026-05-29)

Closes 6 verified external-review issues that surfaced after Phase 3.2 shipped.

**Commits**: C1 = 9fce649, C2 = 545689f, C3 = 40977ca, C4 = 8fc455d, C5 = c6fd238, C6 = 269acdf, C7 = <this commit>

**Deliverables shipped**:
- `run_sequential` applies same slot filter as `run_pipeline` (HIGH): closes silent default-CLI bug where `--slot-start`/`--slot-end` were ignored in sequential mode (default `pipeline_parallel=False`). Sequential slot-range is now first-class for debugging/recovery (commit C1)
- `validate_chunk_alignment` terminal partial-chunk exception + plan-to-ingest contract test (HIGH): closes mismatch where `firecube plan` emitted `(900, 950)` for length=950/chunk=100 products but the gate rejected it. Now allows ONLY `slot_end == global_expected[group]` as terminal partial; non-terminal misalignment still rejected. New contract test prevents future divergence (commit C2)
- URL-encoded `slot_group` in `run_id` + WAL reader full-subpath extraction (MEDIUM): closes path-safety bug where `slot_group` containing `/` (e.g. `multires/0.5deg`) corrupted WAL paths. `derive_pod_run_id` now URL-encodes via `urllib.parse.quote(safe="")`; WAL reader uses proper subpath extraction; EngineConfig warn-only validation guides operators (commit C3)
- Phantom group prevention in capability gate + pod-startup verification (MEDIUM): closes false-success audit-record bug where `_verify_schema_at_pod_startup` silently wrote success for groups in `global_expected` but absent from `zarr_schema()`. New additive `validate_global_expected_subset_of_schema` in `parallel_gate.py` catches at gate (cheap fast-fail); hard-fail in `_verify_schema_at_pod_startup` is defense-in-depth. Obsolete `extras_in_global` DEBUG log removed (commit C4)
- Operator docs refresh (MEDIUM): `docs/concepts/parallel-ingestion.md` plan-output example now shows per-group `slot_size` and per-range `--slot-group` (Phase 3.1/3.2 features); run_id example shows both single-group and multi-group formats with opacity warning. `docs/concepts/best-practices.md` no longer says "until the Phase 3 engine planner lands" (it landed 2026-05-28) (commit C5)
- Sharding test assertions strengthened (LOW): `test_append_to_sharded_store_preserves_shards` now asserts shard/chunk metadata (not just timestamp count); `test_write_dataset_skips_rechunk_when_chunks_already_match` now compares `_tasks_after == _tasks_before` (the actual Flaw 10 invariant) (commit C6)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.3-review-fixes`.

---

## §7-Phase 3.2 — Parallel Ingestion Correctness (2026-05-29)

Closes 6 verified review issues from the Phase 3.1 implementation.

**Commits**: C1 = 37f7521, C2 = 80583a2, C3 = 84bb1bb, C4 = 6b6f3a1, C5 = 450f8f7, C6 = 416a4b6, C7 = <this commit>

**Deliverables shipped**:
- `derive_pod_run_id` includes slot_group for multi-group pod isolation (HIGH): closes run_id collision when two pods share base + slot_range but differ in slot_group (commit C1)
- Strict schema validation via `SchemaDriftError` + `RegionZarrWriter.verify_array_spec` (HIGH): catches dtype/chunks/shape[1:]/rank/fill_value mismatch in setup-schema CLI AND pod-startup verification; NaN-safe fill_value comparison; `spec.fill_value=None` treated as "unspecified — skip check" (preserves idempotent setup); preserves larger-shape[0] tolerance (commit C2)
- Per-group `slot_size` in `firecube plan` JSON (MEDIUM-HIGH): LCM of all chunked arrays per group; chunk-aligned remaining intervals after resume; user `--slot-size N` validated against every group (commit C3)
- `refactor(zarr)`: `_ParallelExecutionState` dataclass introduced; `ctx._ctx._parallel_global_schema` escape hatch removed (MEDIUM): unblocks §13 PluginContext shrinkage; engine-internal state lives on `BaseIngestor.self._parallel_execution_state` (instance attribute, NOT a parameter); `_process_batch` signature and `engine.py` UNCHANGED (commit C4)
- Pod-startup schema verification + ChunkManager audit record (MEDIUM): `_setup_global_zarr_schema` moved out of `_process_batch` hot path; in-memory `schema_verified[group]` flag prevents re-verify within pod; NEW additive method `ChunkManager.record_schema_verification` writes per-pod audit records (each carrying its own `verified_at` — NOT content-idempotent; WAL `events-*.jsonl` per-writer files make this S3-safe); audit observational only — NEVER consulted for skip (commit C5)
- Docs cleanup (LOW): `cli/zarr.py` module docstring no longer claims standalone "read-only"; migration guide accurately describes WriteIntent-only group coverage and capability-gate error class (commit C6)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.2-correctness`.

---

## §7-Phase 3.1 — Parallel Ingestion Hardening (2026-05-28)

Closes 5 verified safety gaps in Phase 3 parallel ingestion surfaced by external review.

**Commits**: C1 = 94d167e, C2 = 4a5212e, C3 = ba90260, C4 = f9401f5

**Deliverables shipped**:
- Strict `global_expected_time_count()` coverage enforcement (HIGH): groups receiving WriteIntents MUST be declared in global_expected; previously only warned (commit C1)
- Intent-group-in-schema hard fail (defense in depth): WriteIntent for unknown group → ConfigurationError before writes (commit C1)
- All-arrays chunk validation (MEDIUM-LOW): validate every chunked array per group, not just first; addresses heterogeneous chunks within groups (commit C1)
- `--slot-group <name>` CLI flag + `EngineConfig.slot_group` + `FIRECUBE_SLOT_GROUP` env var (MEDIUM): backward-compatible per-group slot range targeting for multi-group plugins (commit C1)
- Multi-group capable test fixture (`multi_group_capable_test_plugin`): 2 writable groups, heterogeneous chunks across + within groups (commit C1)
- Group-aware ResumeGuard for non-terminal runs (refactor): disjoint groups on same slot range no longer conflict (commit C2)
- NEW slot-range-aware completed-span check (HIGH): staggered serial pods on disjoint slot ranges no longer need `resume_existing=true` (commit C2)
- Split bypass semantics: `resume_existing` → non-terminal only; `force_reingest` → completed only (commit C2)
- `_ranges_overlap_inclusive_vs_halfopen` helper extracted + tested at boundaries (commit C2)
- `--slot-group` propagation through capability gate (unknown-group hard fail), post-intent assertion (narrowed validation), and `firecube plan` JSON output (per-range --slot-group emission) (commit C3)
- Migration guide docs cleanup: removed misleading "discards items" wording, added "Required vs Optional" table, updated docstring (commit C4)
- Phase 3 single-group regression integration test (commit C4)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.1-hardening`.

---

## §7-sub / Phase 3 — Safe within-group parallel ingestion (2026-05-28)

Closes the final Cat-A item in §7: engine/template-level planner pre-computing
deterministic time-to-index mappings and chunk-aligned ranges.

**Commits**: C1 = 66ce6e0, C2 = be8e248, C3 = 752f7a1, C4 = 7c7f820

**Deliverables shipped**:
- `SUPPORTS_SLOT_RANGE_PARALLELISM` ClassVar + 3 optional hooks on `DirectZarrIngestor`
- `validate_parallel_capability()` fail-fast gate in `BaseIngestor.run()` (before ResumeGuard)
- Chunk-alignment hard failure + post-intent assertion (`WriteIntentRangeError`)
- Global schema setup via `zarr_schema_global` claims (`SchemaSizeMismatchError`)
- `ResumeGuard` 6-row conflict matrix (`RangeOverlapError`)
- Per-pod deterministic `run_id` derivation (`__slot=N-M` suffix)
- `firecube plan <product>` stable JSON output for Argo/Kubeflow orchestrators
- `firecube zarr setup-schema` operator preflight command
- `--slot-start/--slot-end/--slot-size` CLI flags + K8s env discovery
- Phase 2 `_sweep_legacy_zarr_group_claims` compat code removed
- Operator guide: `docs/concepts/parallel-ingestion.md`

---

Date: 2026-05-27 Task: §7-GENERIC

Decision: `GenericZarrIngestor` claim category renamed to `zarr_append` — commit `79682b4` — Files: `src/firecube/ingestor/templates/generic.py` (1-line closure change at ~line 296), `tests/unit/test_generic_zarr_claim_category.py` (new, 3 tests), `tests/integration/test_maintenance_claims.py` and `tests/integration/test_obstore_claim_atomicity.py` (fixture updates). AppendStrategy remains serial (process-local lock); naming now reflects append-mode coordination.

Date: 2026-05-26 Task: §11

Decision: `firecube zarr multires` CLI subcommand implemented — commit 5b4c0c5 — Files: `src/firecube/cli/zarr.py` (added multires subcommand wrapping `ZarrMultiresBuilder`), `tests/unit/test_cli_zarr_multires.py` (new). Verification: `uv run firecube zarr multires --help`. Prerequisite for §25 deletion.

---

This file records accepted decisions and completed work.
New decisions are appended with a date. Read this when context for an existing decision is needed.

---

Date: 2026-05-26 Task: §27

Decision: `build_dataset` signature updated in tutorials and references

Context: Runtime uses `build_dataset(self, group: str, items: list[Any], ctx: PluginContext)` at `src/firecube/ingestor/templates/generic.py:187`. Six doc files still showed the old `batch: PipelineBatch` form, misleading new plugin authors. Updated all six files to the new `items: list[Any]` signature. Added one deprecation-shim note in `docs/guides/subclassing_generic.md` explaining that the legacy signature still works via the shim.

Files changed:
- `docs/tutorials/weather-csv.md` (signature + import updated)
- a detections tutorial page (signature + import updated)
- `docs/tutorials/observability.md` (signature + import updated)
- `docs/guides/subclassing_generic.md` (signature at lines 20, 40, 104 + deprecation paragraph added)
- `docs/concepts/best-practices.md` (signature at lines 14, 21 updated)
- a plugin reference page (Mermaid diagram updated)

Verification: `grep -rn 'build_dataset.*PipelineBatch' docs/` returns exactly 1 match (the deprecation paragraph itself). MkDocs `--strict` build passes.

Evidence: task-5 workspace notes (no-stale grep, new-signature grep, deprecation-paragraph grep, MkDocs `--strict` build, commit hash).

- Source: TODO.md §27

---

Date: 2026-05-26

Decision: Scaffolding templates now generate `PRODUCT_NAME` for all plugin variants

Context: `firecube plugins scaffold` emitted classes that failed `BaseIngestor.__init_subclass__()` at import time because the generated plugin classes omitted the required `PRODUCT_NAME` class attribute. All four templates now emit `PRODUCT_NAME: ClassVar[str] = "{plugin_name}"` beside the existing `name` field.

Consequences:
- Generated base, zarr, parquet, and direct-zarr plugins now satisfy the product-name contract at import time.
- Each emitted template imports `ClassVar` explicitly for the class-level annotation.
- A targeted regression test covers all four scaffold templates.

Verified: `uv run pytest tests/unit/test_scaffolding_product_name.py -q`

Evidence:
- Commit: `76b8b2c`
- Verified by: `src/firecube/ingestor/devtools/scaffolding.py`, `tests/unit/test_scaffolding_product_name.py`
- Source: TODO.md §26

---

Date: 2026-04-12

Decision: Concurrent WAL segment reads in `_load_current_state` via `ThreadPoolExecutor`

Context: `_load_current_state` read run directories and event segments sequentially. For a product with many completed runs past the snapshot cutoff, this was O(N) sequential filesystem opens. On S3, each open is a network round trip, making the read path linearly slower as WAL depth grows. The fix parallelises the I/O phase while preserving deterministic projection ordering.

Consequences:
- `ThreadPoolExecutor(max_workers=8)` at `src/firecube/core/controlplane/_snapshot.py:185` performs parallel WAL segment reads.
- Sequential `apply_events` call after collection preserves upsert-by-key ordering — projection correctness is not affected.
- Single-run fast path bypasses the pool entirely to avoid overhead on the common case.
- `ControlPlaneCorruptionError` from any concurrent reader propagates and aborts the projection rather than being swallowed.
- Note: TODO.md §17 described this as a "candidate direction, not approved design". The implementation has shipped at `src/firecube/core/controlplane/_snapshot.py:185`.

Verified: `grep -n "ThreadPoolExecutor" src/firecube/core/controlplane/_snapshot.py` returns a match at line 185.

Evidence:
- Commit: `c899782` (Confidence: MEDIUM — this is the extraction commit that moved `_snapshot.py` out of `repo.py`; the parallel-WAL implementation itself may predate the file split, but the location is verifiable here)
- Verified by: `src/firecube/core/controlplane/_snapshot.py:185`
- Source: TODO.md §17

---

Date: 2026-04-27

Decision: Time-window maintenance with logical deletion, physical scrub, and targeted window selection

Context: Firecube needed a way to delete and reingest specific time windows without ad-hoc S3 operations. The solution introduces a `firecube_timestamp_state` array for logical deletion tracking, a `scrub.py` module for physical cleanup via `ChunkManager`, and CLI commands for safe, auditable maintenance operations.

Consequences:
- `firecube_timestamp_state` array added with four state values: `{0: unknown, 1: present, 2: deleted_by_firecube, 3: failed_batch}`.
- `attach_timestamp_state_dataset` helper threads state names through write strategies.
- `src/firecube/core/zarr/scrub.py` (186 lines) uses `ChunkManager.create_deletion_plan()` and `execute_deletion()` — no ad-hoc S3 deletes anywhere.
- CLI commands `chunks delete` and `chunks delete-span` added with safety flags.
- `time_overlaps` parameter enables targeted window selection, linking maintenance to the §2 query primitives.

Verified: `grep -rn "firecube_timestamp_state" src/firecube/` returns matches in write strategy and scrub modules.

Evidence:
- Commit: `d8619f3` (Confidence: MEDIUM)
- Verified by: `src/firecube/core/zarr/scrub.py`
- Source: TODO.md §8

---

Date: 2026-04-14

Decision: Zarr write-strategy abstraction with `ZarrWriteStrategy` Protocol, two implementations, and extracted runtime services

Context: The Zarr write path was monolithic, making it hard to add new write strategies or reuse components across templates. The redesign introduces a `ZarrWriteStrategy` Protocol as a stable seam, two concrete implementations for the two write patterns, and extracted `ScratchManager`/`CoverageTracker` services available to all plugin templates.

Consequences:
- `ZarrWriteStrategy` Protocol at `src/firecube/ingestor/runtime/zarr/contracts.py:20` (`@runtime_checkable`).
- `AppendStrategy` at `strategies/append.py:45` wraps `append_time_groups` for xarray-append plugins.
- `IndexedRegionStrategy` at `strategies/indexed_region.py:53` handles direct zarr-python region writes via `RegionZarrWriter`.
- `DirectZarrIngestor` template at `src/firecube/ingestor/templates/direct_zarr.py:96` for region-write plugins.
- `GenericZarrIngestor` refactored to delegate the core append step to `AppendStrategy.write_groups()` at `templates/generic.py:342`; remaining inline ownership (staged metadata seeding, lock construction, strategy construction, claim construction, metrics assembly) tracked as a follow-up in TODO §22.
- `ScratchManager` and `CoverageTracker` extracted and re-exported from `firecube.ingestor.api`.
- `append_time_groups` decomposed from a ~400-line monolith into a 173-line orchestrator at `runtime/zarr/append.py` plus five focused services in `runtime/zarr/append_services.py` (622 lines): `AppendTimestampState` (state array init/updates), `AppendResumeService` (resume cache, cursor inference, overlap detection), `AppendWriteExecutor` (write loop, alignment checks), `AppendCoverageBuilder` (coverage entries, time-range tracking), `AppendMultiresHandler` (multires layer building post-write).
- Public contract types for direct-write plugins: `WriteIntent`, `ZarrArraySpec`, `ZarrGroupSpec` defined in `templates/direct_zarr.py:45-94` and re-exported via `firecube.ingestor.api`. `WriteIntent.kind` ("region" / "1d" / "timestamp") selects the write method; `ts_index: int` is the canonical row key.

Caveats (known gaps surfaced after this entry landed; tracked as TODO follow-ups):
- **Protocol non-conformance**: `ZarrWriteStrategy.write_groups()` declares `(*, group_to_timestamps, dataset_for_batch, batch_size, claim_for_group)` at `runtime/zarr/contracts.py:32-39`. `AppendStrategy.write_groups()` matches. `IndexedRegionStrategy.write_groups()` at `strategies/indexed_region.py:75-81` requires a different parameter set (`group_to_intents`, `schema`). The Protocol is therefore not a real shared interface — it works only for `AppendStrategy`. Tracked as TODO §21.
- **Facade incomplete**: `GenericZarrIngestor._process_batch` is 144 lines and still owns five orchestration responsibilities inline (see TODO §22).
- **Claim granularity is per-group**: `DirectZarrIngestor` builds `WriteDomain(category="zarr_group", name=group_name)` at `templates/direct_zarr.py:179-182`. Per-region/per-slot granularity (needed for safe intra-group parallelism) is tracked as part of TODO §7.

**Status as of 2026-05-27 (Phase 2 completion — supersedes caveats above):**
- Protocol non-conformance addressed by §21 — `ZarrWriteStrategy` was split into `AppendWriteStrategy` + `RegionWriteStrategy`, both `@runtime_checkable` (commit `e16f6b4`).
- Facade complete — §22 DONE (helpers in commit `8ab0fbf`, wiring in commit `e317c2e`). `_process_batch` is now ~66 lines (down from 143), delegating to `runtime/zarr/batch_runner.py` helpers.
- Claim granularity now per-slot for `DirectZarrIngestor` — §7-DIRECT DONE in commit `701ed07`. Schema claims use `name=f"{group}:schema"`, slot claims use `name=f"{group}:slot={ts_index}"`. `GenericZarrIngestor` uses `category="zarr_append"` (§7-GENERIC commit `f332133`). Only the Phase 3 planner/orchestrator (§7-sub) remains open.
- `AppendMultiresHandler` removed in §25 (commit `89b2737`) — ingest-time multires is no longer supported; use `firecube zarr multires` post-step CLI.

Verified: `grep -n "class ZarrWriteStrategy" src/firecube/ingestor/runtime/zarr/contracts.py` returns a match at line 20.

Evidence:
- Commit: `7e7977c` (Confidence: HIGH)
- Verified by: `src/firecube/ingestor/runtime/zarr/contracts.py:20`, `tests/unit/test_coverage_tracker.py`
- Source: TODO.md §4a

---

Date: 2026-04-14

Decision: Storage driver abstraction with `StorageFilesystem` Protocol, `FsspecFilesystem`/`ObstoreFilesystem` implementations, and driver-aware Zarr store construction

Context: Firecube needed to support both fsspec and obstore as storage backends without mixing drivers within a single run. The abstraction introduces a Protocol-based filesystem interface, two concrete implementations, and a factory function for driver-aware Zarr store construction. The `--storage-driver` CLI flag makes the choice explicit.

Consequences:
- `StorageFilesystem` Protocol at `src/firecube/core/filesystem/protocol.py` with `FsspecFilesystem` and `ObstoreFilesystem` implementations.
- `create_zarr_store()` factory function and `ZarrStoreHandle` frozen dataclass in `src/firecube/core/filesystem/store_factory.py` for driver-aware Zarr store construction.
- `--storage-driver [fsspec|obstore]` required CLI flag selects the backend; no mixing within a run.
- `_obstore_compat.py` lazy import guard keeps obstore optional (`uv pip install 'firecube[obstore]'`).
- `tests/integration/test_one_driver_invariant.py` (279 lines) enforces the one-driver-everywhere invariant.
- Note: `ZarrStoreFactory` is not a class. It is `create_zarr_store()` function plus `ZarrStoreHandle` dataclass in `store_factory.py`. The module docstring reads "ZarrStoreFactory" but the implementation is function-based.

Verified: `grep -n "class StorageFilesystem" src/firecube/core/filesystem/protocol.py` returns a match.

Evidence:
- Commit: `1cf537f` (Confidence: HIGH)
- Verified by: `src/firecube/core/filesystem/store_factory.py`, `tests/integration/test_one_driver_invariant.py`
- Source: TODO.md §4b

---

Date: 2026-04-14

Decision: CLI lifecycle migration — engine owns post-execution staging and upload; CLI is purely an entry point

Context: The CLI previously owned staged-output completion, write-mode-specific output resolution, and the upload step after the ingestor returned. That ownership split made the runtime hard to reuse outside the CLI and meant "ingestion complete" was not fully self-contained. The migration moves all post-execution lifecycle into the engine; the CLI now only parses arguments, passes `--write-mode` to the engine, and echoes results.

Consequences:
- Engine owns `complete_output()` at `src/firecube/ingestor/runtime/engine.py:570-616`. It resolves the final output URI, handles staged-vs-direct write modes, and constructs the final manifest.
- Engine owns `_complete_s3_staged()` at `src/firecube/ingestor/runtime/engine.py:717-787`. It performs the actual staged upload to S3.
- `BaseIngestor.run()` calls `engine.complete_output()` before returning to the CLI.
- CLI at `src/firecube/cli/main.py:500-511` only echoes the result manifest and output path.
- `--write-mode [staged|direct]` is a required CLI flag (no inference); passed to the engine via `IngestContext.options["write_mode"]`.

Verified: `grep -n "def complete_output\|def _complete_s3_staged" src/firecube/ingestor/runtime/engine.py` returns matches at lines 570 and 717.

Evidence:
- Commit: `2f23373` (Confidence: HIGH)
- Verified by: `src/firecube/ingestor/runtime/engine.py:570-616`, `src/firecube/cli/main.py:500-511`
- Source: architectural cleanup of the CLI/engine boundary

---

Date: 2026-04-11

Decision: Engine-owned `metrics["storage"]` synthesis — control-plane reporting is core-owned, not plugin-synthesized

Context: Plugins were previously responsible for constructing `metrics["storage"]` themselves, leading to inconsistent or missing control-plane metadata in run summaries. Moving this responsibility to the engine ensures every run gets consistent storage metrics regardless of which plugin ran.

Consequences:
- Engine synthesizes `metrics["storage"]` at `src/firecube/ingestor/runtime/engine.py:535-560`, validating and replacing any non-mapping plugin values.
- Engine injects `control_root` and `latest_pointer` via `describe_control_plane` from `ChunkManager`.
- Scaffolding templates use `merge_batch_metrics()` — no manual storage construction needed in plugins.
- `DirectZarrIngestor` scaffolding added to `scaffolding.py`.
- Plugin docs updated at `docs/concepts/plugins.md:138,158` and `docs/concepts/observability.md:108,146-164`.
- Test `test_finalize_injects_control_plane_storage_metrics` asserts engine ownership.
- Note: TODO.md cites `engine.py:502-527`; actual location is `engine.py:535-560` due to a ~33-line shift from later edits. Same code, just shifted.
- Follow-up: an external plugin's metrics module still synthesizes `control_root` and `latest_pointer` locally, labelled "Backward-compatible alias until core owns control-plane reporting". Core now owns these fields; the alias can be removed in a future plugin cleanup pass.

Verified: `grep -n "describe_control_plane\|merge_batch_metrics" src/firecube/ingestor/runtime/engine.py` returns matches.

Evidence:
- Commit: `b021ed5` (Confidence: HIGH)
- Verified by: `src/firecube/ingestor/runtime/engine.py:535-560`, `tests/unit/test_engine_resilience.py`
- Source: TODO.md §16

---

Date: 2026-04-10

Decision: Meta-aware `ChunkManager` queries with time-range filtering, status filtering, and `time_coverage_summary`

Context: `ChunkManager` previously required callers to fetch the full chunk list and filter in application code. Adding time-range and status parameters to the query methods moves filtering into the control-plane layer, reducing data transfer and making maintenance and resume logic simpler to express.

Consequences:
- `list_chunks()` accepts `time_min_after`, `time_max_before`, and `time_overlaps` parameters.
- `list_runs()` accepts `status` and `non_terminal` filtering parameters.
- `time_coverage_summary()` returns per-group time bounds and span counts for diagnostics and resume overlap detection.
- `timestamps_written` counter available at both `SpanRecord` and `ChunkInfo` levels.
- CLI `--time-range` and `--status` flags exposed in `firecube chunks` subcommands.

Verified: `grep -n "time_coverage_summary\|time_overlaps" src/firecube/core/controlplane/manager.py` returns matches.

Evidence:
- Commit: `fe7870d` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/manager.py`, `src/firecube/core/controlplane/repo.py`
- Source: TODO.md §2

---

Date: 2026-04-07

Decision: `firecube advise batch-size` CLI command for chunk-aligned batch size recommendations

Context: Choosing a `pipeline_batch_size` that avoids partial Zarr chunks is non-obvious. A misaligned batch size causes extra write overhead and can degrade read performance. The `advise` command reads the existing Zarr metadata and emits a recommendation aligned to the time chunk size.

Consequences:
- `src/firecube/cli/advise.py` (117 lines) implements the command.
- `firecube advise batch-size` registered as a Click subcommand, visible in `--help`.
- Opens the Zarr group via `session.zarr.open_group()` and inspects `arr.chunks[0]` of the first multi-dimensional array via `_find_time_chunk_size()`.
- Emits an aligned `pipeline_batch_size` recommendation with partial-chunk-avoidance rationale.
- Special cases: `time_chunk_size==1` and no-time-dim arrays handled without crashing.

Verified: `grep -n "advise batch-size\|def batch_size" src/firecube/cli/advise.py` returns matches.

Evidence:
- Commit: `66f6bc6` (Confidence: HIGH)
- Verified by: `src/firecube/cli/advise.py`
- Source: TODO.md §10

---

Date: 2026-04-07

Decision: NetCDF-to-Zarr shared utilities in `firecube.core.formats.netcdf` (3 of 4 utilities)

Context: Every NetCDF-based plugin independently handled the same set of NetCDF-to-Zarr V3 incompatibilities: HDF5 encoding cleanup, time dimension renaming, and preparation composition. Centralising these in core eliminates duplication and gives plugins a documented, tested preparation path.

Consequences:
- `clean_netcdf_encoding(ds)` strips `chunks`, `chunksizes`, and `preferred_chunks` from variable encodings to prevent xarray validation errors.
- `rename_time_dim(ds, target="timestamp")` renames the `time` dimension to `timestamp` idempotently.
- `prepare_netcdf_for_zarr(ds)` is a convenience wrapper composing the two primitives in the correct order.
- All three utilities present in `src/firecube/core/formats/netcdf.py` (56 lines total).
- Note: `normalize_string_vars` (the fourth utility for fixed-length Unicode conversion) is absent from the codebase. This entry covers only the three shipped utilities; the remainder is tracked in `plans/TODO.md`.

Verified: `grep -n "def clean_netcdf_encoding\|def rename_time_dim\|def prepare_netcdf_for_zarr" src/firecube/core/formats/netcdf.py` returns three matches.

Evidence:
- Commit: `ef771cc` (Confidence: HIGH)
- Verified by: `src/firecube/core/formats/netcdf.py`
- Source: TODO.md §19

---

Date: 2026-04-06

Decision: Logging consistency hardening in the control-plane layer — f-strings removed, silent suppression replaced

Context: The control-plane layer had several silent failure modes: f-string log calls (deferred formatting even when the log level is disabled), `suppress(Exception)` blocks with no log output, and silent early returns in maintenance commands. These made operational debugging harder than necessary.

Consequences:
- Zero f-string log calls remain in `controlplane/` — all use `%`-style parameterized formatting.
- `events.py:62-69` `makedirs` non-fatal try/except now logs `debug` at each site (commit `9cd1ca9`).
- `discover_manifests` and `_list_run_entries` log `WARNING` with path and exception context before returning empty lists (commit `9fd5309`).
- `rebuild_snapshot` logs `DEBUG` when skipping the local lock in remote mode (commit `29174e9`).
- Timing logs added in `_load_current_state` and `rebuild_snapshot` (commit `29174e9`).
- `abandon_run` logs `DEBUG` when the run is already terminal (commit `29174e9`).

Verified: `grep -rn "f\"" src/firecube/core/controlplane/ | grep "\.log\." | wc -l` returns 0.

Evidence:
- Commits: `9cd1ca9`, `9fd5309`, `29174e9` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/repo.py`, `src/firecube/core/controlplane/events.py`
- Source: TODO.md §18.1

---

Date: 2026-04-06

Decision: Error handling hardening in `ManifestRepository` — assert guards replaced with explicit `ManifestError` raises

Context: `repo.py` used bare `assert self._resolver/self._fs is not None` as internal state guards. These produce `AssertionError` with no context when they fire, and are silently disabled under `python -O`. Replacing them with explicit `ManifestError` raises gives operators a clear error message and a consistent exception type.

Consequences:
- 14+ call sites in `repo.py` now use `if X is None: raise ManifestError("Repository not bound — call bind() first")`.
- `ManifestRepository.close()` logs `WARNING` before suppressing flush failures.
- All `ControlPlaneCorruptionError` raises standardized with product/run context; `self.log.error(msg)` precedes every raise.
- `self.log.error(...)` precedes every `raise ControlPlaneCorruptionError(...)` for torn-tail detection.
- Note: TODO.md claimed "All 26 `assert self._resolver/self._fs is not None` replaced". 5 type-narrowing asserts remain at `repo.py:1069, 1077, 1083, 1240, 1245`. Each is preceded by `self._ensure_bound()` which raises `ManifestError` on bind failure, so the asserts are type-narrowing hints for static checkers and never fire at runtime. The literal claim is overstated; the runtime behavior is correct.

Verified: `grep -rn "ManifestError" src/firecube/core/controlplane/ | grep -v __pycache__ | wc -l` returns 24+.

Evidence:
- Commits: `9fd5309`, `29174e9` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/repo.py:1326-1338`
- Source: TODO.md §18.2

---

Date: 2026-04-06

Decision: WAL observability — corruption metrics, snapshot age warnings, and `firecube chunks snapshots status` CLI

Context: The control-plane layer had no metrics for WAL operations. Corruption events, torn-tail recoveries, and snapshot age were invisible to operators. The fix adds a lightweight `contextvars`-based metrics collector, instruments the key detection sites, and surfaces snapshot age via a new CLI command.

Consequences:
- `src/firecube/core/controlplane/metrics.py` (77 lines) with `WalMetrics` dataclass and `contextvars` collector.
- `record_wal_corruption()` fires at 10 detection sites in `repo.py` and `_wal_reader.py`.
- `firecube_control_plane_corruption_total` metric in `RUN_SUMMARY_SCHEMA`, emitted to Pushgateway per run.
- Pipeline integrates via `collect_wal_metrics()` context manager.
- `firecube chunks snapshots status -p <product>` CLI command prints snapshot age, generation, and record count.
- WARNING log emitted in `_load_current_state` when snapshot age exceeds 24 hours (`_snapshot.py:135-146`).
- Snapshot rebuild duration metric at `repo.py:805`.

Verified: `grep -rn "record_wal_corruption\|WalMetrics" src/firecube/core/controlplane/` returns matches.

Evidence:
- Commits: `fb5d9ff`, `3915150`, `29174e9` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/metrics.py`, `src/firecube/core/controlplane/repo.py`
- Source: TODO.md §18.3

---

Date: 2026-04-06

Decision: OTel `TracerProvider` lifecycle management — explicit shutdown in CLI `finally` block

Context: OpenTelemetry `BatchSpanProcessor` background threads can keep the process alive after ingestion completes if the tracer provider is not explicitly shut down. In a Kubeflow/Argo pod, this causes the step to appear still running after the ingest finishes, preventing the orchestrator from recording the correct outcome.

Consequences:
- `shutdown_tracing()` at `src/firecube/core/observability/tracing.py:43` calls `provider.force_flush(timeout_millis//2)` then `provider.shutdown()` with a 5-second default timeout; both calls are exception-guarded.
- `shutdown_observability()` facade at `core/observability/__init__.py:27` wraps `shutdown_tracing()` and resets the init guard.
- CLI ingest at `cli/main.py:378 try ... :512 finally` invokes `shutdown_observability(timeout_millis=5000)`.
- The `finally` block runs on both success and failure paths — no `except`-and-swallow inside.

Verified: `grep -n "shutdown_observability\|shutdown_tracing" src/firecube/cli/main.py` returns a match in the `finally` block.

Evidence:
- Commits: `82ca7e7`, `c185350` (Confidence: HIGH)
- Verified by: `src/firecube/core/observability/tracing.py:43`, `src/firecube/cli/main.py`
- Source: TODO.md §20.2

---

Date: 2026-02-10

Decision: Plugin development documentation in `docs/concepts/plugins.md` and `docs/guides/subclassing_generic.md`

Context: Plugin authors had no canonical reference for building a `BaseIngestor` subclass, understanding the hook surface, or knowing which public API symbols to import. Without documentation, every new plugin required reading engine internals, and the plugin contract was effectively undiscoverable.

Consequences:
- `docs/concepts/plugins.md` created (200+ lines) covering the plugin contract, hook surface, `PluginContext`, metrics, and engine ownership of storage metrics.
- `docs/guides/subclassing_generic.md` created (126+ lines) with a worked example of subclassing `GenericZarrIngestor`.
- Both files contain substantive content, not stubs.
- Plugin authors can now depend on `firecube.ingestor.api` and `firecube.core.api` as stable public surfaces.

Verified: `wc -l docs/concepts/plugins.md docs/guides/subclassing_generic.md` returns 200+ and 126+ lines respectively.

Evidence:
- Commit: `9b34ae6` (Confidence: MEDIUM)
- Verified by: `docs/concepts/plugins.md`, `docs/guides/subclassing_generic.md`
- Source: TODO.md §3

---

Date: 2026-02-10

Decision: `FIRECUBE_*` as the sole primary environment variable namespace; `AWS_*` as a write-only compat layer

Context: Firecube's configuration was mixing `FIRECUBE_*` and `AWS_*` variable names, making it unclear which namespace drove Firecube behavior and which was a passthrough for underlying libraries. The cleanup establishes `FIRECUBE_*` as the authoritative namespace and confines `AWS_*` to an explicit compat layer.

Consequences:
- `FIRECUBE_*` is the sole primary namespace, read by `core/config.py` and used across 10+ modules.
- `AWS_*` variables are a write-only compat layer confined to `core/runtime.py`, derived FROM `FIRECUBE_*` values, never used to drive Firecube behavior directly.
- `core/runtime.py` contains an explicit rationale docstring explaining the compat layer.
- Zero `AWS_*` reads in Firecube's own config resolution path.

Verified: `grep -rn "AWS_" src/firecube/core/config.py` returns no matches (all AWS reads are in `runtime.py` only).

Evidence:
- Commit: `ac8ddd4` (Confidence: MEDIUM)
- Verified by: `src/firecube/core/config.py`, `src/firecube/core/runtime.py`
- Source: TODO.md §4

---

Date: 2026-01-05

Decision: Safer resume semantics via `ResumeGuard` with control-plane-primary authority

Context: Partial runs left the product in an ambiguous state. Without a formal resume guard, re-running ingestion could silently overwrite or duplicate data. The `ResumeGuard` class makes resume decisions based solely on WAL state, blocking on non-terminal runs and optionally validating the Zarr store when explicitly requested.

Consequences:
- `ResumeGuard` class at `src/firecube/ingestor/runtime/resume_guard.py:16` is the single authority for resume decisions.
- Resume is control-plane-primary: WAL state is consulted first; the data store is only read under explicit `validate_zarr=true`.
- `time_overlaps` parameter enables overlap detection at `manager.py:358` and `repo.py:493`.
- Non-terminal runs block resume and require explicit `firecube chunks runs abandon` before proceeding.
- `validate_zarr`, `validate_zarr_group`, `validate_zarr_timeout_s`, `validate_zarr_max_chunks`, `validate_zarr_on_timeout` budget flags in `src/firecube/ingestor/config/engine.py:64-68`.
- Staged-write metadata seeding via `seed_staged_store_metadata()` at `src/firecube/ingestor/runtime/zarr/staged_metadata.py` copies `zarr.json` from the final target into the temp store before staged writes.
- Note: AGENTS.md "Where things live" cites `ingestor/utils/zarr/staged_metadata.py` but the file is at `ingestor/runtime/zarr/staged_metadata.py`. The functionality is fully present; only the AGENTS.md path reference is stale.

Verified: `grep -rn "class ResumeGuard" src/firecube/` returns `src/firecube/ingestor/runtime/resume_guard.py:16`.

Evidence:
- Commit: `b9017c1` (Confidence: MEDIUM)
- Verified by: `src/firecube/ingestor/runtime/resume_guard.py`, `src/firecube/ingestor/runtime/zarr/staged_metadata.py`
- Source: TODO.md §1

---

Date: 2026-01-05

Decision: WAL-backed run/span control-plane records with strict schema versioning and legacy migration

Context: The legacy `._firecube_manifest.jsonl` format was a flat append-only file with no schema version, making it hard to query by run, filter by status, or perform safe maintenance operations. The run/span model replaces it with a structured WAL under `.firecube/runs/<run_id>/events-*.jsonl`, enabling lifecycle-aware queries and explicit schema evolution.

Consequences:
- WAL events under `.firecube/runs/<run_id>/events-*.jsonl` are the authoritative control-plane records.
- `SpanRecorder` is lifecycle-aware with `register_run_started`, `register_run`, and `register_run_failure` methods.
- Strict `SCHEMA_VERSION = "v2"` enforced at 4 read sites — no silent schema drift.
- Legacy v1 forward-only migration via `migrate_legacy_manifest` for existing products.
- `list_runs(status=..., non_terminal=...)` with CLI surface `firecube chunks runs list --status started`.
- `._firecube_manifest.jsonl` is legacy-only; new code does not read or write it.

Verified: `grep -rn "SCHEMA_VERSION" src/firecube/core/controlplane/` returns matches at 4+ read sites.

Evidence:
- Commit: `b9017c1` (Confidence: MEDIUM); hardening in `bc463d4` (2026-03-09)
- Verified by: `src/firecube/core/controlplane/repo.py`, `src/firecube/core/controlplane/events.py`
- Source: TODO.md §6

---

Date: pre-restructure (Confidence: LOW)

Decision: Storage and URI handling hardened — no substring guessing, no `startswith("s3://")` in business logic

Context: URI handling in the storage layer used ad-hoc `startswith("s3://")` checks and substring guessing to determine storage type. These patterns are fragile and break when URI schemes change or when local paths are passed with `file://` prefixes. The fix routes all URI decisions through `parse_uri()` and `is_remote_target()`.

Consequences:
- `ensure_product_uri` uses `parse_uri()` plus last-segment equality — no substring guessing.
- `path_stats` uses `is_remote_target()` instead of `startswith("s3://")`.
- Zero bare `startswith("s3://")` calls anywhere in `src/firecube/`.
- Bucket validation moved to `StorageUri.parse()` / `__post_init__` in `src/firecube/core/storage/uri.py:65-104`, raising `ValueError` at URI construction time.
- Note: TODO.md §12.1 claims "`test_connection` raises `StorageError` explicitly when no bucket is configured". This claim is stale. `S3Storage`, `LocalStorage`, and `BaseStorage` were deleted entirely in commit `1addd97`. The `test_connection` method no longer exists. The equivalent fail-fast guard now lives in `StorageUri.parse()` and raises `ValueError` (not `StorageError`) at URI construction time. The behavior (no silent `ls("/")` fallback) is preserved; the exception type and layer changed.

Verified: `grep -rn 'startswith("s3://')' src/firecube/` returns no matches.

Evidence:
- Commits: `bb8636a`, `923fc4f` (Confidence: LOW — pre-restructure umbrella commits)
- Verified by: `src/firecube/core/storage/uri.py`, `src/firecube/core/uris.py`
- Source: TODO.md §12.1

---

Date: 2026-05-26

Decision: Schema initialization now happens under the group claim in `IndexedRegionStrategy.write_groups()`

Context: `IndexedRegionStrategy.write_groups()` initialized Zarr arrays with `writer.ensure_group(...)` before entering `claim_for_group(group_name)`. That left a schema-before-claim race window for concurrent direct writers.

Consequences:
- `writer.ensure_group(...)` now runs inside the `with claim_ctx:` block in `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py`.
- `tests/unit/test_indexed_region_strategy_ordering.py` asserts the claim enters before schema initialization.
- This closes the direct-write schema race without changing claim acquisition or writer behavior.

Verified: `uv run pytest tests/unit/test_indexed_region_strategy_ordering.py -q`

Evidence:
- Commit: `afe60c1`
- Verified by: `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py`, `tests/unit/test_indexed_region_strategy_ordering.py`
- Source: TODO.md §24

---

Date: 2026-05-26 Task: §25

AppendMultiresHandler removed; zarr_multi_res config field removed — commit 89b2737 — Files: append_services.py (delete AppendMultiresHandler), append.py (remove wiring), strategies/append.py (remove multires param), templates/config.py (remove field + add explicit validator), templates/generic.py (remove mapping), test_append_multires_handler.py (deleted), test_zarr_multi_res_config_rejected.py (new), docs/reference/config.md and a plugin reference page (migration note). Migration: zarr_multi_res now raises ValueError with migration path to firecube zarr multires CLI. External plugins may need follow-up if they set zarr_multi_res.

---

## Date: 2026-05-27 Task: §21
ZarrWriteStrategy split into AppendWriteStrategy and RegionWriteStrategy — commit e16f6b4 — Files: contracts.py (deleted ZarrWriteStrategy, added AppendWriteStrategy + RegionWriteStrategy with @runtime_checkable), strategies/append.py (docstring), strategies/indexed_region.py (docstring), tests/unit/test_strategy_protocols.py (new). BREAKING CHANGE: external plugins importing ZarrWriteStrategy get ImportError; migrate to AppendWriteStrategy or RegionWriteStrategy. Verification: uv run pytest tests/unit/test_strategy_protocols.py -q. External plugins may need a follow-up update if they import ZarrWriteStrategy.

---

## Date: 2026-05-27 Task: §23
RegionZarrWriter pre-allocates time dimension via ZarrArraySpec.expected_time_count — commit 548f892 — Files: templates/direct_zarr.py (added expected_time_count field + __post_init__ validation to ZarrArraySpec), runtime/zarr/strategies/indexed_region.py (effective shape computation in write_groups), core/zarr/region_writer.py (skip-resize early-out in ensure_timestamp_slot + write_timestamp + DeprecationWarning on fallback), tests/unit/test_region_zarr_writer_preallocation.py (new, 10+ tests covering happy path + edge cases + sparse storage). Phase 1 of no-resize-on-write delivery — opt-in via expected_time_count, fallback preserved with DeprecationWarning. Verification: uv run pytest tests/unit/test_region_zarr_writer_preallocation.py -q. T2.5 (next commit) wires auto-compute.

---

## Date: 2026-05-27 Task: §23-AUTO
IndexedRegionStrategy auto-computes expected_time_count from WriteIntent.ts_index — commit 120cde9 — Files: indexed_region.py (added pre-claim_ctx auto-compute block; dataclasses.replace for immutability), tests/unit/test_region_zarr_writer_preallocation.py (added test_auto_compute_from_write_intents, test_plugin_override_wins, test_empty_intents_skips_auto_compute). Effect: ALL DirectZarrIngestor users now get pre-allocation transparently; plugins keep override option. Phase 1.5 follow-up TODO removed (delivered here). Verification: uv run pytest tests/unit/test_region_zarr_writer_preallocation.py -q.

---

## Date: 2026-05-27 Task: §22 (helpers)
Created runtime/zarr/batch_runner.py with 5 named helpers — commit 6b4827d — Files: src/firecube/ingestor/runtime/zarr/batch_runner.py (new, 5 functions: seed_staged_metadata_for_batch, build_zarr_write_context, build_claim_closure_for_append, build_append_strategy, assemble_batch_metrics), tests/unit/test_batch_runner.py (new, 13 tests). Follows §4a append_services.py precedent. Wiring into GenericZarrIngestor._process_batch completed in T5 (§22 wiring). Verification: uv run pytest tests/unit/test_batch_runner.py -q.

---

## Date: 2026-05-27 Task: §7-COMPAT
Stale-only sweep for legacy zarr_group claims (Phase 2 migration) — commit 804533b — Files: core/controlplane/claims.py (added _sweep_legacy_zarr_group_claims + _legacy_sweep_done_per_product instance attr, invoked from acquire()), tests/unit/test_legacy_claim_sweep.py (new, 6 tests). Sweep is product-scoped, category-filtered (":zarr_group:" substring), and stale-only. Active legacy claims PRESERVED with WARNING; expire via heartbeat (default 120s). Single-release compat code; remove in future release once operators have migrated. Verification: uv run pytest tests/unit/test_legacy_claim_sweep.py -q.

---

## Date: 2026-05-27 Task: §7-DIRECT
DirectZarrIngestor per-slot claims via claim_for_slot callback — commit c18b894 — Files: contracts.py (additive claim_for_slot param on RegionWriteStrategy), indexed_region.py (split intent dispatch by ts_index; schema under claim_for_group; intents under claim_for_slot with fallback chain), direct_zarr.py (schema closure name=f"{group}:schema"; new slot closure name=f"{group}:slot={ts_index}"; both use category="zarr_region"), tests/unit/test_direct_zarr_per_slot_claims.py (new, 10 tests incl. high-slot-count). Phase 1 invariants preserved: §23-AUTO auto-compute, §24 ordering, empty-intents skip. Verification: uv run pytest tests/unit/test_direct_zarr_per_slot_claims.py -q.

---

## Date: 2026-05-27 Task: §22 (wiring complete)
GenericZarrIngestor._process_batch thinned from 143 lines to ~66 lines using batch_runner helpers — commit 785c5c6 — Files: templates/generic.py (replaced 5 inline blocks with batch_runner.* calls), tests/unit/test_generic_zarr_process_batch.py (new, 5 behavior-preservation tests). Phase 1 behavior preserved: exception path, cleanup-on-finally, telemetry spans, conditional staged seeding, resume_existing AND force_reingest AND-logic, metrics dict shape. §22 fully complete. Verification: uv run pytest --strict-deps -m "not slow and not s3" -q.

---

## Date: 2026-06-01 — Observability Boundary Consolidation — schema and tracing helpers in `core.observability`

All metric schema, tracing helpers, and domain-collector key constants now live in `src/firecube/core/observability/`. The runtime telemetry module (`src/firecube/ingestor/runtime/telemetry.py`) no longer owns `RUN_SUMMARY_SCHEMA`, `TelemetryService`, or any `METRIC_*` constants; it imports what it needs from `core.observability.metrics`. The CLI and engine no longer import OpenTelemetry directly; they call the six facade helpers in `core.observability.tracing`.

### Reviewer findings addressed

- **F1 (CLI OTel leak confirmed):** `src/firecube/cli/main.py` imported `opentelemetry.trace` directly. Replaced with `span` and `set_current_span_attribute` from `firecube.core.observability` (T7).
- **F2 (schema in runtime confirmed):** `RUN_SUMMARY_SCHEMA`, `TelemetryService`, and all `METRIC_*` constants lived in `src/firecube/ingestor/runtime/telemetry.py`. Moved to `src/firecube/core/observability/metrics.py` (T4/T6).
- **F3 (compute_run_summary zero-use confirmed):** `compute_run_summary` was re-exported from `firecube.ingestor.api` but had no external callers. Hard-removed from the public surface (T11).
- **F4 (collector key coupling confirmed):** `FilesystemMetrics.as_summary()` and `WalMetrics.as_summary()` used inline string literals for summary keys. Replaced with named constants (`FS_SUMMARY_KEY_*`, `WAL_SUMMARY_KEY_*`) imported from `core.observability.metrics` (T9/T10).
- **F5/F6 (logging/Prometheus already clean):** No violations found at audit time. Boundary tests added to lock the clean state going forward (T13).

### Decisions

- **F4 import constants:** Domain collectors import key constants from `core.observability.metrics`; they do not define string-literal summary keys inline.
- **Hard-remove `compute_run_summary`:** Removed from `firecube.ingestor.api.__all__`. The implementation stays in `runtime/telemetry.py` for internal use; the public re-export is gone.
- **Handwritten architecture tests:** `tests/unit/test_observability_boundaries.py` enforces the OTel, Prometheus, and logging-handler boundaries via AST scanning, mirroring `test_no_raw_fsspec_usage.py`. No CI plugin required.

### Forward links

- Module map: see "Where things live" in `AGENTS.md` (observability bullets).
- Enforcement rules: see "Observability Rules" in `plans/DESIGN.md`.

### Evidence

- Commits: `<sha>` (T1 boundary scaffold), `<sha>` (T4 metrics.py), `<sha>` (T5 tracing facade), `<sha>` (T6 telemetry cleanup), `<sha>` (T7 CLI swap), `<sha>` (T8 engine swap), `<sha>` (T9 FS constants), `<sha>` (T10 WAL constants), `<sha>` (T11 api removal), `<sha>` (T13 boundary lockdown)
- Verified by: `tests/unit/test_observability_boundaries.py` (5 passed), `tests/unit/test_telemetry_schema_snapshot.py`, `tests/unit/test_tracing_facade.py`
- Source: observability-boundary-refactor plan

---

## 2026-06-10 — CF-1.8 compliance: configurable time-dim + Tier-1 advisor

**Decision**: Make firecube's hardcoded `"timestamp"` dimension plugin-configurable via `BaseIngestor.time_dim_name: ClassVar[str]` (default `"timestamp"`). Ship a Tier-1 CF-1.8 advisor under `firecube advise compliance --profile cf-18`. Internal `firecube_timestamp_state` array name stays stable. Migration is deferred to a follow-up plan.

**Rationale**:
- Plugins are forced to call `ds.rename({"time": "timestamp"})` to satisfy firecube's hardcoded dim. This is the workaround we want to eliminate.
- Adding `time_dim_name` as a `ClassVar` (not a config-tier field) mirrors `PRODUCT_NAME` and avoids exposing it via `--option` (TypedOptionsParam enumerates dataclass fields automatically — that's the wrong semantics for a per-product property).
- Tier-1 validator (~150 LOC) is enough for the 80% case; Tier 2 (CF Standard Name Table) and Tier 3 (UDUNITS) are deferred. No external library wrap (`nc-check` too immature, `compliance-checker` requires libnetcdf+NCZarr).
- Migration is a separate design problem: it needs a generic framework (Strategy + Registry + safety runner), not a one-off `migrate-dim` command. Deferred to a follow-up plan with a full brief in TODO.md.

**Implementation summary**:
- Class attribute `time_dim_name: ClassVar[str] = "timestamp"` on `BaseIngestor` + `_resolve_time_dim_name()` helper.
- All 8 hardcoded `"timestamp"` write-domain sites now consume the resolved name as a parameter (host-free at the strategy/helper layer).
- New `firecube.core.cf` package with CF001..CF015 check IDs and Tier-1 validator (191 LOC).
- New `firecube advise compliance --profile cf-18` command with `--format text|json` and `--strict` (exit 0/1/2).
- Group-aware existing-cube mismatch check fails with exact text pointing at `plans/TODO.md` migration framework follow-up.
- Test fixture plugin `cf_time_dim_test_plugin` declaring `time_dim_name="time"` proves the end-to-end path.

**See also**: `plans/DESIGN.md` (Decided Questions, Risks To Avoid).

---

## 2026-06-11 — Span deletion resolves time_dim_name (closes maintenance dead-end)

**Decision**: Maintenance span deletion no longer assumes the default `"timestamp"` dimension. Span records now carry `time_dim_name` (recorded by the engine at write time via `SpanCoverage`), and `DeletionEngine.delete_spans` resolves the dim per span in a pre-flight pass: span-recorded value > discovery from the 1-D `firecube_timestamp_state` array's `dimension_names` > explicit `--time-dim` (new `chunks delete-span` flag) > engine default `"timestamp"`. An explicit name that contradicts a recorded or discovered name aborts with zero chunks deleted.

**Rationale**:
- After the 2026-06-10 `time_dim_name` ClassVar shipped, `firecube chunks delete-span` hard-failed on any conforming cube (e.g. dim `"time"`): the CLI constructed `ChunkManager` with the default dim and offered no override — an operational dead-end that invited re-adding the forbidden index-0 fallback.
- Recorded-then-discovered beats flag-first: the flag is an operator assertion about data layout; trusting it over the cube's own metadata is how chunks get deleted along the wrong axis. The state array is a safe discovery anchor because its name is contractually stable and it is written 1-D along the time dim.
- Resolution runs before any deletion (pre-flight) so a misconfigured flag cannot cause partial deletion; `_resolve_time_dim_index` still raises loudly (now with `--time-dim` remediation) and never falls back to index 0.

**Deliverables**:
- `SpanCoverage.time_dim_name` + WAL span payload field (additive, schema v2 unchanged); threaded from `AppendCoverageBuilder` and `CoverageTracker`/`IndexedRegionStrategy`; parsed back in both dict-to-`SpanCoverage` coercers.
- `DeletionEngine._resolve_span_time_dims` pre-flight resolver with per-state-array discovery cache and conflict guard.
- `chunks delete-span --time-dim` flag; `ValueError` surfaced as clean `ClickException`.
- Tests: `tests/unit/test_deletion_time_dim_resolution.py` (resolution matrix, conflict aborts with zero deletion, WAL round-trip, default-dim regression); e2e delete-span on a `time_dim_name="time"` cube in `tests/integration/test_e2e_time_dim_plugin.py`.
- Operator docs: Custom Time Dimensions section in `docs/operations/chunk-manager/delete.md`.

**See also**: `plans/DESIGN.md` (Risks To Avoid, time-dim entry), DONE.md 2026-06-10 (the feature this closes the gap for).

---

## 2026-06-11 — Legacy `output_path=` result constructor kwarg removed

**Decision**: `PipelineResult` and `IngestResult` no longer accept the legacy `output_path=` constructor kwarg; the only construction path is the documented typed contract `outputs=OutputPaths(primary=...)`. The read-only `result.output_path` property is kept as a compatibility view of `outputs.primary` for readers (engine merge/reporting code still reads it); read-path migration is deferred to a later pass.

**Rationale**:
- The public contract (AGENTS.md, DESIGN.md, `docs/concepts/plugins/contract.md`) said "never `output_path=`" while the constructor silently accepted and coerced it — a contract/code contradiction, and the engine itself kept the deprecated path alive in its failure handlers.
- Removing construction first and keeping the read property is the safe order: a plugin constructing the legacy form now fails loudly at `TypeError`, while reads are harmless and fully derivable from `outputs.primary`.

**Migration performed**:
- All 16 internal construction sites rewritten to typed `outputs=` (engine failure handlers, `base.py` seed/no-batch results, `cli/archive.py` synthetic restore result, all four templates). Values preserved exactly, including empty-path failure results.
- `_coerce_output_paths` lost its `output_path` parameter and override branches; the dict-shape coercion and the zarr-mirror rule (`zarr = primary` when `output_format == "zarr"`) are unchanged, so `outputs=OutputPaths(primary=X)` reproduces the old behavior byte-for-byte.
- Control-plane record fields (`record_run_started(output_path=...)`, WAL/manifest `output_path` keys, `SpanRecorder.register_run*`) are intentionally untouched — persisted operational metadata, not the plugin result contract.
- Contract locked by `tests/unit/test_result_constructor_contract.py` (legacy kwarg raises `TypeError` on both classes; property and zarr-mirror behavior pinned). No test or fixture-plugin call sites used the legacy form.

**Still open**: migrate remaining internal reads of `result.output_path` to `result.outputs.primary`, then decide whether the compatibility property itself can go.

---

## 2026-06-11 — Tensogram dependency upgraded 0.17.0 → 0.21.0

**Decision**: Bump `tensogram`, `tensogram-xarray`, and `tensogram-zarr` pins from `>=0.17.0,<0.18` to `>=0.21.0,<0.22` (Phase 0 of the TODO §8 promotion plan; Phase 1 — first-class output format — is a separate feature branch).

**What 0.18–0.21 changed for firecube** (verified empirically, not just from release notes):
- 0.18 made the CBOR metadata frame free-form and removed `GlobalMetadata.version`. Firecube's metadata builders wrote a top-level `"version": 3` (the removed field) — dropped from `make_data_meta`, `make_controlplane_meta`, and `dataset_to_global_meta`; tests updated to assert the key is absent. Custom top-level keys (`firecube`, `base`) are now sanctioned free-form.
- 0.19's `sp_*` wire-key rename does not affect firecube (zstd/none codecs only; no `simple_packing` usage).
- 0.21's `validate_file(level="default")` attempts per-frame hash verification, which legacy (pre-0.21) frames cannot satisfy (`HASH_PRESENT` clear). `firecube archive validate` now classifies "no inline hash recorded" as a legacy note and stays VALID/exit 0; real hash mismatches and structural issues still fail. New 0.21-written archives validate with hashes verified.
- The Python API surface firecube calls (`TensogramFile.create/open/append/decode_message/file_decode_metadata/message_count/read_message`, `validate_file`) is unchanged; `decode_message` gained `verify_hash` (adoption deferred to Phase 1, needs HASH_PRESENT-aware gating for legacy archives).

**Backward compatibility locked**: `tests/fixtures/data/archive_v1_tensogram_0_17_0.tgm` — a real archive generated with 0.17.0 installed (provenance in `tests/fixtures/data/README.md`) — plus `tests/integration/test_tensogram_archive_compat.py` (validate VALID with legacy note, quick-validate, info, full restore + readback). Do not regenerate the fixture with newer tensogram.

**Smoke-tested e2e on 0.21**: ingest → archive create → info → validate → restore → readback: data values byte-identical, coords and CF dataset attrs preserved, control plane restored.

**Known pre-existing issues surfaced by the smoke test** (confirmed identical on 0.17 via a downgraded probe env — NOT upgrade regressions; logged in TODO §8 for Phase 1): `--output-format tensogram` ingest fails with `Is a directory` (control-plane root created inside the `.tgm` target); archive restore loses time-coordinate CF encoding (`units`/`calendar` never serialized) and writes base-entry dicts as variable attrs.

---

## 2026-06-11 — `zarr_chunk` alias deleted; typed-flag keys hard-rejected in `--option`

**Decision** (closes AUDIT.md S1 + S3; TODO §12.2/§12.4 entries updated):
- `ZarrTemplateConfig.zarr_chunk` is deleted with no shim, per STYLE.md's Option Aliases rule. Its only behavior was `__post_init__` silently nulling an explicit `zarr_chunk_shape` — a hidden-precedence veto between two names for one concern. Zero users existed (no matches in src, tests, docs, or fixtures). `zarr_chunk_shape` is the single chunking option; `zarr_chunk` now fails strict unknown-key rejection.
- `--option` hard-rejects keys owned by dedicated `firecube ingest` typed flags: `write_mode`, `slot_start`, `slot_end`, `slot_size`, `slot_group` (`_TYPED_FLAG_OWNED_KEYS` in `src/firecube/cli/_typed_options.py`, enforced at parse time with remediation naming the owning flag). These five were the entire hole: they are real `EngineConfig` fields, so they passed unknown-key validation, and `cli/main.py` merges free-form options *after* typed-flag resolution — `--option write_mode=direct` silently overrode an explicit `--write-mode staged`. Hard-reject chosen over warn: same doctrine as the explicit-flags migration (silent override is silent misconfiguration). All other typed flags (`--target`, `--storage-type`, `--product-name`, ...) are not config fields and were already rejected as unknown keys. Engine options without a dedicated flag (`force_reingest`, `no_progress`, `pipeline_parallel`, ...) remain the sanctioned `--option` surface, locked by a regression test.

**Method**: TDD — 8 contract tests written first and confirmed red (`tests/unit/test_zarr_chunk_alias_removed.py`, parametrized typed-flag rejection + engine-option acceptance in `tests/unit/test_typed_options_param.py`), then the deletion + parse-time guard turned them green.

---

## 2026-06-18 — DirectZarr write-parity + CF-time telemetry fix

### What

Closed 5 DirectZarr API parity gaps (A–E) identified during a production plugin's migration:
- Gap A (shards): added `ZarrArraySpec.shards` field and wired through `ensure_group`.
- Gap B (attrs): added `ZarrArraySpec.attrs` field; reserved-attrs guard via `_reserved_attrs.py`.
- Gap C (float64+units time write): dissolved into Gap B — plugins declare CF attrs on the time coord spec.
- Gap D (static arrays): added `ZarrArraySpec.time_indexed=False`, `WriteIntent.kind="static"`, `RegionZarrWriter.write_static`.
- Gap E (dimension_names): added `ZarrArraySpec.dimension_names` field; `ensure_group` passes to Zarr v3 `create_array`.

Fixed 1970-epoch telemetry bug:
- Root cause: `pd.Timestamp(float64)` interpreted values as nanoseconds-since-epoch.
- Fix: new `firecube.core.zarr.time_decode.decode_time_array(values, attrs)` helper decodes via `(dtype, attrs)` dispatch.
- Applied to `AppendCoverageBuilder.record_batch` and `_coerce_append_value`.
- Removed bare `except Exception: pass` from `AppendCoverageBuilder`.

Architecture guard added: AST-walk test banning `pd.Timestamp(<numeric>)` without `unit=` in write-domain modules.

### Files touched (summary)

- `src/firecube/ingestor/templates/direct_zarr.py` — ZarrArraySpec, WriteIntent.kind, _setup_global_zarr_schema.
- `src/firecube/core/zarr/region_writer.py` — ensure_group, verify_array_spec, write_static.
- `src/firecube/core/zarr/time_decode.py` — new module.
- `src/firecube/core/zarr/_reserved_attrs.py` — new module.
- `src/firecube/ingestor/runtime/zarr/append_services.py` — AppendCoverageBuilder CF-time decode.
- `src/firecube/ingestor/runtime/zarr/append.py` — _coerce_append_value CF-time decode.
- `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — kind=static dispatch, 1d coverage constraint, time_indexed guard.

### Verification

- `uv run pytest tests/integration/test_cf_time_dim_telemetry.py` — locks 1970 bug fix.
- `uv run pytest tests/integration/test_direct_zarr_xr_open_roundtrip.py` — dimension_names round-trip.
- `uv run pytest tests/integration/test_direct_zarr_parity_slot_range.py` — slot-range parity with static arrays.
- `uv run pytest tests/architecture/test_no_raw_pd_timestamp_numeric.py` — pd.Timestamp guard.
- `uv run pytest --strict-deps` passes on `feat/directzarr-parity-and-cf-time-decode`.

---

### DirectZarr per-slot payload retention — bite-the-pill + §F3 promotion (2026-07-12)

**Decision:** A large per-slot payload ingest retains ~14.8 GiB per worker under `list[WriteIntent]` semantics; the operator-observed 12-worker × 12-slot 2h ingest reached ~178 GiB total per pod. Sub-batching was investigated as a plugin-side memory reduction path and explicitly rejected. IDEAS.md §F3 (lazy-payload thunk, originally static-only) is promoted to TODO.md as accepted core work, generalized to time-indexed intents.

**Accepted operational path today (no code change):** `pipeline_workers=1` × N disjoint-range pods via the `firecube zarr slots` planner. Per-pod peak stays at ~14.8 GiB; total cluster memory unchanged from an intra-pod approach but sized per-pod it fits standard nodes. Feature knobs (`include_pixel_time=false` → ~5.5 GiB, `pixel_time_dtype=float32` → ~10 GiB) remain available for further per-pod reduction where downstream tolerates.

**Rejected alternatives:**
- Iterator-lazy `build_write_intents` (`-> Iterable[WriteIntent]`): breaks six load-bearing invariants (slot-range validation, `expected_time_count` autosize, `allow_grow`, group presence, per-slot claim grouping, `len(intents)` metrics); saves list-container references only, not payload bytes.
- Plugin-side sub-batching with shared `ts_index` (yield-per-nc_part / tile / channel): per-slot claim raises `ClaimConflictError` on any concurrent second acquirer (no retry loop at the dispatch site, deterministically reproduced 2026-07-12); `CoverageTracker` records at `(group, ts_index)` only, so mid-slot crash-resume produces silently incomplete data (integrity hazard).
- Internal-iteration sub-batching (single batch, chunked emission): safe but empirically ineffective per 2026-07-12 POC — three configurations (baseline / naive / cached) plateau together within noise at the baseline retention level.

**§F3 promotion scope (see TODO.md §F3):** `WriteIntent.data: np.ndarray | Callable[[], np.ndarray] | Any` — additive union, metadata-eager / payload-lazy at intent level, generalized to time-indexed `kind="region"` and `kind="static"` intents. Design constraints named: scratch lifetime, provider caches, resume-check materialization, `kind="1d"` / `kind="timestamp"` scope.

**Doctrine tightening (see DESIGN.md "Risks To Avoid"):** iterator-lazy `build_write_intents` and shared-`ts_index` sub-batching now explicit anti-patterns with rationale.

**Test gaps opened (see TEST_GAPS.md P2):** DirectZarr retained-payload regression harness and `CoverageTracker` sub-slot granularity as prerequisites for accepting any lazy-payload or sub-batching change.

**Evidence trail:** the plugin's own repository — memory diagnosis (2026-07-11, full memray attribution 99.9% accounted, pixel_time 66% dominant); sub-batching POC report (2026-07-12, three-config plateau at baseline, `ClaimConflictError` deterministic reproduction); plugin-author formal review response.

---

## 2026-07-12 — `normalize_string_vars` (§19 remainder closed)

**Date:** 2026-07-12

**Decision:** `normalize_string_vars` — fourth §19 NetCDF-to-Zarr utility

**Context:** Post-concat vlen-string widening (h5netcdf/h5py version-dependent behaviour)
caused downstream Zarr write failures. A generic string-normalization utility was
promised as the fourth member of the §19 set when the first three utilities shipped
(`clean_netcdf_encoding`, `rename_time_dim`, `prepare_netcdf_for_zarr`; DONE.md §19,
commit ef771cc). This entry closes the §19 remainder.

**Consequences:**
- `normalize_string_vars(ds, *, iso_targets=None, logger=None) -> xr.Dataset`
  added to `src/firecube/core/formats/netcdf.py`.
- Strict UTC ISO-8601 parser with precision-preserving datetime64 conversion
  (`src/firecube/core/formats/_iso.py`; `datetime64[s]` vs `datetime64[us]`
  auto-detected from fractional-second presence).
- Context-sensitive predicate: raises ValueError only when a variable is
  explicitly named in `iso_targets` AND is not processable; silently skips
  non-string object vars otherwise (DEBUG log when logger provided).
- Attribute handling matches xarray's decode convention: `units`/`calendar`
  move from `.attrs` to `.encoding` on ISO-converted vars by default. Callers
  can opt in to verbatim preservation via `preserve_cf_time_attrs=True`; then
  firecube's CF advisor may flag the variable. No silent deletion of metadata.
  Source `.encoding` is fully discarded on ISO-converted vars in both modes
  (fresh encoding populated only from moved attrs) — anti-leak invariant
  covered by test C.
- Re-exported via `firecube.core.formats` and `firecube.core.api`.
- 36 unit tests in `tests/unit/test_netcdf_utils.py` (11 ISO helper tests
  + 18 normalize_string_vars tests + 7 pre-existing).

**Verified:**
- `from firecube.core.api import normalize_string_vars` works
- `uv run pytest tests/unit/test_netcdf_utils.py -q` → 36 passed
- `uv run ruff check src/firecube/core/formats/netcdf.py src/firecube/core/formats/_iso.py` → clean
- `uv run pyright src/firecube/core/formats/netcdf.py` → 0 errors

**Evidence:**
- Commits: f4f7594 feat(core/formats): normalize string vars for NetCDF inputs; 0897bc8 feat(core/formats): implement _iso UTC parser with precision-preserving datetime64 conversion; c6084e7 test(core/formats): RED tests for normalize_string_vars and _iso helpers; ae3b98f refactor(test): fix import order and remove noqa suppressor in netcdf utils tests; 3214274 fix(core/formats): reject timezone-naive strings in utc ISO parser; 1553e25 fix(core/formats): move CF time attrs to encoding with preserve opt-in
- Source: plans/TODO.md §19 remainder (removed)
- Follow-up refinement (2026-07-12+): CF time-attr handling pivoted from
  silent DELETE to xarray `pop_to()`-style MOVE-to-encoding; added
  `preserve_cf_time_attrs: bool = False` opt-in for verbatim retention.
  Rationale: silent deletion was neither ecosystem-conventional (xarray
  moves, not deletes) nor advisory-friendly (violated the CF-is-advisory
  principle). Ref: internal plan `normalize-string-vars-cf-attrs-refinement`.

**Confidence:** HIGH

---

## 2026-09-12 — Append recovery, ordering, and CLI contracts

**Decision.** Force-reingest overwrites matching timestamps in place and appends new tail values; resume skips present slots and refills deleted or failed slots. Insertions, duplicate or unsorted timestamps, and missing state arrays receive typed refusals. A distinct legacy read source without a state array is treated as present with a warning until a write upgrades it.

**Decision.** Append batches commit in planner order and stop at the first failure. Repair marks overwritten regions as failed, truncates partial tails, removes incomplete fresh groups, and rewinds resume state. Successfully committed groups remain valid. Coverage records combine region and tail ranges per batch and group; failed records include affected ranges and repair results.

**Decision.** Alignment uses stored chunk sizes, with warnings and summaries scoped to a run. Direct-mode upload counters are null; staged runs report measured uploads and retain every batch. CLI comparison and validation failures use exit code 1, while layout-only comparison differences warn and return 0. Failed runs require explicit resume or force-reingest. Ingestion's `dry_run` option remains reserved; maintenance previews use their commands' `--dry-run` flags.

**Coverage.** Retained integration tests include `tests/integration/test_force_reingest_region_overwrite.py`, `tests/integration/test_append_failed_batch_repair.py`, `tests/integration/test_region_tail_atomicity.py`, `tests/integration/test_staged_retains_all_batches.py`, `tests/integration/test_manifest_explicit_null.py`, and `tests/integration/test_delete_span_region_nan_fill.py`. The staged-retention and direct-storage cases include moto S3 coverage. Tests now use behavior names instead of review-task codes. The changelog prose assertions were removed: runtime CLI, storage, and recovery tests own those contracts, while release-note wording is reviewed as documentation.
