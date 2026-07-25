# Progress: GitHub issue work-through (2026-07-26)

Working session against the 9 open issues (#4–#12) filed 2026-07-20. Approach:
TDD (tests before implementation), with a benchmark pass on anything
performance-sensitive. This file is a handoff snapshot — see git history /
`gh issue list` for the live state.

## ⚠️ Build status right now

**The package does not currently compile.** `json_test.go` was written
(TDD red phase for issue #5) but `json.go` — the implementation it tests —
does not exist yet. This is expected mid-cycle state, not a regression.

To get back to green, the next step is implementing `json.go` (see below).
Everything up through issue #11 is merged and green; only issue #5 is
mid-flight.

## Done

### Issue #4 — Join operations ✅ complete, merged
Files: `join.go`, `join_test.go` (41 tests), `join_bench_test.go`,
`example_join_test.go`.

- `Join(other, on, how)` and `JoinOn(other, leftKeys, rightKeys, how)`, all
  four `JoinType`s (`InnerJoin`, `LeftJoin`, `RightJoin`, `OuterJoin`).
- Hash-join core (`hashJoinPairs`, generic over `comparable`) with typed fast
  paths for single int64/float64/string/bool/time keys, and a length-prefixed
  tuple encoding (reusing the GroupBy collision-safe scheme) for composite
  keys.
- int64/float64 key mixing is allowed (compared and emitted in float64
  space); any other type mismatch errors via `Error()`.
- Zero-value-is-null convention: a key equal to its type's zero value never
  matches (consistent with the CSV/JSONL missing-cell convention elsewhere in
  the library).
- Column-collision suffixing (`_left`/`_right`), keys emitted once, suffix
  collisions themselves error out.
- Deterministic output order: left-frame order first (multi-matches expand in
  right-frame order), then unmatched right rows in right-frame order.
- Performance: switched the naive `map[K][]int` index to a chained
  head/next-index scheme (like an adjacency list) to cut allocations —
  100k×10k single-key inner join benchmarks at **~3ms, ~60 allocs**.
- README (new "Joining DataFrames" section + roadmap + pandas-comparison
  table) and CHANGELOG updated.
- **Bonus fix while testing panic paths**: `SafeOperation` never actually
  recovered panics — `recover()` was called from inside a helper invoked by
  the deferred closure, where Go treats it as a no-op, so a panic inside the
  wrapped function escaped instead of becoming an error. Fixed by installing
  `recoverFromPanic` directly as the deferred function
  (`defer recoverFromPanic(op, &err)`). This closes part of issue #8 as a
  side effect — see below.

### Issue #11 — Column derivation & transforms ✅ complete, merged
Files: `transform.go`, `transform_test.go`, `example_transform_test.go`.

- `Apply(newColumn, fn)` — derives a column from the full row
  (`map[string]any`).
- `Map(srcColumn, newColumn, fn)` — element-wise transform of one column;
  `newColumn == srcColumn` replaces in place.
- `FillNA(column, value)` / `DropNA(columns...)` — zero-value-is-missing
  convention, documented explicitly (a real `0` is indistinguishable from
  missing in an int column — this is a known, accepted limitation, not a
  bug).
- Derived-column type inference: all-int → `Int64Type`, int/float mix →
  `Float64Type`, uniform string/bool/time kept, any other mix falls back to
  `StringType` with canonical formatting (matches `WriteCSV`/`String`
  rendering).
- Receiver never mutated; a panic inside the user-supplied `fn` is caught
  and returned as an error on the result frame instead of crashing the
  caller.
- README "Data Transformation" section (previously a
  `// implementation coming soon` placeholder) rewritten with real examples;
  roadmap + CHANGELOG updated.

### Partial — Issue #8 (err.go coverage) — panic-recovery fix only
The actual bug issue #8 was hunting for (recoverFromPanic barely tested) was
real: writing the test caught a genuine live bug (see above). The fix and its
direct tests are in `err.go` / `err_test.go` already. **Issue #8's other
acceptance criteria are not yet done** — see Left to do.

## Left to do

### Issue #5 — JSON array (`[{...}]`) read/write — IN PROGRESS
Status: tests written (`json_test.go`, ~20 test cases covering read/write,
type inference parity with JSONL, null handling, type-conflict promotion,
nested-value stringification, `JSONOptions{SkipRows,MaxRows}`, error cases,
round-trips). Refactor started in `jsonl.go`: extracted shared object-decode
logic into provider-neutral helpers (`decodeJSONObject`,
`buildDataFrameFromObjects`, `inferJSONColumnType`, `buildJSONSeries`,
`formatValueForJSON` — renamed from the JSONL-specific names so both formats
can share them).

**Not yet done**: `json.go` itself — `ReadJSON`, `ReadJSONWithOptions`,
`ReadJSONFromString`, `ReadJSONFromStringWithOptions`, `WriteJSON`, and the
`JSONOptions` type. This is the very next task: wire a
`json.Decoder`-based array reader on top of the now-shared
`decodeJSONObject`/`buildDataFrameFromObjects` path, plus a `WriteJSON` that
reuses `formatValueForJSON` (already shared) to emit a JSON array instead of
newline-delimited objects. Should be a small, mechanical diff given the
refactor is already in place. Then: run `json_test.go` to green, add
`Example_json`-style godoc example, update README/CHANGELOG.

### Issue #9 — Streaming/chunked CSV & JSONL reads — not started
Not started. Note: whichever chunk-iteration API gets picked here
(pull-based `CSVReader.Next` vs. push-based `StreamCSV(fn)` — issue leaves it
open) should probably reuse the JSON/JSONL object-decode refactor from #5 if
a `StreamJSON`-shaped variant is ever wanted, though the issue only asks for
CSV/JSONL.

### Issue #10 — Compound Query expressions (AND/OR, parentheses) — not started
Not started. Design is spec'd in the issue: tokenize → parse with AND
binding tighter than OR → either build a predicate tree or evaluate to
row-index sets, reusing the existing single-condition `match*`/`Query`
parsing per leaf.

### Issue #12 — Text visualization helpers (Table/BarChart/Histogram/Info) — not started
Not started. Note: `DataFrame.Info()` **already exists** in `df.go` (basic
shape + column types dump) — the issue's proposed `Info()` signature
overlaps with it. Whoever picks this up needs to decide: extend the existing
`Info()` in place, or treat the issue's version as a superset and merge them,
rather than introducing a second method. Worth flagging before writing code.

### Issue #6 / #7 / #8 — Test-coverage gaps — mostly not started
- **#6** (`lazy.go`: `LazyFrame.Where`/`Error` at 0%) — not started.
- **#7** (`ops.go`: `match*`, `Unique`, `typedComparator` gaps) — not started.
- **#8** (`err.go`: `recoverFromPanic`/`Error` paths) — **partially done**.
  The panic-recovery bug is fixed and the fix itself is tested
  (`TestSafeOperationRecoversPanics`, `TestSafeOperationRecoversRuntimePanic`,
  `TestSentinelMatchingSurvivesRecoveredPath`, updated
  `TestRecoverFromPanic`). Still open: the issue also asks for coverage on
  `DataFrame.Error()` across the no-error / operation-error / chain-
  propagation cases — not yet added as dedicated tests (some of this is
  incidentally exercised by other tests, but not verified against the
  issue's explicit checklist).

## How to resume

1. Fix the build: implement `json.go` per the plan above, get `go test ./...`
   green again.
2. Then work issues in roughly this order: #8 (finish it off, it's mostly
   done), #6, #7 (test-only, fast), #10, #12, #9 (biggest remaining lift).
3. Task tracker (`TaskList`) mirrors this file's status if picking the
   session back up in-context; this file is the durable record if not.
