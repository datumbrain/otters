# Changelog

All notable changes to this project will be documented in this file.

---

## [1.0.6] — 2026-07-15

### Added

- **Lazy views (`LazyFrame`)** — `df.Lazy().Filter(...).Select(...).Sort(...).Collect()` defers materialization: the chain only manipulates a row-index/column view over the source DataFrame, and data is copied once, at `Collect()`. Supports `Filter`, `Where`, `Select`, `Sort`, `SortBy`, `Head`, `Tail`. On a 10k-row Filter→Select→Sort chain this halves memory versus the eager equivalent (664 KB / 45 allocs → **319 KB / 24 allocs**).

- **`errors.Is` support for sentinel errors** — Errors produced by the library now match the exported sentinels: a missing column matches `ErrColumnNotFound`, an invalid row index matches `ErrIndexOutOfRange`, and operations on empty DataFrames match `ErrEmptyDataFrame`. Previously the sentinels existed but never matched any real error.

### Fixed

- **Filter truncated fractional comparison values on int columns** — `Filter("n", "==", 2.5)` matched rows where `n == 2`, and `">=" 2.5` wrongly included 2. Fractional float values are now compared in float64 space instead of being truncated to int64.

- **CSV round-trip rewrote 0/1 columns as true/false** — Type inference used `strconv.ParseBool`, which accepts `"0"`, `"1"`, `"t"`, and `"f"`, so numeric flag columns were loaded as booleans and written back as `true`/`false`. Only explicit `true`/`false` spellings are treated as boolean now.

- **All-empty columns inferred as bool** — A column containing only empty strings carried no type information but was inferred as `BoolType` (converting everything to `false`). It now defaults to `StringType`.

- **`GroupBy.Count()` dropped the count entirely on non-numeric frames** — Count was only emitted per numeric non-group column, so grouping an all-string DataFrame returned just the group columns. Count is now a dedicated `count` column holding each group's row size, independent of other columns.

- **GroupBy results ordered by internal key encoding** — Groups were sorted by their length-prefixed internal key, so `"Phone"` sorted before `"Laptop"` (shorter prefix first). Groups are now ordered by their actual column values.

- **DataFrames aliased caller slices** — `NewSeries` (and everything built on it, like `NewDataFrameFromMap`) stored the caller's slice directly, so mutating the source slice afterwards silently mutated the DataFrame. Input data is now copied on construction; internal operations use an ownership-transferring constructor to avoid double copies.

- **Column-name collisions silently dropped result data** — `ValueCounts` on a column named `count`, `Describe` on a frame with a numeric column named `statistic`, and `Correlation` with a column named `column` all collided with their own result columns and lost data. All three now de-collide.

- **Stats result column order was alphabetical** — `Describe`, `Correlation`, and `ValueCounts` built results through map iteration, so the label column could land mid-table. The label column now always leads, followed by data columns in DataFrame order.

- **`Select` with a duplicated column corrupted internal state** — `Select("a", "a")` produced a frame whose column order and column map disagreed. Duplicates are rejected with an error.

- **Sort was unstable** — Rows with equal sort keys could be reordered arbitrarily between runs. Ties now break on the original row index, producing stable-sort output while keeping the faster unstable algorithm.

- **`Query` failed on quoted values containing spaces** — `df.Query("name == 'John Smith'")` was rejected as malformed; the value portion is now everything after the operator.

- **`ValueCounts` tie order was non-deterministic** — Values with equal frequency now sort by value.

### Performance

Benchmarks on 10,000-row DataFrame (Apple M2 Pro):

| Operation | Before | After | Improvement |
|---|---:|---:|---|
| Sort | 392 µs / 20,039 allocs | 104 µs / **19 allocs** | **3.8× faster, 99.9% fewer allocs** |
| Chained Filter→Select→Sort | 664 KB / 45 allocs | 319 KB / 24 allocs (lazy) | **−52% memory, −47% allocs** |

- **Sort: typed comparator path** (`ops.go`) — `SortBy` builds one typed comparator per sort column instead of boxing every value through `Series.Get` inside the comparison loop.

### Internal

- Removed dead helpers (`newCellError`, `isColumnNotFound`, `isIndexOutOfRange`, `isTypeMismatch`, `clearError`, `hasError`, `DataFrame.reset`) and the tests that existed only to cover them.
- Migrated `interface{}` → `any` across all source and test files.
- Resurrected the five `Demo*` functions in `example_test.go` as real `Example_*` functions with regenerated, verified `// Output:` blocks — they had been renamed away from the example convention and their stale output blocks contained wrong values (std dev, quantiles, top region). All six examples now run as part of the test suite.

### Tests added

- Regression tests for every fix above, consolidated into the per-file suites (`ops_test.go`, `type_test.go`, `csv_test.go`, `df_test.go`, `stats_test.go`).
- `lazy_test.go` — eager/lazy equivalence, Head/Tail parity, chained-sort stability, empty-result schema, error propagation, fractional-float parity, time columns, collect independence, and an eager-vs-lazy benchmark.
- Suite: 186 tests passing with `-race`; `go vet` and `gofmt` clean.

---

## [Unreleased] — 2026-02-17

### Fixed

- **Build failure** — `ExampleBasicDataFrame` referenced the non-existent identifier `BasicDataFrame`, preventing the test suite from compiling. Renamed to `Example_basicDataFrame` (valid package-level example with suffix). No functional change.

- **Silent DataFrame corruption (`setError`)** — Operations that encountered an error (e.g. filtering on a non-existent column) were writing the error state directly onto the receiver DataFrame via `setError`, corrupting it for any subsequent use. `setError` now returns a fresh error-bearing DataFrame, leaving the original untouched.

  ```go
  // Before fix — df was silently corrupted after this call
  result := df.Filter("bad_column", "==", 1)

  // After fix — df is unchanged; error is carried only by result
  result := df.Filter("bad_column", "==", 1)
  ```

- **`Head` and `Tail` panic on time columns** — The internal `slice()` function had no `TimeType` case, causing it to fall through to the error path on any DataFrame that contained a time column. Added the missing `case TimeType:` branch, consistent with the pattern already used in `selectRows()`.

- **GroupBy key collisions on pipe characters** — Group keys were built by joining column values with `"|"`, meaning a value containing that character (e.g. `"a|b"`) would be indistinguishable from two separate values (`"a"` and `"b"`) in a multi-column group. Keys are now length-prefixed (`"3:foo"`) and separated by a null byte, making them unambiguous for all string content. The original values are stored directly on the group struct so no parsing of the key is needed when building the result.

### Tests added

- `TestSetErrorDoesNotMutateCaller` — asserts that a failed operation on a valid DataFrame does not modify the original.
- `TestTimeTypeHeadTail` — asserts that `Head` and `Tail` return correct rows on a DataFrame with a `TimeType` column.
- `TestGroupByKeyCollision` — asserts that groups whose values contain `"|"` are counted and aggregated independently.

---

## [Unreleased] — 2026-02-17 (P1 + P2)

### Fixed

- **Non-deterministic column order (`NewDataFrameFromMap`)** — Map iteration order in Go is randomized; `NewDataFrameFromMap` was building the series slice in that random order, producing different `Columns()` results across runs. Keys are now sorted alphabetically before iteration, giving a stable, predictable column order.

- **Non-deterministic GroupBy row order (`aggregate`)** — The internal `aggregate` function iterated directly over the `groups` map, whose key order is randomized by the Go runtime. Group keys are now collected into a slice and sorted before iteration, so `GroupBy` output is always in a consistent order regardless of runtime or input sequence.

### Tests added

- `TestDeterministicFromMap` — runs `NewDataFrameFromMap` 20 times and asserts column order is always alphabetical (P1 regression).
- `TestDeterministicGroupBy` — runs `GroupBy.Sum()` 10 times and asserts row order is identical across all runs (P1 regression).
- `TestDataFrameManipulation` — covers `Tail`, `Set`, `GetSeries`, `AddColumn`, `DropColumn`, `RenameColumn`, `IsEmpty`, and `HasColumn`.
- `TestOpsOperations` — covers `Drop`, `SortBy`, `Unique`, `Query`, `Where`, and `ResetIndex`.
- `TestGroupByMinMax` — covers `GroupBy.Min()` and `GroupBy.Max()`.
- `TestStringOperators` — covers `Filter` with `contains`, `startswith`, and `endswith` operators.
- `TestStatsOperations` — covers `Median`, `Var`, `Quantile`, `Describe`, `ValueCounts`, `Correlation`, and `NumericSummary`.
- `TestCSVFileOperations` — covers `WriteCSV`/`ReadCSV` roundtrip, tab-delimited `WriteCSVWithOptions`/`ReadCSVWithOptions`, `DetectDelimiter`, `ValidateCSV` (valid and invalid files), headerless CSV, and `MaxRows` option.
- `TestSentinelErrors` — asserts all five sentinel errors (`ErrColumnNotFound`, `ErrIndexOutOfRange`, `ErrTypeMismatch`, `ErrEmptyDataFrame`, `ErrInvalidOperation`) are non-nil, and covers `SafeOperation` and `MustOperation` behaviour.

---

## [Unreleased] — 2026-02-17 (P4 — Performance)

### Performance improvements

Benchmarks on 10,000-row DataFrame (Apple M2 Pro):

| Operation | Before | After | Improvement |
|---|---:|---:|---|
| Filter | 275 µs / 20k allocs | 102 µs / **20 allocs** | **2.7× faster, 99.9% fewer allocs** |
| GroupBy | 2,588 µs / 80k allocs | 865 µs / 30k allocs | **3× faster, 62% fewer allocs** |

#### Changes

- **Typed slice accessors** (`type.go`) — Added `GetInt64`, `GetFloat64`, `GetString`, `Int64Slice`, `Float64Slice`, `StringSlice` methods to `Series` for zero-allocation access to underlying data.

- **Filter: typed comparison path** (`ops.go`) — Replaced per-row `series.Get(i)` + `evaluateCondition` (which boxed every value into `interface{}`) with `filterIndicesTyped`, which type-switches once and iterates the typed slice directly. Reduces Filter allocations from 20,000 to 20 on a 10k-row DataFrame.

- **GroupBy: typed aggregation** (`ops.go`) — Rewrote `calculateAggregation` to access `Int64Slice()` / `Float64Slice()` directly instead of calling `Get()` per row. Added `aggregateInt64` and `aggregateFloat64` helpers that compute sum/mean/min/max in a single typed loop.

- **GroupBy: optimized group building** (`ops.go`) — Pre-cache series pointers for grouping columns; replaced `fmt.Sprintf("%v", value)` with `strconv.FormatInt` / `strconv.FormatFloat`; reuse `strings.Builder` across rows.

- **GroupBy: direct result construction** (`ops.go`) — Build result DataFrame with `NewDataFrameFromSeries` instead of going through `NewDataFrameFromMap`, avoiding map iteration and key sorting overhead.

- **Unique: typed iteration** (`ops.go`) — Type-switch once, iterate typed slice directly, use `strconv` instead of `fmt.Sprintf` for key generation. Pre-allocate result slice with estimated capacity.
