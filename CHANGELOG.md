# Changelog

All notable changes to this project will be documented in this file.

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
