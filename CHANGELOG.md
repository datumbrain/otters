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
