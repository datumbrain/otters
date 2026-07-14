package otters

import (
	"math"
	"sort"
	"time"
)

// LazyFrame is a deferred view over a DataFrame. Row-selection operations
// (Filter, Sort, Head, Tail) and column selection (Select) only manipulate
// an index/column view; the data itself is copied once, at Collect. For a
// chain like Filter → Select → Sort this avoids materializing a full
// intermediate DataFrame per step.
//
// The view references the source DataFrame's data, so the source must not
// be mutated between Lazy() and Collect().
type LazyFrame struct {
	src     *DataFrame
	indices []int    // selected rows in view order; nil = all rows
	cols    []string // selected columns in view order; nil = all columns
	err     error
}

// Lazy returns a lazy view over the DataFrame.
func (df *DataFrame) Lazy() *LazyFrame {
	return &LazyFrame{src: df, err: df.err}
}

// Error returns the first error encountered while building the view.
func (lf *LazyFrame) Error() error {
	return lf.err
}

// fail returns a new LazyFrame carrying the error.
func (lf *LazyFrame) fail(err error) *LazyFrame {
	return &LazyFrame{src: lf.src, err: err}
}

// columnSeries resolves a column that must exist in the source and still be
// part of the current column selection.
func (lf *LazyFrame) columnSeries(op, column string) (*Series, error) {
	if err := lf.src.validateColumnExists(column); err != nil {
		return nil, err
	}
	if lf.cols != nil && !contains(lf.cols, column) {
		return nil, newColumnError(op, column, "column not in current selection")
	}
	return lf.src.columns[column], nil
}

// rowCount returns the number of rows in the current view.
func (lf *LazyFrame) rowCount() int {
	if lf.indices == nil {
		return lf.src.length
	}
	return len(lf.indices)
}

// rowAt maps a view position to a source row index.
func (lf *LazyFrame) rowAt(pos int) int {
	if lf.indices == nil {
		return pos
	}
	return lf.indices[pos]
}

// Filter narrows the view to rows matching the condition.
func (lf *LazyFrame) Filter(column, operator string, value any) *LazyFrame {
	if lf.err != nil {
		return lf
	}

	series, err := lf.columnSeries("Lazy.Filter", column)
	if err != nil {
		return lf.fail(err)
	}

	pred, err := typedPredicate(series, operator, value)
	if err != nil {
		return lf.fail(wrapColumnError("Lazy.Filter", column, err))
	}

	n := lf.rowCount()
	matched := make([]int, 0, n/4)
	for pos := 0; pos < n; pos++ {
		row := lf.rowAt(pos)
		if pred(row) {
			matched = append(matched, row)
		}
	}

	return &LazyFrame{src: lf.src, indices: matched, cols: lf.cols}
}

// Where is an alias for Filter (Pandas compatibility).
func (lf *LazyFrame) Where(column, operator string, value any) *LazyFrame {
	return lf.Filter(column, operator, value)
}

// Select narrows the view to the specified columns, in the given order.
func (lf *LazyFrame) Select(columns ...string) *LazyFrame {
	if lf.err != nil {
		return lf
	}

	if len(columns) == 0 {
		return lf.fail(newOpError("Lazy.Select", "at least one column must be specified"))
	}

	seen := make(map[string]bool, len(columns))
	for _, column := range columns {
		if _, err := lf.columnSeries("Lazy.Select", column); err != nil {
			return lf.fail(err)
		}
		if seen[column] {
			return lf.fail(newColumnError("Lazy.Select", column, "column specified more than once"))
		}
		seen[column] = true
	}

	selected := make([]string, len(columns))
	copy(selected, columns)

	return &LazyFrame{src: lf.src, indices: lf.indices, cols: selected}
}

// Sort orders the view by a single column.
func (lf *LazyFrame) Sort(column string, ascending bool) *LazyFrame {
	return lf.SortBy([]string{column}, []bool{ascending})
}

// SortBy orders the view by multiple columns. Rows with equal keys keep
// their current view order (stable).
func (lf *LazyFrame) SortBy(columns []string, ascending []bool) *LazyFrame {
	if lf.err != nil {
		return lf
	}

	if len(columns) == 0 {
		return lf.fail(newOpError("Lazy.SortBy", "at least one column must be specified"))
	}

	if len(columns) != len(ascending) {
		return lf.fail(newOpError("Lazy.SortBy", "columns and ascending arrays must have the same length"))
	}

	comparators := make([]func(a, b int) int, len(columns))
	for k, column := range columns {
		series, err := lf.columnSeries("Lazy.SortBy", column)
		if err != nil {
			return lf.fail(err)
		}
		cmp := typedComparator(series)
		if cmp == nil {
			return lf.fail(newColumnError("Lazy.SortBy", column, "unsupported column type for sorting"))
		}
		comparators[k] = cmp
	}

	n := lf.rowCount()
	cur := make([]int, n)
	for pos := 0; pos < n; pos++ {
		cur[pos] = lf.rowAt(pos)
	}

	// If the view order still matches ascending source order (true unless a
	// previous Sort reordered it), ties can break on the row index itself:
	// that is a strict total order, so the result equals a stable sort while
	// sorting the indices directly.
	monotonic := true
	for i := 1; i < n; i++ {
		if cur[i] <= cur[i-1] {
			monotonic = false
			break
		}
	}

	if monotonic {
		sort.Slice(cur, func(i, j int) bool {
			a, b := cur[i], cur[j]
			for k, compare := range comparators {
				cmp := compare(a, b)
				if cmp != 0 {
					if ascending[k] {
						return cmp < 0
					}
					return cmp > 0
				}
			}
			return a < b
		})
		return &LazyFrame{src: lf.src, indices: cur, cols: lf.cols}
	}

	// Otherwise sort a permutation of view positions; breaking ties on the
	// position keeps the current view order for equal keys (stable).
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	sort.Slice(perm, func(i, j int) bool {
		a, b := cur[perm[i]], cur[perm[j]]
		for k, compare := range comparators {
			cmp := compare(a, b)
			if cmp != 0 {
				if ascending[k] {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return perm[i] < perm[j]
	})

	sorted := make([]int, n)
	for k, p := range perm {
		sorted[k] = cur[p]
	}

	return &LazyFrame{src: lf.src, indices: sorted, cols: lf.cols}
}

// Head narrows the view to its first n rows.
func (lf *LazyFrame) Head(n int) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if n <= 0 {
		return lf.fail(newOpError("Lazy.Head", "n must be positive"))
	}

	total := lf.rowCount()
	if n > total {
		n = total
	}

	head := make([]int, n)
	for pos := 0; pos < n; pos++ {
		head[pos] = lf.rowAt(pos)
	}

	return &LazyFrame{src: lf.src, indices: head, cols: lf.cols}
}

// Tail narrows the view to its last n rows.
func (lf *LazyFrame) Tail(n int) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if n <= 0 {
		return lf.fail(newOpError("Lazy.Tail", "n must be positive"))
	}

	total := lf.rowCount()
	if n > total {
		n = total
	}

	tail := make([]int, n)
	for pos := 0; pos < n; pos++ {
		tail[pos] = lf.rowAt(total - n + pos)
	}

	return &LazyFrame{src: lf.src, indices: tail, cols: lf.cols}
}

// Collect materializes the view into a new, independent DataFrame. This is
// the only point in a lazy chain where column data is copied.
func (lf *LazyFrame) Collect() (*DataFrame, error) {
	if lf.err != nil {
		return nil, lf.err
	}

	order := lf.cols
	if order == nil {
		order = lf.src.order
	}

	newDf := NewDataFrame()
	for _, colName := range order {
		series := lf.src.columns[colName]

		var newSeries *Series
		var err error
		switch {
		case lf.indices == nil:
			newSeries = series.Copy()
		case len(lf.indices) == 0:
			newSeries, err = newSeriesOwned(series.Name, emptySliceForType(series.Type))
		default:
			newData := selectSeriesRows(series, lf.indices)
			if newData == nil {
				return nil, newColumnError("Lazy.Collect", colName, "unsupported column type")
			}
			newSeries, err = newSeriesOwned(series.Name, newData)
		}
		if err != nil {
			return nil, wrapColumnError("Lazy.Collect", colName, err)
		}

		if err := newDf.addSeriesUnsafe(newSeries); err != nil {
			return nil, wrapColumnError("Lazy.Collect", colName, err)
		}
	}

	if lf.indices == nil {
		newDf.length = lf.src.length
	} else {
		newDf.length = len(lf.indices)
	}

	return newDf, nil
}

// typedPredicate builds a row predicate for the condition, bound to the
// series' typed data so evaluation involves no boxing.
func typedPredicate(series *Series, operator string, value any) (func(row int) bool, error) {
	switch series.Type {
	case Int64Type:
		data := series.Data.([]int64)
		// Fractional values cannot be truncated to int64 without changing
		// the predicate; compare in float64 space (same rule as Filter).
		if f, isFloat := value.(float64); isFloat && f != math.Trunc(f) {
			return func(row int) bool { return matchFloat64(float64(data[row]), operator, f) }, nil
		}
		cmp, ok := toInt64(value)
		if !ok {
			return nil, newOpError("Filter", "cannot convert value to int64")
		}
		return func(row int) bool { return matchInt64(data[row], operator, cmp) }, nil

	case Float64Type:
		data := series.Data.([]float64)
		cmp, ok := toFloat64(value)
		if !ok {
			return nil, newOpError("Filter", "cannot convert value to float64")
		}
		return func(row int) bool { return matchFloat64(data[row], operator, cmp) }, nil

	case StringType:
		data := series.Data.([]string)
		cmp, ok := value.(string)
		if !ok {
			return nil, newOpError("Filter", "cannot convert value to string")
		}
		return func(row int) bool { return matchString(data[row], operator, cmp) }, nil

	case BoolType:
		data := series.Data.([]bool)
		cmp, ok := value.(bool)
		if !ok {
			return nil, newOpError("Filter", "cannot convert value to bool")
		}
		return func(row int) bool { return matchBool(data[row], operator, cmp) }, nil

	case TimeType:
		data := series.Data.([]time.Time)
		cmp, ok := value.(time.Time)
		if !ok {
			return nil, newOpError("Filter", "cannot convert value to time.Time")
		}
		return func(row int) bool { return matchTime(data[row], operator, cmp) }, nil
	}

	return nil, newOpError("Filter", "unsupported column type")
}
