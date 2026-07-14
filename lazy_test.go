package otters

import (
	"errors"
	"testing"
	"time"
)

func lazyTestFrame(t *testing.T) *DataFrame {
	t.Helper()
	df, err := NewDataFrameFromMap(map[string]any{
		"name":   []string{"Alice", "Bob", "Carol", "David", "Eve", "Frank"},
		"dept":   []string{"Eng", "Eng", "Mkt", "Eng", "Mkt", "Sales"},
		"salary": []int64{75000, 80000, 60000, 70000, 65000, 55000},
		"score":  []float64{4.5, 3.9, 4.1, 4.5, 3.7, 4.0},
	})
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// assertFramesEqual compares two DataFrames cell by cell.
func assertFramesEqual(t *testing.T, got, want *DataFrame) {
	t.Helper()

	gotRows, gotCols := got.Shape()
	wantRows, wantCols := want.Shape()
	if gotRows != wantRows || gotCols != wantCols {
		t.Fatalf("shape = (%d, %d), want (%d, %d)", gotRows, gotCols, wantRows, wantCols)
	}

	gotColumns := got.Columns()
	wantColumns := want.Columns()
	for i := range wantColumns {
		if gotColumns[i] != wantColumns[i] {
			t.Fatalf("columns = %v, want %v", gotColumns, wantColumns)
		}
	}

	for i := 0; i < wantRows; i++ {
		for _, col := range wantColumns {
			gotVal, err := got.Get(i, col)
			if err != nil {
				t.Fatal(err)
			}
			wantVal, err := want.Get(i, col)
			if err != nil {
				t.Fatal(err)
			}
			if gotVal != wantVal {
				t.Errorf("cell [%d, %s] = %v, want %v", i, col, gotVal, wantVal)
			}
		}
	}
}

// TestLazyMatchesEagerChain verifies that a lazy chain produces exactly the
// same result as the equivalent eager chain.
func TestLazyMatchesEagerChain(t *testing.T) {
	df := lazyTestFrame(t)

	eager := df.
		Filter("dept", "==", "Eng").
		Filter("salary", ">=", 70000).
		Select("name", "salary").
		Sort("salary", false)
	if eager.Error() != nil {
		t.Fatal(eager.Error())
	}

	lazy, err := df.Lazy().
		Filter("dept", "==", "Eng").
		Filter("salary", ">=", 70000).
		Select("name", "salary").
		Sort("salary", false).
		Collect()
	if err != nil {
		t.Fatal(err)
	}

	assertFramesEqual(t, lazy, eager)
}

// TestLazyHeadTailMatchEager verifies Head/Tail parity with the eager path.
func TestLazyHeadTailMatchEager(t *testing.T) {
	df := lazyTestFrame(t)

	eagerHead := df.Sort("salary", true).Head(3)
	lazyHead, err := df.Lazy().Sort("salary", true).Head(3).Collect()
	if err != nil {
		t.Fatal(err)
	}
	assertFramesEqual(t, lazyHead, eagerHead)

	eagerTail := df.Sort("salary", true).Tail(2)
	lazyTail, err := df.Lazy().Sort("salary", true).Tail(2).Collect()
	if err != nil {
		t.Fatal(err)
	}
	assertFramesEqual(t, lazyTail, eagerTail)

	// n larger than the view is clamped, like the eager path's full copy.
	all, err := df.Lazy().Head(100).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if all.Len() != df.Len() {
		t.Errorf("Head(100) collected %d rows, want %d", all.Len(), df.Len())
	}
}

// TestLazySortStable verifies that a lazy sort keeps the current view order
// for equal keys, including after a previous sort.
func TestLazySortStable(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]any{
		"grp": []int64{1, 0, 1, 0, 1, 0},
		"seq": []int64{0, 1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Sort by seq descending first, then by grp: within each grp value the
	// descending seq order must be preserved.
	result, err := df.Lazy().Sort("seq", false).Sort("grp", true).Collect()
	if err != nil {
		t.Fatal(err)
	}

	wantSeq := []int64{5, 3, 1, 4, 2, 0}
	for i, want := range wantSeq {
		v, err := result.Get(i, "seq")
		if err != nil {
			t.Fatal(err)
		}
		if v.(int64) != want {
			t.Errorf("seq[%d] = %v, want %d", i, v, want)
		}
	}
}

// TestLazyEmptyResultKeepsSchema verifies that a filter matching nothing
// still collects a frame with the right columns and types.
func TestLazyEmptyResultKeepsSchema(t *testing.T) {
	df := lazyTestFrame(t)

	result, err := df.Lazy().
		Filter("salary", ">", 1000000).
		Select("name", "salary").
		Collect()
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 0 || cols != 2 {
		t.Errorf("shape = (%d, %d), want (0, 2)", rows, cols)
	}
	colType, err := result.GetColumnType("salary")
	if err != nil {
		t.Fatal(err)
	}
	if colType != Int64Type {
		t.Errorf("salary type = %v, want int64", colType)
	}
}

// TestLazyErrorPropagation verifies error handling through a lazy chain.
func TestLazyErrorPropagation(t *testing.T) {
	df := lazyTestFrame(t)

	// Missing column errors and short-circuits the rest of the chain.
	_, err := df.Lazy().Filter("missing", "==", 1).Sort("salary", true).Collect()
	if !errors.Is(err, ErrColumnNotFound) {
		t.Errorf("expected ErrColumnNotFound, got %v", err)
	}

	// Filtering on a column dropped by Select errors.
	_, err = df.Lazy().Select("name").Filter("salary", ">", 0).Collect()
	if err == nil {
		t.Error("Filter on unselected column should error")
	}

	// Duplicate Select errors.
	_, err = df.Lazy().Select("name", "name").Collect()
	if err == nil {
		t.Error("duplicate Select should error")
	}

	// A source DataFrame in error state propagates.
	bad := df.Filter("missing", "==", 1)
	_, err = bad.Lazy().Select("name").Collect()
	if err == nil {
		t.Error("error state of source DataFrame should propagate")
	}
}

// TestLazyFractionalFloatFilter verifies the lazy path applies the same
// fractional-float rule as the eager Filter.
func TestLazyFractionalFloatFilter(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]any{
		"n": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := df.Lazy().Filter("n", ">=", 2.5).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if result.Len() != 1 {
		t.Errorf("Filter(n >= 2.5) collected %d rows, want 1", result.Len())
	}
}

// TestLazyTimeColumns verifies lazy operations on time columns.
func TestLazyTimeColumns(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	df, err := NewDataFrameFromMap(map[string]any{
		"when": []time.Time{t2, t3, t1},
		"v":    []int64{2, 3, 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := df.Lazy().Filter("when", ">", t1).Sort("when", true).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if result.Len() != 2 {
		t.Fatalf("collected %d rows, want 2", result.Len())
	}
	first, err := result.Get(0, "when")
	if err != nil {
		t.Fatal(err)
	}
	if !first.(time.Time).Equal(t2) {
		t.Errorf("first row = %v, want %v", first, t2)
	}
}

// TestLazyCollectIsIndependent verifies the collected frame does not share
// data with the source.
func TestLazyCollectIsIndependent(t *testing.T) {
	df := lazyTestFrame(t)

	result, err := df.Lazy().Filter("dept", "==", "Eng").Collect()
	if err != nil {
		t.Fatal(err)
	}

	if err := result.Set(0, "salary", int64(1)); err != nil {
		t.Fatal(err)
	}

	orig, err := df.Get(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if orig.(int64) != 75000 {
		t.Errorf("mutating the collected frame changed the source: got %v", orig)
	}
}

// BenchmarkChainedOps compares an eager Filter→Select→Sort chain against the
// equivalent lazy chain (P4-B2).
func BenchmarkChainedOps(b *testing.B) {
	size := 10000
	ids := make([]int64, size)
	values := make([]float64, size)
	labels := make([]string, size)
	for i := 0; i < size; i++ {
		ids[i] = int64(i)
		values[i] = float64(i%1000) * 1.5
		labels[i] = "cat" + string(rune('A'+i%5))
	}
	df, err := NewDataFrameFromMap(map[string]any{
		"id":    ids,
		"value": values,
		"label": labels,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Eager", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := df.
				Filter("value", ">", 500.0).
				Select("id", "value").
				Sort("value", false)
			if result.Error() != nil {
				b.Fatal(result.Error())
			}
		}
	})

	b.Run("Lazy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := df.Lazy().
				Filter("value", ">", 500.0).
				Select("id", "value").
				Sort("value", false).
				Collect()
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})
}
