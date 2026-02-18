package otters

import (
	"errors"
	"testing"
)

func TestStats_Quantile_InterpolationAndExactIndex(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]interface{}{"x": []float64{4, 1, 3, 2}})
	if err != nil {
		t.Fatalf("NewDataFrameFromMap error: %v", err)
	}
	q50, err := df.Quantile("x", 0.5)
	if err != nil || q50 != 2.5 {
		t.Fatalf("Quantile(0.5) = %v, %v, want 2.5", q50, err)
	}
	qExact, err := df.Quantile("x", 2.0/3.0)
	if err != nil || qExact != 3.0 {
		t.Fatalf("Quantile(2/3) = %v, %v, want 3", qExact, err)
	}
	q0, _ := df.Quantile("x", 0)
	q1, _ := df.Quantile("x", 1)
	if q0 != 1.0 || q1 != 4.0 {
		t.Fatalf("Quantile endpoints wrong: q0=%v q1=%v", q0, q1)
	}
}

func TestStats_Quantile_ErrorBranches(t *testing.T) {
	df, _ := NewDataFrameFromMap(map[string]interface{}{"x": []string{"a", "b"}})
	if _, err := df.Quantile("x", -0.1); err == nil {
		t.Error("expected error for q < 0")
	}
	if _, err := df.Quantile("x", 1.1); err == nil {
		t.Error("expected error for q > 1")
	}
	if _, err := df.Quantile("missing", 0.5); err == nil {
		t.Error("expected error for missing column")
	}
	if _, err := df.Quantile("x", 0.5); err == nil {
		t.Error("expected error for non-numeric column")
	}
	dfErr := NewDataFrame()
	dfErr.err = errors.New("boom")
	if _, err := dfErr.Quantile("x", 0.5); err == nil {
		t.Error("expected error when df has error")
	}
}

func TestStats_NumericSummary_SuccessAndErrors(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]interface{}{
		"i": []int64{1, 2, 3, 4},
		"f": []float64{1.5, 2.5, 3.5, 4.5},
		"s": []string{"a", "b", "c", "d"},
	})
	if err != nil {
		t.Fatalf("NewDataFrameFromMap error: %v", err)
	}
	statsI, err := df.NumericSummary("i")
	if err != nil || statsI.Count != 4 {
		t.Fatalf("NumericSummary int failed: %v", err)
	}
	statsF, err := df.NumericSummary("f")
	if err != nil || statsF.Count != 4 {
		t.Fatalf("NumericSummary float failed: %v", err)
	}
	if _, err := df.NumericSummary("s"); err == nil {
		t.Error("expected error for non-numeric column")
	}
	if _, err := df.NumericSummary("missing"); err == nil {
		t.Error("expected error for missing column")
	}
	dfErr := NewDataFrame()
	dfErr.err = errors.New("boom")
	if _, err := dfErr.NumericSummary("i"); err == nil {
		t.Error("expected error when df has error")
	}
}

func TestStats_ConvertToFloat64_AllBranches(t *testing.T) {
	if convertToFloat64(int64(2)) != 2.0 {
		t.Error("int64 conversion failed")
	}
	if convertToFloat64(float64(2.5)) != 2.5 {
		t.Error("float64 conversion failed")
	}
	if convertToFloat64(int(3)) != 3.0 {
		t.Error("int conversion failed")
	}
	if convertToFloat64("x") != 0.0 {
		t.Error("default conversion failed")
	}
}

func TestStats_MinMaxFloat64(t *testing.T) {
	data := []float64{5.5, 2.2, 8.8, 1.1}
	indices := []int{0, 1, 2, 3}
	if minFloat64(data, indices) != 1.1 {
		t.Error("minFloat64 failed")
	}
	if maxFloat64(data, indices) != 8.8 {
		t.Error("maxFloat64 failed")
	}
}

func TestStats_MinMaxInt64(t *testing.T) {
	data := []int64{5, 2, 8, 1}
	indices := []int{0, 1, 2, 3}
	if minInt64(data, indices) != 1.0 {
		t.Error("minInt64 failed")
	}
	if maxInt64(data, indices) != 8.0 {
		t.Error("maxInt64 failed")
	}
}

func TestStats_AggregateWithEmptyIndices(t *testing.T) {
	result, err := aggregateInt64([]int64{1, 2, 3}, []int{}, "sum")
	if err != nil || result != 0 {
		t.Error("aggregate with empty indices should return 0")
	}
}

func TestStats_GroupByBuildGroupsSingleColumn(t *testing.T) {
	data := map[string]interface{}{
		"group": []string{"A", "B", "A"},
		"val":   []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)
	groups := df.GroupBy("group").buildGroups()
	if len(groups) != 2 {
		t.Errorf("buildGroups should create 2 groups, got %d", len(groups))
	}
}
