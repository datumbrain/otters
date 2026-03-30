package otters

import (
	"testing"
	"time"
)

func TestSelectEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
		"col3": []float64{1.1, 2.2, 3.3},
	}
	df, _ := NewDataFrameFromMap(data)

	result := df.Select("col1", "col3")
	if result.Width() != 2 {
		t.Error("Select should return 2 columns")
	}

	result2 := df.Select("nonexistent")
	if result2.Error() == nil {
		t.Error("Select should error on nonexistent column")
	}
}

func TestDropEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)

	result := df.Drop("col1")
	if result.Width() != 1 {
		t.Error("Drop should remove column")
	}

	result2 := df.Drop("col1", "col2")
	if result2.Width() != 0 {
		t.Error("Drop should remove all columns")
	}
}

func TestSortByEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{3, 1, 2},
		"col2": []string{"c", "a", "b"},
	}
	df, _ := NewDataFrameFromMap(data)

	result := df.SortBy([]string{"col1"}, []bool{true})
	if result.Error() != nil {
		t.Errorf("SortBy error: %v", result.Error())
	}

	val, _ := result.Get(0, "col1")
	if val != int64(1) {
		t.Error("SortBy ascending should sort correctly")
	}

	result2 := df.SortBy([]string{"col1"}, []bool{false})
	val2, _ := result2.Get(0, "col1")
	if val2 != int64(3) {
		t.Error("SortBy descending should sort correctly")
	}
}

func TestUniqueEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 1, 3, 2},
	}
	df, _ := NewDataFrameFromMap(data)

	result, err := df.Unique("col1")
	if err != nil {
		t.Errorf("Unique error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("Unique should return 3 unique values, got %d", len(result))
	}
}

func TestQueryEdgeCases(t *testing.T) {
	data := map[string]any{
		"age": []int64{25, 30, 35, 40},
	}
	df, _ := NewDataFrameFromMap(data)

	result := df.Query("age > 30")
	if result.Error() != nil {
		t.Errorf("Query error: %v", result.Error())
	}
	if result.Len() != 2 {
		t.Errorf("Query should return 2 rows, got %d", result.Len())
	}
}

func TestMatchStringEdgeCases(t *testing.T) {
	if !matchString("hello", "==", "hello") {
		t.Error("matchString == should work")
	}
	if matchString("hello", "==", "world") {
		t.Error("matchString == should return false for different strings")
	}
	if !matchString("hello", "!=", "world") {
		t.Error("matchString != should work")
	}
	if !matchString("hello", "contains", "ell") {
		t.Error("matchString contains should work")
	}
	if matchString("hello", "contains", "xyz") {
		t.Error("matchString contains should return false")
	}
}

func TestMatchInt64EdgeCases(t *testing.T) {
	if !matchInt64(5, "==", int64(5)) {
		t.Error("matchInt64 == should work")
	}
	if !matchInt64(5, ">", int64(3)) {
		t.Error("matchInt64 > should work")
	}
	if !matchInt64(5, ">=", int64(5)) {
		t.Error("matchInt64 >= should work")
	}
	if !matchInt64(3, "<", int64(5)) {
		t.Error("matchInt64 < should work")
	}
	if !matchInt64(5, "<=", int64(5)) {
		t.Error("matchInt64 <= should work")
	}
	if !matchInt64(5, "!=", int64(3)) {
		t.Error("matchInt64 != should work")
	}
}

func TestSeriesValueToStringEdgeCases(t *testing.T) {
	s1, _ := NewSeries("test", []string{"hello"})
	str := seriesValueToString(s1, 0)
	if str != "hello" {
		t.Errorf("seriesValueToString for string = %s", str)
	}

	s2, _ := NewSeries("test", []int64{42})
	str2 := seriesValueToString(s2, 0)
	if str2 != "42" {
		t.Errorf("seriesValueToString for int64 = %s", str2)
	}

	s3, _ := NewSeries("test", []float64{3.14})
	str3 := seriesValueToString(s3, 0)
	if str3 == "" {
		t.Error("seriesValueToString for float64 should not be empty")
	}

	s4, _ := NewSeries("test", []bool{true})
	str4 := seriesValueToString(s4, 0)
	if str4 != "true" {
		t.Errorf("seriesValueToString for bool = %s", str4)
	}

	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("test", []time.Time{tm})
	str5 := seriesValueToString(s5, 0)
	if str5 == "" {
		t.Error("seriesValueToString for time should not be empty")
	}
}

func TestToInt64EdgeCases(t *testing.T) {
	val, ok := toInt64(int64(42))
	if !ok || val != 42 {
		t.Error("toInt64 should handle int64")
	}

	val2, ok2 := toInt64(42)
	if !ok2 || val2 != 42 {
		t.Error("toInt64 should handle int")
	}

	val3, ok3 := toInt64(42.7)
	if !ok3 || val3 != 42 {
		t.Error("toInt64 should handle float64")
	}

	_, ok4 := toInt64("not a number")
	if ok4 {
		t.Error("toInt64 should return false for string")
	}
}

func TestFilterIndicesTypedEdgeCases(t *testing.T) {
	s1, _ := NewSeries("test", []int64{1, 2, 3, 4, 5})
	indices, err := filterIndicesTyped(s1, ">", int64(3))
	if err != nil || len(indices) != 2 {
		t.Errorf("filterIndicesTyped for int64: %v, %v", indices, err)
	}

	s2, _ := NewSeries("test", []float64{1.1, 2.2, 3.3})
	indices2, err2 := filterIndicesTyped(s2, ">", 2.0)
	if err2 != nil || len(indices2) != 2 {
		t.Errorf("filterIndicesTyped for float64: %v, %v", indices2, err2)
	}

	s3, _ := NewSeries("test", []string{"a", "b", "c"})
	indices3, err3 := filterIndicesTyped(s3, "==", "b")
	if err3 != nil || len(indices3) != 1 {
		t.Errorf("filterIndicesTyped for string: %v, %v", indices3, err3)
	}

	s4, _ := NewSeries("test", []bool{true, false, true})
	indices4, err4 := filterIndicesTyped(s4, "==", true)
	if err4 != nil || len(indices4) != 2 {
		t.Errorf("filterIndicesTyped for bool: %v, %v", indices4, err4)
	}

	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("test", []time.Time{tm, tm})
	indices5, err5 := filterIndicesTyped(s5, "==", tm)
	if err5 != nil || len(indices5) != 2 {
		t.Errorf("filterIndicesTyped for time: %v, %v", indices5, err5)
	}
}

func TestGroupBy_MeanCount(t *testing.T) {
	data := map[string]any{
		"category": []string{"A", "B", "A", "B"},
		"value":    []int64{10, 20, 30, 40},
	}
	df, _ := NewDataFrameFromMap(data)

	gb := df.GroupBy("category")

	meanDf, err := gb.Mean()
	if err != nil {
		t.Errorf("GroupBy.Mean() error = %v", err)
	}
	if meanDf.Len() != 2 {
		t.Error("GroupBy.Mean() should return 2 groups")
	}

	countDf, err := gb.Count()
	if err != nil {
		t.Errorf("GroupBy.Count() error = %v", err)
	}
	if countDf.Len() != 2 {
		t.Error("GroupBy.Count() should return 2 groups")
	}
}

func TestUniqueStrings(t *testing.T) {
	result := uniqueStrings([]string{"a", "b", "a", "c", "b"})
	if len(result) != 3 {
		t.Errorf("uniqueStrings() = %v, want length 3", result)
	}
}

func TestUniqueFloat64(t *testing.T) {
	result := uniqueFloat64([]float64{1.1, 2.2, 1.1, 3.3})
	if len(result) != 3 {
		t.Errorf("uniqueFloat64() = %v, want length 3", result)
	}
}

func TestUniqueBool(t *testing.T) {
	result := uniqueBool([]bool{true, false, true, false})
	if len(result) != 2 {
		t.Errorf("uniqueBool() = %v, want length 2", result)
	}
}

func TestUniqueTime(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	result := uniqueTime([]time.Time{t1, t2, t1})
	if len(result) != 2 {
		t.Errorf("uniqueTime() = %v, want length 2", result)
	}
}

func TestSelectFloat64Rows(t *testing.T) {
	data := []float64{1.1, 2.2, 3.3, 4.4}
	result := selectFloat64Rows(data, []int{0, 2})
	if len(result) != 2 || result[0] != 1.1 || result[1] != 3.3 {
		t.Errorf("selectFloat64Rows() failed")
	}
}

func TestSelectBoolRows(t *testing.T) {
	data := []bool{true, false, true, false}
	result := selectBoolRows(data, []int{0, 2})
	if len(result) != 2 || !result[0] || !result[1] {
		t.Errorf("selectBoolRows() failed")
	}
}

func TestSelectTimeRows(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	data := []time.Time{t1, t2, t1}
	result := selectTimeRows(data, []int{0, 2})
	if len(result) != 2 {
		t.Errorf("selectTimeRows() failed")
	}
}

func TestCompareStrings(t *testing.T) {
	if compareStrings("a", "b") >= 0 {
		t.Error("compareStrings(a, b) should be < 0")
	}
	if compareStrings("b", "a") <= 0 {
		t.Error("compareStrings(b, a) should be > 0")
	}
	if compareStrings("a", "a") != 0 {
		t.Error("compareStrings(a, a) should be 0")
	}
}

func TestCompareFloat64(t *testing.T) {
	if compareFloat64(1.1, 2.2) >= 0 {
		t.Error("compareFloat64(1.1, 2.2) should be < 0")
	}
	if compareFloat64(2.2, 1.1) <= 0 {
		t.Error("compareFloat64(2.2, 1.1) should be > 0")
	}
	if compareFloat64(1.1, 1.1) != 0 {
		t.Error("compareFloat64(1.1, 1.1) should be 0")
	}
}

func TestCompareBool(t *testing.T) {
	if compareBool(false, true) >= 0 {
		t.Error("compareBool(false, true) should be < 0")
	}
	if compareBool(true, false) <= 0 {
		t.Error("compareBool(true, false) should be > 0")
	}
	if compareBool(true, true) != 0 {
		t.Error("compareBool(true, true) should be 0")
	}
}

func TestCompareTime(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)

	if compareTime(t1, t2) >= 0 {
		t.Error("compareTime should return < 0 for earlier time")
	}
	if compareTime(t2, t1) <= 0 {
		t.Error("compareTime should return > 0 for later time")
	}
	if compareTime(t1, t1) != 0 {
		t.Error("compareTime should return 0 for equal times")
	}
}

func TestFilterFloat64Indices(t *testing.T) {
	data := []float64{1.1, 2.2, 3.3, 4.4}
	indices, err := filterFloat64Indices(data, ">", 2.0)
	if err != nil || len(indices) != 3 {
		t.Errorf("filterFloat64Indices() = %v, %v, want length 3", indices, err)
	}
}

func TestFilterBoolIndices(t *testing.T) {
	data := []bool{true, false, true, false}
	indices, err := filterBoolIndices(data, "==", true)
	if err != nil || len(indices) != 2 {
		t.Errorf("filterBoolIndices() = %v, %v, want length 2", indices, err)
	}
}

func TestFilterTimeIndices(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	data := []time.Time{t1, t2, t1}
	indices, err := filterTimeIndices(data, "==", t1)
	if err != nil || len(indices) != 2 {
		t.Errorf("filterTimeIndices() = %v, %v, want length 2", indices, err)
	}
}

func TestMatchFloat64(t *testing.T) {
	if !matchFloat64(2.2, ">", 1.1) {
		t.Error("matchFloat64(2.2, >, 1.1) should be true")
	}
	if matchFloat64(1.1, ">", 2.2) {
		t.Error("matchFloat64(1.1, >, 2.2) should be false")
	}
}

func TestMatchBool(t *testing.T) {
	if !matchBool(true, "==", true) {
		t.Error("matchBool(true, ==, true) should be true")
	}
	if matchBool(true, "==", false) {
		t.Error("matchBool(true, ==, false) should be false")
	}
}

func TestMatchTime(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)

	if !matchTime(t2, ">", t1) {
		t.Error("matchTime should return true for later time")
	}
	if matchTime(t1, ">", t2) {
		t.Error("matchTime should return false")
	}
}

func TestEmptySliceForType(t *testing.T) {
	tests := []struct {
		ct   ColumnType
		want any
	}{
		{StringType, []string{}},
		{Int64Type, []int64{}},
		{Float64Type, []float64{}},
		{BoolType, []bool{}},
		{TimeType, []time.Time{}},
	}

	for _, tt := range tests {
		got := emptySliceForType(tt.ct)
		if got == nil {
			t.Errorf("emptySliceForType(%v) returned nil", tt.ct)
		}
	}
}

func TestToFloat64(t *testing.T) {
	val, ok := toFloat64("not a number")
	if ok {
		t.Error("toFloat64 should return false on invalid input")
	}

	val, ok = toFloat64(int64(42))
	if !ok || val != 42.0 {
		t.Errorf("toFloat64(42) = %v, %v", val, ok)
	}

	val, ok = toFloat64(3.14)
	if !ok || val != 3.14 {
		t.Errorf("toFloat64(3.14) = %v, %v", val, ok)
	}
}

func TestAggregateInt64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	indices := []int{0, 2, 4}

	sum, _ := aggregateInt64(data, indices, "sum")
	if sum != 9.0 {
		t.Errorf("aggregateInt64 sum = %v, want 9", sum)
	}

	mean, _ := aggregateInt64(data, indices, "mean")
	if mean != 3.0 {
		t.Errorf("aggregateInt64 mean = %v, want 3", mean)
	}

	min, _ := aggregateInt64(data, indices, "min")
	if min != 1.0 {
		t.Errorf("aggregateInt64 min = %v, want 1", min)
	}

	max, _ := aggregateInt64(data, indices, "max")
	if max != 5.0 {
		t.Errorf("aggregateInt64 max = %v, want 5", max)
	}

	count, _ := aggregateInt64(data, indices, "count")
	if count != 3.0 {
		t.Errorf("aggregateInt64 count = %v, want 3", count)
	}

	_, err := aggregateInt64(data, indices, "invalid")
	if err == nil {
		t.Error("aggregateInt64 should error on invalid operation")
	}
}

func TestSumInt64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	result := sumInt64(data, []int{0, 2, 4})
	if result != 9.0 {
		t.Errorf("sumInt64 = %v, want 9", result)
	}
}

func TestMeanInt64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	result := meanInt64(data, []int{0, 2, 4})
	if result != 3.0 {
		t.Errorf("meanInt64 = %v, want 3", result)
	}
}

func TestMinInt64(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9}
	result := minInt64(data, []int{0, 1, 2})
	if result != 2.0 {
		t.Errorf("minInt64 = %v, want 2", result)
	}
}

func TestMaxInt64(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9}
	result := maxInt64(data, []int{0, 1, 2})
	if result != 8.0 {
		t.Errorf("maxInt64 = %v, want 8", result)
	}
}

func TestMeanFloat64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	result := meanFloat64(data, []int{0, 2, 4})
	if result != 3.0 {
		t.Errorf("meanFloat64 = %v, want 3", result)
	}
}
