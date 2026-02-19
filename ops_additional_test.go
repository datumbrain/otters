package otters

import (
	"testing"
	"time"
)

func TestSelectEdgeCases(t *testing.T) {
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
