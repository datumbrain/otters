package otters

import (
	"testing"
	"time"
)

func TestOps_MatchInt64_AllOperators(t *testing.T) {
	if !matchInt64(5, "==", int64(5)) || !matchInt64(5, "!=", int64(3)) {
		t.Error("matchInt64 == or != failed")
	}
	if !matchInt64(5, ">", int64(3)) || !matchInt64(5, ">=", int64(5)) {
		t.Error("matchInt64 > or >= failed")
	}
	if !matchInt64(3, "<", int64(5)) || !matchInt64(5, "<=", int64(5)) {
		t.Error("matchInt64 < or <= failed")
	}
	if matchInt64(5, "???", int64(3)) {
		t.Error("matchInt64 invalid operator should return false")
	}
}

func TestOps_MatchString_AllOperators(t *testing.T) {
	if !matchString("hello", "==", "hello") || !matchString("hello", "!=", "world") {
		t.Error("matchString == or != failed")
	}
	if !matchString("hello", "contains", "ell") || matchString("hello", "contains", "xyz") {
		t.Error("matchString contains failed")
	}
	if !matchString("hello", ">", "abc") || !matchString("hello", ">=", "hello") {
		t.Error("matchString > or >= failed")
	}
	if !matchString("abc", "<", "xyz") || !matchString("hello", "<=", "hello") {
		t.Error("matchString < or <= failed")
	}
	if matchString("hello", "???", "world") {
		t.Error("matchString invalid operator should return false")
	}
}

func TestOps_MatchFloat64_AllOperators(t *testing.T) {
	if !matchFloat64(5.0, "==", 5.0) || matchFloat64(5.0, "==", 3.0) {
		t.Error("matchFloat64 == failed")
	}
	if !matchFloat64(5.0, "!=", 3.0) || !matchFloat64(5.0, ">", 3.0) {
		t.Error("matchFloat64 != or > failed")
	}
	if !matchFloat64(5.0, ">=", 5.0) || !matchFloat64(3.0, "<", 5.0) {
		t.Error("matchFloat64 >= or < failed")
	}
	if !matchFloat64(5.0, "<=", 5.0) || matchFloat64(5.0, "???", 3.0) {
		t.Error("matchFloat64 <= or invalid failed")
	}
}

func TestOps_MatchBool_AllOperators(t *testing.T) {
	if !matchBool(true, "==", true) || !matchBool(true, "!=", false) {
		t.Error("matchBool == or != failed")
	}
	if matchBool(true, "invalid", false) {
		t.Error("matchBool invalid operator should return false")
	}
}

func TestOps_MatchTime_AllOperators(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	if !matchTime(t1, "==", t1) || matchTime(t1, "==", t2) {
		t.Error("matchTime == failed")
	}
	if !matchTime(t1, "!=", t2) || !matchTime(t2, ">", t1) {
		t.Error("matchTime != or > failed")
	}
	if !matchTime(t1, ">=", t1) || !matchTime(t1, "<", t2) {
		t.Error("matchTime >= or < failed")
	}
	if !matchTime(t1, "<=", t1) || matchTime(t1, "???", t2) {
		t.Error("matchTime <= or invalid failed")
	}
}

func TestOps_FilterInt64Indices_AllOperators(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	eq, _ := filterInt64Indices(data, "==", int64(3))
	ne, _ := filterInt64Indices(data, "!=", int64(3))
	gt, _ := filterInt64Indices(data, ">", int64(3))
	gte, _ := filterInt64Indices(data, ">=", int64(3))
	lt, _ := filterInt64Indices(data, "<", int64(3))
	lte, _ := filterInt64Indices(data, "<=", int64(3))
	if len(eq) != 1 || len(ne) != 4 || len(gt) != 2 || len(gte) != 3 || len(lt) != 2 || len(lte) != 3 {
		t.Error("filterInt64Indices operator results wrong")
	}
}

func TestOps_FilterStringIndices_AllOperators(t *testing.T) {
	data := []string{"apple", "banana", "cherry", "date"}
	eq, _ := filterStringIndices(data, "==", "banana")
	ne, _ := filterStringIndices(data, "!=", "banana")
	con, _ := filterStringIndices(data, "contains", "an")
	gt, _ := filterStringIndices(data, ">", "banana")
	gte, _ := filterStringIndices(data, ">=", "banana")
	lt, _ := filterStringIndices(data, "<", "cherry")
	lte, _ := filterStringIndices(data, "<=", "cherry")
	if len(eq) != 1 || len(ne) != 3 || len(con) != 1 || len(gt) != 2 || len(gte) != 3 || len(lt) != 2 || len(lte) != 3 {
		t.Error("filterStringIndices operator results wrong")
	}
}

func TestOps_ToInt64_AllCases(t *testing.T) {
	v1, ok1 := toInt64(int64(42))
	v2, ok2 := toInt64(42)
	v3, ok3 := toInt64(42.7)
	_, ok4 := toInt64("not a number")
	if !ok1 || v1 != 42 || !ok2 || v2 != 42 || !ok3 || v3 != 42 || ok4 {
		t.Error("toInt64 cases failed")
	}
}

func TestOps_ToFloat64_AllCases(t *testing.T) {
	v1, ok1 := toFloat64(3.14)
	v2, ok2 := toFloat64(int64(42))
	v3, ok3 := toFloat64(42)
	_, ok4 := toFloat64("not a number")
	if !ok1 || v1 != 3.14 || !ok2 || v2 != 42.0 || !ok3 || v3 != 42.0 || ok4 {
		t.Error("toFloat64 cases failed")
	}
}

func TestOps_AggregateFloat64_AllOperations(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	indices := []int{0, 2, 4}
	sum, _ := aggregateFloat64(data, indices, "sum")
	mean, _ := aggregateFloat64(data, indices, "mean")
	count, _ := aggregateFloat64(data, indices, "count")
	min, _ := aggregateFloat64(data, indices, "min")
	max, _ := aggregateFloat64(data, indices, "max")
	_, err := aggregateFloat64(data, indices, "invalid")
	if sum != 9.0 || mean != 3.0 || count != 3.0 || min != 1.0 || max != 5.0 || err == nil {
		t.Error("aggregateFloat64 operations failed")
	}
}

func TestOps_CalculateAggregation_AllTypes(t *testing.T) {
	data := map[string]interface{}{
		"int_col":   []int64{1, 2, 3, 4, 5},
		"float_col": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
	}
	df, _ := NewDataFrameFromMap(data)
	gb := df.GroupBy("int_col")
	_, err1 := gb.calculateAggregation("int_col", []int{0, 1}, "sum")
	_, err2 := gb.calculateAggregation("float_col", []int{0, 1}, "sum")
	result, err3 := gb.calculateAggregation("int_col", []int{}, "sum")
	if err1 != nil || err2 != nil || err3 != nil || result != 0 {
		t.Error("calculateAggregation failed")
	}
}

func TestOps_BuildGroups_MultipleColumns(t *testing.T) {
	data := map[string]interface{}{
		"col1": []string{"A", "B", "A", "B"},
		"col2": []string{"X", "X", "Y", "Y"},
		"val":  []int64{1, 2, 3, 4},
	}
	df, _ := NewDataFrameFromMap(data)
	groups := df.GroupBy("col1", "col2").buildGroups()
	if len(groups) != 4 {
		t.Errorf("buildGroups should create 4 groups, got %d", len(groups))
	}
}

func TestOps_SortGroupKeys(t *testing.T) {
	groups := map[string]*groupKey{"c": {}, "a": {}, "b": {}}
	sorted := sortGroupKeys(groups)
	if len(sorted) != 3 || sorted[0] != "a" || sorted[1] != "b" || sorted[2] != "c" {
		t.Error("sortGroupKeys should sort keys alphabetically")
	}
}

func TestOps_AllocateGroupColumns(t *testing.T) {
	result := allocateGroupColumns([]string{"col1", "col2"}, 5)
	if len(result) != 2 || cap(result[0]) != 5 {
		t.Error("allocateGroupColumns failed")
	}
}

func TestOps_IdentifyNumericColumns(t *testing.T) {
	data := map[string]interface{}{
		"group":  []string{"A", "B"},
		"int":    []int64{1, 2},
		"float":  []float64{1.1, 2.2},
		"string": []string{"x", "y"},
	}
	df, _ := NewDataFrameFromMap(data)
	numCols := identifyNumericColumns(df, []string{"group"}, 2)
	if len(numCols) != 2 {
		t.Errorf("identifyNumericColumns should find 2 numeric columns, got %d", len(numCols))
	}
}

func TestOps_ProcessGroups(t *testing.T) {
	data := map[string]interface{}{"group": []string{"A", "B"}, "val": []int64{1, 2}}
	df, _ := NewDataFrameFromMap(data)
	gb := df.GroupBy("group")
	groups := gb.buildGroups()
	sortedKeys := sortGroupKeys(groups)
	groupColData := allocateGroupColumns([]string{"group"}, 2)
	numCols := identifyNumericColumns(df, []string{"group"}, 2)
	if err := processGroups(gb, groups, sortedKeys, groupColData, numCols, "sum"); err != nil {
		t.Errorf("processGroups error: %v", err)
	}
}

func TestOps_BuildResultDataFrame(t *testing.T) {
	df, err := buildResultDataFrame(
		[]string{"group"},
		[][]string{{"A", "B"}},
		[]numericCol{{name: "sum", data: []float64{10.0, 20.0}}},
	)
	if err != nil || df.Len() != 2 {
		t.Errorf("buildResultDataFrame failed: %v", err)
	}
}

func TestOps_CompareInt64(t *testing.T) {
	if compareInt64(1, 2) >= 0 || compareInt64(2, 1) <= 0 || compareInt64(1, 1) != 0 {
		t.Error("compareInt64 failed")
	}
}

func TestOps_SelectStringAndInt64Rows(t *testing.T) {
	if r := selectStringRows([]string{"a", "b", "c", "d"}, []int{0, 2}); len(r) != 2 || r[0] != "a" || r[1] != "c" {
		t.Error("selectStringRows failed")
	}
	if r := selectInt64Rows([]int64{1, 2, 3, 4}, []int{1, 3}); len(r) != 2 || r[0] != 2 || r[1] != 4 {
		t.Error("selectInt64Rows failed")
	}
}

func TestOps_SumFloat64(t *testing.T) {
	result := sumFloat64([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, []int{0, 2, 4})
	expected := 1.1 + 3.3 + 5.5
	if result < expected-0.01 || result > expected+0.01 {
		t.Errorf("sumFloat64 = %v, want ~%v", result, expected)
	}
}

func TestOps_UniqueInt64(t *testing.T) {
	if len(uniqueInt64([]int64{1, 2, 1, 3, 2})) != 3 {
		t.Error("uniqueInt64 failed")
	}
}

func TestOps_UniqueFromSeries_AllBranches(t *testing.T) {
	s1, _ := NewSeries("t", []string{"a", "b", "a", "c", "b"})
	s2, _ := NewSeries("t", []int64{1, 2, 1, 3, 2})
	s3, _ := NewSeries("t", []float64{1.1, 2.2, 1.1, 3.3})
	s4, _ := NewSeries("t", []bool{true, false, true, false})
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("t", []time.Time{t1, t2, t1, t2})
	if len(uniqueFromSeries(s1)) != 3 || len(uniqueFromSeries(s2)) != 3 || len(uniqueFromSeries(s3)) != 3 || len(uniqueFromSeries(s4)) != 2 || len(uniqueFromSeries(s5)) != 2 {
		t.Error("uniqueFromSeries failed for one or more types")
	}
}

func TestOps_SelectSeriesRows_AllBranches(t *testing.T) {
	indices := []int{0, 2}
	s1, _ := NewSeries("t", []string{"a", "b", "c", "d"})
	s2, _ := NewSeries("t", []int64{1, 2, 3, 4})
	s3, _ := NewSeries("t", []float64{1.1, 2.2, 3.3, 4.4})
	s4, _ := NewSeries("t", []bool{true, false, true, false})
	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("t", []time.Time{tm, tm, tm, tm})
	s6 := &Series{Type: ColumnType(99), Data: []int{1, 2, 3}}
	if selectSeriesRows(s1, indices) == nil || selectSeriesRows(s2, indices) == nil || selectSeriesRows(s3, indices) == nil || selectSeriesRows(s4, indices) == nil || selectSeriesRows(s5, indices) == nil {
		t.Error("selectSeriesRows returned nil for valid type")
	}
	if selectSeriesRows(s6, indices) != nil {
		t.Error("selectSeriesRows unknown type should return nil")
	}
}

func TestOps_CompareValues_AllBranches(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	if compareValues("a", "b", StringType) >= 0 || compareValues(int64(1), int64(2), Int64Type) >= 0 {
		t.Error("compareValues string/int64 failed")
	}
	if compareValues(1.1, 2.2, Float64Type) >= 0 || compareValues(false, true, BoolType) >= 0 {
		t.Error("compareValues float64/bool failed")
	}
	if compareValues(t1, t2, TimeType) >= 0 || compareValues("a", "b", ColumnType(99)) != 0 {
		t.Error("compareValues time/unknown failed")
	}
}

func TestOps_Filter_AllTypeBranches(t *testing.T) {
	df1, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []int64{1, 2, 3, 4, 5}})
	df2, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []float64{1.1, 2.2, 3.3, 4.4}})
	df3, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []string{"apple", "banana", "cherry"}})
	df4, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []bool{true, false, true, false}})
	if r := df1.Filter("col1", ">", int64(3)); r.Error() != nil || r.Len() != 2 {
		t.Error("Filter int64 failed")
	}
	if r := df2.Filter("col1", ">", 2.5); r.Error() != nil || r.Len() != 2 {
		t.Error("Filter float64 failed")
	}
	if r := df3.Filter("col1", "contains", "an"); r.Error() != nil || r.Len() != 1 {
		t.Error("Filter string failed")
	}
	if r := df4.Filter("col1", "==", true); r.Error() != nil || r.Len() != 2 {
		t.Error("Filter bool failed")
	}
}

func TestOps_Select_AllBranches(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3}, "col2": []string{"a", "b", "c"}, "col3": []float64{1.1, 2.2, 3.3}}
	df, _ := NewDataFrameFromMap(data)
	if r := df.Select("col1"); r.Error() != nil || r.Width() != 1 {
		t.Error("Select single column failed")
	}
	if r := df.Select("col1", "col3"); r.Error() != nil || r.Width() != 2 {
		t.Error("Select multiple columns failed")
	}
	if r := df.Select("nonexistent"); r.Error() == nil {
		t.Error("Select nonexistent column should error")
	}
}

func TestOps_Drop_AllBranches(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3}, "col2": []string{"a", "b", "c"}, "col3": []float64{1.1, 2.2, 3.3}}
	df, _ := NewDataFrameFromMap(data)
	if r := df.Drop("col1"); r.Error() != nil || r.Width() != 2 {
		t.Error("Drop single column failed")
	}
	if r := df.Drop("col1", "col2"); r.Error() != nil || r.Width() != 1 {
		t.Error("Drop multiple columns failed")
	}
}

func TestOps_SortBy_AllBranches(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{3, 1, 2}, "col2": []string{"c", "a", "b"}}
	df, _ := NewDataFrameFromMap(data)
	r1 := df.SortBy([]string{"col1"}, []bool{true})
	v1, _ := r1.Get(0, "col1")
	r2 := df.SortBy([]string{"col1"}, []bool{false})
	v2, _ := r2.Get(0, "col1")
	if r1.Error() != nil || v1 != int64(1) || r2.Error() != nil || v2 != int64(3) {
		t.Error("SortBy asc/desc failed")
	}
	if r := df.SortBy([]string{"col1"}, []bool{true, false}); r.Error() == nil {
		t.Error("SortBy mismatched lengths should error")
	}
	if r := df.SortBy([]string{"nonexistent"}, []bool{true}); r.Error() == nil {
		t.Error("SortBy nonexistent column should error")
	}
}

func TestOps_GroupBy_SumMinMax(t *testing.T) {
	data := map[string]interface{}{"group": []string{"A", "B", "A", "B"}, "val": []int64{1, 2, 3, 4}}
	df, _ := NewDataFrameFromMap(data)
	sumDf, err1 := df.GroupBy("group").Sum()
	minDf, err2 := df.GroupBy("group").Min()
	maxDf, err3 := df.GroupBy("group").Max()
	if err1 != nil || sumDf.Len() != 2 || err2 != nil || minDf.Len() != 2 || err3 != nil || maxDf.Len() != 2 {
		t.Error("GroupBy Sum/Min/Max failed")
	}
}

func TestOps_SelectRows_EmptyAndNonEmpty(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	data := map[string]interface{}{
		"s": []string{"a", "b"}, "i": []int64{1, 2},
		"f": []float64{1.1, 2.2}, "b": []bool{true, false}, "t": []time.Time{t1, t2},
	}
	df, _ := NewDataFrameFromMap(data)
	out := df.selectRows([]int{}, "SelectRows")
	if out.Error() != nil || out.Len() != 0 || out.Width() != 5 {
		t.Errorf("selectRows empty indices failed: %v", out.Error())
	}
	out2 := df.selectRows([]int{0, 1}, "SelectRows")
	if out2.Error() != nil || out2.Len() != 2 {
		t.Errorf("selectRows non-empty indices failed: %v", out2.Error())
	}
}

func TestOps_SeriesValueToString_BoolFalseBranch(t *testing.T) {
	s, _ := NewSeries("b", []bool{false})
	if seriesValueToString(s, 0) != "false" {
		t.Error("seriesValueToString bool=false branch failed")
	}
	s2, _ := NewSeries("b", []bool{true})
	if seriesValueToString(s2, 0) != "true" {
		t.Error("seriesValueToString bool=true branch failed")
	}
	// default branch
	su := &Series{Name: "u", Type: ColumnType(99), Data: []int64{1}, Length: 1}
	if seriesValueToString(su, 0) != "" {
		t.Error("seriesValueToString unknown type should return empty string")
	}
}

func TestOps_GroupBy_ZeroColumns_Errors(t *testing.T) {
	df, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []int64{1, 2}})
	gb := df.GroupBy()
	if gb.err == nil {
		t.Error("GroupBy with zero columns should error")
	}
}

func TestOps_GroupBy_MissingColumn_Errors(t *testing.T) {
	df, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []int64{1, 2}})
	gb := df.GroupBy("nonexistent")
	if gb.err == nil {
		t.Error("GroupBy with missing column should error")
	}
}

func TestOps_Select_WithErrorDF(t *testing.T) {
	df := NewDataFrame()
	df.err = newOpError("test", "error")
	if r := df.Select("col1"); r.Error() == nil {
		t.Error("Select should propagate DataFrame error")
	}
}

func TestOps_SelectRows_UnsupportedType(t *testing.T) {
	df := NewDataFrame()
	df.length = 2
	df.columns["x"] = &Series{Name: "x", Type: ColumnType(99), Data: []int64{1, 2}, Length: 2}
	df.order = append(df.order, "x")
	if df.selectRows([]int{}, "SelectRows").Error() == nil {
		t.Error("expected error for unsupported type (empty indices)")
	}
	if df.selectRows([]int{0}, "SelectRows").Error() == nil {
		t.Error("expected error for unsupported type (non-empty indices)")
	}
}

func TestOps_Unique_ErrorAndMissingColumn(t *testing.T) {
	df := NewDataFrame()
	df.err = newOpError("test", "boom")
	if _, err := df.Unique("x"); err == nil {
		t.Error("expected error when df has error")
	}
	df2, _ := NewDataFrameFromMap(map[string]interface{}{"a": []int64{1, 2, 3}})
	if _, err := df2.Unique("missing"); err == nil {
		t.Error("expected error for missing column")
	}
}

func TestOps_Query_ErrorBranchesAndQuotedValue(t *testing.T) {
	df1, _ := NewDataFrameFromMap(map[string]interface{}{"age": []int64{1, 2}})
	if df1.Query("age>1").Error() == nil {
		t.Error("expected error for invalid query format")
	}
	if df1.Query("nope == 1").Error() == nil {
		t.Error("expected error for missing column")
	}
	if df1.Query("age == notanumber").Error() == nil {
		t.Error("expected error for convert value")
	}
	df2, _ := NewDataFrameFromMap(map[string]interface{}{"name": []string{"Alice", "Bob"}})
	res := df2.Query("name == 'Alice'")
	if res.Error() != nil || res.Len() != 1 {
		t.Errorf("Query quoted value failed: %v", res.Error())
	}
}
