package otters

import (
	"errors"
	"testing"
	"time"
)

func TestDF_ClearErrorAndHasError(t *testing.T) {
	df := NewDataFrame()
	if df.hasError() {
		t.Error("new DataFrame should have no error")
	}
	df.err = errors.New("boom")
	if !df.hasError() {
		t.Error("hasError should be true after setting err")
	}
	df.clearError()
	if df.hasError() {
		t.Error("hasError should be false after clearError")
	}
}

func TestDF_Slice_AllTypes(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	data := map[string]interface{}{
		"s": []string{"a", "b", "c", "d"},
		"i": []int64{1, 2, 3, 4},
		"f": []float64{1.1, 2.2, 3.3, 4.4},
		"b": []bool{true, false, true, false},
		"t": []time.Time{t1, t2, t3, t1},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("NewDataFrameFromMap error: %v", err)
	}
	sliced := df.slice(1, 3, "Slice")
	if sliced.Error() != nil {
		t.Fatalf("slice error: %v", sliced.Error())
	}
	if sliced.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", sliced.Len())
	}
}

func TestDF_Slice_InvalidRanges(t *testing.T) {
	data := map[string]interface{}{"i": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)
	if df.slice(-1, 1, "Slice").Error() == nil {
		t.Error("expected error for negative start")
	}
	if df.slice(0, 10, "Slice").Error() == nil {
		t.Error("expected error for end > length")
	}
	if df.slice(2, 2, "Slice").Error() == nil {
		t.Error("expected error for start >= end")
	}
}

func TestDF_Slice_UnsupportedType(t *testing.T) {
	df := NewDataFrame()
	df.length = 2
	df.columns["x"] = &Series{Name: "x", Type: ColumnType(99), Data: []int64{1, 2}, Length: 2}
	df.order = append(df.order, "x")
	if df.slice(0, 1, "Slice").Error() == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestDF_Copy_ErrorBranch(t *testing.T) {
	df := NewDataFrame()
	df.err = errors.New("boom")
	copied := df.Copy()
	if copied.Error() == nil {
		t.Error("expected Copy to keep error")
	}
	if copied.Len() != 0 || copied.Width() != 0 {
		t.Error("expected error DataFrame to have zero shape")
	}
}

func TestDF_Copy_DeepCopy(t *testing.T) {
	data := map[string]interface{}{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
		"col3": []float64{1.1, 2.2, 3.3},
	}
	df, _ := NewDataFrameFromMap(data)
	copied := df.Copy()
	if copied.Width() != 3 || copied.Len() != 3 {
		t.Error("Copy should preserve dimensions")
	}
	df.Set(0, "col1", int64(99))
	val, _ := copied.Get(0, "col1")
	if val == int64(99) {
		t.Error("Copy should be independent")
	}
}

func TestDF_String_SmallAndLarge(t *testing.T) {
	df1, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []int64{1, 2}})
	if df1.String() == "" {
		t.Error("String() should return representation")
	}
	largeData := make([]int64, 20)
	for i := range largeData {
		largeData[i] = int64(i)
	}
	df2, _ := NewDataFrameFromMap(map[string]interface{}{"col1": largeData})
	if df2.String() == "" {
		t.Error("String() should return representation for large DataFrame")
	}
}

func TestDF_AddColumn_AllBranches(t *testing.T) {
	data := map[string]interface{}{"existing": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)

	s1, _ := NewSeries("new", []int64{4, 5, 6})
	if r := df.AddColumn(s1); r.Error() != nil {
		t.Errorf("AddColumn matching length should succeed: %v", r.Error())
	}

	s2, _ := NewSeries("existing", []int64{7, 8, 9})
	if r := df.AddColumn(s2); r.Error() == nil {
		t.Error("AddColumn with duplicate name should error")
	}

	s3, _ := NewSeries("mismatch", []int64{1, 2})
	if r := df.AddColumn(s3); r.Error() == nil {
		t.Error("AddColumn with mismatched length should error")
	}
}

func TestDF_NewDataFrameFromSeries_AllBranches(t *testing.T) {
	df1, err1 := NewDataFrameFromSeries()
	if err1 != nil || df1 == nil {
		t.Error("NewDataFrameFromSeries with no series should succeed")
	}

	s1, _ := NewSeries("col1", []int64{1, 2, 3})
	df2, err2 := NewDataFrameFromSeries(s1)
	if err2 != nil || df2.Width() != 1 {
		t.Error("NewDataFrameFromSeries with single series should succeed")
	}

	s2, _ := NewSeries("col2", []int64{4, 5, 6})
	df3, err3 := NewDataFrameFromSeries(s1, s2)
	if err3 != nil || df3.Width() != 2 {
		t.Error("NewDataFrameFromSeries with multiple series should succeed")
	}

	s3, _ := NewSeries("col3", []int64{1, 2})
	df4, err4 := NewDataFrameFromSeries(s1, s3)
	if err4 == nil || df4 != nil {
		t.Error("NewDataFrameFromSeries with mismatched lengths should error")
	}
}

func TestDF_NewDataFrameFromMap_AllBranches(t *testing.T) {
	df1, err1 := NewDataFrameFromMap(map[string]interface{}{})
	if err1 != nil || df1 == nil {
		t.Error("NewDataFrameFromMap with empty map should succeed")
	}

	df2, err2 := NewDataFrameFromMap(map[string]interface{}{"col1": []int64{1, 2, 3}})
	if err2 != nil || df2.Width() != 1 {
		t.Error("NewDataFrameFromMap with single column should succeed")
	}

	df3, err3 := NewDataFrameFromMap(map[string]interface{}{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	})
	if err3 != nil || df3.Width() != 2 {
		t.Error("NewDataFrameFromMap with multiple columns should succeed")
	}

	df4, err4 := NewDataFrameFromMap(map[string]interface{}{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b"},
	})
	if err4 == nil || df4 != nil {
		t.Error("NewDataFrameFromMap with mismatched lengths should error")
	}
}

func TestDF_HasColumn_GetColumnType_AllBranches(t *testing.T) {
	data := map[string]interface{}{
		"str":   []string{"a", "b"},
		"int":   []int64{1, 2},
		"float": []float64{1.1, 2.2},
		"bool":  []bool{true, false},
	}
	df, _ := NewDataFrameFromMap(data)

	if !df.HasColumn("str") || df.HasColumn("nonexistent") {
		t.Error("HasColumn failed")
	}

	strType, _ := df.GetColumnType("str")
	intType, _ := df.GetColumnType("int")
	floatType, _ := df.GetColumnType("float")
	boolType, _ := df.GetColumnType("bool")
	if strType != StringType || intType != Int64Type || floatType != Float64Type || boolType != BoolType {
		t.Error("GetColumnType returned wrong type")
	}

	if _, err := df.GetColumnType("nonexistent"); err == nil {
		t.Error("GetColumnType should error for nonexistent column")
	}
}

func TestDF_HasColumn_GetColumnType_ErrorBranch(t *testing.T) {
	df := NewDataFrame()
	df.err = errors.New("boom")
	if df.HasColumn("anything") {
		t.Error("HasColumn should be false when DataFrame has error")
	}
	if _, err := df.GetColumnType("anything"); err == nil {
		t.Error("GetColumnType should error when DataFrame has error")
	}
}

func TestDF_Tail_EdgeCases(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3, 4, 5}}
	df, _ := NewDataFrameFromMap(data)
	if df.Tail(0).Len() != 0 {
		t.Error("Tail(0) should return 0 rows")
	}
	if df.Tail(10).Len() != 5 {
		t.Error("Tail(10) should return all 5 rows")
	}
	if df.Tail(5).Len() != 5 {
		t.Error("Tail(5) should return all 5 rows")
	}
	if df.Tail(2).Len() != 2 {
		t.Error("Tail(2) should return 2 rows")
	}
}

func TestDF_Get_Set_EdgeCases(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)

	val, err := df.Get(0, "col1")
	if err != nil || val != int64(1) {
		t.Error("Get should succeed")
	}
	if _, err := df.Get(10, "col1"); err == nil {
		t.Error("Get should error on invalid row")
	}
	if _, err := df.Get(0, "nonexistent"); err == nil {
		t.Error("Get should error on invalid column")
	}
	if err := df.Set(0, "col1", int64(99)); err != nil {
		t.Error("Set should succeed")
	}
	if err := df.Set(10, "col1", int64(99)); err == nil {
		t.Error("Set should error on invalid row")
	}
	if err := df.Set(0, "nonexistent", int64(99)); err == nil {
		t.Error("Set should error on invalid column")
	}
}

func TestDF_GetSeries_EdgeCases(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)
	if s, err := df.GetSeries("col1"); err != nil || s == nil {
		t.Error("GetSeries should succeed")
	}
	if _, err := df.GetSeries("nonexistent"); err == nil {
		t.Error("GetSeries should error on nonexistent column")
	}
}

func TestDF_Shape_Columns_ErrorBranch(t *testing.T) {
	df := NewDataFrame()
	df.err = newOpError("test", "error")
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Error("Shape should return 0,0 on error")
	}
	if len(df.Columns()) != 0 {
		t.Error("Columns should return empty on error")
	}
}

func TestDF_Count_ResetIndex_ErrorBranch(t *testing.T) {
	df := NewDataFrame()
	df.err = errors.New("boom")
	if df.Count() != 0 {
		t.Error("Count should return 0 when DataFrame has error")
	}
	if df.ResetIndex().Error() == nil {
		t.Error("ResetIndex should preserve error")
	}
}

func TestDF_Count_ResetIndex_SuccessBranch(t *testing.T) {
	df, _ := NewDataFrameFromMap(map[string]interface{}{"col1": []int64{1, 2, 3, 4}})
	if df.Count() != 4 {
		t.Fatalf("expected count 4, got %d", df.Count())
	}
	res := df.ResetIndex()
	if res.Error() != nil || res.Len() != 4 {
		t.Fatalf("ResetIndex failed: %v", res.Error())
	}
}

func TestDF_ValidationFunctions(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3}, "col2": []string{"a", "b", "c"}}
	df, _ := NewDataFrameFromMap(data)

	if err := df.validateColumnExists("col1"); err != nil {
		t.Error("validateColumnExists should succeed for existing column")
	}
	if err := df.validateColumnExists("nonexistent"); err == nil {
		t.Error("validateColumnExists should error for nonexistent column")
	}
	if err := df.validateRowIndex(0); err != nil {
		t.Error("validateRowIndex should succeed for valid index")
	}
	if err := df.validateRowIndex(-1); err == nil {
		t.Error("validateRowIndex should error for negative index")
	}
	if err := df.validateRowIndex(100); err == nil {
		t.Error("validateRowIndex should error for out of bounds index")
	}
	if err := df.validateNotEmpty(); err != nil {
		t.Error("validateNotEmpty should succeed for non-empty DataFrame")
	}
	if err := NewDataFrame().validateNotEmpty(); err == nil {
		t.Error("validateNotEmpty should error for empty DataFrame")
	}
	if err := df.validateColumnsExist([]string{"col1", "col2"}); err != nil {
		t.Error("validateColumnsExist should succeed for existing columns")
	}
	if err := df.validateColumnsExist([]string{"col1", "nonexistent"}); err == nil {
		t.Error("validateColumnsExist should error for nonexistent column")
	}
}

func TestDF_SortFilterGroupBy_ErrorPropagation(t *testing.T) {
	df := NewDataFrame()
	df.err = newOpError("test", "error")

	if df.SortBy([]string{"col1"}, []bool{true}).Error() == nil {
		t.Error("SortBy should propagate error")
	}
	if df.Filter("col1", "==", 1).Error() == nil {
		t.Error("Filter should propagate error")
	}
	if df.GroupBy("col1").err == nil {
		t.Error("GroupBy should propagate error")
	}
}

func TestDF_AddColumnWithEmptyDF(t *testing.T) {
	df := NewDataFrame()
	s, _ := NewSeries("col1", []int64{1, 2, 3})
	err := df.AddColumn(s)
	if err == nil && df.Width() != 1 {
		t.Error("AddColumn should either error or add column")
	}
}

func TestDF_DropColumnLastColumn(t *testing.T) {
	data := map[string]interface{}{"col1": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)
	result := df.DropColumn("col1")
	if result.Error() != nil || result.Width() != 0 {
		t.Errorf("DropColumn last column failed: %v", result.Error())
	}
}

func TestDF_RenameColumnSuccess(t *testing.T) {
	data := map[string]interface{}{"old": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)
	result := df.RenameColumn("old", "new")
	if result.Error() != nil || !result.HasColumn("new") || result.HasColumn("old") {
		t.Errorf("RenameColumn failed: %v", result.Error())
	}
}
