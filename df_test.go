package otters

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDataFrame_Len(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	if df.Len() != 3 {
		t.Errorf("Len() = %v, want 3", df.Len())
	}

	emptyDf := NewDataFrame()
	if emptyDf.Len() != 0 {
		t.Error("Len() should return 0 for empty DataFrame")
	}
}

func TestDataFrame_Width(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)

	if df.Width() != 2 {
		t.Errorf("Width() = %v, want 2", df.Width())
	}

	emptyDf := NewDataFrame()
	if emptyDf.Width() != 0 {
		t.Error("Width() should return 0 for empty DataFrame")
	}
}

func TestDataFrame_String(t *testing.T) {
	data := map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{25, 30},
	}
	df, _ := NewDataFrameFromMap(data)

	str := df.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
	if !strings.Contains(str, "Alice") || !strings.Contains(str, "Bob") {
		t.Error("String() should contain data values")
	}
}

func TestDataFrame_Info(t *testing.T) {
	data := map[string]any{
		"name":   []string{"Alice", "Bob", "Carol"},
		"age":    []int64{25, 30, 35},
		"salary": []float64{50000, 60000, 70000},
	}
	df, _ := NewDataFrameFromMap(data)

	info := df.Info()
	if info == "" {
		t.Error("Info() should not return empty string")
	}
	if !strings.Contains(info, "3") {
		t.Error("Info() should contain row/column counts")
	}
}

func TestDataFrame_GetEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	_, err := df.Get(-1, "col1")
	if err == nil {
		t.Error("Get() should error on negative index")
	}

	_, err = df.Get(0, "nonexistent")
	if err == nil {
		t.Error("Get() should error on nonexistent column")
	}

	_, err = df.Get(100, "col1")
	if err == nil {
		t.Error("Get() should error on out of bounds index")
	}
}

func TestDataFrame_SetEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	err := df.Set(-1, "col1", int64(99))
	if err == nil {
		t.Error("Set() should error on negative index")
	}

	err = df.Set(0, "nonexistent", int64(99))
	if err == nil {
		t.Error("Set() should error on nonexistent column")
	}

	err = df.Set(100, "col1", int64(99))
	if err == nil {
		t.Error("Set() should error on out of bounds index")
	}

	err = df.Set(0, "col1", "wrong type")
	if err == nil {
		t.Error("Set() should error on type mismatch")
	}
}

func TestDataFrame_HeadEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	head := df.Head(-1)
	if head.Len() != 0 {
		t.Error("Head(-1) should return empty DataFrame")
	}

	head = df.Head(100)
	if head.Len() != 3 {
		t.Error("Head(100) should return all rows")
	}
}

func TestDataFrame_TailEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3, 4, 5},
	}
	df, _ := NewDataFrameFromMap(data)

	tail := df.Tail(-1)
	if tail.Len() != 0 {
		t.Error("Tail(-1) should return empty DataFrame")
	}

	tail = df.Tail(100)
	if tail.Len() != 5 {
		t.Error("Tail(100) should return all rows")
	}

	tail = df.Tail(2)
	if tail.Len() != 2 {
		t.Error("Tail(2) should return 2 rows")
	}
	val, _ := tail.Get(0, "col1")
	if val != int64(4) {
		t.Error("Tail should return last rows")
	}
}

func TestDataFrame_GetSeriesEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	_, err := df.GetSeries("nonexistent")
	if err == nil {
		t.Error("GetSeries() should error on nonexistent column")
	}

	series, err := df.GetSeries("col1")
	if err != nil || series == nil {
		t.Error("GetSeries() should succeed for existing column")
	}
}

func TestDataFrame_AddColumnEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)

	s1, _ := NewSeries("col1", []int64{4, 5, 6})
	err := df.AddColumn(s1)
	if err == nil {
		t.Error("AddColumn() should error on duplicate column")
	}

	s2, _ := NewSeries("col3", []int64{1, 2})
	err = df.AddColumn(s2)
	if err == nil {
		t.Error("AddColumn() should error on length mismatch")
	}
}

func TestDataFrame_DropColumnEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)

	result := df.DropColumn("nonexistent")
	if result.Error() == nil {
		t.Error("DropColumn() should error on nonexistent column")
	}

	result = df.DropColumn("col1")
	if result.Error() != nil {
		t.Error("DropColumn() should succeed")
	}
	if result.Width() != 1 {
		t.Error("DropColumn() should remove column")
	}
}

func TestDataFrame_RenameColumnEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)

	result := df.RenameColumn("nonexistent", "new")
	if result.Error() == nil {
		t.Error("RenameColumn() should error on nonexistent column")
	}

	result = df.RenameColumn("col1", "col2")
	if result.Error() == nil {
		t.Error("RenameColumn() should error on duplicate name")
	}

	result = df.RenameColumn("col1", "newcol")
	if result.Error() != nil {
		t.Error("RenameColumn() should succeed")
	}
	if !result.HasColumn("newcol") {
		t.Error("RenameColumn() should create new column name")
	}
}

func TestDataFrame_GetColumnTypeEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	_, err := df.GetColumnType("nonexistent")
	if err == nil {
		t.Error("GetColumnType() should error on nonexistent column")
	}

	ct, err := df.GetColumnType("col1")
	if err != nil || ct != Int64Type {
		t.Error("GetColumnType() should return correct type")
	}
}

func TestDataFrame_HasColumnEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	if !df.HasColumn("col1") {
		t.Error("HasColumn() should return true for existing column")
	}

	if df.HasColumn("nonexistent") {
		t.Error("HasColumn() should return false for nonexistent column")
	}
}

func TestDataFrame_CopyEdgeCases(t *testing.T) {
	data := map[string]any{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	copied := df.Copy()
	if copied.Len() != df.Len() || copied.Width() != df.Width() {
		t.Error("Copy() should create identical DataFrame")
	}

	copied.Set(0, "col1", int64(99))
	val, _ := df.Get(0, "col1")
	if val == int64(99) {
		t.Error("Copy() should create independent DataFrame")
	}
}

func TestDF_Slice_AllTypes(t *testing.T) {
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	data := map[string]any{
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
	data := map[string]any{"i": []int64{1, 2, 3}}
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
	data := map[string]any{
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
	df1, _ := NewDataFrameFromMap(map[string]any{"col1": []int64{1, 2}})
	if df1.String() == "" {
		t.Error("String() should return representation")
	}
	largeData := make([]int64, 20)
	for i := range largeData {
		largeData[i] = int64(i)
	}
	df2, _ := NewDataFrameFromMap(map[string]any{"col1": largeData})
	if df2.String() == "" {
		t.Error("String() should return representation for large DataFrame")
	}
}

func TestDF_AddColumn_AllBranches(t *testing.T) {
	data := map[string]any{"existing": []int64{1, 2, 3}}
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
	df1, err1 := NewDataFrameFromMap(map[string]any{})
	if err1 != nil || df1 == nil {
		t.Error("NewDataFrameFromMap with empty map should succeed")
	}

	df2, err2 := NewDataFrameFromMap(map[string]any{"col1": []int64{1, 2, 3}})
	if err2 != nil || df2.Width() != 1 {
		t.Error("NewDataFrameFromMap with single column should succeed")
	}

	df3, err3 := NewDataFrameFromMap(map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	})
	if err3 != nil || df3.Width() != 2 {
		t.Error("NewDataFrameFromMap with multiple columns should succeed")
	}

	df4, err4 := NewDataFrameFromMap(map[string]any{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b"},
	})
	if err4 == nil || df4 != nil {
		t.Error("NewDataFrameFromMap with mismatched lengths should error")
	}
}

func TestDF_HasColumn_GetColumnType_AllBranches(t *testing.T) {
	data := map[string]any{
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
	data := map[string]any{"col1": []int64{1, 2, 3, 4, 5}}
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
	data := map[string]any{"col1": []int64{1, 2, 3}}
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
	data := map[string]any{"col1": []int64{1, 2, 3}}
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
	df, _ := NewDataFrameFromMap(map[string]any{"col1": []int64{1, 2, 3, 4}})
	if df.Count() != 4 {
		t.Fatalf("expected count 4, got %d", df.Count())
	}
	res := df.ResetIndex()
	if res.Error() != nil || res.Len() != 4 {
		t.Fatalf("ResetIndex failed: %v", res.Error())
	}
}

func TestDF_ValidationFunctions(t *testing.T) {
	data := map[string]any{"col1": []int64{1, 2, 3}, "col2": []string{"a", "b", "c"}}
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
	data := map[string]any{"col1": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)
	result := df.DropColumn("col1")
	if result.Error() != nil || result.Width() != 0 {
		t.Errorf("DropColumn last column failed: %v", result.Error())
	}
}

func TestDF_RenameColumnSuccess(t *testing.T) {
	data := map[string]any{"old": []int64{1, 2, 3}}
	df, _ := NewDataFrameFromMap(data)
	result := df.RenameColumn("old", "new")
	if result.Error() != nil || !result.HasColumn("new") || result.HasColumn("old") {
		t.Errorf("RenameColumn failed: %v", result.Error())
	}
}

// Regression: NewSeries/NewDataFrameFromMap used to keep a reference to the
// caller's slice; mutating the source slice afterwards mutated the DataFrame.
func TestDataFrameDoesNotAliasCallerSlice(t *testing.T) {
	src := []int64{1, 2, 3}
	df, err := NewDataFrameFromMap(map[string]any{"n": src})
	if err != nil {
		t.Fatal(err)
	}

	src[0] = 999

	v, err := df.Get(0, "n")
	if err != nil {
		t.Fatal(err)
	}
	if v.(int64) != 1 {
		t.Errorf("mutating the source slice changed the DataFrame: got %v, want 1", v)
	}
}

// TestDataFrameBasics covers basic DataFrame creation and operations.
func TestDataFrameBasics(t *testing.T) {
	data := map[string]any{
		"numbers": []int64{1, 2, 3, 4, 5},
		"names":   []string{"a", "b", "c", "d", "e"},
	}

	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Test shape
	rows, cols := df.Shape()
	if rows != 5 || cols != 2 {
		t.Errorf("Expected shape (5, 2), got (%d, %d)", rows, cols)
	}

	// Test columns
	columns := df.Columns()
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	// Test filtering
	filtered := df.Filter("numbers", ">", int64(3))
	if err := filtered.Error(); err != nil {
		t.Errorf("Filter error: %v", err)
	}

	filteredRows, _ := filtered.Shape()
	if filteredRows != 2 {
		t.Errorf("Expected 2 filtered rows, got %d", filteredRows)
	}
}

// TestTimeTypeHeadTail verifies that Head and Tail work on DataFrames with
// TimeType columns (regression for missing TimeType case in slice()).
func TestTimeTypeHeadTail(t *testing.T) {
	t1, _ := time.Parse("2006-01-02", "2024-01-01")
	t2, _ := time.Parse("2006-01-02", "2024-01-02")
	t3, _ := time.Parse("2006-01-02", "2024-01-03")

	s, err := NewSeries("date", []time.Time{t1, t2, t3})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	df, err := NewDataFrameFromSeries(s)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	head := df.Head(2)
	if head.Error() != nil {
		t.Fatalf("Head on TimeType column failed: %v", head.Error())
	}
	if rows, _ := head.Shape(); rows != 2 {
		t.Errorf("Head(2) returned %d rows, want 2", rows)
	}

	tail := df.Tail(1)
	if tail.Error() != nil {
		t.Fatalf("Tail on TimeType column failed: %v", tail.Error())
	}
	if rows, _ := tail.Shape(); rows != 1 {
		t.Errorf("Tail(1) returned %d rows, want 1", rows)
	}

	// Verify the value is correct
	val, err := tail.Get(0, "date")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !val.(time.Time).Equal(t3) {
		t.Errorf("Tail value = %v, want %v", val, t3)
	}
}

// TestSetErrorDoesNotMutateCaller verifies that a failed operation does not
// corrupt the original DataFrame (regression for the setError mutation bug).
func TestSetErrorDoesNotMutateCaller(t *testing.T) {
	data := map[string]any{
		"a": []int64{1, 2, 3},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_ = df.Filter("nonexistent", "==", int64(1))

	if df.Error() != nil {
		t.Errorf("Filter on nonexistent column mutated the original DataFrame: %v", df.Error())
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 1 {
		t.Errorf("original DataFrame shape changed after failed Filter: got (%d, %d), want (3, 1)", rows, cols)
	}
}

// TestDeterministicFromMap verifies that NewDataFrameFromMap always produces
// columns in alphabetical order, regardless of map iteration order.
func TestDeterministicFromMap(t *testing.T) {
	data := map[string]any{
		"zebra": []int64{1, 2, 3},
		"apple": []int64{4, 5, 6},
		"mango": []int64{7, 8, 9},
	}
	expected := []string{"apple", "mango", "zebra"}
	for i := 0; i < 20; i++ {
		df, err := NewDataFrameFromMap(data)
		if err != nil {
			t.Fatalf("NewDataFrameFromMap failed on iteration %d: %v", i, err)
		}
		cols := df.Columns()
		if len(cols) != len(expected) {
			t.Fatalf("iteration %d: expected %d columns, got %d", i, len(expected), len(cols))
		}
		for j, col := range cols {
			if col != expected[j] {
				t.Errorf("iteration %d: column[%d] = %q, want %q", i, j, col, expected[j])
			}
		}
	}
}

// TestDataFrameManipulation covers Tail, Set, GetSeries, AddColumn, DropColumn,
// RenameColumn, IsEmpty, and HasColumn.
func TestDataFrameManipulation(t *testing.T) {
	data := map[string]any{
		"id":   []int64{1, 2, 3, 4, 5},
		"name": []string{"a", "b", "c", "d", "e"},
	}
	df, err := NewDataFrameFromMap(data)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Tail
	tail := df.Tail(2)
	if err := tail.Error(); err != nil {
		t.Fatalf("Tail error: %v", err)
	}
	rows, _ := tail.Shape()
	if rows != 2 {
		t.Errorf("Tail(2) returned %d rows, want 2", rows)
	}

	// Set
	if err := df.Set(0, "id", int64(99)); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	val, err := df.Get(0, "id")
	if err != nil {
		t.Fatalf("Get after Set error: %v", err)
	}
	if val.(int64) != 99 {
		t.Errorf("Set: got %v, want 99", val)
	}

	// GetSeries
	s, err := df.GetSeries("id")
	if err != nil {
		t.Fatalf("GetSeries error: %v", err)
	}
	if s == nil || s.Name != "id" {
		t.Errorf("GetSeries returned unexpected series: %v", s)
	}

	// AddColumn (mutates df in place)
	scoreSeries, err := NewSeries("score", []float64{10.0, 20.0, 30.0, 40.0, 50.0})
	if err != nil {
		t.Fatalf("NewSeries error: %v", err)
	}
	df.AddColumn(scoreSeries)
	if !df.HasColumn("score") {
		t.Error("AddColumn: 'score' column not found")
	}

	// DropColumn (returns copy)
	dfDropped := df.DropColumn("score")
	if err := dfDropped.Error(); err != nil {
		t.Fatalf("DropColumn error: %v", err)
	}
	if dfDropped.HasColumn("score") {
		t.Error("DropColumn: 'score' still present in returned DataFrame")
	}
	if !df.HasColumn("score") {
		t.Error("DropColumn: 'score' removed from original DataFrame unexpectedly")
	}

	// RenameColumn (returns copy)
	dfRenamed := df.RenameColumn("id", "user_id")
	if err := dfRenamed.Error(); err != nil {
		t.Fatalf("RenameColumn error: %v", err)
	}
	if !dfRenamed.HasColumn("user_id") {
		t.Error("RenameColumn: 'user_id' not found in result")
	}
	if dfRenamed.HasColumn("id") {
		t.Error("RenameColumn: old 'id' still present in result")
	}

	// IsEmpty
	empty := NewDataFrame()
	if !empty.IsEmpty() {
		t.Error("IsEmpty: expected true for new empty DataFrame")
	}
	if df.IsEmpty() {
		t.Error("IsEmpty: expected false for non-empty DataFrame")
	}

	// HasColumn
	if !df.HasColumn("name") {
		t.Error("HasColumn: 'name' should exist")
	}
	if df.HasColumn("nonexistent") {
		t.Error("HasColumn: 'nonexistent' should not exist")
	}
}
