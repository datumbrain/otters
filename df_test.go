package otters

import (
	"strings"
	"testing"
)

func TestDataFrame_Len(t *testing.T) {
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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

func TestDataFrame_Reset(t *testing.T) {
	data := map[string]interface{}{
		"col1": []int64{1, 2, 3},
	}
	df, _ := NewDataFrameFromMap(data)

	df.reset()

	if df.Len() != 0 || df.Width() != 0 {
		t.Error("reset() should clear DataFrame")
	}
}

func TestDataFrame_GetEdgeCases(t *testing.T) {
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
	data := map[string]interface{}{
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
