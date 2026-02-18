package otters

import (
	"os"
	"testing"
	"time"
)

func TestCSV_ConvertStringSliceToType_Success_AllTypes(t *testing.T) {
	// int64
	intData := []string{"1", "2", "3"}
	result, err := convertStringSliceToType(intData, Int64Type)
	if err != nil {
		t.Errorf("convertStringSliceToType int64 error: %v", err)
	}
	intSlice, ok := result.([]int64)
	if !ok || len(intSlice) != 3 || intSlice[0] != 1 {
		t.Error("convertStringSliceToType should convert to []int64")
	}

	// float64
	floatData := []string{"1.1", "2.2", "3.3"}
	result2, err2 := convertStringSliceToType(floatData, Float64Type)
	if err2 != nil {
		t.Errorf("convertStringSliceToType float64 error: %v", err2)
	}
	floatSlice, ok2 := result2.([]float64)
	if !ok2 || len(floatSlice) != 3 {
		t.Error("convertStringSliceToType should convert to []float64")
	}

	// bool
	boolData := []string{"true", "false", "true"}
	result3, err3 := convertStringSliceToType(boolData, BoolType)
	if err3 != nil {
		t.Errorf("convertStringSliceToType bool error: %v", err3)
	}
	boolSlice, ok3 := result3.([]bool)
	if !ok3 || len(boolSlice) != 3 || !boolSlice[0] {
		t.Error("convertStringSliceToType should convert to []bool")
	}

	// time
	timeData := []string{"2023-01-01", "2023-01-02"}
	result4, err4 := convertStringSliceToType(timeData, TimeType)
	if err4 != nil {
		t.Errorf("convertStringSliceToType time error: %v", err4)
	}
	timeSlice, ok4 := result4.([]time.Time)
	if !ok4 || len(timeSlice) != 2 {
		t.Error("convertStringSliceToType should convert to []time.Time")
	}

	// string
	strData := []string{"a", "b", "c"}
	result5, err5 := convertStringSliceToType(strData, StringType)
	if err5 != nil {
		t.Errorf("convertStringSliceToType string error: %v", err5)
	}
	strSlice, ok5 := result5.([]string)
	if !ok5 || len(strSlice) != 3 {
		t.Error("convertStringSliceToType should keep []string")
	}
}

func TestCSV_ConvertStringSliceToType_Failure_InvalidData(t *testing.T) {
	invalidInt := []string{"not", "a", "number"}
	_, err := convertStringSliceToType(invalidInt, Int64Type)
	if err == nil {
		t.Error("convertStringSliceToType should error on invalid int64")
	}

	invalidFloat := []string{"not", "a", "float"}
	_, err2 := convertStringSliceToType(invalidFloat, Float64Type)
	if err2 == nil {
		t.Error("convertStringSliceToType should error on invalid float64")
	}

	invalidBool := []string{"not", "a", "bool"}
	_, err3 := convertStringSliceToType(invalidBool, BoolType)
	if err3 == nil {
		t.Error("convertStringSliceToType should error on invalid bool")
	}

	invalidTime := []string{"not", "a", "time"}
	_, err4 := convertStringSliceToType(invalidTime, TimeType)
	if err4 == nil {
		t.Error("convertStringSliceToType should error on invalid time")
	}
}

func TestOps_FilterIndicesTyped_AllTypeBranches(t *testing.T) {
	// int64
	s1, _ := NewSeries("test", []int64{1, 2, 3})
	indices1, _ := filterIndicesTyped(s1, ">", int64(1))
	if len(indices1) != 2 {
		t.Error("filterIndicesTyped int64 failed")
	}

	// float64
	s2, _ := NewSeries("test", []float64{1.1, 2.2, 3.3})
	indices2, _ := filterIndicesTyped(s2, ">", 2.0)
	if len(indices2) != 2 {
		t.Error("filterIndicesTyped float64 failed")
	}

	// string
	s3, _ := NewSeries("test", []string{"a", "b", "c"})
	indices3, _ := filterIndicesTyped(s3, "contains", "b")
	if len(indices3) != 1 {
		t.Error("filterIndicesTyped string failed")
	}

	// bool
	s4, _ := NewSeries("test", []bool{true, false, true})
	indices4, _ := filterIndicesTyped(s4, "==", true)
	if len(indices4) != 2 {
		t.Error("filterIndicesTyped bool failed")
	}

	// time
	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("test", []time.Time{tm, tm})
	indices5, _ := filterIndicesTyped(s5, "==", tm)
	if len(indices5) != 2 {
		t.Error("filterIndicesTyped time failed")
	}
}

func TestOps_SeriesValueToString_AllTypes(t *testing.T) {
	s1, _ := NewSeries("test", []string{"hello"})
	if seriesValueToString(s1, 0) != "hello" {
		t.Error("seriesValueToString string failed")
	}

	s2, _ := NewSeries("test", []int64{42})
	if seriesValueToString(s2, 0) != "42" {
		t.Error("seriesValueToString int64 failed")
	}

	s3, _ := NewSeries("test", []float64{3.14})
	result := seriesValueToString(s3, 0)
	if result == "" {
		t.Error("seriesValueToString float64 failed")
	}

	s4, _ := NewSeries("test", []bool{true})
	if seriesValueToString(s4, 0) != "true" {
		t.Error("seriesValueToString bool failed")
	}

	tm := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s5, _ := NewSeries("test", []time.Time{tm})
	result2 := seriesValueToString(s5, 0)
	if result2 == "" {
		t.Error("seriesValueToString time failed")
	}
}

func TestCSV_BuildDataFrameFromRows_EdgeCases(t *testing.T) {
	// Empty headers
	df, err := buildDataFrameFromRows([]string{}, [][]string{})
	if err != nil || df.Width() != 0 {
		t.Error("buildDataFrameFromRows empty should work")
	}

	// No rows
	df2, err2 := buildDataFrameFromRows([]string{"col1", "col2"}, [][]string{})
	if err2 != nil || df2.Width() != 2 {
		t.Error("buildDataFrameFromRows no rows should create empty DataFrame with columns")
	}
}

func TestCSV_ReadCSV_EmptyFile_ReturnsEmptyDataFrame(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	df, err := ReadCSV(tmpfile.Name())
	if err != nil {
		t.Errorf("ReadCSV empty file error: %v", err)
	}
	if df.Len() != 0 {
		t.Error("ReadCSV empty file should return empty DataFrame")
	}
}

func TestCSV_ReadCSV_RowLengthMismatch_Errors(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("a,b,c\n1,2,3\n4,5\n")
	tmpfile.Close()

	_, err := ReadCSV(tmpfile.Name())
	if err == nil {
		t.Error("ReadCSV should error on row length mismatch")
	}
}

func TestCSV_ReadCSVWithOptions_SkipRowsPastEOF_ReturnsEmpty(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("header\n")
	tmpfile.Close()

	df, err := ReadCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: true,
		Delimiter: ',',
		SkipRows:  10,
	})
	if err != nil {
		t.Errorf("ReadCSVWithOptions error: %v", err)
	}
	if df.Len() != 0 {
		t.Error("Should return empty DataFrame when skipping past EOF")
	}
}

func TestCSV_ReadCSVWithOptions_EOF_ReturnsEmpty(t *testing.T) {
	// Exercise the EOF path in the underlying read logic.
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("")
	tmpfile.Close()

	df, _ := ReadCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: true,
		Delimiter: ',',
	})
	if df.Len() != 0 {
		t.Error("ReadCSVWithOptions EOF should return empty DataFrame")
	}
}

func TestCSV_ReadCSVWithOptions_MaxRows_NoHeader_LimitsRows(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("1,2,3\n4,5,6\n7,8,9\n10,11,12")
	tmpfile.Close()

	df, err := ReadCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: false,
		Delimiter: ',',
		MaxRows:   2,
	})
	if err != nil {
		t.Errorf("ReadCSVWithOptions error: %v", err)
	}
	if df.Len() != 2 {
		t.Errorf("Expected 2 rows with MaxRows, got %d", df.Len())
	}
}

func TestCSV_ReadCSVFromStringWithOptions_NoHeader_GeneratesColumnNames(t *testing.T) {
	csvData := "1,2,3\n4,5,6"
	df, err := ReadCSVFromStringWithOptions(csvData, CSVOptions{
		HasHeader: false,
		Delimiter: ',',
	})
	if err != nil {
		t.Errorf("ReadCSVFromStringWithOptions error: %v", err)
	}
	if df.Width() != 3 {
		t.Error("Should generate column names")
	}
}

func TestCSV_ReadCSVFromStringWithOptions_RowMismatch_Errors(t *testing.T) {
	csvData := "a,b,c\n1,2,3\n4,5"
	_, err := ReadCSVFromStringWithOptions(csvData, CSVOptions{
		HasHeader: true,
		Delimiter: ',',
	})
	if err == nil {
		t.Error("Should error on row length mismatch")
	}
}

func TestCSV_ReadCSVFromStringWithOptions_MaxRows_LimitsRows(t *testing.T) {
	csvData := "a,b\n1,2\n3,4\n5,6\n7,8"
	df, err := ReadCSVFromStringWithOptions(csvData, CSVOptions{
		HasHeader: true,
		Delimiter: ',',
		MaxRows:   2,
	})
	if err != nil {
		t.Errorf("ReadCSVFromStringWithOptions error: %v", err)
	}
	if df.Len() != 2 {
		t.Errorf("Expected 2 rows with MaxRows, got %d", df.Len())
	}
}

func TestCSV_WriteCSV_PropagatesDataFrameError(t *testing.T) {
	df := NewDataFrame()
	df.err = newOpError("test", "error")

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	err := df.WriteCSV(tmpfile.Name())
	if err == nil {
		t.Error("WriteCSV should propagate error")
	}
}

func TestCSV_WriteCSVWithOptions_WritesFile(t *testing.T) {
	data := map[string]interface{}{
		"col1": []int64{1, 2, 3},
		"col2": []float64{1.1, 2.2, 3.3},
		"col3": []bool{true, false, true},
	}
	df, _ := NewDataFrameFromMap(data)

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	err := df.WriteCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: true,
		Delimiter: ',',
	})
	if err != nil {
		t.Errorf("WriteCSVWithOptions error: %v", err)
	}
}

func TestCSV_WriteCSV_TimeColumn_WritesFile(t *testing.T) {
	tm := time.Date(2023, 1, 1, 12, 30, 0, 0, time.UTC)
	data := map[string]interface{}{
		"col1": []time.Time{tm, tm},
	}
	df, _ := NewDataFrameFromMap(data)

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	err := df.WriteCSV(tmpfile.Name())
	if err != nil {
		t.Errorf("WriteCSV with time error: %v", err)
	}
}

func TestCSV_DetectDelimiter_Tab(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("a\tb\tc\n1\t2\t3")
	tmpfile.Close()

	delim, err := DetectDelimiter(tmpfile.Name())
	if err != nil || delim != '\t' {
		t.Errorf("DetectDelimiter = %c, %v, want tab", delim, err)
	}
}

func TestCSV_DetectDelimiter_Pipe(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("a|b|c\n1|2|3")
	tmpfile.Close()

	delim, err := DetectDelimiter(tmpfile.Name())
	if err != nil || delim != '|' {
		t.Errorf("DetectDelimiter = %c, %v, want |", delim, err)
	}
}

func TestCSV_DetectDelimiter_DefaultComma(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("abc")
	tmpfile.Close()

	delim, err := DetectDelimiter(tmpfile.Name())
	if err != nil || delim != ',' {
		t.Errorf("DetectDelimiter should default to comma, got %c", delim)
	}
}

func TestCSV_DetectDelimiter_ErrorOnMissingFile(t *testing.T) {
	_, err := DetectDelimiter("/nonexistent/file.csv")
	if err == nil {
		t.Error("DetectDelimiter should error on nonexistent file")
	}
}

func TestCSV_CleanHeader_TrimsSpaces(t *testing.T) {
	header := "   Name   "
	cleaned := cleanHeader(header)
	if cleaned != "Name" {
		t.Errorf("cleanHeader = %s, want Name", cleaned)
	}
}

func TestCSV_CleanHeader_StripsBOMAndSpaces(t *testing.T) {
	header := "\ufeff  Name  "
	cleaned := cleanHeader(header)
	if cleaned != "Name" {
		t.Errorf("cleanHeader = %s, want Name", cleaned)
	}
}

func TestCSV_ValidateCSV_ErrorOnMissingFile(t *testing.T) {
	_, err := ValidateCSV("/nonexistent/file.csv")
	if err == nil {
		t.Error("ValidateCSV should error on nonexistent file")
	}
}

func TestCSV_ValidateCSV_ReturnsInfo(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("a,b,c\n1,2,3\n4,5,6")
	tmpfile.Close()

	info, err := ValidateCSV(tmpfile.Name())
	if err != nil {
		t.Errorf("ValidateCSV error: %v", err)
	}
	if info.Columns != 3 {
		t.Errorf("CSVInfo.Columns = %d, want 3", info.Columns)
	}
	if info.Rows < 2 {
		t.Errorf("CSVInfo.Rows = %d, want at least 2", info.Rows)
	}
	if info.Delimiter != ',' {
		t.Errorf("CSVInfo.Delimiter = %c, want ,", info.Delimiter)
	}
}
