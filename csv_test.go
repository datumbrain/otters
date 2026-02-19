package otters

import (
	"os"
	"testing"
)

func TestReadCSVEdgeCases(t *testing.T) {
	// Test with skip rows
	csvData := `header1,header2
skip1,skip2
data1,data2
data3,data4`

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString(csvData)
	tmpfile.Close()

	df, err := ReadCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: true,
		Delimiter: ',',
		SkipRows:  1,
	})
	if err != nil {
		t.Errorf("ReadCSVWithOptions error: %v", err)
	}
	if df.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.Len())
	}
}

func TestReadCSVWithoutHeaders(t *testing.T) {
	csvData := `1,2,3
4,5,6
7,8,9`

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString(csvData)
	tmpfile.Close()

	df, err := ReadCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: false,
		Delimiter: ',',
	})
	if err != nil {
		t.Errorf("ReadCSVWithOptions error: %v", err)
	}
	if df.Width() != 3 {
		t.Errorf("Expected 3 columns, got %d", df.Width())
	}
	if !df.HasColumn("Column_0") {
		t.Error("Should have generated column names")
	}
}

func TestReadCSVMaxRows(t *testing.T) {
	csvData := `a,b
1,2
3,4
5,6
7,8`

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString(csvData)
	tmpfile.Close()

	df, err := ReadCSVWithOptions(tmpfile.Name(), CSVOptions{
		HasHeader: true,
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

func TestReadCSVFromStringEdgeCases(t *testing.T) {
	csvData := `name,age
Alice,25
Bob,30`

	df, err := ReadCSVFromStringWithOptions(csvData, CSVOptions{
		HasHeader: true,
		Delimiter: ',',
	})
	if err != nil {
		t.Errorf("ReadCSVFromStringWithOptions error: %v", err)
	}
	if df.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.Len())
	}
}

func TestWriteCSVEdgeCases(t *testing.T) {
	data := map[string]interface{}{
		"col1": []int64{1, 2, 3},
		"col2": []string{"a", "b", "c"},
	}
	df, _ := NewDataFrameFromMap(data)

	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	err := df.WriteCSV(tmpfile.Name())
	if err != nil {
		t.Errorf("WriteCSV error: %v", err)
	}

	// Read it back
	df2, err := ReadCSV(tmpfile.Name())
	if err != nil {
		t.Errorf("ReadCSV error: %v", err)
	}
	if df2.Len() != 3 {
		t.Error("Written CSV should be readable")
	}
}

func TestDetectDelimiter(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("a;b;c\n1;2;3")
	tmpfile.Close()

	delim, err := DetectDelimiter(tmpfile.Name())
	if err != nil || delim != ';' {
		t.Errorf("DetectDelimiter = %c, %v, want ;", delim, err)
	}
}

func TestValidateCSV(t *testing.T) {
	tmpfile, _ := os.CreateTemp("", "test*.csv")
	defer os.Remove(tmpfile.Name())
	tmpfile.WriteString("a,b,c\n1,2,3\n4,5,6")
	tmpfile.Close()

	info, err := ValidateCSV(tmpfile.Name())
	if err != nil {
		t.Errorf("ValidateCSV error: %v", err)
	}
	if info.Columns != 3 {
		t.Errorf("ValidateCSV columns = %d, want 3", info.Columns)
	}
}

func TestCleanHeader(t *testing.T) {
	// Test BOM removal
	header := "\ufeffName"
	cleaned := cleanHeader(header)
	if cleaned != "Name" {
		t.Errorf("cleanHeader should remove BOM, got %s", cleaned)
	}

	// Test whitespace trimming
	header2 := "  Name  "
	cleaned2 := cleanHeader(header2)
	if cleaned2 != "Name" {
		t.Errorf("cleanHeader should trim spaces, got %s", cleaned2)
	}
}
