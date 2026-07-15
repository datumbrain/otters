package otters

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadJSONLFromStringBasicTypes(t *testing.T) {
	data := `{"name":"Alice","age":30,"score":91.5,"active":true}
{"name":"Bob","age":25,"score":85.0,"active":false}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 2 || cols != 4 {
		t.Fatalf("expected shape (2, 4), got (%d, %d)", rows, cols)
	}

	expectedTypes := map[string]ColumnType{
		"name":   StringType,
		"age":    Int64Type,
		"score":  Float64Type,
		"active": BoolType,
	}
	for col, want := range expectedTypes {
		got, err := df.GetColumnType(col)
		if err != nil {
			t.Fatalf("GetColumnType(%q) failed: %v", col, err)
		}
		if got != want {
			t.Errorf("column %q: expected type %v, got %v", col, want, got)
		}
	}

	// Column order should be first-seen order, not alphabetical
	wantOrder := []string{"name", "age", "score", "active"}
	gotOrder := df.Columns()
	for i, col := range wantOrder {
		if gotOrder[i] != col {
			t.Errorf("column order[%d]: expected %q, got %q", i, col, gotOrder[i])
		}
	}

	age, err := df.Get(1, "age")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if age != int64(25) {
		t.Errorf("expected age 25, got %v", age)
	}
}

func TestReadJSONLKeyUnionAndNulls(t *testing.T) {
	data := `{"a":1,"b":"x"}
{"a":null,"c":2.5}
{"b":"y","d":true}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 4 {
		t.Fatalf("expected shape (3, 4), got (%d, %d)", rows, cols)
	}

	// null and missing values fill with the column type's zero value
	checks := []struct {
		row  int
		col  string
		want any
	}{
		{0, "a", int64(1)},
		{1, "a", int64(0)}, // explicit null
		{2, "a", int64(0)}, // missing key
		{1, "b", ""},       // missing key
		{0, "c", float64(0)},
		{1, "c", 2.5},
		{0, "d", false},
		{2, "d", true},
	}
	for _, c := range checks {
		got, err := df.Get(c.row, c.col)
		if err != nil {
			t.Fatalf("Get(%d, %q) failed: %v", c.row, c.col, err)
		}
		if got != c.want {
			t.Errorf("Get(%d, %q): expected %v, got %v", c.row, c.col, c.want, got)
		}
	}
}

func TestReadJSONLTypeConflictPromotesToString(t *testing.T) {
	data := `{"id":1}
{"id":"abc"}
{"id":true}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	colType, err := df.GetColumnType("id")
	if err != nil {
		t.Fatalf("GetColumnType failed: %v", err)
	}
	if colType != StringType {
		t.Fatalf("expected StringType for mixed column, got %v", colType)
	}

	want := []string{"1", "abc", "true"}
	for i, w := range want {
		got, err := df.Get(i, "id")
		if err != nil {
			t.Fatalf("Get(%d) failed: %v", i, err)
		}
		if got != w {
			t.Errorf("row %d: expected %q, got %v", i, w, got)
		}
	}
}

func TestReadJSONLStringsKeepJSONTypes(t *testing.T) {
	// A JSON string "123" must stay a string, unlike CSV inference
	data := `{"code":"123"}
{"code":"456"}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	colType, _ := df.GetColumnType("code")
	if colType != StringType {
		t.Errorf("expected StringType for JSON string column, got %v", colType)
	}
}

func TestReadJSONLIntFloatPromotion(t *testing.T) {
	data := `{"n":1}
{"n":2.5}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	colType, _ := df.GetColumnType("n")
	if colType != Float64Type {
		t.Fatalf("expected Float64Type, got %v", colType)
	}

	got, _ := df.Get(0, "n")
	if got != float64(1) {
		t.Errorf("expected 1.0, got %v", got)
	}
}

func TestReadJSONLTimeInference(t *testing.T) {
	data := `{"ts":"2026-07-16T10:30:00Z","msg":"started"}
{"ts":"2026-07-16T10:31:05Z","msg":"done"}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	colType, _ := df.GetColumnType("ts")
	if colType != TimeType {
		t.Fatalf("expected TimeType, got %v", colType)
	}

	got, err := df.Get(0, "ts")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	want := time.Date(2026, 7, 16, 10, 30, 0, 0, time.UTC)
	if !got.(time.Time).Equal(want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestReadJSONLNestedValuesStringified(t *testing.T) {
	data := `{"user":{"name":"Alice"},"tags":["a","b"],"n":1}
{"user":{"name":"Bob"},"tags":[],"n":2}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	for _, col := range []string{"user", "tags"} {
		colType, _ := df.GetColumnType(col)
		if colType != StringType {
			t.Errorf("column %q: expected StringType, got %v", col, colType)
		}
	}

	user, _ := df.Get(0, "user")
	if user != `{"name":"Alice"}` {
		t.Errorf("expected compact JSON string, got %v", user)
	}
	tags, _ := df.Get(1, "tags")
	if tags != `[]` {
		t.Errorf("expected [], got %v", tags)
	}

	nType, _ := df.GetColumnType("n")
	if nType != Int64Type {
		t.Errorf("flat column next to nested ones should stay Int64Type, got %v", nType)
	}
}

func TestReadJSONLSkipRowsAndMaxRows(t *testing.T) {
	data := `{"n":1}
{"n":2}
{"n":3}
{"n":4}`

	df, err := ReadJSONLFromStringWithOptions(data, JSONLOptions{SkipRows: 1, MaxRows: 2})
	if err != nil {
		t.Fatalf("ReadJSONLFromStringWithOptions failed: %v", err)
	}

	rows, _ := df.Shape()
	if rows != 2 {
		t.Fatalf("expected 2 rows, got %d", rows)
	}

	first, _ := df.Get(0, "n")
	last, _ := df.Get(1, "n")
	if first != int64(2) || last != int64(3) {
		t.Errorf("expected rows [2, 3], got [%v, %v]", first, last)
	}
}

func TestReadJSONLBlankLinesSkipped(t *testing.T) {
	data := "{\"n\":1}\n\n   \n{\"n\":2}\n"

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	rows, _ := df.Shape()
	if rows != 2 {
		t.Errorf("expected 2 rows, got %d", rows)
	}
}

func TestReadJSONLMalformedLine(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"invalid JSON", "{\"n\":1}\n{broken\n"},
		{"non-object array", "{\"n\":1}\n[1,2,3]\n"},
		{"non-object scalar", "{\"n\":1}\n42\n"},
		{"trailing garbage", "{\"n\":1}\n{\"n\":2} extra\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadJSONLFromString(tc.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var otterErr *OtterError
			if !errors.As(err, &otterErr) {
				t.Fatalf("expected *OtterError, got %T", err)
			}
			if otterErr.Row != 2 {
				t.Errorf("expected error on line 2, got line %d", otterErr.Row)
			}
		})
	}
}

func TestReadJSONLEmptyInput(t *testing.T) {
	df, err := ReadJSONLFromString("")
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}
	if !df.IsEmpty() {
		t.Error("expected empty DataFrame")
	}
}

func TestReadJSONLFileNotFound(t *testing.T) {
	_, err := ReadJSONL("nonexistent_file.jsonl")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestJSONLRoundTrip(t *testing.T) {
	data := `{"name":"Alice","age":30,"score":91.5,"active":true,"joined":"2024-01-15T09:00:00Z"}
{"name":"Bob","age":25,"score":85.25,"active":false,"joined":"2025-06-01T14:30:00Z"}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONLFromString failed: %v", err)
	}

	path := filepath.Join(t.TempDir(), "roundtrip.jsonl")
	if err := df.WriteJSONL(path); err != nil {
		t.Fatalf("WriteJSONL failed: %v", err)
	}

	df2, err := ReadJSONL(path)
	if err != nil {
		t.Fatalf("ReadJSONL failed: %v", err)
	}

	rows, cols := df2.Shape()
	if rows != 2 || cols != 5 {
		t.Fatalf("expected shape (2, 5), got (%d, %d)", rows, cols)
	}

	// Column order and types must survive the round trip
	for i, col := range df.Columns() {
		if df2.Columns()[i] != col {
			t.Errorf("column order[%d]: expected %q, got %q", i, col, df2.Columns()[i])
		}
		t1, _ := df.GetColumnType(col)
		t2, _ := df2.GetColumnType(col)
		if t1 != t2 {
			t.Errorf("column %q: type changed %v -> %v", col, t1, t2)
		}
	}

	for i := range rows {
		for _, col := range df.Columns() {
			v1, _ := df.Get(i, col)
			v2, _ := df2.Get(i, col)
			if tv1, ok := v1.(time.Time); ok {
				if !tv1.Equal(v2.(time.Time)) {
					t.Errorf("(%d, %q): %v != %v", i, col, v1, v2)
				}
			} else if v1 != v2 {
				t.Errorf("(%d, %q): %v != %v", i, col, v1, v2)
			}
		}
	}
}

func TestWriteJSONLSpecialValues(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]any{
		"f": []float64{math.NaN(), math.Inf(1), 1.5},
		"t": []time.Time{{}, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), {}},
	})
	if err != nil {
		t.Fatalf("NewDataFrameFromMap failed: %v", err)
	}

	path := filepath.Join(t.TempDir(), "special.jsonl")
	if err := df.WriteJSONL(path); err != nil {
		t.Fatalf("WriteJSONL failed: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	if lines[0] != `{"f":null,"t":null}` {
		t.Errorf("line 1: NaN and zero time should be null, got %s", lines[0])
	}
	if lines[1] != `{"f":null,"t":"2026-01-02T00:00:00Z"}` {
		t.Errorf("line 2: +Inf should be null, got %s", lines[1])
	}
	if lines[2] != `{"f":1.5,"t":null}` {
		t.Errorf("line 3: got %s", lines[2])
	}

	// null reads back as the zero value
	df2, err := ReadJSONL(path)
	if err != nil {
		t.Fatalf("ReadJSONL failed: %v", err)
	}
	v, _ := df2.Get(0, "f")
	if v != float64(0) {
		t.Errorf("expected null to read back as 0, got %v", v)
	}
}

func TestWriteJSONLStringEscaping(t *testing.T) {
	df, err := NewDataFrameFromMap(map[string]any{
		"msg": []string{`he said "hi"` + "\n\ttab"},
	})
	if err != nil {
		t.Fatalf("NewDataFrameFromMap failed: %v", err)
	}

	path := filepath.Join(t.TempDir(), "escape.jsonl")
	if err := df.WriteJSONL(path); err != nil {
		t.Fatalf("WriteJSONL failed: %v", err)
	}

	df2, err := ReadJSONL(path)
	if err != nil {
		t.Fatalf("ReadJSONL failed: %v", err)
	}
	got, _ := df2.Get(0, "msg")
	if got != `he said "hi"`+"\n\ttab" {
		t.Errorf("string with quotes/newline/tab did not round-trip, got %q", got)
	}
}

func TestReadJSONLFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	content := `{"event":"login","user":"alice"}
{"event":"logout","user":"bob"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	df, err := ReadJSONL(path)
	if err != nil {
		t.Fatalf("ReadJSONL failed: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 2 || cols != 2 {
		t.Errorf("expected shape (2, 2), got (%d, %d)", rows, cols)
	}
}

func TestWriteJSONLPropagatesErrorState(t *testing.T) {
	df, _ := NewDataFrameFromMap(map[string]any{"a": []int64{1}})
	bad := df.Filter("nonexistent", "==", 1)

	err := bad.WriteJSONL(filepath.Join(t.TempDir(), "never.jsonl"))
	if err == nil {
		t.Fatal("expected error state to propagate, got nil")
	}
}

func ExampleReadJSONLFromString() {
	data := `{"product":"Laptop","price":1200.5,"units":3}
{"product":"Phone","price":799.0,"units":10}`

	df, err := ReadJSONLFromString(data)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	rows, cols := df.Shape()
	fmt.Printf("Shape: (%d, %d)\n", rows, cols)
	fmt.Println("Columns:", df.Columns())

	total, _ := df.Sum("units")
	fmt.Printf("Total units: %.0f\n", total)

	// Output:
	// Shape: (2, 3)
	// Columns: [product price units]
	// Total units: 13
}
