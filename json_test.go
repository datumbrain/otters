package otters

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- reading basics ---

func TestReadJSONFromStringBasic(t *testing.T) {
	data := `[
		{"name": "Alice", "age": 30, "score": 95.5, "active": true},
		{"name": "Bob", "age": 25, "score": 87.2, "active": false}
	]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatalf("ReadJSONFromString: %v", err)
	}
	checkColumns(t, df, "name", "age", "score", "active")
	checkColumn(t, df, "name", []string{"Alice", "Bob"})
	checkColumn(t, df, "age", []int64{30, 25})
	checkColumn(t, df, "score", []float64{95.5, 87.2})
	checkColumn(t, df, "active", []bool{true, false})
}

func TestReadJSONRespectsNativeTypes(t *testing.T) {
	// JSON string "123" must stay a string; integer numbers stay Int64Type.
	data := `[{"n": 123, "s": "123"}]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatal(err)
	}
	if ct, _ := df.GetColumnType("n"); ct != Int64Type {
		t.Errorf("n type = %v, want Int64Type", ct)
	}
	if ct, _ := df.GetColumnType("s"); ct != StringType {
		t.Errorf("s type = %v, want StringType", ct)
	}
}

func TestReadJSONTimeColumn(t *testing.T) {
	data := `[{"ts": "2026-01-15T10:30:00Z"}, {"ts": "2026-02-20T08:00:00Z"}]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatal(err)
	}
	if ct, _ := df.GetColumnType("ts"); ct != TimeType {
		t.Errorf("ts type = %v, want TimeType", ct)
	}
	v, _ := df.Get(0, "ts")
	want := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	if !v.(time.Time).Equal(want) {
		t.Errorf("ts[0] = %v, want %v", v, want)
	}
}

func TestReadJSONUnionSchemaFirstSeenOrder(t *testing.T) {
	data := `[{"a": 1}, {"b": "x", "a": 2}, {"c": true}]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatal(err)
	}
	checkColumns(t, df, "a", "b", "c")
	// Missing keys fill with zero values.
	checkColumn(t, df, "a", []int64{1, 2, 0})
	checkColumn(t, df, "b", []string{"", "x", ""})
	checkColumn(t, df, "c", []bool{false, false, true})
}

func TestReadJSONNullsFillZero(t *testing.T) {
	data := `[{"n": 5, "s": "x"}, {"n": null, "s": null}]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatal(err)
	}
	checkColumn(t, df, "n", []int64{5, 0})
	checkColumn(t, df, "s", []string{"x", ""})
}

func TestReadJSONTypeConflictPromotesToString(t *testing.T) {
	data := `[{"v": 1}, {"v": "two"}, {"v": true}]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatal(err)
	}
	if ct, _ := df.GetColumnType("v"); ct != StringType {
		t.Errorf("v type = %v, want StringType", ct)
	}
	checkColumn(t, df, "v", []string{"1", "two", "true"})
}

func TestReadJSONNestedValuesStringify(t *testing.T) {
	data := `[{"obj": {"x": 1}, "arr": [1, 2]}]`
	df, err := ReadJSONFromString(data)
	if err != nil {
		t.Fatal(err)
	}
	checkColumn(t, df, "obj", []string{`{"x":1}`})
	checkColumn(t, df, "arr", []string{"[1,2]"})
}

func TestReadJSONEmptyArray(t *testing.T) {
	df, err := ReadJSONFromString(`[]`)
	if err != nil {
		t.Fatalf("empty array should not error: %v", err)
	}
	if !df.IsEmpty() {
		t.Error("empty array should produce an empty DataFrame")
	}
}

func TestReadJSONWhitespaceTolerance(t *testing.T) {
	df, err := ReadJSONFromString("\n\t  [ {\"a\": 1} ,\n {\"a\": 2} ]  \n\t")
	if err != nil {
		t.Fatalf("whitespace around/inside the array should be fine: %v", err)
	}
	checkColumn(t, df, "a", []int64{1, 2})
}

// --- reading errors ---

func TestReadJSONErrors(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"top-level object", `{"a": 1}`},
		{"top-level scalar", `42`},
		{"top-level string", `"hello"`},
		{"empty input", ``},
		{"whitespace only", "  \n\t "},
		{"non-object element", `[1, 2]`},
		{"string element", `["a"]`},
		{"malformed", `[{"a": }]`},
		{"unterminated array", `[{"a": 1}`},
		{"trailing garbage", `[{"a": 1}] extra`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ReadJSONFromString(tc.data); err == nil {
				t.Errorf("expected error for %s, got none", tc.name)
			}
		})
	}
}

func TestReadJSONErrorReportsElementIndex(t *testing.T) {
	_, err := ReadJSONFromString(`[{"a": 1}, {"a": 2}, [1]]`)
	if err == nil {
		t.Fatal("expected error")
	}
	oe, ok := err.(*OtterError)
	if !ok {
		t.Fatalf("expected *OtterError, got %T", err)
	}
	if oe.Row != 3 {
		t.Errorf("error row = %d, want 3 (1-based element index)", oe.Row)
	}
}

func TestReadJSONMissingFile(t *testing.T) {
	if _, err := ReadJSON(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Error("missing file should error")
	}
}

// --- options ---

func TestReadJSONOptions(t *testing.T) {
	data := `[{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]`

	skip, err := ReadJSONFromStringWithOptions(data, JSONOptions{SkipRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	checkColumn(t, skip, "n", []int64{3, 4})

	max, err := ReadJSONFromStringWithOptions(data, JSONOptions{MaxRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	checkColumn(t, max, "n", []int64{1, 2})

	both, err := ReadJSONFromStringWithOptions(data, JSONOptions{SkipRows: 1, MaxRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	checkColumn(t, both, "n", []int64{2, 3})

	past, err := ReadJSONFromStringWithOptions(data, JSONOptions{SkipRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if past.Len() != 0 {
		t.Errorf("skipping past the end should give 0 rows, got %d", past.Len())
	}
}

// --- file round trips ---

func TestJSONFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")

	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	df := dfFromSeries(t,
		"id", []int64{1, 2},
		"name", []string{"a", "b"},
		"score", []float64{1.5, 2.5},
		"ok", []bool{true, false},
		"ts", []time.Time{ts, ts},
	)
	if err := df.WriteJSON(path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	back, err := ReadJSON(path)
	if err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	checkColumns(t, back, "id", "name", "score", "ok", "ts")
	checkColumn(t, back, "id", []int64{1, 2})
	checkColumn(t, back, "name", []string{"a", "b"})
	checkColumn(t, back, "score", []float64{1.5, 2.5})
	checkColumn(t, back, "ok", []bool{true, false})
	if ct, _ := back.GetColumnType("ts"); ct != TimeType {
		t.Errorf("round-tripped ts type = %v, want TimeType", ct)
	}
}

func TestWriteJSONSpecialValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "special.json")

	var zeroTime time.Time
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	df := dfFromSeries(t,
		"f", []float64{math.NaN(), math.Inf(1), 1.5},
		"t", []time.Time{zeroTime, ts, ts},
	)
	if err := df.WriteJSON(path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(raw)
	// NaN/Inf and zero time serialize as null.
	if !strings.Contains(content, `"f":null`) {
		t.Errorf("NaN should serialize as null, got: %s", content)
	}
	if !strings.Contains(content, `"t":null`) {
		t.Errorf("zero time should serialize as null, got: %s", content)
	}
	// Output must be valid JSON that reads back.
	if _, err := ReadJSON(path); err != nil {
		t.Errorf("written JSON should read back cleanly: %v", err)
	}
}

func TestWriteJSONEmptyFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	df := dfFromSeries(t, "a", []int64{})
	if err := df.WriteJSON(path); err != nil {
		t.Fatalf("WriteJSON empty: %v", err)
	}
	back, err := ReadJSON(path)
	if err != nil {
		t.Fatalf("ReadJSON of empty output: %v", err)
	}
	if back.Len() != 0 {
		t.Errorf("expected 0 rows, got %d", back.Len())
	}
}

func TestWriteJSONErroredFrame(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1}).Filter("ghost", "==", 1)
	if err := df.WriteJSON(filepath.Join(t.TempDir(), "x.json")); err == nil {
		t.Error("WriteJSON on errored frame should return the error")
	}
}

// --- JSONL/JSON shared behavior parity ---

func TestJSONAndJSONLProduceIdenticalFrames(t *testing.T) {
	jsonData := `[{"a": 1, "b": "x"}, {"a": 2, "c": 3.5}]`
	jsonlData := "{\"a\": 1, \"b\": \"x\"}\n{\"a\": 2, \"c\": 3.5}\n"

	fromJSON, err := ReadJSONFromString(jsonData)
	if err != nil {
		t.Fatal(err)
	}
	fromJSONL, err := ReadJSONLFromString(jsonlData)
	if err != nil {
		t.Fatal(err)
	}
	if fromJSON.String() != fromJSONL.String() {
		t.Errorf("JSON and JSONL should produce identical frames:\n%s\nvs\n%s",
			fromJSON.String(), fromJSONL.String())
	}
}
