package otters

import (
	"strings"
	"testing"
	"time"
)

// --- Apply ---

func TestApplyDerivedColumn(t *testing.T) {
	df := dfFromSeries(t,
		"salary", []float64{1000, 2000},
		"bonus_rate", []float64{0.1, 0.2},
	)
	res := df.Apply("bonus", func(row map[string]any) any {
		return row["salary"].(float64) * row["bonus_rate"].(float64)
	})
	if res.Error() != nil {
		t.Fatalf("Apply error: %v", res.Error())
	}
	checkColumns(t, res, "salary", "bonus_rate", "bonus")
	checkColumn(t, res, "bonus", []float64{100, 400})
}

func TestApplyTypeInference(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 2, 3})
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		fn       func(map[string]any) any
		wantType ColumnType
		want     any
	}{
		{"all int64", func(r map[string]any) any { return r["n"].(int64) * 2 }, Int64Type, []int64{2, 4, 6}},
		{"plain int", func(r map[string]any) any { return int(r["n"].(int64)) }, Int64Type, []int64{1, 2, 3}},
		{"all float", func(r map[string]any) any { return float64(r["n"].(int64)) / 2 }, Float64Type, []float64{0.5, 1, 1.5}},
		{"int and float mix", func(r map[string]any) any {
			if r["n"].(int64) == 2 {
				return 2.5
			}
			return r["n"].(int64)
		}, Float64Type, []float64{1, 2.5, 3}},
		{"all string", func(r map[string]any) any { return "x" }, StringType, []string{"x", "x", "x"}},
		{"all bool", func(r map[string]any) any { return r["n"].(int64) > 1 }, BoolType, []bool{false, true, true}},
		{"all time", func(r map[string]any) any { return ts }, TimeType, []time.Time{ts, ts, ts}},
		{"mixed kinds fall back to string", func(r map[string]any) any {
			switch r["n"].(int64) {
			case 1:
				return int64(7)
			case 2:
				return "seven"
			default:
				return true
			}
		}, StringType, []string{"7", "seven", "true"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := df.Apply("out", tc.fn)
			if res.Error() != nil {
				t.Fatalf("Apply error: %v", res.Error())
			}
			ct, _ := res.GetColumnType("out")
			if ct != tc.wantType {
				t.Fatalf("inferred type = %v, want %v", ct, tc.wantType)
			}
			checkColumn(t, res, "out", tc.want)
		})
	}
}

func TestApplyNilValuesFillZero(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 2, 3})
	res := df.Apply("out", func(r map[string]any) any {
		if r["n"].(int64) == 2 {
			return nil
		}
		return r["n"].(int64) * 10
	})
	checkColumn(t, res, "out", []int64{10, 0, 30})
}

func TestApplyAllNilIsStringColumn(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 2})
	res := df.Apply("out", func(map[string]any) any { return nil })
	ct, _ := res.GetColumnType("out")
	if ct != StringType {
		t.Errorf("all-nil derived column type = %v, want StringType", ct)
	}
	checkColumn(t, res, "out", []string{"", ""})
}

func TestApplyReplacesExistingColumnInPlace(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1, 2}, "b", []int64{3, 4}, "c", []int64{5, 6})
	res := df.Apply("b", func(r map[string]any) any { return "replaced" })
	// Position preserved, type changed.
	checkColumns(t, res, "a", "b", "c")
	checkColumn(t, res, "b", []string{"replaced", "replaced"})
}

func TestApplyRowMapContainsAllColumns(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1}, "b", []string{"x"}, "c", []bool{true})
	res := df.Apply("out", func(r map[string]any) any {
		if len(r) != 3 {
			t.Errorf("row map has %d entries, want 3", len(r))
		}
		if r["a"] != int64(1) || r["b"] != "x" || r["c"] != true {
			t.Errorf("row map contents wrong: %#v", r)
		}
		return int64(0)
	})
	if res.Error() != nil {
		t.Fatal(res.Error())
	}
}

func TestApplyErrors(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1})

	if res := df.Apply("", func(map[string]any) any { return 1 }); res.Error() == nil {
		t.Error("empty column name should error")
	}
	if res := df.Apply("out", nil); res.Error() == nil {
		t.Error("nil fn should error")
	}
	// Unsupported return type.
	if res := df.Apply("out", func(map[string]any) any { return struct{}{} }); res.Error() == nil {
		t.Error("unsupported return type should error")
	}
	// Error frame propagates.
	bad := df.Filter("ghost", "==", 1)
	if res := bad.Apply("out", func(map[string]any) any { return 1 }); res.Error() == nil {
		t.Error("Apply on errored frame should carry the error")
	}
}

func TestApplyRecoversPanicInUserFn(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1, 0})
	res := df.Apply("inv", func(r map[string]any) any {
		return 10 / r["a"].(int64) // divide by zero on row 2
	})
	if res.Error() == nil {
		t.Fatal("panic in user fn must surface as an error, not crash")
	}
	if !strings.Contains(res.Error().Error(), "divide") {
		t.Errorf("error should mention the panic, got: %v", res.Error())
	}
}

func TestApplyDoesNotMutateReceiver(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1, 2})
	before := df.String()
	_ = df.Apply("b", func(map[string]any) any { return 1 })
	_ = df.Apply("a", func(map[string]any) any { return "overwrite" })
	if df.String() != before {
		t.Error("Apply mutated the receiver")
	}
	if df.Width() != 1 {
		t.Error("Apply added a column to the receiver")
	}
}

func TestApplyEmptyFrame(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{})
	res := df.Apply("out", func(map[string]any) any { return int64(1) })
	if res.Error() != nil {
		t.Fatalf("Apply on empty frame: %v", res.Error())
	}
	if res.Len() != 0 || !res.HasColumn("out") {
		t.Error("Apply on empty frame should add an empty column")
	}
}

// --- Map ---

func TestMapBasic(t *testing.T) {
	df := dfFromSeries(t, "name", []string{"alice", "bob"}, "age", []int64{30, 40})
	res := df.Map("name", "upper", func(v any) any {
		return strings.ToUpper(v.(string))
	})
	if res.Error() != nil {
		t.Fatalf("Map error: %v", res.Error())
	}
	checkColumns(t, res, "name", "age", "upper")
	checkColumn(t, res, "upper", []string{"ALICE", "BOB"})
}

func TestMapReplaceSourceColumn(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 2}, "s", []string{"a", "b"})
	res := df.Map("n", "n", func(v any) any { return v.(int64) * 100 })
	checkColumns(t, res, "n", "s")
	checkColumn(t, res, "n", []int64{100, 200})
}

func TestMapTypeChange(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 2})
	res := df.Map("n", "label", func(v any) any {
		return "n=" + string(rune('0'+v.(int64)))
	})
	ct, _ := res.GetColumnType("label")
	if ct != StringType {
		t.Errorf("mapped column type = %v, want StringType", ct)
	}
	checkColumn(t, res, "label", []string{"n=1", "n=2"})
}

func TestMapErrors(t *testing.T) {
	df := dfFromSeries(t, "a", []int64{1})
	if res := df.Map("ghost", "out", func(v any) any { return v }); res.Error() == nil {
		t.Error("missing source column should error")
	}
	if res := df.Map("a", "", func(v any) any { return v }); res.Error() == nil {
		t.Error("empty target column name should error")
	}
	if res := df.Map("a", "out", nil); res.Error() == nil {
		t.Error("nil fn should error")
	}
}

func TestMapRecoversPanic(t *testing.T) {
	df := dfFromSeries(t, "a", []string{"x"})
	res := df.Map("a", "out", func(v any) any {
		return v.(int64) // wrong type assertion panics
	})
	if res.Error() == nil {
		t.Fatal("panic in Map fn must surface as an error")
	}
}

// --- FillNA ---

func TestFillNAInt64(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 0, 3, 0})
	res := df.FillNA("n", int64(-1))
	checkColumn(t, res, "n", []int64{1, -1, 3, -1})

	// Plain int coerces.
	res2 := df.FillNA("n", 9)
	checkColumn(t, res2, "n", []int64{1, 9, 3, 9})
}

func TestFillNAFloat64(t *testing.T) {
	df := dfFromSeries(t, "f", []float64{1.5, 0, 2.5})
	res := df.FillNA("f", 99.9)
	checkColumn(t, res, "f", []float64{1.5, 99.9, 2.5})

	// Int value coerces to float column.
	res2 := df.FillNA("f", 7)
	checkColumn(t, res2, "f", []float64{1.5, 7, 2.5})
}

func TestFillNAString(t *testing.T) {
	df := dfFromSeries(t, "s", []string{"a", "", "c"})
	res := df.FillNA("s", "missing")
	checkColumn(t, res, "s", []string{"a", "missing", "c"})
}

func TestFillNABool(t *testing.T) {
	df := dfFromSeries(t, "b", []bool{true, false})
	res := df.FillNA("b", true)
	checkColumn(t, res, "b", []bool{true, true})
}

func TestFillNATime(t *testing.T) {
	ts := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	fill := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var zero time.Time
	df := dfFromSeries(t, "t", []time.Time{ts, zero})
	res := df.FillNA("t", fill)
	checkColumn(t, res, "t", []time.Time{ts, fill})
}

func TestFillNAErrors(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 0}, "s", []string{"a", ""})

	if res := df.FillNA("ghost", 1); res.Error() == nil {
		t.Error("missing column should error")
	}
	if res := df.FillNA("n", "not a number"); res.Error() == nil {
		t.Error("string fill on int column should error")
	}
	if res := df.FillNA("n", 2.5); res.Error() == nil {
		t.Error("fractional fill on int column should error")
	}
	if res := df.FillNA("s", 42); res.Error() == nil {
		t.Error("int fill on string column should error")
	}
	if res := df.FillNA("n", nil); res.Error() == nil {
		t.Error("nil fill value should error")
	}
}

func TestFillNADoesNotMutateReceiver(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 0})
	_ = df.FillNA("n", int64(5))
	checkColumn(t, df, "n", []int64{1, 0})
}

// --- DropNA ---

func TestDropNASingleColumn(t *testing.T) {
	df := dfFromSeries(t,
		"n", []int64{1, 0, 3},
		"s", []string{"a", "b", ""},
	)
	res := df.DropNA("n")
	checkColumn(t, res, "n", []int64{1, 3})
	checkColumn(t, res, "s", []string{"a", ""})
}

func TestDropNAMultipleColumns(t *testing.T) {
	df := dfFromSeries(t,
		"n", []int64{1, 0, 3, 4},
		"s", []string{"a", "b", "", "d"},
	)
	res := df.DropNA("n", "s")
	// Row 1 dropped (n=0), row 2 dropped (s="").
	checkColumn(t, res, "n", []int64{1, 4})
	checkColumn(t, res, "s", []string{"a", "d"})
}

func TestDropNAAllColumnsByDefault(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var zero time.Time
	df := dfFromSeries(t,
		"n", []int64{1, 0, 3, 4},
		"f", []float64{1, 2, 0, 4},
		"t", []time.Time{ts, ts, ts, zero},
	)
	res := df.DropNA()
	// Only row 0 has no zero values.
	checkColumn(t, res, "n", []int64{1})
	checkColumn(t, res, "f", []float64{1})
}

func TestDropNAPreservesOrder(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{5, 0, 3, 0, 1})
	res := df.DropNA("n")
	checkColumn(t, res, "n", []int64{5, 3, 1})
}

func TestDropNANoMatches(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{0, 0})
	res := df.DropNA("n")
	if res.Error() != nil {
		t.Fatalf("DropNA error: %v", res.Error())
	}
	if res.Len() != 0 {
		t.Errorf("expected 0 rows, got %d", res.Len())
	}
	if !res.HasColumn("n") {
		t.Error("columns should survive even when all rows drop")
	}
}

func TestDropNAErrors(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1})
	if res := df.DropNA("ghost"); res.Error() == nil {
		t.Error("missing column should error")
	}
	bad := df.Filter("ghost", "==", 1)
	if res := bad.DropNA(); res.Error() == nil {
		t.Error("DropNA on errored frame should carry the error")
	}
}

func TestDropNADoesNotMutateReceiver(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{1, 0})
	_ = df.DropNA("n")
	if df.Len() != 2 {
		t.Error("DropNA mutated the receiver")
	}
}

// --- chaining transforms ---

func TestTransformChain(t *testing.T) {
	df := dfFromSeries(t,
		"name", []string{"alice", "", "carol"},
		"salary", []float64{1000, 2000, 0},
	)
	res := df.
		FillNA("name", "unknown").
		DropNA("salary").
		Apply("bonus", func(r map[string]any) any {
			return r["salary"].(float64) * 0.1
		})
	if res.Error() != nil {
		t.Fatalf("chain error: %v", res.Error())
	}
	checkColumn(t, res, "name", []string{"alice", "unknown"})
	checkColumn(t, res, "bonus", []float64{100, 200})
}

func TestApplyAllNumericWidths(t *testing.T) {
	df := dfFromSeries(t, "n", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	res := df.Apply("out", func(r map[string]any) any {
		switch r["n"].(int64) {
		case 0:
			return int8(10)
		case 1:
			return int16(11)
		case 2:
			return int32(12)
		case 3:
			return uint(13)
		case 4:
			return uint8(14)
		case 5:
			return uint16(15)
		case 6:
			return uint32(16)
		case 7:
			return int(17)
		default:
			return int64(18)
		}
	})
	if res.Error() != nil {
		t.Fatalf("Apply error: %v", res.Error())
	}
	checkColumn(t, res, "out", []int64{10, 11, 12, 13, 14, 15, 16, 17, 18})

	// float32 promotes to Float64Type alongside ints.
	res2 := df.Apply("f", func(r map[string]any) any {
		if r["n"].(int64) == 0 {
			return float32(1.5)
		}
		return r["n"].(int64)
	})
	ct, _ := res2.GetColumnType("f")
	if ct != Float64Type {
		t.Errorf("float32+int mix type = %v, want Float64Type", ct)
	}

	// Mixed numeric + string exercises canonical float/int formatting.
	res3 := df.Head(3).Apply("m", func(r map[string]any) any {
		switch r["n"].(int64) {
		case 0:
			return 2.5
		case 1:
			return "txt"
		default:
			return uint16(9)
		}
	})
	checkColumn(t, res3, "m", []string{"2.5", "txt", "9"})
}
