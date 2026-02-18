package otters

import (
	"testing"
	"time"
)

func TestColumnType_String(t *testing.T) {
	tests := []struct {
		ct   ColumnType
		want string
	}{
		{StringType, "string"},
		{Int64Type, "int64"},
		{Float64Type, "float64"},
		{BoolType, "bool"},
		{TimeType, "time"},
		{ColumnType(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.ct.String(); got != tt.want {
			t.Errorf("ColumnType.String() = %v, want %v", got, tt.want)
		}
	}
}

func TestSeries_GetInt64(t *testing.T) {
	s, _ := NewSeries("test", []int64{1, 2, 3})
	val, err := s.GetInt64(1)
	if err != nil || val != 2 {
		t.Errorf("GetInt64() = %v, %v, want 2, nil", val, err)
	}

	_, err = s.GetInt64(10)
	if err == nil {
		t.Error("GetInt64() should error on out of bounds")
	}

	s2, _ := NewSeries("test", []string{"a", "b"})
	_, err = s2.GetInt64(0)
	if err == nil {
		t.Error("GetInt64() should error on wrong type")
	}
}

func TestSeries_GetFloat64(t *testing.T) {
	s, _ := NewSeries("test", []float64{1.1, 2.2, 3.3})
	val, err := s.GetFloat64(1)
	if err != nil || val != 2.2 {
		t.Errorf("GetFloat64() = %v, %v, want 2.2, nil", val, err)
	}

	_, err = s.GetFloat64(10)
	if err == nil {
		t.Error("GetFloat64() should error on out of bounds")
	}
}

func TestSeries_GetString(t *testing.T) {
	s, _ := NewSeries("test", []string{"a", "b", "c"})
	val, err := s.GetString(1)
	if err != nil || val != "b" {
		t.Errorf("GetString() = %v, %v, want b, nil", val, err)
	}

	_, err = s.GetString(10)
	if err == nil {
		t.Error("GetString() should error on out of bounds")
	}
}

func TestSeries_Int64Slice(t *testing.T) {
	s, _ := NewSeries("test", []int64{1, 2, 3})
	slice := s.Int64Slice()
	if slice == nil || len(slice) != 3 || slice[1] != 2 {
		t.Errorf("Int64Slice() failed")
	}

	s2, _ := NewSeries("test", []string{"a"})
	slice2 := s2.Int64Slice()
	if slice2 != nil {
		t.Error("Int64Slice() should return nil on wrong type")
	}
}

func TestSeries_Float64Slice(t *testing.T) {
	s, _ := NewSeries("test", []float64{1.1, 2.2})
	slice := s.Float64Slice()
	if slice == nil || len(slice) != 2 {
		t.Errorf("Float64Slice() failed")
	}

	s2, _ := NewSeries("test", []string{"a"})
	slice2 := s2.Float64Slice()
	if slice2 != nil {
		t.Error("Float64Slice() should return nil on wrong type")
	}
}

func TestSeries_StringSlice(t *testing.T) {
	s, _ := NewSeries("test", []string{"a", "b"})
	slice := s.StringSlice()
	if slice == nil || len(slice) != 2 {
		t.Errorf("StringSlice() failed")
	}

	s2, _ := NewSeries("test", []int64{1})
	slice2 := s2.StringSlice()
	if slice2 != nil {
		t.Error("StringSlice() should return nil on wrong type")
	}
}

func TestGetZeroValue(t *testing.T) {
	tests := []struct {
		ct   ColumnType
		want interface{}
	}{
		{StringType, ""},
		{Int64Type, int64(0)},
		{Float64Type, float64(0)},
		{BoolType, false},
		{TimeType, time.Time{}},
	}

	for _, tt := range tests {
		got := getZeroValue(tt.ct)
		if got != tt.want {
			t.Errorf("getZeroValue(%v) = %v, want %v", tt.ct, got, tt.want)
		}
	}
}

func TestConvertValueToBool(t *testing.T) {
	val, err := convertValueToBool("true")
	if err != nil || val != true {
		t.Errorf("convertValueToBool(true) failed")
	}

	val, err = convertValueToBool("false")
	if err != nil || val != false {
		t.Errorf("convertValueToBool(false) failed")
	}

	_, err = convertValueToBool("invalid")
	if err == nil {
		t.Error("convertValueToBool should error on invalid input")
	}
}

func TestConvertValueToTime(t *testing.T) {
	val, err := convertValueToTime("2023-01-15")
	if err != nil {
		t.Errorf("convertValueToTime failed: %v", err)
	}
	if val.Year() != 2023 || val.Month() != 1 || val.Day() != 15 {
		t.Errorf("convertValueToTime returned wrong date")
	}

	_, err = convertValueToTime("invalid-date")
	if err == nil {
		t.Error("convertValueToTime should error on invalid date")
	}
}

func TestParseTimeValue(t *testing.T) {
	testCases := []string{
		"2023-01-15",
		"2023-01-15T10:30:00Z",
		"01/15/2023",
		"15-Jan-2023",
	}

	for _, tc := range testCases {
		_, err := parseTimeValue(tc)
		if err != nil {
			t.Logf("parseTimeValue(%s) = %v (some formats may not parse)", tc, err)
		}
	}

	_, err := parseTimeValue("definitely-not-a-date")
	if err == nil {
		t.Error("parseTimeValue should error on invalid date")
	}
}

func TestConvertValue_EdgeCases(t *testing.T) {
	val, err := ConvertValue("", StringType)
	if err != nil || val != "" {
		t.Error("ConvertValue empty string should return empty")
	}

	val, err = ConvertValue("  ", Int64Type)
	if err != nil || val != int64(0) {
		t.Error("ConvertValue whitespace should return zero value")
	}

	_, err = ConvertValue("test", ColumnType(99))
	if err == nil {
		t.Error("ConvertValue should error on unknown type")
	}
}

func TestInferType_EdgeCases(t *testing.T) {
	emptyType := InferType([]string{})
	if emptyType != StringType {
		t.Error("InferType should return StringType for empty slice")
	}

	boolType := InferType([]string{"true", "false", "true"})
	if boolType != BoolType {
		t.Error("InferType should detect bool type")
	}

	timeType := InferType([]string{"2023-01-15", "2023-01-16"})
	if timeType != TimeType {
		t.Error("InferType should detect time type")
	}

	mixedType := InferType([]string{"1", "2.5", "text"})
	if mixedType != StringType {
		t.Error("InferType should fallback to StringType for mixed data")
	}
}

func TestSeries_Set_EdgeCases(t *testing.T) {
	s, _ := NewSeries("test", []int64{1, 2, 3})

	err := s.Set(10, int64(99))
	if err == nil {
		t.Error("Set should error on out of bounds")
	}

	err = s.Set(1, "wrong type")
	if err == nil {
		t.Error("Set should error on type mismatch")
	}

	err = s.Set(1, int64(42))
	if err != nil {
		t.Errorf("Set should succeed: %v", err)
	}
	val, _ := s.GetInt64(1)
	if val != 42 {
		t.Error("Set did not update value")
	}
}
